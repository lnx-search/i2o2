#![doc = include_str!("../README.md")]

use std::any::Any;
use std::io;

use liburing_rs::{io_uring_sqe, io_uring_sqe_set_data64};

use crate::opcode::sealed::RegisterOp;

mod builder;
mod flags;
mod handle;
pub mod opcode;
mod queue;
mod reply;
mod ring;
#[cfg(test)]
mod tests;
mod wake;

pub use self::builder::I2o2Builder;
pub use self::handle::{I2o2Handle, RegisterError, SchedulerClosed, SubmitResult};
pub use self::opcode::types;
pub use self::reply::{ReplyReceiver, TryGetResultError};

#[cfg(not(target_os = "linux"))]
compiler_error!(
    "I2o2 only supports linux based operating systems, and requires relatively new kernel versions"
);

/// A guard type that can be any object.
pub type DynamicGuard = Box<dyn Any + Send>;

pub(crate) const MAGIC_ERRNO_NO_CAPACITY: i32 = -999;
pub(crate) const MAGIC_ERRNO_NOT_SIZE128: i32 = -1000;

/// Create a new [I2o2Scheduler] and [I2o2Handle] pair backed by io_uring.
///
/// This will use the default settings for the scheduler, you can optionally
/// use the [builder] to customise the ring behaviour.
///
/// NOTE: The scheduler cannot be sent across threads.
///
/// ## Example
///
/// ```rust
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
///
/// let (scheduler, handle) = i2o2::create_for_current_thread::<()>()?;
///
/// // ... do work
///
/// # Ok(())
/// # }
/// ```
pub fn create_for_current_thread<G>() -> io::Result<(I2o2Scheduler<G>, I2o2Handle<G>)> {
    I2o2Builder::default().try_create()
}

/// Create a new [I2o2Scheduler] and [I2o2Handle] pair backed by io_uring and spawn the scheduler
/// in a background worker thread.
///
/// This will use the default settings for the scheduler, you can optionally
/// use the [builder] to customise the ring behaviour.
///
/// NOTE: The scheduler cannot be sent across threads.
///
/// ## Example
///
/// ```rust
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
///
/// let (scheduler_handle, handle) = i2o2::create_and_spawn::<()>()?;
///
/// // ... do work
///
/// # Ok(())
/// # }
/// ```
pub fn create_and_spawn<G>()
-> io::Result<(std::thread::JoinHandle<io::Result<()>>, I2o2Handle<G>)>
where
    G: Send + 'static,
{
    I2o2Builder::default().try_spawn()
}

/// Create a new [I2o2Scheduler] and [I2o2Handle] pair backed by io_uring
/// with a custom configuration.
///
/// ## Example
///
/// ```rust
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::time::Duration;
///
/// let (scheduler, handle) = i2o2::builder()
///     .with_io_polling(true)
///     .with_sq_polling(true)
///     .with_sq_polling_timeout(Duration::from_millis(100))
///     .try_create::<()>()?;
///
/// // ... do work
///
/// # Ok(())
/// # }
/// ```
pub const fn builder() -> I2o2Builder {
    I2o2Builder::const_default()
}

/// The [I2o2Scheduler] runs an io_uring ring in the current thread and submits
/// IO events from the handle into the ring.
///
/// Communication between the handles and the scheduler can be done both synchronously
/// and asynchronously.
pub struct I2o2Scheduler<G = DynamicGuard> {
    ring: ring::IoRing,
    ring_size128: bool,
    state: TrackedState<G>,
    /// A waker handle for triggering a completion event on `self`
    /// intern causing events to be processed.
    waker_controller: wake::RingWakerController,
    /// A stream of incoming IO events to process.
    incoming_ops: queue::SchedulerReceiver<Packaged<opcode::AnyOp, G>>,
    /// A stream of incoming resource events to process.
    incoming_resources: queue::SchedulerReceiver<ResourceMessage<G>>,
    /// A null pointer used to prevent people from sending the scheduler across threads.
    _anti_send_ptr: *mut u8,
}

impl<G> I2o2Scheduler<G> {
    /// Run the scheduler in the current thread until it is shut down.
    ///
    /// This will wait for all remaining tasks to complete.
    pub fn run(mut self) -> io::Result<()> {
        tracing::debug!("scheduler is running");

        #[cfg(test)]
        fail::fail_point!("scheduler_run_fail", |_| {
            Err(io::Error::other("test error triggered by failpoints"))
        });

        tracing::debug!("running scheduler event loop");

        self.run_event_loop()?;

        tracing::debug!("scheduler unregistering resources");
        if let Err(e) = self.unregister_resources() {
            tracing::warn!(error = ?e, "scheduler failed to gracefully unregister resources");
        }

        self.wait_for_remaining()?;
        tracing::debug!("scheduler shutting down");

        Ok(())
    }

    fn run_event_loop(&mut self) -> io::Result<()> {
        loop {
            if self.incoming_ops.is_disconnected() {
                break;
            }

            for _ in 0..50 {
                self.drain_incoming_io()?;
                self.drain_completions()?;
            }

            self.drain_incoming_resources()?;
            self.maybe_wait_for_events();
        }

        Ok(())
    }

    /// Attempts to drain all incoming IO ops and submit them to the ring.
    fn drain_incoming_io(&mut self) -> io::Result<()> {
        let pop_n = self.incoming_ops.len();

        #[cfg(feature = "trace-hotpath")]
        tracing::trace!(pop_n = pop_n, "attempting to draining incoming IO ops");

        let mut n_read = 0;
        for _ in 0..pop_n {
            let Some(sqe) = self.ring.get_available_sqe() else {
                break;
            };

            let msg = match self.incoming_ops.pop() {
                Some(msg) => msg,
                None => {
                    write_filler_op(sqe);
                    break;
                },
            };

            n_read += 1;

            if msg.entry.requires_size128() && !self.ring_size128 {
                #[cfg(feature = "trace-hotpath")]
                tracing::trace!(
                    "rejecting op because size128 is required but not active"
                );
                write_filler_op(sqe);
                let _ = msg.reply.set_result(MAGIC_ERRNO_NOT_SIZE128);
                continue;
            }

            self.state.register(sqe, msg);
        }

        let result = self.ring.submit();

        if self.incoming_ops.is_empty() {
            self.incoming_ops.wake_n(n_read);
        }

        result?;

        Ok(())
    }

    /// Attempts to drain all incoming resource ops and tie them to the ring.
    fn drain_incoming_resources(&mut self) -> io::Result<()> {
        #[cfg(feature = "trace-hotpath")]
        tracing::trace!(
            pop_n = self.incoming_resources.len(),
            "attempting to draining incoming resource ops"
        );

        let mut processed = 0;
        while let Some(msg) = self.incoming_resources.pop() {
            match msg {
                ResourceMessage::RegisterResource(op) => {
                    self.handle_resource_register_op(op);
                },
                ResourceMessage::UnregisterResource(op) => {
                    self.handle_resource_unregister_op(op);
                },
            }
            processed += 1;
        }

        if processed > 0 {
            self.incoming_resources.wake_all();
        }

        Ok(())
    }

    /// Processes all completion events from the ring.
    fn drain_completions(&mut self) -> io::Result<()> {
        #[cfg(feature = "trace-hotpath")]
        tracing::trace!("draining completion events");

        for cqe in self.ring.iter_completions() {
            let (flag, reply_idx, guard_idx) = flags::unpack(cqe.user_data);

            #[cfg(feature = "trace-hotpath")]
            tracing::trace!(flag = ?flag, task_id = reply_idx, result = cqe.result, "completion");

            match flag {
                flags::Flag::FillerOp | flags::Flag::EventFdWaker => {},
                flags::Flag::Guarded => {
                    self.state.acknowledge_reply(reply_idx, cqe.result);
                    self.state.drop_guard_if_exists(guard_idx);
                },
                flags::Flag::Unguarded => {
                    self.state.acknowledge_reply(reply_idx, cqe.result);
                },
                flags::Flag::GuardedResourceBuffer => {
                    self.state.drop_buffer_guard(guard_idx);
                },
                flags::Flag::GuardedResourceFile => {
                    self.state.drop_file_guard(guard_idx);
                },
            }
        }

        Ok(())
    }

    /// Wait for events if there is no outstanding work to be done.
    fn maybe_wait_for_events(&mut self) {
        if !self.has_outstanding_work() {
            self.waker_controller.ask_for_wake();

            // Check again because of atomics, yada, yada...
            if !self.has_outstanding_work() {
                return;
            }

            #[cfg(feature = "trace-hotpath")]
            tracing::trace!("scheduler waiting on eventfd events...");
            self.waker_controller.wait_for_events();
        }
    }

    /// Waits for remaining inflight operations to complete.
    fn wait_for_remaining(&mut self) -> io::Result<()> {
        #[cfg(feature = "trace-hotpath")]
        tracing::debug!("scheduler is draining remaining events");

        while !self.incoming_ops.is_empty() || self.state.remaining_tasks() > 0 {
            self.drain_incoming_io()?;
            self.drain_completions()?;
            self.maybe_wait_for_events();
        }

        #[cfg(feature = "trace-hotpath")]
        tracing::debug!("scheduler has drained all events");

        Ok(())
    }

    /// Unregisters any resources currently tied to the ring.
    fn unregister_resources(&mut self) -> io::Result<()> {
        if !self.state.resource_buffer_guards.is_empty() {
            self.ring.unregister_buffers()?;
        }

        if !self.state.resource_file_guards.is_empty() {
            self.ring.unregister_files()?;
        }

        Ok(())
    }

    /// Registers a new resource with the ring providing there is capacity.
    fn handle_resource_register_op(&mut self, op: Packaged<Resource, G>) {
        let Packaged {
            entry,
            reply,
            guard,
        } = op;

        let result = if entry.is_buffer() {
            self.state.register_buffer_guard(guard)
        } else {
            self.state.register_file_guard(guard)
        };

        let (tag, offset) = match result {
            None => {
                reply.set_result(MAGIC_ERRNO_NO_CAPACITY);
                return;
            },
            Some(offset) if entry.is_buffer() => {
                let packed = flags::pack(flags::Flag::GuardedResourceBuffer, 0, offset);
                (packed, offset)
            },
            Some(offset) => {
                let packed = flags::pack(flags::Flag::GuardedResourceFile, 0, offset);
                (packed, offset)
            },
        };

        let result = match entry {
            Resource::Buffer(iovec) => self.ring.register_buffer(offset, iovec, tag),
            Resource::File(fd) => self.ring.register_file(offset, fd, tag),
        };

        if result.is_ok() {
            // This can never wrap because `offset` is only ever 32 bits.
            reply.set_result(offset as i32);
            return;
        }

        // We have to ensure we don't leak mem if there is an error.
        if entry.is_buffer() {
            self.state.drop_buffer_guard(offset);
        } else {
            self.state.drop_file_guard(offset);
        };

        let err = result.unwrap_err();
        reply.set_result(err.raw_os_error().unwrap());
    }

    /// Unregisters a resource tied to the ring.
    ///
    /// Cleanup of guards, etc... Will be handled on the completion event triggered
    /// when the resource is no longer required by the ring.
    fn handle_resource_unregister_op(&mut self, op: Packaged<ResourceIndex, G>) {
        let Packaged { entry, reply, .. } = op;

        let result = match entry {
            ResourceIndex::File(id) => self.ring.unregister_file(id),
        };

        if let Err(err) = result {
            reply.set_result(err.raw_os_error().unwrap());
        } else {
            reply.set_result(0);
        }
    }

    fn has_outstanding_work(&self) -> bool {
        !self.incoming_ops.is_empty()
            || !self.incoming_resources.is_empty()
            // incoming_ops & incoming_resources lifetimes are tied together, we only need to check 1 atomic.
            || self.incoming_ops.is_disconnected()
    }
}

fn write_filler_op(sqe: &mut io_uring_sqe) {
    let user_data = flags::pack(flags::Flag::FillerOp, 0, 0);
    let op = opcode::Nop::new();
    op.register_with_sqe(sqe);
    unsafe { io_uring_sqe_set_data64(sqe, user_data) }
}

struct TrackedState<G> {
    free_registered_files: u32,
    free_registered_buffers: u32,
    /// Guards for allocated registered files.
    resource_file_guards: slab::Slab<Option<G>>,
    /// Guards for allocated registered buffers.
    resource_buffer_guards: slab::Slab<Option<G>>,
    /// A set of guards that should be kept alive as long as the ring requires.
    guards: slab::Slab<G>,
    /// A slab of reply handles for scheduled tasks.
    replies: slab::Slab<reply::ReplyNotify>,
}

impl<G> TrackedState<G> {
    fn new(free_registered_files: u32, free_registered_buffers: u32) -> Self {
        Self {
            free_registered_files,
            free_registered_buffers,
            resource_file_guards: slab::Slab::with_capacity(
                free_registered_files as usize,
            ),
            resource_buffer_guards: slab::Slab::with_capacity(
                free_registered_buffers as usize,
            ),
            guards: slab::Slab::default(),
            replies: slab::Slab::default(),
        }
    }

    fn remaining_tasks(&self) -> usize {
        self.replies.len()
    }

    fn register(&mut self, free_sqe: &mut io_uring_sqe, op: Packaged<opcode::AnyOp, G>) {
        let Packaged {
            entry,
            reply,
            guard,
        } = op;

        let reply_idx = self.replies.insert(reply);

        let flag = if guard.is_none() {
            flags::Flag::Unguarded
        } else {
            flags::Flag::Guarded
        };

        let guard_idx = guard.map(|g| self.register_guard(g)).unwrap_or(0);

        #[cfg(feature = "trace-hotpath")]
        tracing::trace!(task_id = reply_idx, flag = ?flag, "registered entry");

        let user_data = flags::pack(flag, reply_idx as u32, guard_idx);

        // Write the entry to the SQE in the ring.
        entry.register_with_sqe(free_sqe);
        unsafe { io_uring_sqe_set_data64(free_sqe, user_data) };
    }

    fn register_guard(&mut self, guard: G) -> u32 {
        self.guards.insert(guard) as u32
    }

    fn acknowledge_reply(&mut self, reply_idx: u32, result: i32) {
        let reply = self.replies.remove(reply_idx as usize);
        reply.set_result(result);
    }

    fn drop_guard_if_exists(&mut self, guard_idx: u32) {
        drop(self.guards.try_remove(guard_idx as usize));
    }

    fn register_buffer_guard(&mut self, guard: Option<G>) -> Option<u32> {
        if self.free_registered_buffers > 0 {
            self.free_registered_buffers -= 1;
            Some(self.resource_buffer_guards.insert(guard) as u32)
        } else {
            None
        }
    }

    fn drop_buffer_guard(&mut self, guard_idx: u32) {
        let value = self.resource_buffer_guards.try_remove(guard_idx as usize);
        if value.is_some() {
            self.free_registered_buffers += 1;
        }
    }

    fn register_file_guard(&mut self, guard: Option<G>) -> Option<u32> {
        if self.free_registered_files > 0 {
            self.free_registered_files -= 1;
            Some(self.resource_file_guards.insert(guard) as u32)
        } else {
            None
        }
    }

    fn drop_file_guard(&mut self, guard_idx: u32) {
        let value = self.resource_file_guards.try_remove(guard_idx as usize);
        if value.is_some() {
            self.free_registered_files += 1;
        }
    }
}

enum ResourceMessage<G> {
    /// Register a new resource to the ring.
    RegisterResource(Packaged<Resource, G>),
    /// Unregister an existing resource on the ring.
    UnregisterResource(Packaged<ResourceIndex, G>),
}

#[repr(align(64))]
struct Packaged<E, G> {
    entry: E,
    reply: reply::ReplyNotify,
    guard: Option<G>,
}

enum Resource {
    Buffer(liburing_rs::iovec),
    File(std::os::fd::RawFd),
}

impl Resource {
    fn is_buffer(&self) -> bool {
        matches!(self, Resource::Buffer { .. })
    }
}

// SAFETY: The handle ensures the buffers are safe to send across a thread boundary.
unsafe impl Send for Resource {}

enum ResourceIndex {
    File(u32),
}
