#![doc = include_str!("../README.md")]

use std::any::Any;
use std::ffi::c_void;
use std::io;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use liburing_rs::*;
use parking_lot::Mutex;
use crate::opcode::sealed::RegisterOp;

mod builder;
mod handle;
pub mod opcode;
mod queue;
mod reply;
mod ring;
#[cfg(test)]
mod tests;
mod wake;

pub use self::builder::{I2o2Builder, CpuSet};
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


struct SubmissionWorker<G> {
    ring: ring::IoRing,
    ring_size128: bool,
    /// A waker handle for triggering a completion event on `self`
    /// intern causing events to be processed.
    waker_controller: wake::RingWakerController,
    /// A stream of incoming IO events to process.
    incoming_ops: queue::SchedulerReceiver<Packaged<opcode::AnyOp, G>>,    
    /// A stream of incoming resource events to process.
    incoming_resources: queue::SchedulerReceiver<ResourceMessage<G>>,
    /// A set of operations enqueued with the kernel.
    enqueued_ops: EnqueuedOps<G>,
    /// The pre-defined number of declared buffers the ring can register.
    resource_declared_num_buffers: usize,
    /// The pre-defined number of declared files the ring can register.
    resource_declared_num_files: usize,
    /// The slab used for holding onto resource guards until the resource is no longer used.
    resource_guards: Arc<Mutex<slab::Slab<G>>>,
}

impl<G> SubmissionWorker<G> {
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
            }
            
            self.drain_incoming_resources()?;
            self.maybe_wait_for_events();
        }

        Ok(())
    }

    fn unregister_resources(&mut self) -> io::Result<()> {
        if self.resource_declared_num_files {
            self.ring.unregister_files()?;
        }
        
        if self.resource_declared_num_buffers {
            self.ring.unregister_buffers()?;
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
                    self.enqueued_ops.push_nop_enqueue_ring(sqe);
                    break;
                },
            };

            n_read += 1;

            if msg.entry.requires_size128() && !self.ring_size128 {
                #[cfg(feature = "trace-hotpath")]
                tracing::trace!(
                    "rejecting op because size128 is required but not active"
                );
                self.enqueued_ops.push_nop_enqueue_ring(sqe);
                let _ = msg.reply.set_result(MAGIC_ERRNO_NOT_SIZE128);
                continue;
            }
            
            self.enqueued_ops.push_io_enqueue_ring(sqe, msg);
        }

        let result = self.ring.submit();
        self.incoming_ops.wake_n(n_read);

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

    fn has_outstanding_work(&self) -> bool {
        !self.incoming_ops.is_empty()
            // incoming_ops & incoming_resources lifetimes are tied together, we only need to check 1 atomic.
            || self.incoming_ops.is_disconnected()
    }
    
}


struct EnqueuedOps<G> {
    size: usize,
    /// A buffer of ops that have been submitted to the kernel
    /// and have yet to be completed. 
    ///
    /// This is used as a mini-allocator to hold the inflight entries
    /// which can then be processed by the consumer worker.
    enqueued_ops: Box<[MaybeUninit<EnqueuedEntry<G>>]>,
    /// A queue of free slots available to overwrite entries in the `enqueued_ops`.
    free_slots: crossbeam_queue::ArrayQueue<usize>,
}

impl<G> EnqueuedOps<G> {
    /// Creates a new [EnqueuedOps] buffer with a given capacity.
    /// 
    /// This may grow to larger than this value depending on pressure.
    fn new(size: usize) -> Self {
        Self {
            size,
            enqueued_ops: Box::new_uninit_slice(size),
            free_slots: crossbeam_queue::ArrayQueue::new(size),
        }
    }

    
    /// Pushes the entry into the enqueued ops buffer.
    pub(self) fn push(&mut self, entry: EnqueuedEntry<G>) -> *mut EnqueuedEntry<G> {
        if let Some(slot) = self.free_slots.pop() {
            self.enqueued_ops[slot].write(entry);
            (&raw mut self.enqueued_ops[slot]) as *mut EnqueuedEntry<G>    
        } else {
            tracing::error!("system ran out of space in the enqueued entries ring");
            std::process::abort();
        }           
    }

    fn push_io_enqueue_ring(&mut self, free_sqe: &mut io_uring_sqe, op: Packaged<opcode::AnyOp, G>) {
        let Packaged {
            entry,
            reply,
            guard,
        } = op;

        let enqueued = EnqueuedEntry::IoOp(IoOpEntry {
            reply,
            guard,
        });

        let ptr = self.push(enqueued);

        #[cfg(feature = "trace-hotpath")]
        tracing::trace!(task_id = ptr.addr(), "registered entry");

        // Write the entry to the SQE in the ring.
        entry.register_with_sqe(free_sqe);
        unsafe { io_uring_sqe_set_data(free_sqe, ptr as *mut c_void) };
    }

    fn push_nop_enqueue_ring(&mut self, free_sqe: &mut io_uring_sqe) {
        let ptr = self.push(EnqueuedEntry::Nop);

        #[cfg(feature = "trace-hotpath")]
        tracing::trace!(task_id = ptr.addr(), "registered nop");

        // Write the entry to the SQE in the ring.
        let op = opcode::Nop::new();
        op.register_with_sqe(free_sqe);
        unsafe { io_uring_sqe_set_data(free_sqe, ptr as *mut c_void) };
    }
}


#[repr(align(32))]
enum EnqueuedEntry<G> {
    /// The op is a nop and should be ignored.
    Nop,
    /// The entry is an IO op.
    IoOp(IoOpEntry<G>),
    /// The entry is part of a resource, this is triggered
    /// from being deallocated.
    ResourceOp(usize),
}

struct IoOpEntry<G> {
    reply: reply::ReplyNotify,
    guard: Option<G>,
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
