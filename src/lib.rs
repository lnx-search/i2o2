#![doc = include_str!("../README.md")]

use std::any::Any;
use std::ffi::c_void;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::{cmp, io, ptr};

use liburing_rs::*;

use crate::inventory::InflightInventory;
use crate::opcode::sealed::RegisterOp;

mod builder;
mod handle;
pub mod opcode;
mod queue;
mod reply;
mod ring;
// #[cfg(test)]
// mod tests;
mod dms;
mod inventory;
mod wake;

pub use self::builder::{CpuSet, I2o2Builder};
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

/// The maximum number of IO ops to schedule before checking
/// if any resources are waiting to be registered.
///
/// This is used to prevent starvation of the resource queue.
const MAX_IO_OPS_BEFORE_RESOURCE_CONSIDER: usize = 500;

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
/// let (submission_worker, completion_worker, handle) = i2o2::create_for_current_thread::<()>()?;
///
/// // ... do work
///
/// # Ok(())
/// # }
/// ```
pub fn create_for_current_thread<G>() -> io::Result<(
    I2o2SubmissionWorker<G>,
    I2o2CompletionWorker<G>,
    I2o2Handle<G>,
)> {
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
pub fn create_and_spawn<G>() -> io::Result<(I2o2SchedulerThreadHandle, I2o2Handle<G>)>
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
/// let (submission_worker, completion_worker, handle) = i2o2::builder()
///     .with_io_polling(true)
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

/// The thread handles for the i2o2 scheduler worker threads.
pub struct I2o2SchedulerThreadHandle {
    switch: dms::WeakDeadMansSwitch,
    submission_worker: std::thread::JoinHandle<io::Result<()>>,
    completion_worker: std::thread::JoinHandle<io::Result<()>>,
}

impl I2o2SchedulerThreadHandle {
    /// Returns if the scheduler is currently running or not.
    pub fn is_running(&self) -> bool {
        !self.switch.is_set()
    }

    /// Set the flag to shut down the scheduler.
    ///
    /// This method does not block, instead you should call shutdown
    /// and _then_ [I2o2SchedulerThreadHandle::join].
    pub fn shutdown(&self) {
        self.switch.set();
    }

    /// Join the scheduler threads and wait for them to complete, returning
    ///  the end result.
    pub fn join(self) -> io::Result<()> {
        self.submission_worker.join().unwrap()?;
        self.completion_worker.join().unwrap()?;
        Ok(())
    }
}

/// The submission half of the i2o2 scheduler.
///
/// This worker is responsible for reading incoming operations
/// and submitting them to the ring.
///
/// This worker has a dependency on its sibling, the [I2o2CompletionWorker],
/// if _either_ of the halves are dropped, the scheduler will exit to prevent deadlocks.
pub struct I2o2SubmissionWorker<G> {
    ring: ring::IoRing,
    switch: dms::DeadMansSwitch,
    /// A waker handle for triggering a completion event on `self`
    /// intern causing events to be processed.
    waker_controller: wake::RingWakerController,
    incoming_io: queue::Receiver<Packaged<opcode::AnyOp, G>>,
    incoming_resources: queue::Receiver<ResourceMessage<G>>,
    inflight_inventory: Arc<InflightInventory<InflightEntry<G>>>,

    /// Registered buffers and files
    resources: Arc<RegisteredResources>,

    // Op counters, mostly used for debugging.
    op_counter_io: u64,
    op_counter_resource: u64,
    op_counter_nop: u64,
}

impl<G> I2o2SubmissionWorker<G> {
    /// Run the submission worker.
    pub fn run(mut self) -> io::Result<()> {
        tracing::debug!("submission worker running");

        #[cfg(test)]
        fail::fail_point!("submission_worker_run_fail", |_| {
            Err(io::Error::other("test error triggered by failpoints"))
        });

        'event_loop: while !self.should_close() {
            let n_consumed = self.process_incoming_io()?;
            self.process_incoming_resources()?;

            // If we haven't processed any entries then we should wait for new ops to come
            // in if we've consumed all IO, or wait for submission queue entries to be
            // available on the ring.
            //
            // We spin a little bit here to try and avoid stalls.
            if !self.incoming_resources.is_empty() {
                continue;
            } else if n_consumed == 0 && self.incoming_io.is_empty() {
                for _ in 0..50 {
                    if !self.incoming_io.is_empty()
                        || !self.incoming_resources.is_empty()
                    {
                        continue 'event_loop;
                    }
                }

                self.waker_controller.wait_for_events();
            } else if n_consumed == 0 && self.ring.num_sqe_available() == 0 {
                for _ in 0..50 {
                    if self.ring.num_sqe_available() > 0
                        || !self.incoming_resources.is_empty()
                    {
                        continue 'event_loop;
                    }
                }

                self.ring.wait_for_sq_capacity();
            }
        }

        tracing::debug!("submission worker draining remaining");

        self.drain_remaining()?;

        tracing::debug!("submission worker closed");

        Ok(())
    }

    fn should_close(&self) -> bool {
        self.switch.is_set() || self.incoming_io.is_disconnected()
    }

    fn drain_remaining(&mut self) -> io::Result<()> {
        // Only drain IO ops, resources will be immediately dropped anyway.
        while !self.incoming_io.is_empty() {
            self.ring.wait_for_sq_capacity();
            self.process_incoming_io()?;
        }

        Ok(())
    }

    fn process_incoming_io(&mut self) -> io::Result<usize> {
        let pop_n =
            cmp::min(self.incoming_io.len(), MAX_IO_OPS_BEFORE_RESOURCE_CONSIDER);

        let mut n_read = 0;
        let mut n_sqe_consumed = 0;
        for _ in 0..pop_n {
            let Some(sqe) = self.ring.get_available_sqe() else {
                break;
            };

            n_sqe_consumed += 1;

            let msg = match self.incoming_io.pop() {
                Some(msg) => msg,
                // Should never happen, but we need to handle it anyway
                // to avoid replaying past events.
                None => {
                    self.write_nop(sqe);
                    break;
                },
            };

            n_read += 1;

            if msg.entry.requires_size128() && !self.ring.is_size128() {
                #[cfg(feature = "trace-hotpath")]
                tracing::trace!(
                    "rejecting op because size128 is required but not active"
                );
                self.write_nop(sqe);
                let _ = msg.reply.set_result(MAGIC_ERRNO_NOT_SIZE128);
                continue;
            }

            self.handle_io_op(sqe, msg);
        }

        let result = self.ring.submit();
        self.incoming_io.wake_n(n_read);

        result?;

        Ok(n_sqe_consumed)
    }

    fn process_incoming_resources(&mut self) -> io::Result<()> {
        while let Some(msg) = self.incoming_resources.pop() {
            match msg {
                ResourceMessage::RegisterResource(msg) => {
                    self.handle_register_resource_op(msg)
                },
                ResourceMessage::UnregisterResource(msg) => {
                    self.handle_unregister_resource_op(msg)
                },
            }
        }

        self.incoming_resources.wake_all();

        Ok(())
    }

    fn handle_io_op(&mut self, sqe: *mut io_uring_sqe, msg: Packaged<opcode::AnyOp, G>) {
        self.op_counter_io += 1;

        let Packaged {
            entry,
            reply,
            guard,
        } = msg;

        unsafe { entry.register_with_sqe(&mut (*sqe)) };

        #[cfg(feature = "trace-hotpath")]
        tracing::trace!(
            io_id = self.op_counter_io,
            has_guard = guard.is_some(),
            "processing IO"
        );

        let entry = InflightEntry::IoOp(IoOpEntry {
            io_id: self.op_counter_io,
            reply,
            guard,
        });

        let ptr = self.inflight_inventory.write_to_free_ptr(entry);
        unsafe { io_uring_sqe_set_data(sqe, ptr as *mut c_void) };
    }

    fn handle_register_resource_op(&mut self, msg: Packaged<Resource, G>) {
        self.op_counter_resource += 1;

        let Packaged {
            entry: resource,
            reply,
            guard,
        } = msg;

        let resource_type = resource.resource_type();

        let maybe_resource_id = match resource_type {
            ResourceType::File => self.resources.alloc_file(),
            ResourceType::Buffer => self.resources.alloc_buffer(),
        };

        let resource_id = match maybe_resource_id {
            Some(resource_id) => resource_id,
            None => {
                reply.set_result(MAGIC_ERRNO_NO_CAPACITY);
                return;
            },
        };

        #[cfg(feature = "trace-hotpath")]
        tracing::trace!(
            resource_type = ?resource_type,
            resource_id = resource_id,
            "processing resource"
        );

        let entry = InflightEntry::ResourceOp(ResourceOpEntry {
            resource_id,
            resource_type: resource.resource_type(),
            guard,
        });

        let ptr = self.inflight_inventory.write_to_free_ptr(entry);
        let tag = (ptr as *mut c_void) as u64;

        let result = match resource {
            Resource::Buffer(buf) => self.ring.register_buffer(resource_id, buf, tag),
            Resource::File(fd) => self.ring.register_file(resource_id, fd, tag),
        };

        // Happy path, we can just reply and complete.
        if result.is_ok() {
            reply.set_result(resource_id as i32);
            return;
        }

        // Sad path.
        // We need to clean up the inventory entry as it will not be done
        // by the completion worker due to the error.
        unsafe { ptr::drop_in_place(ptr) };
        self.inflight_inventory
            .push_free_ptr(ptr as *mut MaybeUninit<InflightEntry<G>>);

        let err = result.unwrap_err();
        reply.set_result(err.raw_os_error().unwrap());
    }

    fn handle_unregister_resource_op(&mut self, op: Packaged<ResourceIndex, G>) {
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

    fn write_nop(&mut self, sqe: *mut io_uring_sqe) {
        self.op_counter_nop += 1;

        #[cfg(feature = "trace-hotpath")]
        tracing::trace!(nop_id = self.op_counter_nop, "processing nop");

        let op = opcode::Nop::new();
        unsafe { op.register_with_sqe(&mut (*sqe)) };

        let ptr = self
            .inflight_inventory
            .write_to_free_ptr(InflightEntry::Nop);
        unsafe { io_uring_sqe_set_data(sqe, ptr as *mut c_void) };
    }
}

/// The completion half of the i2o2 scheduler.
///
/// This worker is responsible for reading incoming completed operations
/// off of the ring and sending the results back to the tasks.
///
/// This worker has a dependency on its sibling, the [I2o2SubmissionWorker],
/// if _either_ of the halves are dropped, the scheduler will exit to prevent deadlocks.
pub struct I2o2CompletionWorker<G> {
    ring: ring::IoRing,
    switch: dms::DeadMansSwitch,
    inflight_inventory: Arc<InflightInventory<InflightEntry<G>>>,

    /// Registered buffers and files
    resources: Arc<RegisteredResources>,
}

impl<G> I2o2CompletionWorker<G> {
    /// Run the completion worker.
    pub fn run(mut self) -> io::Result<()> {
        tracing::debug!("completion worker running");

        #[cfg(test)]
        fail::fail_point!("completion_worker_run_fail", |_| {
            Err(io::Error::other("test error triggered by failpoints"))
        });

        while !self.switch.is_set() {
            let num_consumed = self.process_completions();

            if num_consumed == 0 {
                for _ in 0..50 {
                    let num_consumed = self.process_completions();
                    if num_consumed != 0 {
                        break;
                    }
                }
                self.ring.wait_for_completion();
            }
        }

        tracing::debug!("completion worker draining remaining");

        self.drain_remaining();

        tracing::debug!("completion worker closed");

        Ok(())
    }

    fn drain_remaining(&mut self) {
        while self.inflight_inventory.num_inflight() > 0 {
            self.ring.wait_for_completion();
            self.process_completions();
        }
    }

    fn process_completions(&mut self) -> usize {
        let mut num_consumed = 0;
        for cqe in self.ring.iter_completions() {
            let entry = unsafe { ptr::read(cqe.user_data as *mut InflightEntry<G>) };

            match entry {
                InflightEntry::Nop => {},
                InflightEntry::ResourceOp(entry) => {
                    #[cfg(feature = "trace-hotpath")]
                    tracing::debug!(
                        resource_id = entry.resource_id,
                        "completed resource op"
                    );
                    match entry.resource_type {
                        ResourceType::File => {
                            self.resources.free_file(entry.resource_id)
                        },
                        ResourceType::Buffer => {
                            self.resources.free_buffer(entry.resource_id)
                        },
                    }
                },
                InflightEntry::IoOp(entry) => {
                    #[cfg(feature = "trace-hotpath")]
                    tracing::debug!(io_id = entry.io_id, "completed IO op");
                    entry.reply.set_result(cqe.result);
                },
            }

            self.inflight_inventory
                .push_free_ptr(cqe.user_data as *mut MaybeUninit<_>);

            num_consumed += 1;
        }
        num_consumed
    }
}

struct RegisteredResources {
    max_files: u32,
    files: parking_lot::Mutex<slab::Slab<()>>,
    max_buffers: u32,
    buffers: parking_lot::Mutex<slab::Slab<()>>,
}

impl RegisteredResources {
    fn new(num_files: u32, num_buffers: u32) -> Self {
        Self {
            max_files: num_files,
            files: parking_lot::Mutex::new(slab::Slab::with_capacity(
                num_files as usize,
            )),
            max_buffers: num_buffers,
            buffers: parking_lot::Mutex::new(slab::Slab::with_capacity(
                num_buffers as usize,
            )),
        }
    }

    fn alloc_file(&self) -> Option<u32> {
        let mut lock = self.files.lock();
        if lock.len() as u32 >= self.max_files {
            None
        } else {
            Some(lock.insert(()) as u32)
        }
    }

    fn alloc_buffer(&self) -> Option<u32> {
        let mut lock = self.buffers.lock();
        if lock.len() as u32 >= self.max_buffers {
            None
        } else {
            Some(lock.insert(()) as u32)
        }
    }

    fn free_file(&self, id: u32) {
        let mut lock = self.files.lock();
        lock.try_remove(id as usize);
    }

    fn free_buffer(&self, id: u32) {
        let mut lock = self.buffers.lock();
        lock.try_remove(id as usize);
    }
}

enum InflightEntry<G> {
    Nop,
    ResourceOp(ResourceOpEntry<G>),
    IoOp(IoOpEntry<G>),
}

struct ResourceOpEntry<G> {
    resource_id: u32,
    resource_type: ResourceType,
    #[allow(unused)]
    guard: Option<G>,
}

struct IoOpEntry<G> {
    #[allow(unused)]
    io_id: u64,
    reply: reply::ReplyNotify,
    #[allow(unused)]
    guard: Option<G>,
}

#[derive(Copy, Clone, Debug)]
enum ResourceType {
    File,
    Buffer,
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
    Buffer(iovec),
    File(std::os::fd::RawFd),
}

impl Resource {
    fn resource_type(&self) -> ResourceType {
        match self {
            Resource::Buffer(_) => ResourceType::Buffer,
            Resource::File(_) => ResourceType::File,
        }
    }
}

// SAFETY: The handle ensures the buffers are safe to send across a thread boundary.
unsafe impl Send for Resource {}

enum ResourceIndex {
    File(u32),
}
