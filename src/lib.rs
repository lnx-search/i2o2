#![doc = include_str!("../README.md")]

use std::any::Any;
use std::{io, ptr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use crate::inventory::InflightInventory;
use crate::opcode::sealed::RegisterOp;

// mod builder;
// mod handle;
pub mod opcode;
mod queue;
mod reply;
mod ring;
// #[cfg(test)]
// mod tests;
mod wake;
mod inventory;
mod dms;

// pub use self::builder::{I2o2Builder, CpuSet};
// pub use self::handle::{I2o2Handle, RegisterError, SchedulerClosed, SubmitResult};
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

// /// Create a new [I2o2Scheduler] and [I2o2Handle] pair backed by io_uring.
// ///
// /// This will use the default settings for the scheduler, you can optionally
// /// use the [builder] to customise the ring behaviour.
// ///
// /// NOTE: The scheduler cannot be sent across threads.
// ///
// /// ## Example
// ///
// /// ```rust
// /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
// ///
// /// let (scheduler, handle) = i2o2::create_for_current_thread::<()>()?;
// ///
// /// // ... do work
// ///
// /// # Ok(())
// /// # }
// /// ```
// pub fn create_for_current_thread<G>() -> io::Result<(I2o2Scheduler<G>, I2o2Handle<G>)> {
//     I2o2Builder::default().try_create()
// }
//
// /// Create a new [I2o2Scheduler] and [I2o2Handle] pair backed by io_uring and spawn the scheduler
// /// in a background worker thread.
// ///
// /// This will use the default settings for the scheduler, you can optionally
// /// use the [builder] to customise the ring behaviour.
// ///
// /// NOTE: The scheduler cannot be sent across threads.
// ///
// /// ## Example
// ///
// /// ```rust
// /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
// ///
// /// let (scheduler_handle, handle) = i2o2::create_and_spawn::<()>()?;
// ///
// /// // ... do work
// ///
// /// # Ok(())
// /// # }
// /// ```
// pub fn create_and_spawn<G>()
// -> io::Result<(std::thread::JoinHandle<io::Result<()>>, I2o2Handle<G>)>
// where
//     G: Send + 'static,
// {
//     I2o2Builder::default().try_spawn()
// }
//
// /// Create a new [I2o2Scheduler] and [I2o2Handle] pair backed by io_uring
// /// with a custom configuration.
// ///
// /// ## Example
// ///
// /// ```rust
// /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
// /// use std::time::Duration;
// ///
// /// let (scheduler, handle) = i2o2::builder()
// ///     .with_io_polling(true)
// ///     .with_sq_polling(true)
// ///     .with_sq_polling_timeout(Duration::from_millis(100))
// ///     .try_create::<()>()?;
// ///
// /// // ... do work
// ///
// /// # Ok(())
// /// # }
// /// ```
// pub const fn builder() -> I2o2Builder {
//     I2o2Builder::const_default()
// }


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
        self.submission_worker
            .join()
            .unwrap()?;
        self.completion_worker
            .join()
            .unwrap()?;
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
    incoming_io: queue::Receiver<()>,
    incoming_resources: queue::Receiver<()>,
    inflight_inventory: Arc<InflightInventory<InflightEntry<G>>>,
}

impl<G> I2o2SubmissionWorker<G> {
    /// Run the submission worker.
    pub fn run(mut self) -> io::Result<()> {
        tracing::debug!("submission worker running");
        
        while !self.should_close() {
            
        }
        
        tracing::debug!("submission worker draining remaining");

        self.drain_remaining();

        tracing::debug!("submission worker closed");
        
        Ok(())
    }
    
    fn should_close(&self) -> bool {
        self.switch.is_set()  || self.incoming_io.is_disconnected()
    }
    
    fn drain_remaining(&mut self) {
        // Only drain IO ops, resources will be immediately dropped anyway.
        while let Some(op) = self.incoming_io.pop() {
            
        }
    }
    
    fn process_incoming_io(&mut self) {
        
    }
    
    fn process_incoming_resources(&mut self) {
        
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
}

impl<G> I2o2CompletionWorker<G> {
    /// Run the completion worker.
    pub fn run(mut self) -> io::Result<()> {
        tracing::debug!("completion worker running");
        
        let mut num_completions_available = self.ring.num_completions_available();
        while !self.switch.is_set() {
            if num_completions_available == 0 {
                self.ring.wait_for_completion();
            }
            
            self.process_completions();
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

    fn process_completions(&mut self) {
        for cqe in self.ring.iter_completions() {
            let entry = unsafe { ptr::read(cqe.user_data as *mut InflightEntry<G>) };
            self.handle_entry_completion(entry, cqe.result);
        }
    }
    
    fn handle_entry_completion(&mut self, entry: InflightEntry<G>, result: i32) {
        match entry {
            InflightEntry::Nop => {},
            InflightEntry::ResourceOp(entry) => {
                #[cfg(feature = "trace-hotpath")]
                tracing::debug!(resource_id = entry.resource_id, "completed resource op");
                drop(entry);                
            },
            InflightEntry::IoOp(entry) => {
                #[cfg(feature = "trace-hotpath")]
                tracing::debug!(io_id = entry.io_id, "completed IO op");
                entry.reply.set_result(result);
            },
        }
    }
}


enum InflightEntry<G> {
    Nop,
    ResourceOp(ResourceOpEntry<G>),
    IoOp(IoOpEntry<G>),
}

struct ResourceOpEntry<G> {
    resource_id: u32,
    guard: Option<G>,
}

struct IoOpEntry<G> {
    io_id: u64,
    reply: reply::ReplyNotify,
    guard: Option<G>,
}

