#![doc = include_str!("../README.md")]

use std::any::Any;
use std::io;

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

