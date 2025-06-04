use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::io;
use std::io::ErrorKind;
use std::time::Duration;

use io_uring::IoUring;

use crate::handle::I2o2Handle;
use crate::{I2o2Scheduler, TrackedState, mode, wake};

type SchedulerThreadHandle = std::thread::JoinHandle<io::Result<()>>;

#[derive(Debug, Clone)]
/// A set of configuration options for customising the [I2o2Scheduler] scheduler.
///
/// ## Example
///
/// ```rust
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::time::Duration;
///
/// let (scheduler, handle) = i2o2::I2o2Builder::default()
///     .with_defer_task_run(false)
///     .with_io_polling(true)
///     .with_sqe_polling(true)
///     .with_sqe_polling_timeout(Duration::from_millis(100))
///     .try_create::<()>()?;
///
/// // ... do work
///
/// # Ok(())
/// # }
/// ```
pub struct I2o2Builder {
    queue_size: u32,
    io_poll: bool,
    sqe_poll: Option<Duration>,
    sqe_poll_cpu: Option<u32>,
    defer_task_run: bool,
    num_registered_files: u32,
    num_registered_buffers: u32,
}

impl Default for I2o2Builder {
    fn default() -> Self {
        Self::const_default()
    }
}

impl I2o2Builder {
    pub(super) const fn const_default() -> Self {
        Self {
            queue_size: 128,
            io_poll: false,
            sqe_poll: None,
            sqe_poll_cpu: None,
            defer_task_run: false,
            num_registered_buffers: 0,
            num_registered_files: 0,
        }
    }

    /// Set the queue size of the ring and handler buffer.
    ///
    /// The provided value should be a power of `2`.
    ///
    /// By default, this is `128`.
    pub const fn with_queue_size(mut self, size: u32) -> Self {
        assert!(
            size != 0 && (size & (size - 1)) == 0,
            "provided `size` value must be a power of 2"
        );
        self.queue_size = size;
        self
    }

    /// Set the maximum number of registered buffers that might be
    /// registered with the ring.
    ///
    /// This is value cannot be updated once the ring in created
    /// and is used to allocate the necessary structures for the ring.
    ///
    /// WARNING: You must have a kernel version **5.19+** in order
    /// for this API to not error on creation.
    ///
    /// By default, this is `0`.
    pub const fn with_num_registered_buffers(mut self, size: u32) -> Self {
        assert!(
            size <= super::flags::MAX_SAFE_IDX,
            "total number of registered buffers exceeds maximum allowance"
        );
        self.num_registered_buffers = size;
        self
    }

    /// Set the maximum number of registered files that might be
    /// registered with the ring.
    ///
    /// This is value cannot be updated once the ring in created
    /// and is used to allocate the necessary structures for the ring.
    ///
    /// By default, this is `0`.
    pub const fn with_num_registered_files(mut self, size: u32) -> Self {
        assert!(
            size <= super::flags::MAX_SAFE_IDX,
            "total number of registered files exceeds maximum allowance"
        );
        self.num_registered_files = size;
        self
    }

    /// Enable/disable IO polling.
    ///
    /// Sets `IORING_SETUP_IOPOLL`
    ///
    /// <https://www.man7.org/linux/man-pages/man2/io_uring_setup.2.html>
    ///
    /// > Perform busy-waiting for an I/O completion, as opposed to
    /// > getting notifications via an asynchronous IRQ (Interrupt
    /// > Request).
    ///
    /// **WARNING: Enabling this option requires all file IO events to be O_DIRECT**
    ///
    /// By default, this is `disabled`.
    pub const fn with_io_polling(mut self, enable: bool) -> Self {
        self.io_poll = enable;
        self
    }

    /// Enables/disables submission queue polling by the kernel.
    ///
    /// Sets `IORING_SETUP_SQPOLL`
    ///
    /// <https://www.man7.org/linux/man-pages/man2/io_uring_setup.2.html>
    ///
    /// > When this flag is specified, a kernel thread is created to
    /// > perform submission queue polling.  An io_uring instance
    /// > configured in this way enables an application to issue I/O
    /// > without ever context switching into the kernel.  By using
    /// > the submission queue to fill in new submission queue
    /// > entries and watching for completions on the completion
    /// > queue, the application can submit and reap I/Os without
    /// > doing a single system call.
    ///
    /// By default, the system will use a `10ms` idle timeout, you can configure
    /// this value using [I2o2Builder::with_sqe_polling_timeout].
    pub const fn with_sqe_polling(mut self, enable: bool) -> Self {
        if enable {
            self.sqe_poll = Some(Duration::from_millis(20));
        } else {
            self.sqe_poll = None;
        }
        self
    }

    /// Set the submission queue polling idle timeout.
    ///
    /// <https://www.man7.org/linux/man-pages/man2/io_uring_setup.2.html>
    ///
    /// This overwrites the default timeout value I2o2 sets of `10ms`.
    ///
    /// NOTE: `with_sqe_polling` must be enabled first before calling this method.
    pub const fn with_sqe_polling_timeout(mut self, timeout: Duration) -> Self {
        if self.sqe_poll.is_none() {
            panic!(
                "submission queue polling is not already enabled at the time of calling this method"
            );
        }
        assert!(
            timeout.as_secs_f32() <= 10.0,
            "timeout has gone beyond sane levels"
        );

        self.sqe_poll = Some(timeout);
        self
    }

    /// Set cpu core the polling thread should be pinned to.
    ///
    /// Sets `IORING_SETUP_SQ_AFF`
    ///
    /// <https://www.man7.org/linux/man-pages/man2/io_uring_setup.2.html>
    ///
    /// NOTE: `with_sqe_polling` must be enabled first before calling this method.
    pub const fn with_sqe_polling_pin_cpu(mut self, cpu: u32) -> Self {
        if self.sqe_poll.is_none() {
            panic!(
                "submission queue polling is not already enabled at the time of calling this method"
            );
        }
        self.sqe_poll_cpu = Some(cpu);
        self
    }

    /// Enables/disables submission queue polling by the kernel.
    ///
    /// Sets `IORING_SETUP_DEFER_TASKRUN`
    ///
    /// <https://www.man7.org/linux/man-pages/man2/io_uring_setup.2.html>
    ///
    /// > By default, io_uring will process all outstanding work at
    /// > the end of any system call or thread interrupt. This can
    /// > delay the application from making other progress.  Setting
    /// > this flag will hint to io_uring that it should defer work
    /// > until an io_uring_enter(2) call with the
    /// > IORING_ENTER_GETEVENTS flag set.
    ///
    /// WARNING: You must have a kernel version **6.1+** in order
    /// for this API to not error on creation.
    ///
    /// By default, this is `disabled`.
    pub const fn with_defer_task_run(mut self, enable: bool) -> Self {
        self.defer_task_run = enable;
        self
    }

    /// Attempt to create the scheduler using the current configuration.
    ///
    /// This uses the default [mode::EntrySize64] operating mode.
    pub fn try_create<G>(self) -> io::Result<(I2o2Scheduler<G>, I2o2Handle<G>)> {
        self.try_create_inner()
    }

    /// Attempt to create the scheduler using the current configuration.
    ///
    /// This uses the larger [mode::EntrySize128] operating mode which supports NVME
    /// pass through.
    pub fn try_create_size128<G>(self) -> io::Result<(I2o2Scheduler<G>, I2o2Handle<G>)> {
        self.try_create_inner()
    }

    /// Attempt to create the scheduler and run it in a background thread using the
    /// current configuration.
    ///
    /// This uses the default [mode::EntrySize64] operating mode.
    pub fn try_spawn<G>(
        self,
    ) -> io::Result<(std::thread::JoinHandle<io::Result<()>>, I2o2Handle<G>)>
    where
        G: Send + 'static,
    {
        self.try_spawn_inner()
    }

    /// Attempt to create the scheduler and run it in a background thread using the
    /// current configuration.
    ///
    /// This uses the larger [mode::EntrySize128] operating mode which supports NVME
    /// pass through.
    pub fn try_spawn_size128<G>(
        self,
    ) -> io::Result<(SchedulerThreadHandle, I2o2Handle<G, mode::EntrySize128>)>
    where
        G: Send + 'static,
    {
        self.try_spawn_inner()
    }

    fn try_create_inner<G, M>(
        self,
    ) -> io::Result<(I2o2Scheduler<G, M>, I2o2Handle<G, M>)>
    where
        M: mode::RingMode,
    {
        #[cfg(test)]
        fail::fail_point!("scheduler_create_fail", |_| {
            eprintln!("invoked???");
            Err(io::Error::other("test error triggered by failpoints"))
        });

        let (tx, rx) = super::queue::new(self.queue_size as usize);
        let (waker, controller) = wake::new()?;

        let probe = load_kernel_uring_probe()?;
        if !kernel_is_at_least(&probe, VersionInterest::V5_15) {
            return Err(kernel_too_old(VersionInterest::V5_15));
        }

        let ring = self.setup_io_ring::<M>(&probe)?;
        tracing::debug!(features = ?ring.params(), "ring created with features");

        self.setup_registered_resources::<M>(&probe, &ring)?;
        tracing::debug!("successfully registered resources with ring");

        let handle = I2o2Handle::new(tx, waker);

        let scheduler = I2o2Scheduler {
            ring,
            state: TrackedState::new(
                self.num_registered_files,
                self.num_registered_buffers,
            ),
            waker_controller: controller,
            incoming: rx,
            backlog: VecDeque::new(),
            _anti_send_ptr: std::ptr::null_mut(),
        };

        Ok((scheduler, handle))
    }

    fn try_spawn_inner<G, M>(
        self,
    ) -> io::Result<(SchedulerThreadHandle, I2o2Handle<G, M>)>
    where
        G: Send + 'static,
        M: mode::RingMode,
    {
        let (tx, rx) = flume::bounded(1);

        let task = move || {
            let (scheduler, handle) = self.try_create_inner()?;

            if tx.send(handle).is_err() {
                return Ok(());
            }

            scheduler.run()?;
            Ok::<_, io::Error>(())
        };

        let scheduler_thread_handle = std::thread::Builder::new()
            .name("i2o2-scheduler-thread".to_string())
            .spawn(task)
            .expect("spawn background worker thread");

        if let Ok(handle) = rx.recv() {
            Ok((scheduler_thread_handle, handle))
        } else {
            let error = scheduler_thread_handle.join().unwrap().expect_err(
                "thread aborted before sending handle back but still returns Ok(())",
            );
            Err(error)
        }
    }

    fn setup_io_ring<M>(
        &self,
        probe: &io_uring::Probe,
    ) -> io::Result<IoUring<M::SQEntry, M::CQEntry>>
    where
        M: mode::RingMode,
    {
        let mut builder = IoUring::builder();

        if kernel_is_at_least(probe, VersionInterest::V6_0) {
            tracing::debug!("kernel has single issuer feature enabled, using...");
            builder.setup_single_issuer();
        }

        builder.dontfork();

        if self.io_poll {
            builder.setup_iopoll();
        }

        if let Some(idle) = self.sqe_poll {
            builder.setup_sqpoll(idle.as_millis() as u32);

            if let Some(cpu) = self.sqe_poll_cpu {
                builder.setup_sqpoll_cpu(cpu);
            }
        } else if kernel_is_at_least(probe, VersionInterest::V5_19) {
            tracing::debug!("kernel has coop task run feature enabled, using...");
            // This functionality effectively gets implicitly enabled by SQPOLL
            // we should only enable this if SQPOLL is disabled.
            builder.setup_coop_taskrun();
        }

        if self.defer_task_run {
            if kernel_is_at_least(probe, VersionInterest::V6_1) {
                builder.setup_defer_taskrun();
            } else {
                return Err(unsupported_version(VersionInterest::V6_1));
            }
        }

        builder.build(self.queue_size)
    }

    fn setup_registered_resources<M>(
        &self,
        probe: &io_uring::Probe,
        ring: &IoUring<M::SQEntry, M::CQEntry>,
    ) -> io::Result<()>
    where
        M: mode::RingMode,
    {
        let submitter = ring.submitter();

        if self.num_registered_files > 0 {
            tracing::debug!(
                num_registered_files = self.num_registered_files,
                "registering files with ring",
            );

            // Setting up the registered files API.
            if kernel_is_at_least(probe, VersionInterest::V5_19) {
                submitter.register_files_sparse(self.num_registered_files)?;
            } else {
                let descriptors = vec![-1; self.num_registered_files as usize];
                let flags = vec![0; self.num_registered_files as usize];
                submitter.register_files_tags(&descriptors, &flags)?;
            }
        }

        if self.num_registered_buffers > 0 {
            tracing::debug!(
                num_registered_buffers = self.num_registered_buffers,
                "registering buffers with ring",
            );

            // Check if we have kernel 5.19+, IORING_OP_SOCKET was added in this version
            // so we can check the version using the probe.
            if kernel_is_at_least(probe, VersionInterest::V5_19) {
                submitter.register_buffers_sparse(self.num_registered_buffers)?;
            } else {
                return Err(unsupported_version(VersionInterest::V5_19));
            }
        }

        Ok(())
    }
}

/// Create a default ring to load a set of probe options.
fn load_kernel_uring_probe() -> io::Result<io_uring::Probe> {
    let ring = IoUring::new(8)?;
    let mut probe = io_uring::Probe::new();
    let submitter = ring.submitter();
    submitter.register_probe(&mut probe)?;

    tracing::debug!("loaded ring probe");

    Ok(probe)
}

#[cfg(not(test))]
fn kernel_is_at_least(probe: &io_uring::Probe, interest: VersionInterest) -> bool {
    probe.is_supported(interest as u8)
}

#[cfg(test)]
fn kernel_is_at_least(probe: &io_uring::Probe, interest: VersionInterest) -> bool {
    match interest {
        VersionInterest::V5_15 => {
            fail::fail_point!("read-dir", |_| false);
            probe.is_supported(interest as u8)
        },
        VersionInterest::V5_19 => {
            fail::fail_point!("kernel_v5_13", |_| false);
            fail::fail_point!("kernel_v5_15", |_| false);
            probe.is_supported(interest as u8)
        },
        VersionInterest::V6_0 => {
            fail::fail_point!("kernel_v5_13", |_| false);
            fail::fail_point!("kernel_v5_15", |_| false);
            fail::fail_point!("kernel_v5_19", |_| false);
            probe.is_supported(interest as u8)
        },
        VersionInterest::V6_1 => {
            fail::fail_point!("kernel_v5_13", |_| false);
            fail::fail_point!("kernel_v5_15", |_| false);
            fail::fail_point!("kernel_v5_19", |_| false);
            fail::fail_point!("kernel_v6_0", |_| false);
            probe.is_supported(interest as u8)
        },
    }
}

#[repr(u8)]
enum VersionInterest {
    V5_15 = io_uring::opcode::OpenAt::CODE,
    V5_19 = io_uring::opcode::Socket::CODE,
    V6_0 = io_uring::opcode::SendZc::CODE,
    V6_1 = io_uring::opcode::SendMsgZc::CODE,
}

impl Display for VersionInterest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            VersionInterest::V5_15 => write!(f, "5.15"),
            VersionInterest::V5_19 => write!(f, "5.19"),
            VersionInterest::V6_0 => write!(f, "6.0"),
            VersionInterest::V6_1 => write!(f, "6.1"),
        }
    }
}

fn unsupported_version(required: VersionInterest) -> io::Error {
    io::Error::new(
        ErrorKind::Unsupported,
        format!(
            "feature not available, kernel version {required}+ \
        required to use this feature"
        ),
    )
}

fn kernel_too_old(required: VersionInterest) -> io::Error {
    io::Error::new(
        ErrorKind::Unsupported,
        format!("I2o2 requires a kernel version of {required} or newer"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case(
        VersionInterest::V5_15,
        "feature not available, kernel version 5.15+ required to use this feature"
    )]
    #[case(
        VersionInterest::V5_19,
        "feature not available, kernel version 5.19+ required to use this feature"
    )]
    #[case(
        VersionInterest::V6_0,
        "feature not available, kernel version 6.0+ required to use this feature"
    )]
    #[case(
        VersionInterest::V6_1,
        "feature not available, kernel version 6.1+ required to use this feature"
    )]
    fn test_unsupported_version_error_display(
        #[case] version: VersionInterest,
        #[case] expected: &str,
    ) {
        let msg = unsupported_version(version).to_string();
        assert_eq!(msg, expected)
    }

    #[rstest::rstest]
    #[case(
        VersionInterest::V5_15,
        "I2o2 requires a kernel version of 5.15 or newer"
    )]
    #[case(
        VersionInterest::V5_19,
        "I2o2 requires a kernel version of 5.19 or newer"
    )]
    #[case(
        VersionInterest::V6_0,
        "I2o2 requires a kernel version of 6.0 or newer"
    )]
    #[case(
        VersionInterest::V6_1,
        "I2o2 requires a kernel version of 6.1 or newer"
    )]
    fn test_kernel_too_old_error_display(
        #[case] version: VersionInterest,
        #[case] expected: &str,
    ) {
        let msg = kernel_too_old(version).to_string();
        assert_eq!(msg, expected)
    }
}
