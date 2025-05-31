use std::collections::VecDeque;
use std::io;
use std::time::Duration;

use io_uring::IoUring;

use crate::handle::I2o2Handle;
use crate::wake::RingWaker;
use crate::{I2o2Scheduler, TrackedState};

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
            self.sqe_poll = Some(Duration::from_millis(2000));
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
    /// By default, this is `disabled`.
    pub const fn with_defer_task_run(mut self, enable: bool) -> Self {
        self.defer_task_run = enable;
        self
    }

    /// Attempt to create the scheduler using the current configuration.
    pub fn try_create<G>(self) -> io::Result<(I2o2Scheduler<G>, I2o2Handle<G>)> {
        let (tx, rx) = flume::bounded(self.queue_size as usize);
        let waker = RingWaker::new()?;

        let ring = self.setup_io_ring()?;

        let handle = I2o2Handle::new(tx, waker.task_waker());

        let scheduler = I2o2Scheduler {
            ring,
            state: TrackedState::default(),
            self_waker: waker,
            incoming: rx,
            backlog: VecDeque::new(),
            _anti_send_ptr: std::ptr::null_mut(),
        };

        Ok((scheduler, handle))
    }

    /// Attempt to create the scheduler and run it in a background thread using the
    /// current configuration.
    pub fn try_spawn<G>(
        self,
    ) -> io::Result<(std::thread::JoinHandle<io::Result<()>>, I2o2Handle<G>)>
    where
        G: Send + 'static,
    {
        let (tx, rx) = flume::bounded(1);

        let task = move || {
            let (scheduler, handle) = self.try_create()?;

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

    fn setup_io_ring(&self) -> io::Result<IoUring> {
        let mut builder = IoUring::builder();
        builder.setup_single_issuer();
        builder.dontfork();

        if self.io_poll {
            builder.setup_iopoll();
        }

        if let Some(idle) = self.sqe_poll {
            builder.setup_sqpoll(idle.as_millis() as u32);

            if let Some(cpu) = self.sqe_poll_cpu {
                builder.setup_sqpoll_cpu(cpu);
            }
        } else {
            // This functionality effectively gets implicitly enabled by SQPOLL
            // we should only enable this if SQPOLL is disabled.
            builder.setup_coop_taskrun();
        }

        if self.defer_task_run {
            builder.setup_defer_taskrun();
        }

        builder.build(self.queue_size)
    }
}
