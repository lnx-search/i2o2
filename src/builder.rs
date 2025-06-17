use std::io;
use std::time::Duration;

use liburing_rs::*;

use crate::handle::I2o2Handle;
use crate::opcode::RingProbe;
use crate::{I2o2Scheduler, TrackedState, ring, wake};

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
    ring_depth: u32,
    io_poll: bool,
    size128: bool,
    sqe_poll: Option<Duration>,
    sqe_poll_cpu: Option<u32>,
    coop_task_run: bool,
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
            ring_depth: 128,
            io_poll: false,
            size128: false,
            sqe_poll: None,
            sqe_poll_cpu: None,
            coop_task_run: false,
            num_registered_buffers: 0,
            num_registered_files: 0,
        }
    }

    /// Set the queue size of the message queue between clients and the scheduler.
    ///
    /// This value will be rounded to the nearest power of two.
    ///
    /// By default, this is `128`.
    pub const fn with_queue_size(mut self, size: u32) -> Self {
        self.queue_size = size;
        self
    }

    /// Set the ring depth.
    ///
    /// This is the SQ size of the ring itself.
    ///
    /// This value must be a power of two.
    ///
    /// By default, this is `128`.
    pub const fn with_ring_depth(mut self, size: u32) -> Self {
        self.ring_depth = size;
        self
    }

    /// Set the size of the SQ entry to be 128 bytes instead of 64.
    ///
    /// This is only required for the [opcode::UringCmd80](crate::opcode::UringCmd80)
    /// op, for NVME pass through.
    ///
    /// By default, this is `false`.
    pub const fn with_sqe_size128(mut self, enabled: bool) -> Self {
        self.size128 = enabled;
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

    /// Enables/disables the coop task run io_uring flag.
    ///
    /// Sets `IORING_SETUP_COOP_TASKRUN`
    ///
    /// <https://www.man7.org/linux/man-pages/man2/io_uring_setup.2.html>
    ///
    /// > By default, io_uring will interrupt a task running in userspace when a completion event comes in.
    /// > This is to ensure that completions run in a timely manner. For a lot of use cases,
    /// > this is overkill and can cause reduced performance from both the inter-processor interrupt
    /// > used to do this, the kernel/user transition, the needless interruption of the tasks userspace
    /// > activities, and reduced batching if completions come in at a rapid rate. Most applications
    /// > don't need the forceful interruption, as the events are processed at any kernel/user
    /// > transition. The exception are setups where the application uses multiple threads
    /// > operating on the same ring, where the application waiting on completions isn't
    /// > the one that submitted them. For most other use cases, setting this flag will
    /// > improve performance.
    ///
    /// WARNING: You must have a kernel version **5.19+** in order
    /// for this API to not error on creation.
    ///
    /// By default, this is `disabled`.
    pub const fn with_coop_task_run(mut self, enable: bool) -> Self {
        self.coop_task_run = enable;
        self
    }

    /// Attempt to create the scheduler using the current configuration.
    pub fn try_create<G>(self) -> io::Result<(I2o2Scheduler<G>, I2o2Handle<G>)> {
        self.try_create_inner()
    }

    /// Attempt to create the scheduler and run it in a background thread using the
    /// current configuration.
    pub fn try_spawn<G>(
        self,
    ) -> io::Result<(std::thread::JoinHandle<io::Result<()>>, I2o2Handle<G>)>
    where
        G: Send + 'static,
    {
        self.try_spawn_inner()
    }

    fn try_create_inner<G>(self) -> io::Result<(I2o2Scheduler<G>, I2o2Handle<G>)> {
        #[cfg(test)]
        fail::fail_point!("scheduler_create_fail", |_| {
            eprintln!("invoked???");
            Err(io::Error::other("test error triggered by failpoints"))
        });

        let (io_queue_tx, io_queue_rx) = super::queue::new(self.queue_size as usize);
        let (resource_queue_tx, resource_queue_rx) = super::queue::new(32);

        let (waker, controller) = wake::new()?;

        let mut ring = self.setup_io_ring()?;
        ring.register_eventfd(controller.fd())?;
        tracing::debug!("ring created");

        self.setup_registered_resources(&mut ring)?;
        tracing::debug!("successfully registered resources with ring");

        let handle = I2o2Handle::new(io_queue_tx, resource_queue_tx, waker);

        let scheduler = I2o2Scheduler {
            ring,
            ring_size128: self.size128,
            state: TrackedState::new(
                self.num_registered_files,
                self.num_registered_buffers,
            ),
            waker_controller: controller,
            incoming_ops: io_queue_rx,
            incoming_resources: resource_queue_rx,
            _anti_send_ptr: std::ptr::null_mut(),
        };

        Ok((scheduler, handle))
    }

    fn try_spawn_inner<G>(self) -> io::Result<(SchedulerThreadHandle, I2o2Handle<G>)>
    where
        G: Send + 'static,
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

    fn setup_io_ring(&self) -> io::Result<ring::IoRing> {
        let probe = RingProbe::new()?;

        let mut params: io_uring_params = unsafe { std::mem::zeroed() };

        if self.size128 {
            params.flags |= IORING_SETUP_SQE128;
            params.flags |= IORING_SETUP_CQE32;
        }

        if probe.is_kernel_v6_0_or_newer() {
            params.flags |= IORING_SETUP_SINGLE_ISSUER;
        }

        if self.io_poll {
            params.flags |= IORING_SETUP_IOPOLL;
        }

        if let Some(idle) = self.sqe_poll {
            params.flags |= IORING_SETUP_SQPOLL;
            params.sq_thread_idle = idle.as_millis() as u32;
        }

        if let Some(pin_cpu) = self.sqe_poll_cpu {
            params.sq_thread_cpu = pin_cpu;
        }

        if self.coop_task_run {
            params.flags |= IORING_SETUP_COOP_TASKRUN;
        }

        ring::IoRing::new(self.ring_depth, params)
    }

    fn setup_registered_resources(&self, ring: &mut ring::IoRing) -> io::Result<()> {
        if self.num_registered_files > 0 {
            tracing::debug!(
                num_registered_files = self.num_registered_files,
                "registering files with ring",
            );
            ring.register_files_sparse(self.num_registered_files)?;
        }

        if self.num_registered_buffers > 0 {
            tracing::debug!(
                num_registered_buffers = self.num_registered_buffers,
                "registering buffers with ring",
            );
            ring.register_buffers_sparse(self.num_registered_buffers)?;
        }

        Ok(())
    }
}
