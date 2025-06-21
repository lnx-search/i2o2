use std::sync::Arc;
use std::{io, mem};

use libc::{CPU_SET, sched_setaffinity};
use liburing_rs::*;

use crate::handle::I2o2Handle;
use crate::inventory::InflightInventory;
use crate::opcode::RingProbe;
use crate::{
    I2o2CompletionWorker,
    I2o2SchedulerThreadHandle,
    I2o2SubmissionWorker,
    RegisteredResources,
    dms,
    ring,
    wake,
};

const MAX_SAFE_IDX: u32 = 1 << 30;

#[derive(Debug, Clone)]
/// A set of configuration options for customising the [I2o2Scheduler] scheduler.
///
/// ## Example
///
/// ```rust
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::time::Duration;
///
/// let (submission_worker, completion_worker, handle) = i2o2::I2o2Builder::default()
///     .with_queue_size(256)
///     .with_num_registered_files(4)
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
            size <= MAX_SAFE_IDX,
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
            size <= MAX_SAFE_IDX,
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

    /// Attempt to create the scheduler using the current configuration.
    pub fn try_create<G>(
        self,
    ) -> io::Result<(
        I2o2SubmissionWorker<G>,
        I2o2CompletionWorker<G>,
        I2o2Handle<G>,
    )> {
        self.try_create_inner()
    }

    /// Attempt to create the scheduler and run it in a background thread using the
    /// current configuration.
    pub fn try_spawn<G>(self) -> io::Result<(I2o2SchedulerThreadHandle, I2o2Handle<G>)>
    where
        G: Send + 'static,
    {
        self.try_spawn_inner(None, None)
    }

    /// Attempt to create the scheduler and run it in a background thread using the
    /// current configuration and pin the thread to a specific CPU.
    pub fn try_spawn_and_pin<G>(
        self,
        submission_cpu_set: CpuSet,
        completion_cpu_set: CpuSet,
    ) -> io::Result<(I2o2SchedulerThreadHandle, I2o2Handle<G>)>
    where
        G: Send + 'static,
    {
        self.try_spawn_inner(Some(submission_cpu_set), Some(completion_cpu_set))
    }

    fn try_create_inner<G>(
        self,
    ) -> io::Result<(
        I2o2SubmissionWorker<G>,
        I2o2CompletionWorker<G>,
        I2o2Handle<G>,
    )> {
        #[cfg(test)]
        fail::fail_point!("scheduler_create_fail", |_| {
            Err(io::Error::other("test error triggered by failpoints"))
        });

        let switch = dms::DeadMansSwitch::default();
        let (io_queue_tx, io_queue_rx) = super::queue::new(self.queue_size as usize);
        let (resource_queue_tx, resource_queue_rx) = super::queue::new(32);

        let (submission_waker, submission_wc) = wake::new()?;
        let submission_wake_on_drop = submission_wc.make_wake_on_drop();

        let (completion_waker, completion_wc) = wake::new()?;

        let resources = Arc::new(RegisteredResources::new(
            self.num_registered_files,
            self.num_registered_buffers,
        ));

        let inventory = InflightInventory::new((self.queue_size * 4) as usize);

        let mut ring = self.setup_io_ring()?;
        tracing::debug!("ring created");

        self.setup_registered_resources(&mut ring)?;
        tracing::debug!("successfully registered resources with ring");

        ring.register_eventfd(completion_wc.fd())?;

        let handle = I2o2Handle::new(
            switch.clone().into_weak(),
            io_queue_tx,
            resource_queue_tx,
            submission_waker,
        );

        let submission = I2o2SubmissionWorker {
            ring: unsafe { ring.clone_ref() },
            switch: switch.clone(),
            waker_controller: submission_wc,
            completion_waker,
            incoming_io: io_queue_rx,
            incoming_resources: resource_queue_rx,
            inflight_inventory: inventory.clone(),
            resources: resources.clone(),
            op_counter_io: 0,
            op_counter_resource: 0,
            op_counter_nop: 0,
        };

        let completion = I2o2CompletionWorker {
            ring,
            switch,
            waker_controller: completion_wc,
            submission_worker_waker: submission_wake_on_drop,
            inflight_inventory: inventory,
            resources,
        };

        Ok((submission, completion, handle))
    }

    fn try_spawn_inner<G>(
        self,
        submission_cpu_set: Option<CpuSet>,
        completion_cpu_set: Option<CpuSet>,
    ) -> io::Result<(I2o2SchedulerThreadHandle, I2o2Handle<G>)>
    where
        G: Send + 'static,
    {
        let (tx, rx) = flume::bounded(1);

        let submission_task = move || {
            if let Some(set) = submission_cpu_set {
                let success = set.set_current_thread();
                tracing::debug!(
                    success,
                    "set scheduler submission worker thread affinity"
                );
            }

            let (submission_worker, completion_worker, handle) =
                self.try_create_inner()?;

            #[cfg(test)]
            fail::fail_point!("scheduler_spawn_fail", |_| {
                Err(io::Error::other("test error triggered by failpoints"))
            });

            tx.send((completion_worker, handle)).unwrap();

            if let Err(e) = submission_worker.run() {
                tracing::error!(error = %e, "submission worker aborted early");
                return Err(e);
            }

            Ok::<_, io::Error>(())
        };

        let submission_worker = std::thread::Builder::new()
            .name("i2o2-submission-thread".to_string())
            .spawn(submission_task)
            .expect("spawn background worker thread");

        let (completion_worker, handle) = match rx.recv() {
            Ok(pair) => pair,
            Err(_) => {
                submission_worker.join().unwrap()?;
                unreachable!("worker exited, this can never be reached");
            },
        };

        let switch = completion_worker.switch.clone().into_weak();

        let completion_task = move || {
            if let Some(set) = completion_cpu_set {
                let success = set.set_current_thread();
                tracing::debug!(
                    success,
                    "set scheduler submission worker thread affinity"
                );
            }

            if let Err(e) = completion_worker.run() {
                tracing::error!(error = %e, "completion worker aborted early");
                return Err(e);
            }

            Ok::<_, io::Error>(())
        };

        let completion_worker = std::thread::Builder::new()
            .name("i2o2-completion-thread".to_string())
            .spawn(completion_task)
            .expect("spawn background worker thread");

        let thread_handle = I2o2SchedulerThreadHandle {
            switch,
            submission_worker,
            completion_worker,
        };

        Ok((thread_handle, handle))
    }

    fn setup_io_ring(&self) -> io::Result<ring::IoRing> {
        let probe = RingProbe::new()?;

        let mut params: io_uring_params = unsafe { mem::zeroed() };

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

        if probe.is_kernel_v5_19_or_newer() {
            params.flags |= IORING_SETUP_COOP_TASKRUN;
        }

        params.cq_entries = self.ring_depth * 4;
        params.features |= IORING_FEAT_NODROP;
        params.features |= IORING_FEAT_FAST_POLL;

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

#[derive(Clone)]
/// The cpu set restricts what CPU cores the thread can run on.
pub struct CpuSet(libc::cpu_set_t);

impl CpuSet {
    /// Creates a blank [CpuSet] with no cores set.
    pub fn blank() -> Self {
        Self(unsafe { mem::zeroed::<libc::cpu_set_t>() })
    }

    /// Set the target `cpu_id` in the CPU set.
    pub fn set(&mut self, cpu_id: usize) {
        unsafe { CPU_SET(cpu_id, &mut self.0) };
    }

    fn set_current_thread(&self) -> bool {
        let res = unsafe {
            sched_setaffinity(
                0, // Defaults to current thread
                size_of::<libc::cpu_set_t>(),
                &self.0,
            )
        };
        res == 0
    }
}
