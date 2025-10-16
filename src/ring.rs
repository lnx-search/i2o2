use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{io, mem, ptr};

use liburing_rs::*;

use crate::opcode::RingProbe;

macro_rules! check_err {
    ($status:expr) => {{
        if $status < 0 {
            Err(io::Error::from_raw_os_error(-$status))
        } else {
            Ok(())
        }
    }};
}

pub(super) struct IoRing {
    ring: io_uring,
    probe: RingProbe,
    closed: Arc<AtomicBool>,
}

impl IoRing {
    #[cfg(test)]
    pub(super) fn for_test(queue_size: u32) -> io::Result<Self> {
        Self::new(queue_size, unsafe { mem::zeroed() })
    }

    /// Creates a new ring using the given queue size and flags.
    pub(super) fn new(queue_size: u32, mut params: io_uring_params) -> io::Result<Self> {
        let probe = RingProbe::new()?;

        if !probe.is_kernel_v5_15_or_newer() {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "kernel version must be v5.15+",
            ));
        }

        probe.validate_ring_setup_flags(params.flags)?;

        let mut ring = unsafe { mem::zeroed::<io_uring>() };

        let result = unsafe {
            io_uring_queue_init_params(queue_size, &raw mut ring, &raw mut params)
        };
        check_err!(result)?;

        if probe.is_kernel_v5_18_or_newer() {
            let result = unsafe { io_uring_register_ring_fd(&raw mut ring) };
            check_err!(result)?;
        }

        Ok(Self {
            ring,
            probe,
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Creates a new [RingWaker] for the ring.
    pub(super) fn create_waker(&self) -> RingWaker {
        RingWaker {
            ring_fd: self.ring.ring_fd,
            closed: self.closed.clone(),
        }
    }

    /// Preallocate a sparse set of files.
    ///
    /// The number of files must be provided upfront.
    pub(super) fn register_files_sparse(&mut self, num_files: u32) -> io::Result<()> {
        let result = if self.probe.is_kernel_v5_19_or_newer() {
            unsafe { io_uring_register_files_sparse(&raw mut self.ring, num_files) }
        } else {
            let files = vec![-1; num_files as usize];
            let tags = vec![0; num_files as usize];
            unsafe {
                io_uring_register_files_tags(
                    &raw mut self.ring,
                    files.as_ptr(),
                    tags.as_ptr(),
                    num_files,
                )
            }
        };
        check_err!(result)
    }

    /// Register the provided file descriptor with the ring.
    pub(super) fn register_file(
        &mut self,
        slot: u32,
        fd: std::os::fd::RawFd,
        tag: u64,
    ) -> io::Result<()> {
        let fds = [fd];
        let tags = [tag];
        let result = unsafe {
            io_uring_register_files_update_tag(
                &raw mut self.ring,
                slot,
                fds.as_ptr(),
                tags.as_ptr(),
                1,
            )
        };
        check_err!(result)
    }

    /// Unregister a single file slot.
    pub(super) fn unregister_file(&mut self, slot: u32) -> io::Result<()> {
        let fds = [-1];
        let result = unsafe {
            io_uring_register_files_update(&raw mut self.ring, slot, fds.as_ptr(), 1)
        };
        check_err!(result)
    }

    /// Preallocate a sparse set of buffers.
    pub(super) fn register_buffers_sparse(
        &mut self,
        num_buffers: u32,
    ) -> io::Result<()> {
        let result =
            unsafe { io_uring_register_buffers_sparse(&raw mut self.ring, num_buffers) };
        check_err!(result)
    }

    /// Register the provided buffer with the ring.
    pub(super) fn register_buffer(
        &mut self,
        slot: u32,
        iovec: iovec,
        tag: u64,
    ) -> io::Result<()> {
        let buffers = [iovec];
        let tags = [tag];
        let result = unsafe {
            io_uring_register_buffers_update_tag(
                &raw mut self.ring,
                slot,
                buffers.as_ptr(),
                tags.as_ptr(),
                1,
            )
        };
        check_err!(result)
    }

    /// Retrieves the next available SQE in the ring which can be written to.
    ///
    /// Returns `None` if the queue is full.
    pub(super) fn get_available_sqe(&mut self) -> Option<&mut io_uring_sqe> {
        unsafe {
            let sqe = io_uring_get_sqe(&raw mut self.ring);
            sqe.as_mut()
        }
    }

    /// Iterates over the available CQEs in the queue.
    pub(super) fn iter_completions(&mut self) -> CqeIterator<'_> {
        let cqe = unsafe { io_uring_cqe_iter_init(&raw mut self.ring) };
        CqeIterator {
            ring: self,
            iter: cqe,
        }
    }

    /// Returns whether the ring has completions available.
    pub(super) fn has_completions_ready(&mut self) -> bool {
        unsafe {
            let mut cqe = ptr::null_mut::<io_uring_cqe>();
            io_uring_peek_cqe(&raw mut self.ring, &raw mut cqe) == 0
        }
    }

    /// Advances the CQEs seen in the queue.
    pub(self) fn advance_seen_cqe(&mut self, cqe: *mut io_uring_cqe) {
        unsafe { io_uring_cqe_seen(&raw mut self.ring, cqe) }
    }

    /// Submit any outstanding submissions to the kernel.
    pub(super) fn submit(&mut self) -> io::Result<usize> {
        let result = unsafe { io_uring_submit(&raw mut self.ring) };
        check_err!(result)?;
        Ok(result as usize)
    }

    /// Submit any outstanding submissions to the kernel and wait for at least
    /// 1 completion event to be ready.
    pub(super) fn submit_and_wait_one(&mut self) -> io::Result<()> {
        let result = unsafe { io_uring_submit_and_wait(&raw mut self.ring, 1) };
        check_err!(result)
    }
}

impl Drop for IoRing {
    fn drop(&mut self) {
        let is_closed = self.closed.load(Ordering::Acquire);
        if !is_closed {
            let result = unsafe { io_uring_close_ring_fd(&raw mut self.ring) };
            if result < 0 {
                let err = io::Error::from_raw_os_error(-result);
                tracing::error!(error = %err, "cannot shutdown ring");
            } else {
                self.closed.store(true, Ordering::Release);
            }
        }
    }
}

pub(super) struct CqeIterator<'ring> {
    ring: &'ring mut IoRing,
    iter: io_uring_cqe_iter,
}

impl<'ring> Iterator for CqeIterator<'ring> {
    type Item = CqeEntry;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            let mut cqe = ptr::null_mut::<io_uring_cqe>();
            let did_read = io_uring_cqe_iter_next(&raw mut self.iter, &raw mut cqe);
            if !did_read {
                None
            } else {
                let entry = CqeEntry {
                    result: (*cqe).res,
                    user_data: (*cqe).user_data,
                };
                self.ring.advance_seen_cqe(cqe);
                Some(entry)
            }
        }
    }
}

/// The ring waker allows threads outside the main scheduler thread
/// to wake up the scheduler by sending a message to the ring.
pub(super) struct RingWaker {
    ring_fd: libc::c_int,
    closed: Arc<AtomicBool>,
}

impl RingWaker {
    /// Signals whether the ring the waker belongs to is closed.
    pub(super) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Wake the ring.
    ///
    /// Returns `true` if the ring was successfully woken.
    pub(super) fn wake(&self, user_data: u64) -> bool {
        if !self.is_closed() {
            let result = unsafe {
                let mut sqe = mem::zeroed::<io_uring_sqe>();
                io_uring_prep_msg_ring(&raw mut sqe, self.ring_fd, 0x01, user_data, 0);
                io_uring_register_sync_msg(&raw mut sqe)
            };
            result >= 0
        } else {
            false
        }
    }
}

#[derive(Debug)]
/// The CQ entry for a IO op.
pub struct CqeEntry {
    /// The result of the syscall.
    pub result: i32,
    /// The user data tied to the OP when it was submitted.
    pub user_data: u64,
}

#[cfg(test)]
mod tests {
    use std::os::fd::AsRawFd;

    use super::*;
    use crate::opcode::sealed::RegisterOp;

    #[test]
    fn test_basic_ring_creation() {
        let _ring = IoRing::for_test(64).expect("ring should be created ok");
    }

    #[test]
    fn test_ring_submit_and_recv_no_op() {
        let mut ring = IoRing::for_test(8).expect("create ring");

        let sqe = ring.get_available_sqe().expect("sqe should be available");

        let op = crate::opcode::Nop::new();
        op.register_with_sqe(sqe);
        sqe.user_data = 123;

        ring.submit_and_wait_one().expect("submit should succeed");

        let mut iter = ring.iter_completions();
        let cqe = iter.next();
        assert!(cqe.is_some(), "expected completion entry to be available");

        let cqe = cqe.unwrap();
        let user_data = cqe.user_data;
        assert_eq!(user_data, 123);
        assert!(iter.next().is_none());
    }

    #[rstest::rstest]
    #[case::failpoint_kernel_v5_15_default_flags()]
    #[case::failpoint_kernel_v5_19_default_flags()]
    fn test_ring_register_files() {
        let scenario = fail::FailScenario::setup();
        let mut ring = IoRing::for_test(8).expect("create ring");

        ring.register_files_sparse(8)
            .expect("create sparse file array");

        let file = tempfile::tempfile().unwrap();
        ring.register_file(0, file.as_raw_fd(), 1)
            .expect("registering file should succeed");

        scenario.teardown();
    }

    #[test]
    fn test_ring_register_buffers() {
        let mut ring = IoRing::for_test(8).expect("create ring");

        ring.register_buffers_sparse(8)
            .expect("create sparse file array");

        let mut buffer = vec![0; 128];
        ring.register_buffer(
            0,
            iovec {
                iov_base: buffer.as_mut_ptr().cast(),
                iov_len: buffer.len() as _,
            },
            1,
        )
        .expect("registering file should succeed");

        drop(buffer);
    }

    #[rstest::rstest]
    #[case::default_flags(0)]
    #[should_panic]
    #[case::failpoint_kernel_v5_13_default_flags(0)]
    #[case::failpoint_kernel_v5_15_default_flags(0)]
    #[case::failpoint_kernel_v5_18_default_flags(0)]
    #[case::failpoint_kernel_v5_19_default_flags(0)]
    #[case::failpoint_kernel_v6_0_default_flags(0)]
    #[case::failpoint_kernel_v6_1_default_flags(0)]
    #[case::failpoint_kernel_v5_15_sqe_poll_flags(IORING_SETUP_SQPOLL)]
    #[case::failpoint_kernel_v5_18_sqe_poll_flags(IORING_SETUP_SQPOLL)]
    #[case::failpoint_kernel_v5_19_sqe_poll_flags(IORING_SETUP_SQPOLL)]
    #[case::failpoint_kernel_v6_0_sqe_poll_flags(IORING_SETUP_SQPOLL)]
    #[case::failpoint_kernel_v6_1_sqe_poll_flags(IORING_SETUP_SQPOLL)]
    #[should_panic]
    #[case::failpoint_kernel_v5_15_submit_all_flags(IORING_SETUP_SUBMIT_ALL)]
    #[case::failpoint_kernel_v5_18_submit_all_flags(IORING_SETUP_SUBMIT_ALL)]
    #[case::failpoint_kernel_v5_19_submit_all_flags(IORING_SETUP_SUBMIT_ALL)]
    #[case::failpoint_kernel_v6_0_submit_all_flags(IORING_SETUP_SUBMIT_ALL)]
    #[case::failpoint_kernel_v6_1_submit_all_flags(IORING_SETUP_SUBMIT_ALL)]
    #[should_panic]
    #[case::failpoint_kernel_v5_15_coop_taskrun_flags(IORING_SETUP_COOP_TASKRUN)]
    #[should_panic]
    #[case::failpoint_kernel_v5_18_coop_taskrun_flags(IORING_SETUP_COOP_TASKRUN)]
    #[case::failpoint_kernel_v5_19_coop_taskrun_flags(IORING_SETUP_COOP_TASKRUN)]
    #[case::failpoint_kernel_v6_0_coop_taskrun_flags(IORING_SETUP_COOP_TASKRUN)]
    #[case::failpoint_kernel_v6_1_coop_taskrun_flags(IORING_SETUP_COOP_TASKRUN)]
    #[should_panic]
    #[case::failpoint_kernel_v5_15_single_issuer_flags(IORING_SETUP_SINGLE_ISSUER)]
    #[should_panic]
    #[case::failpoint_kernel_v5_18_single_issuer_flags(IORING_SETUP_SINGLE_ISSUER)]
    #[should_panic]
    #[case::failpoint_kernel_v5_19_single_issuer_flags(IORING_SETUP_SINGLE_ISSUER)]
    #[case::failpoint_kernel_v6_0_single_issuer_flags(IORING_SETUP_SINGLE_ISSUER)]
    #[case::failpoint_kernel_v6_1_single_issuer_flags(IORING_SETUP_SINGLE_ISSUER)]
    #[should_panic]
    #[case::failpoint_kernel_v5_15_defer_taskrun_flags(IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN)]
    #[should_panic]
    #[case::failpoint_kernel_v5_18_defer_taskrun_flags(IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN)]
    #[should_panic]
    #[case::failpoint_kernel_v5_19_defer_taskrun_flags(IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN)]
    #[should_panic]
    #[case::failpoint_kernel_v6_0_defer_taskrun_flags(IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN)]
    #[case::failpoint_kernel_v6_1_defer_taskrun_flags(IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN)]
    fn test_ring_setup_flags(#[case] flags: u32) {
        let scenario = fail::FailScenario::setup();

        let mut params: io_uring_params = unsafe { std::mem::zeroed() };
        params.flags = flags;
        let _ring = IoRing::new(8, params).expect("create ring");
        scenario.teardown();
    }
}
