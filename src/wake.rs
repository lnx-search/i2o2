use std::io;
use std::os::fd::RawFd;
use std::sync::Arc;

use io_uring::{SubmissionQueue, opcode, types};

use super::flags;

/// An EventFD based waker for the io_uring ring.
pub(super) struct RingWaker {
    event_fd: RawFd,
    counter: u64,
    waker: std::task::Waker,
    is_set: bool,
}

impl RingWaker {
    /// Creates a new waker for the ring.
    pub(super) fn new() -> io::Result<Self> {
        let event_fd = unsafe { libc::eventfd(0, 0) };
        if event_fd < 0 {
            return Err(io::Error::last_os_error());
        }

        let inner = Arc::new(EventFdWaker {
            event_fd,
            is_closed: false,
        });
        let waker = std::task::Waker::from(inner);

        Ok(Self {
            event_fd,
            counter: 0,
            waker,
            is_set: false,
        })
    }

    pub(super) fn mark_unset(&mut self) {
        #[cfg(feature = "trace-hotpath")]
        tracing::trace!("waker has been unset");
        self.is_set = false;
    }

    pub(super) fn maybe_submit_self<E: io_uring::squeue::EntryMarker>(
        &mut self,
        submission: &mut SubmissionQueue<'_, E>,
    ) -> bool {
        if self.is_set {
            #[cfg(feature = "trace-hotpath")]
            tracing::trace!("waker already registered");
            return true;
        }

        let entry = opcode::Read::new(
            types::Fd(self.event_fd),
            (&mut self.counter) as *mut u64 as *mut _,
            size_of::<u64>() as u32,
        )
        .build()
        .user_data(flags::pack(flags::Flag::EventFdWaker, 0, 0))
        .into();
        self.is_set = unsafe { submission.push(&entry).is_ok() };

        #[cfg(feature = "trace-hotpath")]
        tracing::trace!(is_set = self.is_set, "submitting self waker to queue");

        self.is_set
    }

    pub(super) fn task_waker(&self) -> std::task::Waker {
        self.waker.clone()
    }

    /// Creates a new [std::task::Context] with a waker for the current ring.
    pub(super) fn context(&self) -> std::task::Context {
        std::task::Context::from_waker(&self.waker)
    }
}

struct EventFdWaker {
    event_fd: RawFd,
    is_closed: bool,
}

impl EventFdWaker {
    fn wake_inner(&self) {
        let result = unsafe { libc::eventfd_write(self.event_fd, 1) };
        assert_ne!(result, -1);
    }
}

impl std::task::Wake for EventFdWaker {
    fn wake(self: Arc<Self>) {
        self.wake_inner()
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wake_inner()
    }
}

impl Drop for EventFdWaker {
    fn drop(&mut self) {
        // We can close the fd on drop here because this inner type
        // is only dropped once all references to the waker are dropped, including
        // the ring.
        if !self.is_closed {
            let _ = unsafe { libc::close(self.event_fd) };
            self.is_closed = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wake_behaviour() {
        let ring_waker = RingWaker::new().expect("create eventfd");

        let task_waker = ring_waker.task_waker();
        task_waker.wake();
        let mut result: u64 = 0;
        unsafe { libc::eventfd_read(ring_waker.event_fd, (&mut result) as *mut _) };
        assert_eq!(result, 1);

        let task_waker = ring_waker.task_waker();
        task_waker.wake_by_ref();
        let mut result: u64 = 0;
        unsafe { libc::eventfd_read(ring_waker.event_fd, (&mut result) as *mut _) };
        assert_eq!(result, 1);
    }
}
