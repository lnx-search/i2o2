use std::io;
use std::os::fd::RawFd;
use std::sync::Arc;

/// An EventFD based waker for the io_uring ring.
pub(super) struct RingWaker {
    event_fd: RawFd,
    waker: std::task::Waker,
}

impl RingWaker {
    /// Creates a new waker for the ring.
    pub(super) fn new() -> io::Result<Self> {
        let event_fd = unsafe { libc::eventfd(0, 0) };
        if event_fd == -1 {
            return Err(io::Error::last_os_error());
        }

        let inner = Arc::new(EventFdWaker {
            event_fd,
            is_closed: false,
        });
        let waker = std::task::Waker::from(inner);

        Ok(Self { event_fd, waker })
    }

    pub(super) fn event_fd(&self) -> RawFd {
        self.event_fd
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
