use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) struct RawEventFd {
    fd: libc::c_int,
    is_closed: bool,
}

impl RawEventFd {
    pub(super) fn new() -> io::Result<Self> {
        let fd = unsafe { libc::eventfd(0, 0) };
        if fd < 0 {
            return Err(io::Error::from_raw_os_error(-fd));
        }

        Ok(Self {
            fd,
            is_closed: false,
        })
    }

    pub(super) fn wake(&self) {
        unsafe { libc::eventfd_write(self.fd, 1) };
    }

    pub(super) fn wait_for_events(&self) -> u64 {
        let mut value = 0;
        unsafe { libc::eventfd_read(self.fd, &raw mut value) };
        value
    }
}

impl Drop for RawEventFd {
    fn drop(&mut self) {
        if !self.is_closed {
            unsafe { libc::close(self.fd) };
            self.is_closed = true;
        }
    }
}

/// Create a new waker pair.
pub(super) fn new() -> io::Result<(RingWaker, RingWakerController)> {
    let inner = WakerInner::new()?;

    let waker_ref = Arc::new(inner);
    let guard = Arc::new(waker_ref.clone().wake_on_drop());

    let waker = RingWaker {
        inner: waker_ref.clone(),
        _guard: guard,
    };

    let controller = RingWakerController {
        is_set: false,
        inner: waker_ref,
    };

    Ok((waker, controller))
}

#[derive(Clone)]
/// The ring waker notifies the scheduler that there is outstanding operations
/// to submit to the submission queue ring.
pub(super) struct RingWaker {
    /// The guard ensures that at least one event is triggered when all
    /// handles are dropped. This is to ensure the scheduler is aware and
    /// shuts down gracefully.
    _guard: Arc<WakeOnDrop>,
    inner: Arc<WakerInner>,
}

impl RingWaker {
    pub(super) fn maybe_wake(&self) {
        let waker = self.inner.as_ref();

        let wants_wake = waker.wants_wake.load(Ordering::SeqCst);

        #[cfg(feature = "trace-hotpath")]
        tracing::trace!(wants_wake, "checking if i need to wake the scheduler");

        if wants_wake {
            if let Some(_guard) = waker.guard.try_lock() {
                waker.wake();
            }
        }
    }
}

/// A waker that automatically wakes the ring on drop.
pub(super) struct WakeOnDrop {
    inner: Arc<WakerInner>,
}

impl Drop for WakeOnDrop {
    fn drop(&mut self) {
        #[cfg(feature = "trace-hotpath")]
        tracing::debug!("waking from WakeOnDrop drop");
        self.inner.wake();
    }
}

pub(super) struct RingWakerController {
    is_set: bool,
    inner: Arc<WakerInner>,
}

impl RingWakerController {
    pub(super) fn fd(&self) -> libc::c_int {
        self.inner.raw_waker.fd
    }

    pub(super) fn make_wake_on_drop(&self) -> WakeOnDrop {
        self.inner.clone().wake_on_drop()
    }

    pub(super) fn ask_for_wake(&mut self) {
        self.is_set = true;
        let waker = self.inner.as_ref();
        waker.wants_wake.store(true, Ordering::SeqCst);
    }

    pub(super) fn mark_unset(&mut self) {
        self.is_set = false;
        let waker = self.inner.as_ref();
        waker.wants_wake.store(false, Ordering::SeqCst);
    }

    pub(super) fn wait_for_events(&mut self) {
        #[cfg(feature = "trace-hotpath")]
        tracing::trace!("waiting for event fd");

        self.inner.raw_waker.wait_for_events();
        self.mark_unset();

        #[cfg(feature = "trace-hotpath")]
        tracing::trace!("task was woken");
    }
}

/// The waker guard wraps an eventfd handle and gracefully
/// closes the eventfd when it is dropped.
struct WakerInner {
    raw_waker: RawEventFd,
    wants_wake: AtomicBool,
    guard: parking_lot::Mutex<()>,
}

impl WakerInner {
    fn new() -> io::Result<Self> {
        let raw_waker = RawEventFd::new()?;

        Ok(Self {
            raw_waker,
            wants_wake: AtomicBool::new(false),
            guard: parking_lot::Mutex::new(()),
        })
    }

    pub(super) fn wake_on_drop(self: Arc<Self>) -> WakeOnDrop {
        WakeOnDrop { inner: self }
    }

    pub(super) fn wake(&self) {
        #[cfg(feature = "trace-hotpath")]
        tracing::trace!("waking");
        self.raw_waker.wake();
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_wake_behaviour() {
        let (waker, mut controller) = super::new().expect("create eventfd");

        // Used to prevent the eventfd blocking
        waker.inner.wake();

        // We shouldn't wake because the controller hasn't asked for it.
        waker.maybe_wake();

        let value = waker.inner.raw_waker.wait_for_events();
        assert_eq!(value, 1);

        controller.ask_for_wake();

        // We should wake now since the controller has asked for it.
        waker.maybe_wake();

        let value = waker.inner.raw_waker.wait_for_events();
        assert_eq!(value, 1);
    }

    #[test]
    fn test_wake_on_drop() {
        let (waker, _controller) = super::new().expect("create eventfd");
        let wake_on_drop = waker.inner.clone().wake_on_drop();
        drop(wake_on_drop);

        let value = waker.inner.raw_waker.wait_for_events();
        assert_eq!(value, 1);
    }
}
