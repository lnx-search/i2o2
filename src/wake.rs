use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use io_uring::{opcode, types};

use crate::flags;
use crate::mode::SQEntryOptions;

/// Create a new waker pair.
pub(super) fn new() -> io::Result<(RingWaker, RingWakerController)> {
    let inner = WakerInner::new()?;

    let guard = Arc::new(WakerGuard(inner.fd));
    let waker_ref = Arc::new(inner);

    let waker = RingWaker {
        inner: waker_ref.clone(),
        _guard: guard,
    };

    let controller = RingWakerController {
        value: 0,
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
    _guard: Arc<WakerGuard>,
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

pub(super) struct RingWakerController {
    value: u64,
    is_set: bool,
    inner: Arc<WakerInner>,
}

impl RingWakerController {
    pub(super) fn is_set(&self) -> bool {
        self.is_set
    }

    pub(super) fn mark_set(&mut self) {
        self.is_set = true;
        let waker = self.inner.as_ref();
        waker.wants_wake.store(true, Ordering::SeqCst);
    }

    pub(super) fn mark_unset(&mut self) {
        self.is_set = false;
        let waker = self.inner.as_ref();
        waker.wants_wake.store(false, Ordering::SeqCst);
    }

    pub(super) fn get_sqe_if_needed<E: SQEntryOptions>(&mut self) -> Option<E> {
        if self.is_set {
            return None;
        }

        let waker = self.inner.as_ref();
        let entry = opcode::Read::new(
            types::Fd(waker.fd),
            (&mut self.value) as *mut u64 as *mut _,
            8,
        )
        .build()
        .user_data(flags::pack(flags::Flag::EventFdWaker, 0, 0))
        .into();

        Some(entry)
    }
}

/// The waker guard wraps an eventfd handle and gracefully
/// closes the eventfd when it is dropped.
struct WakerInner {
    fd: libc::c_int,
    wants_wake: AtomicBool,
    guard: parking_lot::Mutex<()>,
    is_closed: bool,
}

impl WakerInner {
    fn new() -> io::Result<Self> {
        let fd = unsafe { libc::eventfd(0, 0) };
        if fd < 0 {
            return Err(io::Error::from_raw_os_error(-fd));
        }

        Ok(Self {
            fd,
            wants_wake: AtomicBool::new(false),
            guard: parking_lot::Mutex::new(()),
            is_closed: false,
        })
    }

    pub(super) fn wake(&self) {
        #[cfg(feature = "trace-hotpath")]
        tracing::trace!("waking");
        unsafe {
            libc::eventfd_write(self.fd, 1);
        }
    }
}

impl Drop for WakerInner {
    fn drop(&mut self) {
        if self.is_closed {
            self.wake();
            unsafe {
                let _ = libc::close(self.fd);
            };
            self.is_closed = true;
        }
    }
}

/// The waker guard ensures that at least one event is triggered
/// when all [RingWaker]s are dropped.
struct WakerGuard(libc::c_int);

impl Drop for WakerGuard {
    fn drop(&mut self) {
        unsafe {
            let _ = libc::eventfd_write(self.0, 1);
        };
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_wake_behaviour() {
        let (waker, mut controller) = super::new().expect("create eventfd");

        // Used to prevent the eventfd blocking
        unsafe {
            libc::eventfd_write(controller.inner.fd, 1);
        };

        // We shouldn't wake because the controller hasn't asked for it.
        waker.maybe_wake();

        let mut value: u64 = 0;
        unsafe {
            libc::eventfd_read(controller.inner.fd, (&mut value) as *mut _);
        }
        assert_eq!(value, 1);

        controller.mark_set();

        // We should wake now since the controller has asked for it.
        waker.maybe_wake();

        let mut value: u64 = 0;
        unsafe {
            libc::eventfd_read(controller.inner.fd, (&mut value) as *mut _);
        }
        assert_eq!(value, 1);
    }
}
