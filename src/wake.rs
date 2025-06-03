use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use io_uring::{opcode, types};

use crate::flags;
use crate::mode::SQEntryOptions;

/// Create a new waker pair.
pub(super) fn new() -> io::Result<(RingWaker, RingWakerController)> {
    let guard = WakerGuard::new()?;
    let waker_ref = Arc::new(guard);

    let waker = RingWaker {
        waker_ref: waker_ref.clone(),
    };

    let controller = RingWakerController {
        value: 0,
        is_set: false,
        waker_ref,
    };

    Ok((waker, controller))
}

#[derive(Clone)]
/// The ring waker notifies the scheduler that there is outstanding operations
/// to submit to the submission queue ring.
pub(super) struct RingWaker {
    waker_ref: Arc<WakerGuard>,
}

impl RingWaker {
    pub(super) fn maybe_wake(&self) {
        let waker = self.waker_ref.as_ref();

        let wants_wake = waker.wants_wake.load(Ordering::SeqCst);
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
    waker_ref: Arc<WakerGuard>,
}

impl RingWakerController {
    pub(super) fn mark_set(&mut self) {
        self.is_set = true;
        let waker = self.waker_ref.as_ref();
        waker.wants_wake.store(true, Ordering::SeqCst);
    }

    pub(super) fn mark_unset(&mut self) {
        self.is_set = false;
        let waker = self.waker_ref.as_ref();
        waker.wants_wake.store(false, Ordering::SeqCst);
    }

    pub(super) fn get_sqe_if_needed<E: SQEntryOptions>(&mut self) -> Option<E> {
        if self.is_set {
            return None;
        }

        let waker = self.waker_ref.as_ref();
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
struct WakerGuard {
    fd: libc::c_int,
    wants_wake: AtomicBool,
    guard: parking_lot::Mutex<()>,
    is_closed: bool,
}

impl WakerGuard {
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
        unsafe {
            libc::eventfd_write(self.fd, 1);
        }
    }
}

impl Drop for WakerGuard {
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

#[cfg(test)]
mod tests {

    #[test]
    fn test_wake_behaviour() {
        let (waker, mut controller) = super::new().expect("create eventfd");

        // Used to prevent the eventfd blocking
        unsafe {
            libc::eventfd_write(controller.waker_ref.fd, 1);
        };

        // We shouldn't wake because the controller hasn't asked for it.
        waker.maybe_wake();

        let mut value: u64 = 0;
        unsafe {
            libc::eventfd_read(controller.waker_ref.fd, (&mut value) as *mut _);
        }
        assert_eq!(value, 1);

        controller.mark_set();

        // We should wake now since the controller has asked for it.
        waker.maybe_wake();

        let mut value: u64 = 0;
        unsafe {
            libc::eventfd_read(controller.waker_ref.fd, (&mut value) as *mut _);
        }
        assert_eq!(value, 1);
    }
}
