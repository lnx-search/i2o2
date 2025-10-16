use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::flags;
use crate::flags::Flag;
use crate::ring::RingWaker;

/// Create a new waker using the provided [RingWaker].
pub(super) fn new(ring_waker: RingWaker) -> Waker {
    let inner = WakerInner::new(ring_waker);
    Waker(Arc::new(inner))
}

#[derive(Clone)]
/// The waker allows external threads to wake the scheduler when it is waiting
/// for CQEs to complete.
///
/// This is implemented using ring messaging which is much more efficient than
/// eventfd.
pub(super) struct Waker(Arc<WakerInner>);

impl Waker {
    /// Sets the waker flag to ask future tasks to wake the scheduler
    /// when they have submitted work to it.
    pub(super) fn ask_for_wake(&self) {
        self.0.ask_for_wake();
    }

    /// Wake the scheduler if it has asked to be woken.
    pub(super) fn maybe_wake(&self) -> bool {
        self.0.maybe_wake()
    }
}

impl Drop for Waker {
    fn drop(&mut self) {
        // We always have 1 waker held by the scheduler itself, so we set this to 1.
        if Arc::strong_count(&self.0) <= 1 {
            self.maybe_wake();
        }
    }
}

struct WakerInner {
    ring_waker: RingWaker,
    wants_wake: AtomicBool,
}

impl WakerInner {
    fn new(ring_waker: RingWaker) -> Self {
        Self {
            ring_waker,
            wants_wake: AtomicBool::new(false),
        }
    }

    fn ask_for_wake(&self) {
        #[cfg(feature = "trace-hotpath")]
        tracing::trace!("asking for wake");
        self.wants_wake.store(true, Ordering::Release);
    }

    fn maybe_wake(&self) -> bool {
        let wants_wake = self.wants_wake.swap(false, Ordering::AcqRel);
        #[cfg(feature = "trace-hotpath")]
        tracing::trace!(wants_wake = wants_wake, "waking ring");
        if wants_wake {
            let user_data = flags::pack(Flag::Wake, 0, 0);
            self.ring_waker.wake(user_data)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_wake_behaviour() {
        let ring = crate::ring::IoRing::for_test(10).unwrap();
        let ring_waker = ring.create_waker();
        let waker = super::new(ring_waker);

        let did_wake = waker.maybe_wake();
        assert!(!did_wake);

        waker.ask_for_wake();

        let did_wake = waker.maybe_wake();
        assert!(did_wake);
        let did_wake = waker.maybe_wake();
        assert!(!did_wake);
    }
}
