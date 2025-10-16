use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use crate::flags;
use crate::flags::Flag;
use crate::ring::RingWaker;

/// Create a new waker using the provided [RingWaker].
pub(super) fn new(ring_waker: RingWaker) -> Waker {
    let inner = WakerInner::new(ring_waker);
    Waker(Arc::new(inner))
}

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

    /// Signal to the scheduler that work was added.
    ///
    /// Returns whether it woke the scheduler from being parked.
    pub(super) fn signal_work_added(&self) -> bool {
        self.0.increment_work_counter();
        self.0.maybe_wake()
    }

    /// Load the current work counter to check if there is
    /// work to process.
    pub(super) fn current_work_counter(&self) -> u64 {
        self.0.current_work_counter()
    }
}

impl Clone for Waker {
    fn clone(&self) -> Self {
        self.0.ref_count.fetch_add(1, Ordering::Release);
        Self(self.0.clone())
    }
}

impl Drop for Waker {
    fn drop(&mut self) {
        // We always have 1 waker held by the scheduler itself, so we set this to 2.
        // in order to account for the value we just subbed.
        let ref_count = self.0.ref_count.fetch_sub(1, Ordering::AcqRel);
        if ref_count <= 2 {
            self.signal_work_added();
        }
    }
}

struct WakerInner {
    ring_waker: RingWaker,
    wants_wake: AtomicBool,
    work_counter: AtomicU64,
    ref_count: AtomicU32,
}

impl WakerInner {
    fn new(ring_waker: RingWaker) -> Self {
        Self {
            ring_waker,
            wants_wake: AtomicBool::new(false),
            work_counter: AtomicU64::new(0),
            ref_count: AtomicU32::new(1),
        }
    }

    fn increment_work_counter(&self) {
        self.work_counter.fetch_add(1, Ordering::Release);
    }

    fn current_work_counter(&self) -> u64 {
        self.work_counter.load(Ordering::Acquire)
    }

    fn ask_for_wake(&self) {
        #[cfg(feature = "trace-hotpath")]
        tracing::trace!("scheduler: asking for wake");
        self.wants_wake.store(true, Ordering::SeqCst);
    }

    fn maybe_wake(&self) -> bool {
        let wants_wake = self.wants_wake.swap(false, Ordering::SeqCst);
        #[cfg(feature = "trace-hotpath")]
        tracing::trace!(wants_wake = wants_wake, "waking ring if it wants");
        if wants_wake {
            #[cfg(feature = "trace-hotpath")]
            tracing::trace!("scheduler has asked us to wake them up");
            let user_data = flags::pack(Flag::Wake, 0, 0);
            self.ring_waker.wake(user_data)
        } else {
            #[cfg(feature = "trace-hotpath")]
            tracing::trace!("scheduler has not asked us to wake them");
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

        let did_wake = waker.signal_work_added();
        assert!(!did_wake);

        waker.ask_for_wake();

        let did_wake = waker.signal_work_added();
        assert!(did_wake);
        let did_wake = waker.signal_work_added();
        assert!(!did_wake);
    }
}
