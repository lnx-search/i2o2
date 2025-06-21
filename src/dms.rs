use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Default, Debug)]
/// A dead man's switch automatically sets itself to `set` if one of its handles is dropped.
///
/// Alternatively it can be explicitly set.
pub struct DeadMansSwitch {
    inner: Arc<AtomicBool>,
    disabled: bool,
}

impl DeadMansSwitch {
    /// Is the switch set or not.
    pub fn is_set(&self) -> bool {
        self.inner.load(Ordering::Relaxed)
    }

    /// Set the switch explicitly.
    pub fn set(&self) {
        self.inner.store(true, Ordering::Relaxed);
    }

    /// Turn the current [DeadMansSwitch] into a [WeakDeadMansSwitch]
    /// de-activating the set-on-drop nature of thw switch.
    pub fn into_weak(mut self) -> WeakDeadMansSwitch {
        self.disabled = true;
        WeakDeadMansSwitch {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for DeadMansSwitch {
    fn drop(&mut self) {
        if !self.disabled {
            self.set();
        }
    }
}

#[derive(Debug, Clone)]
/// A handle that can read and set the dead man's switch but does not set automatically
/// on drop.
pub struct WeakDeadMansSwitch {
    inner: Arc<AtomicBool>,
}

impl WeakDeadMansSwitch {
    /// Is the switch set or not.
    pub fn is_set(&self) -> bool {
        self.inner.load(Ordering::Relaxed)
    }

    /// Set the switch explicitly.
    pub fn set(&self) {
        self.inner.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dead_mans_switch() {
        let switch = DeadMansSwitch::default();
        assert!(!switch.is_set());

        let handle = switch.clone();
        drop(handle);

        assert!(switch.is_set());
    }
    #[test]
    fn test_weak_dead_mans_switch() {
        let switch = DeadMansSwitch::default();
        assert!(!switch.is_set());

        let handle = switch.clone().into_weak();
        assert!(!handle.is_set());
        drop(handle);

        assert!(!switch.is_set());
    }

    #[test]
    fn test_weak_dead_mans_switch_explicit() {
        let switch = DeadMansSwitch::default();
        assert!(!switch.is_set());

        let handle = switch.clone().into_weak();
        assert!(!handle.is_set());
        handle.set();
        assert!(switch.is_set());
    }
}
