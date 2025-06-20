use std::mem::MaybeUninit;
use std::ptr;
use std::sync::Arc;

/// The [InflightInventory] tracks state currently attached to ops being executed by the ring.
///
/// It allows for a submitter thread to add state and re-use a consistent buffer of memory for state.
///
/// The inventory can hold upto a given number of `T` values, if the submitter attempts to push
/// another entry beyond this size, it will block and wait for an entry to become free
/// before returning allowing for backpressure between the consumer and submitter.
pub(super) struct InflightInventory<T> {
    /// The backing heap that stores N free ptrs.
    buffer: Box<[MaybeUninit<T>]>,
    /// An atomic queue of free ptrs which can be written to.
    ///
    /// The ptrs are owned by `buffer`.
    free_ptrs: crossbeam_queue::ArrayQueue<SendPtrWrapper<MaybeUninit<T>>>,
    waiter: parking_lot::Mutex<()>,
    condvar: parking_lot::Condvar,
}

impl<T> InflightInventory<T> {
    /// Create a new [InflightInventory] with a given size.
    pub(super) fn new(size: usize) -> Arc<Self> {
        let mut buffer = Box::new_uninit_slice(size);
        let buffer_ptr = buffer.as_mut_ptr();

        let free_ptrs = crossbeam_queue::ArrayQueue::new(size);
        for i in 0..size {
            let free_ptr = unsafe { buffer_ptr.add(i) };
            if free_ptrs.push(SendPtrWrapper(free_ptr)).is_err() {
                panic!("queue is full despite being in the process of setup, this should never happen");
            }
        }

        Arc::new(Self {
            buffer,
            free_ptrs,
            waiter: parking_lot::Mutex::new(()),
            condvar: parking_lot::Condvar::new(),
        })
    }

    /// Write a given `value` to the next available slot in the inventory.
    ///
    /// If the inventory is full, this will block until a slot is available.
    pub(super) fn write_to_free_ptr(&self, value: T) -> *mut T {
        let free_ptr = match self.try_pop_free_ptr_spin() {
            Some(slot) => slot,
            None => self.wait_for_free_ptr(),
        };

        #[cfg(feature = "trace-hotpath")]
        tracing::trace!("got pointer to write value");

        unsafe { free_ptr.write(MaybeUninit::new(value)) };

        // We've just written to the pointer, we can cast it safely to an
        // initialised value.
        free_ptr as *mut T
    }

    /// Push a pointer allocated by the inventory buffer back into the inventory
    /// allowing future tasks to write to it and reuse the allocation.
    pub(super) fn push_free_ptr(&self, ptr: *mut T) {
        // Drop the value the ptr holds so it is ready to be overwritten.
        unsafe { ptr::drop_in_place(ptr) };
        let _ = self.free_ptrs.push(SendPtrWrapper(ptr as *mut MaybeUninit<T>));
        self.wake_if_needed();

        #[cfg(feature = "trace-hotpath")]
        tracing::trace!("pointer has been marked as free for inventory");
    }

    fn try_pop_free_ptr_spin(&self) -> Option<*mut MaybeUninit<T>> {
        if let Some(ptr) = self.free_ptrs.pop() {
            return Some(ptr.0);
        }

        for _ in 0..50 {
            if let Some(ptr) = self.free_ptrs.pop() {
                return Some(ptr.0);
            }
        }

        None
    }

    fn wait_for_free_ptr(&self) -> *mut MaybeUninit<T> {
        #[cfg(feature = "trace-hotpath")]
        tracing::trace!("waiting for wakeup as no free poiters available");

        loop {
            let mut lock = self.waiter.lock();

            if let Some(ptr) = self.free_ptrs.pop() {
                return ptr.0;
            }

            self.condvar.wait(&mut lock);

            if let Some(ptr) = self.free_ptrs.pop() {
                return ptr.0;
            }
        }
    }

    fn wake_if_needed(&self) -> bool {
        let did_wake = self.condvar.notify_one();
        #[cfg(feature = "trace-hotpath")]
        tracing::trace!(did_wake = did_wake, "I attempted to wake an outstanding task if applicable");       
        did_wake
    }
}

struct SendPtrWrapper<T>(*mut T);

unsafe impl<T: Send> Send for SendPtrWrapper<T> {}


#[cfg(test)]
mod tests {
    use std::time::Duration;
    use super::*;

    #[test]
    fn test_inventory_non_blocking() {
        let inventory = InflightInventory::new(2);
        assert_eq!(inventory.free_ptrs.len(), 2);

        let ptr1 = inventory.write_to_free_ptr(String::from("Hello, world!"));
        unsafe { assert_eq!((*ptr1).as_str(), "Hello, world!"); };

        let ptr2 = inventory.write_to_free_ptr(String::from("Hello, world 2"));
        unsafe { assert_eq!((*ptr2).as_str(), "Hello, world 2"); };
        assert_eq!(inventory.free_ptrs.len(), 0);

        inventory.push_free_ptr(ptr1);
        assert_eq!(inventory.free_ptrs.len(), 1);

        let ptr3 = inventory.write_to_free_ptr(String::from("Hello, world 3"));
        assert!(ptr::addr_eq(ptr1, ptr3));
        assert_eq!(inventory.free_ptrs.len(), 0);
    }

    #[test]
    fn test_inventory_blocking() {
        let _ = tracing_subscriber::fmt::try_init();

        let inventory = InflightInventory::new(2);
        assert_eq!(inventory.free_ptrs.len(), 2);

        let ptr1 = inventory.write_to_free_ptr(String::from("Hello, world!"));
        unsafe { assert_eq!((*ptr1).as_str(), "Hello, world!"); };

        let ptr2 = inventory.write_to_free_ptr(String::from("Hello, world 2"));
        unsafe { assert_eq!((*ptr2).as_str(), "Hello, world 2"); };
        assert_eq!(inventory.free_ptrs.len(), 0);

        let inventory_copy = inventory.clone();
        let handle = std::thread::spawn(move || {
            let ptr = inventory_copy.write_to_free_ptr(String::from("Hello, world 3"));
            SendPtrWrapper(ptr)
        });

        std::thread::sleep(Duration::from_millis(10));
        assert!(!handle.is_finished());

        inventory.push_free_ptr(ptr1);

        let ptr3 = handle.join().unwrap().0;
        assert!(ptr::addr_eq(ptr1, ptr3));
        assert_eq!(inventory.free_ptrs.len(), 0);
    }
}
