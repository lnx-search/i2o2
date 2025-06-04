use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Creates a new bounded queue.
pub fn new<T>(size: usize) -> (SchedulerSender<T>, SchedulerReceiver<T>) {
    let inner = Arc::new(Inner::new(size));
    
    let tx = SchedulerSender {
        inner: inner.clone(),  
    };
    
    let rx = SchedulerReceiver {
        inner
    };

    (tx, rx)    
}

/// The scheduler queue is an abstraction over a FIFO concurrent queue.
/// 
/// This is the sender half of the queue.
pub struct SchedulerSender<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Clone for SchedulerSender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> SchedulerSender<T> {
    /// Is the sender disconnected from the receiver.
    ///
    /// This is a bit of an odd thing to have for a queue where
    /// it can both send _and_ receive, but from our POV,
    /// a queue with only 1 instance is a disconnected queue.
    pub fn is_disconnected(&self) -> bool {
        self.inner.receiver_disconnected.load(Ordering::Acquire)
    }
    
    /// Send a new message to the scheduler.
    pub fn send(&self, value: T) {
        if self.inner.queue.push(value).is_err() {
            panic!("we ran out of space? {}", self.inner.queue.len());
        }
    }
    
    /// Returns if the queue holds no items.
    pub fn is_empty(&self) -> bool {
        self.inner.queue.is_empty()
    }
}


/// The scheduler queue is an abstraction over a FIFO concurrent queue.
///
/// This is the receiver half of the queue.
pub struct SchedulerReceiver<T> {
    inner: Arc<Inner<T>>,
}

impl<T> SchedulerReceiver<T> {
    /// Pop an item from the queue.
    pub fn pop(&self) -> Option<T> {
        self.inner.queue.pop()
    }

    /// Returns if the queue holds no items.
    pub fn is_empty(&self) -> bool {
        self.inner.queue.is_empty()
    }
    
    #[cfg(feature = "trace-hotpath")]
    /// Returns the number of elements in the queue
    pub fn len(&self) -> usize {
        self.inner.queue.len()
    }
    
    /// Is the receiver disconnected from the sender.
    ///
    /// This is a bit of an odd thing to have for a queue where
    /// it can both send _and_ receive, but from our POV,
    /// a queue with only 1 instance is a disconnected queue.
    pub fn is_disconnected(&self) -> bool {
        Arc::strong_count(&self.inner) == 1
    }
}

impl<T> Drop for SchedulerReceiver<T> {
    fn drop(&mut self) {
        self.inner.receiver_disconnected.store(true, Ordering::Release);
    }
}


struct Inner<T> {
    // TODO: make bounded.
    queue: crossbeam_queue::ArrayQueue<T>,
    receiver_disconnected: AtomicBool,
}

impl<T> Inner<T> {
    fn new(_size: usize) -> Self {
        Self {
            queue: crossbeam_queue::ArrayQueue::new(1024),
            receiver_disconnected: AtomicBool::new(false),
        }
    }
}