use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Creates a new bounded queue.
pub fn new<T>(size: usize) -> (SchedulerSender<T>, SchedulerReceiver<T>) {
    let inner = Arc::new(Inner::new(size));

    let tx = SchedulerSender {
        inner: inner.clone(),
    };

    let rx = SchedulerReceiver { inner };

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
    /// Send a new message to the scheduler.
    pub fn send(&self, mut value: T) -> Result<(), T> {
        match self.try_send_spin(value) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Disconnected(value)) => return Err(value),
            Err(TrySendError::Full(v)) => {
                value = v;
            },
        }

        futures_executor::block_on(self.send_async(value))
    }

    /// Send a new message to the scheduler.
    pub async fn send_async(&self, mut value: T) -> Result<(), T> {
        match self.try_send_spin(value) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Disconnected(value)) => return Err(value),
            Err(TrySendError::Full(v)) => {
                value = v;
            },
        }

        let notify_fut = self.inner.notify.notified();
        tokio::pin!(notify_fut);

        loop {
            notify_fut.as_mut().enable();

            match self.inner.try_send(value) {
                Err(TrySendError::Disconnected(value)) => return Err(value),
                Err(TrySendError::Full(v)) => {
                    value = v;
                    notify_fut.as_mut().await;
                    notify_fut.set(self.inner.notify.notified());
                },
                Ok(()) => return Ok(()),
            }
        }
    }

    fn try_send_spin(&self, mut value: T) -> Result<(), TrySendError<T>> {
        match self.inner.try_send(value) {
            Err(TrySendError::Disconnected(value)) => {
                return Err(TrySendError::Disconnected(value));
            },
            Err(TrySendError::Full(v)) => {
                value = v;
            },
            Ok(()) => return Ok(()),
        }

        for _ in 0..5 {
            match self.inner.try_send(value) {
                Err(TrySendError::Disconnected(value)) => {
                    return Err(TrySendError::Disconnected(value));
                },
                Err(TrySendError::Full(v)) => {
                    value = v;
                },
                Ok(()) => return Ok(()),
            }
        }

        Err(TrySendError::Full(value))
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

    /// Wake all pending senders.
    pub fn wake_all(&self) {
        self.inner.notify.notify_waiters();
    }

    /// Returns if the queue holds no items.
    pub fn is_empty(&self) -> bool {
        self.inner.queue.is_empty()
    }

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
        self.inner
            .receiver_disconnected
            .store(true, Ordering::Release);
    }
}

struct Inner<T> {
    queue: crossbeam_queue::ArrayQueue<T>,
    notify: tokio::sync::Notify,
    receiver_disconnected: AtomicBool,
}

impl<T> Inner<T> {
    fn new(size: usize) -> Self {
        Self {
            queue: crossbeam_queue::ArrayQueue::new(size),
            notify: tokio::sync::Notify::new(),
            receiver_disconnected: AtomicBool::new(false),
        }
    }

    fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        if self.receiver_disconnected.load(Ordering::Acquire) {
            return Err(TrySendError::Disconnected(value));
        }

        if let Err(v) = self.queue.push(value) {
            Err(TrySendError::Full(v))
        } else {
            Ok(())
        }
    }
}

enum TrySendError<T> {
    Disconnected(T),
    Full(T),
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_queue_sync_handling() {
        let _ = tracing_subscriber::fmt::try_init();

        let (tx, rx) = super::new(9);

        for i in 0..8 {
            tracing::info!(i);
            assert!(tx.send(i).is_ok());
        }

        tracing::info!("spawn thread");
        let handle = std::thread::spawn(move || tx.send(8));

        assert_eq!(rx.pop(), Some(0));
        assert_eq!(rx.pop(), Some(1));
        assert_eq!(rx.pop(), Some(2));

        rx.wake_all();

        let result = handle.join().unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_queue_async_handling() {
        let _ = tracing_subscriber::fmt::try_init();

        let (tx, rx) = super::new(9);

        for i in 0..8 {
            tracing::info!(i);
            assert!(tx.send_async(i).await.is_ok());
        }

        tracing::info!("spawn thread");
        let handle = tokio::spawn(async move { tx.send_async(8).await });

        assert_eq!(rx.pop(), Some(0));
        assert_eq!(rx.pop(), Some(1));
        assert_eq!(rx.pop(), Some(2));

        rx.wake_all();

        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
}
