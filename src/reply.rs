//! Efficiently relay IO operation replies to wakers.
//!
//! This is a system very similar to a oneshot channel, but we pack the result
//! in with the signalling atomic value.
//!
//! This is because we only return the IO result code and don't need to worry about
//! anything else that would require more atomic loads, allocations, etc...
//!

use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::task::{Context, Poll, Waker};

use parking_lot::lock_api::Mutex;

const FLAG_PENDING: i64 = i64::MAX;
const FLAG_CANCELLED: i64 = i64::MAX - 1;

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
/// Attempt to get the result of the operation.
pub enum TryGetResultError {
    #[error("task pending")]
    /// The IO operation is still pending.
    Pending,
    #[error("task cancelled")]
    /// The IO operation was cancelled.
    Cancelled,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("task cancelled")]
/// The task was cancelled due to the notify half being dropped.
pub struct Cancelled;

pub fn new() -> (ReplyNotify, ReplyReceiver) {
    let inner = Arc::new(Inner {
        result: AtomicI64::new(FLAG_PENDING),
        waker: Mutex::new(None),
    });

    let tx = ReplyNotify {
        inner: inner.clone(),
        has_set_result: false,
    };
    let rx = ReplyReceiver { inner };

    (tx, rx)
}

/// Wait for a reply for an IO result.
pub struct ReplyReceiver {
    inner: Arc<Inner>,
}

impl Debug for ReplyReceiver {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = self.inner.result.load(Ordering::Relaxed);
        if value == FLAG_PENDING {
            write!(f, "ReplyFuture(state=PENDING, result=<unknown>)")
        } else if value == FLAG_CANCELLED {
            write!(f, "ReplyFuture(state=CANCELLED, result=<unknown>)")
        } else {
            write!(f, "ReplyFuture(state=READY, result={value})")
        }
    }
}

impl ReplyReceiver {
    /// Attempt to get the result without waiting.
    ///
    /// Returns `None` if the result is not ready yet.
    pub fn try_get_result(&self) -> Result<i32, TryGetResultError> {
        #[cfg(feature = "fail")]
        fail::fail_point!("i2o2::fail::try_get_result", parse_fail_return);

        let inner = self.inner.as_ref();

        let value = inner.result.load(Ordering::SeqCst);
        if value == FLAG_PENDING {
            return Err(TryGetResultError::Pending);
        } else if value == FLAG_CANCELLED {
            return Err(TryGetResultError::Cancelled);
        }

        // This should always be available since the sender half will be dropped
        // since `value` is set.
        if let Some(mut waker_opt) = inner.waker.try_lock() {
            drop(waker_opt.take());
        }

        Ok(value as i32)
    }

    /// Synchronously wait for the result to complete.
    pub fn wait(self) -> Result<i32, Cancelled> {
        futures_executor::block_on(self)
    }
}

impl Future for ReplyReceiver {
    type Output = Result<i32, Cancelled>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        #[cfg(feature = "fail")]
        fail::fail_point!("i2o2::fail::poll_reply_future", |code| {
            match parse_fail_return(code) {
                Ok(code) => Poll::Ready(Ok(code)),
                Err(TryGetResultError::Cancelled) => Poll::Ready(Err(Cancelled)),
                Err(TryGetResultError::Pending) => Poll::Pending,
            }
        });

        let inner = self.inner.as_ref();

        let value = inner.result.load(Ordering::SeqCst);
        // Check to see if the task has completed or if it is still in the pending state.
        //
        // Note that the acquisition of the `waker` lock should never fail while the
        // `value` is in the `FLAG_PENDING` state.
        if value == FLAG_PENDING {
            #[cfg(feature = "trace-hotpath")]
            tracing::trace!("reply is pending");

            let task = cx.waker().clone();
            if let Some(mut lock) = inner.waker.try_lock() {
                *lock = Some(task);
            }
        }

        let value = inner.result.load(Ordering::SeqCst);
        if value == FLAG_CANCELLED {
            #[cfg(feature = "trace-hotpath")]
            tracing::trace!("reply is cancelled");

            Poll::Ready(Err(Cancelled))
        } else if value == FLAG_PENDING {
            Poll::Pending
        } else {
            #[cfg(feature = "trace-hotpath")]
            tracing::trace!("reply is ready");
            Poll::Ready(Ok(value as i32))
        }
    }
}

/// Notify the `ReplyFuture` that the result is ready to be read.
pub struct ReplyNotify {
    inner: Arc<Inner>,
    has_set_result: bool,
}

impl ReplyNotify {
    /// Set the result of the operation and notify the future.
    pub fn set_result(mut self, result: i32) {
        let inner = self.inner.as_ref();
        inner.result.store(result as i64, Ordering::SeqCst);
        self.has_set_result = true;
        self.complete_waker();
    }

    fn complete_waker(&mut self) {
        let inner = self.inner.as_ref();
        #[allow(clippy::collapsible_if)]
        if let Some(mut slot) = inner.waker.try_lock() {
            if let Some(waker) = slot.take() {
                drop(slot);
                waker.wake();
            }
        }
    }
}

impl Drop for ReplyNotify {
    fn drop(&mut self) {
        if self.has_set_result {
            return;
        }

        let inner = self.inner.as_ref();
        inner.result.store(FLAG_CANCELLED, Ordering::SeqCst);

        self.complete_waker();
    }
}

struct Inner {
    result: AtomicI64,
    waker: parking_lot::Mutex<Option<Waker>>,
}

#[cfg(feature = "fail")]
fn parse_fail_return(value: Option<String>) -> Result<i32, TryGetResultError> {
    let code_str = value.expect("i2o2 fail point triggered");
    match code_str.as_str() {
        "pending" => Err(TryGetResultError::Pending),
        "cancelled" => Err(TryGetResultError::Cancelled),
        _ => Ok(code_str
            .parse::<i32>()
            .expect("invalid fail point return value provided")),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Cancelled, TryGetResultError};

    #[tokio::test]
    async fn test_normal_notify() {
        let (tx, rx) = super::new();

        assert_eq!(rx.try_get_result(), Err(TryGetResultError::Pending));

        tx.set_result(1234);

        assert_eq!(rx.try_get_result(), Ok(1234));
        let result = rx.await;
        assert_eq!(result, Ok(1234));
    }

    #[tokio::test]
    async fn test_notify_drop_sender() {
        let (tx, rx) = super::new();

        assert_eq!(rx.try_get_result(), Err(TryGetResultError::Pending));
        drop(tx);
        assert_eq!(rx.try_get_result(), Err(TryGetResultError::Cancelled));
        let result = rx.await;
        assert_eq!(result, Err(Cancelled));
    }

    #[test]
    fn test_notify_drop_reply() {
        let (tx, rx) = super::new();
        drop(rx);

        // Should complete ok, the sender does not care if the task gets dropped.
        tx.set_result(1234);
    }

    #[tokio::test]
    async fn test_concurrent_tasks() {
        let (tx, rx) = super::new();

        let handle = tokio::spawn(rx);

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            tx.set_result(1234);
        });

        let result = handle.await.unwrap();
        assert_eq!(result, Ok(1234));
    }
}
