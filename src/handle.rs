use std::sync::Arc;

use io_uring::squeue::Entry;
use smallvec::SmallVec;

use crate::{DynamicGuard, Message, Packaged, SchedulerClosed, SubmitResult, reply};

/// The [I2o2Handle] allows you to interact with the [I2o2Scheduler](crate::I2o2Scheduler) and
/// submit IO events to it.
pub struct I2o2Handle<G = DynamicGuard> {
    inner: flume::Sender<Message<G>>,
    /// A guard that ensures the runtime is woken when the handle is dropped.
    wake_on_drop: Arc<WakeOnDrop>,
}

impl<G> Clone for I2o2Handle<G> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            wake_on_drop: self.wake_on_drop.clone(),
        }
    }
}

impl<G> I2o2Handle<G> {
    pub(super) fn new(tx: flume::Sender<Message<G>>, waker: std::task::Waker) -> Self {
        Self {
            inner: tx,
            wake_on_drop: Arc::new(WakeOnDrop(waker)),
        }
    }
}

impl<G> I2o2Handle<G>
where
    G: Send + 'static,
{
    /// Submit an op to the scheduler.
    ///
    /// This may block if th scheduler queue is currently full.
    ///
    /// A `guard` value can be passed, which can be used to ensure data required by the entry
    /// lives at _least_ as long as necessary for the scheduler. It is your responsibility to
    /// ensure that the `guard` actually impacts the dependencies of the entry, but the scheduler
    /// will guarantee that the `guard` lives as long as io_uring requires.
    ///
    /// # Safety
    ///
    /// It is the callers responsibility to ensure that the op contained within the entry is:
    /// - Safe to send across thread boundaries.
    /// - Valid throughout the entire execution of the syscall until complete.
    /// - Obeys any additional safety constraints specified by the [i2o2::opcode](crate::opcode).
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::io;   
    ///
    /// fn main() -> io::Result<()> {
    ///     let (scheduler, scheduler_handle) = i2o2::create_for_current_thread::<()>()?;
    ///     let op = i2o2::opcode::Nop::new().build();
    ///     
    ///     let reply = unsafe {
    ///         scheduler_handle
    ///             .submit(op, None)
    ///             .expect("submit op to scheduler")
    ///     };    
    ///     
    ///     drop(scheduler_handle);
    ///     scheduler.run()?;
    ///     
    ///     let result = reply.wait();
    ///     assert_eq!(result, Ok(0));
    ///     
    ///     Ok(())
    /// }
    /// ```
    pub unsafe fn submit(
        &self,
        entry: Entry,
        guard: Option<G>,
    ) -> SubmitResult<reply::ReplyReceiver> {
        let (reply, rx) = reply::new();
        let message = Message::OpOne(Packaged {
            data: entry,
            reply,
            guard,
        });

        self.inner
            .send(message)
            .map_err(|_| SchedulerClosed)
            .map(|_| rx)
    }

    /// Submit multiple ops to the scheduler.
    ///
    /// This may block if th scheduler queue is currently full.
    ///
    /// A `guard` value can be passed, which can be used to ensure data required by the entry
    /// lives at _least_ as long as necessary for the scheduler. It is your responsibility to
    /// ensure that the `guard` actually impacts the dependencies of the entry, but the scheduler
    /// will guarantee that the `guard` lives as long as io_uring requires.
    ///
    /// # Safety
    ///
    /// It is the callers responsibility to ensure that the op contained within the entry is:
    /// - Safe to send across thread boundaries.
    /// - Valid throughout the entire execution of the syscall until complete.
    /// - Obeys any additional safety constraints specified by the [i2o2::opcode](crate::opcode).
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::io;   
    ///
    /// fn main() -> io::Result<()> {
    ///     let (thread_handle, scheduler_handle) = i2o2::create_and_spawn::<()>()?;
    ///     
    ///     let ops = std::iter::repeat_with(|| (i2o2::opcode::Nop::new().build(), None)).take(5);
    ///     
    ///     let replies = unsafe {
    ///         scheduler_handle
    ///             .submit_many_entries(ops)
    ///             .expect("submit ops to scheduler")
    ///     };    
    ///     
    ///     for reply in replies {
    ///         let result = reply.wait();
    ///         assert_eq!(result, Ok(0));
    ///     }
    ///
    ///     drop(scheduler_handle);
    ///     thread_handle.join().unwrap()?;
    ///     
    ///     Ok(())
    /// }
    /// ```
    pub unsafe fn submit_many_entries(
        &self,
        pairs: impl IntoIterator<Item = (Entry, Option<G>)>,
    ) -> SubmitResult<impl IntoIterator<Item = reply::ReplyReceiver>> {
        let (message, replies) = prepare_many_entries(pairs);

        self.inner.send(message).map_err(|_| SchedulerClosed)?;

        Ok(replies.into_iter())
    }

    /// Submit an op to the scheduler asynchronously waiting if the queue is currently
    /// full.
    ///
    /// A `guard` value can be passed, which can be used to ensure data required by the entry
    /// lives at _least_ as long as necessary for the scheduler. It is your responsibility to
    /// ensure that the `guard` actually impacts the dependencies of the entry, but the scheduler
    /// will guarantee that the `guard` lives as long as io_uring requires.
    ///
    /// # Safety
    ///
    /// It is the callers responsibility to ensure that the op contained within the entry is:
    /// - Safe to send across thread boundaries.
    /// - Valid throughout the entire execution of the syscall until complete.
    /// - Obeys any additional safety constraints specified by the [i2o2::opcode](crate::opcode).
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::io;   
    ///
    /// #[tokio::main]
    /// async fn main() -> io::Result<()> {
    ///     let (thread_handle, scheduler_handle) = i2o2::create_and_spawn::<()>()?;
    ///     let op = i2o2::opcode::Nop::new().build();
    ///     
    ///     let reply = unsafe {
    ///         scheduler_handle
    ///             .submit_async(op, None)
    ///             .await
    ///             .expect("submit op to scheduler")
    ///     };    
    ///     
    ///     let result = reply.wait();
    ///     assert_eq!(result, Ok(0));
    ///
    ///     drop(scheduler_handle);
    ///     thread_handle.join().unwrap()?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub unsafe fn submit_async(
        &self,
        entry: Entry,
        guard: Option<G>,
    ) -> impl Future<Output = SubmitResult<reply::ReplyReceiver>> + '_ {
        use futures_util::TryFutureExt;

        let (reply, rx) = reply::new();
        let message = Message::OpOne(Packaged {
            data: entry,
            reply,
            guard,
        });

        async {
            self.inner
                .send_async(message)
                .map_err(|_| SchedulerClosed)
                .await
                .map(|_| rx)
        }
    }

    /// Submit multiple ops to the scheduler asynchronously waiting if the queue is currently
    /// full.
    ///
    /// A `guard` value can be passed, which can be used to ensure data required by the entry
    /// lives at _least_ as long as necessary for the scheduler. It is your responsibility to
    /// ensure that the `guard` actually impacts the dependencies of the entry, but the scheduler
    /// will guarantee that the `guard` lives as long as io_uring requires.
    ///
    /// # Safety
    ///
    /// It is the callers responsibility to ensure that the op contained within the entry is:
    /// - Safe to send across thread boundaries.
    /// - Valid throughout the entire execution of the syscall until complete.
    /// - Obeys any additional safety constraints specified by the [i2o2::opcode](crate::opcode).
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::io;   
    ///
    /// #[tokio::main]
    /// async fn main() -> io::Result<()> {
    ///     let (thread_handle, scheduler_handle) = i2o2::create_and_spawn::<()>()?;
    ///     let ops = std::iter::repeat_with(|| (i2o2::opcode::Nop::new().build(), None)).take(5);
    ///     
    ///     let replies = unsafe {
    ///         scheduler_handle
    ///             .submit_many_entries(ops)
    ///             .expect("submit ops to scheduler")
    ///     };    
    ///  
    ///     for reply in replies {
    ///         let result = reply.wait();
    ///         assert_eq!(result, Ok(0));
    ///     }
    ///
    ///     drop(scheduler_handle);
    ///     thread_handle.join().unwrap()?;
    ///     
    ///     Ok(())
    /// }
    /// ```
    pub unsafe fn submit_many_entries_async(
        &self,
        pairs: impl IntoIterator<Item = (Entry, Option<G>)>,
    ) -> impl Future<Output = SubmitResult<impl IntoIterator<Item = reply::ReplyReceiver>>>
    {
        let (message, replies) = prepare_many_entries(pairs);

        async {
            self.inner
                .send_async(message)
                .await
                .map_err(|_| SchedulerClosed)?;
            Ok(replies.into_iter())
        }
    }
}

fn prepare_many_entries<G>(
    pairs: impl IntoIterator<Item = (Entry, Option<G>)>,
) -> (Message<G>, SmallVec<[reply::ReplyReceiver; 4]>) {
    let mut replies = SmallVec::<[reply::ReplyReceiver; 4]>::new();
    let iter = pairs.into_iter().map(|(entry, guard)| {
        let (reply, rx) = reply::new();
        replies.push(rx);

        Packaged {
            data: entry,
            reply,
            guard,
        }
    });

    (Message::OpMany(SmallVec::from_iter(iter)), replies)
}

struct WakeOnDrop(std::task::Waker);

impl Drop for WakeOnDrop {
    fn drop(&mut self) {
        self.0.wake_by_ref();
    }
}
