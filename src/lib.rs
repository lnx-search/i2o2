#![doc = include_str!("../README.md")]

mod reply;
#[cfg(test)]
mod tests;
mod wake;

use std::any::Any;
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use flume::r#async::RecvFut;
use futures_util::{FutureExt, TryFutureExt};
use io_uring::squeue::Entry;
use io_uring::{CompletionQueue, IoUring, SubmissionQueue, Submitter};
pub use io_uring::{opcode, types};
use smallvec::SmallVec;

use crate::wake::RingWaker;

#[cfg(not(target_os = "linux"))]
compiler_error!(
    "I2o2 only supports linux based operating systems, and requires relatively new kernel versions"
);

/// A guard type that can be any object.
pub type DynamicGuard = Box<dyn Any>;
/// A submission result for the scheduler.
pub type SubmitResult<T> = Result<T, SchedulerClosed>;

mod reserved_user_data {
    pub const EVENT_FD_WAKER: u64 = super::pack_indexes(u32::MAX, u32::MAX);
}

#[derive(Debug, thiserror::Error)]
#[error("scheduler has closed")]
/// The scheduler has shutdown and is no longer accepting events.
pub struct SchedulerClosed;

/// Create a new [I2o2Scheduler] and [I2o2Handle] pair backed by io_uring.
///
/// This will use the default settings for the scheduler, you can optionally
/// use the [builder] to customise the ring behaviour.
///
/// NOTE: The scheduler cannot be sent across threads.
///
/// ## Example
///
/// ```rust
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
///
/// let (scheduler, handle) = i2o2::create_for_current_thread::<()>()?;
///
/// // ... do work
///
/// # Ok(())
/// # }
/// ```
pub fn create_for_current_thread<G>() -> io::Result<(I2o2Scheduler<G>, I2o2Handle<G>)> {
    I2o2Builder::default().try_create()
}

/// Create a new [I2o2Scheduler] and [I2o2Handle] pair backed by io_uring and spawn the scheduler
/// in a background worker thread.
///
/// This will use the default settings for the scheduler, you can optionally
/// use the [builder] to customise the ring behaviour.
///
/// NOTE: The scheduler cannot be sent across threads.
///
/// ## Example
///
/// ```rust
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
///
/// let (scheduler_handle, handle) = i2o2::create_and_spawn::<()>()?;
///
/// // ... do work
///
/// # Ok(())
/// # }
/// ```
pub fn create_and_spawn<G>()
-> io::Result<(std::thread::JoinHandle<io::Result<()>>, I2o2Handle<G>)>
where
    G: Send + 'static,
{
    I2o2Builder::default().try_spawn()
}

/// Create a new [I2o2Scheduler] and [I2o2Handle] pair backed by io_uring
/// with a custom configuration.
///
/// ## Example
///
/// ```rust
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::time::Duration;
///
/// let (scheduler, handle) = i2o2::builder()
///     .with_defer_task_run(false)
///     .with_io_polling(true)
///     .with_sqe_polling(true)
///     .with_sqe_polling_timeout(Duration::from_millis(100))
///     .try_create::<()>()?;
///
/// // ... do work
///
/// # Ok(())
/// # }
/// ```
pub const fn builder() -> I2o2Builder {
    I2o2Builder::const_default()
}

#[derive(Debug, Clone)]
/// A set of configuration options for customising the [I2o2Scheduler] scheduler.
///
/// ## Example
///
/// ```rust
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::time::Duration;
///
/// let (scheduler, handle) = i2o2::I2o2Builder::default()
///     .with_defer_task_run(false)
///     .with_io_polling(true)
///     .with_sqe_polling(true)
///     .with_sqe_polling_timeout(Duration::from_millis(100))
///     .try_create::<()>()?;
///
/// // ... do work
///
/// # Ok(())
/// # }
/// ```
pub struct I2o2Builder {
    queue_size: u32,
    io_poll: bool,
    sqe_poll: Option<Duration>,
    sqe_poll_cpu: Option<u32>,
    defer_task_run: bool,
}

impl Default for I2o2Builder {
    fn default() -> Self {
        Self::const_default()
    }
}

impl I2o2Builder {
    const fn const_default() -> Self {
        Self {
            queue_size: 128,
            io_poll: false,
            sqe_poll: None,
            sqe_poll_cpu: None,
            defer_task_run: false,
        }
    }

    /// Set the queue size of the ring and handler buffer.
    ///
    /// The provided value should be a power of `2`.
    ///
    /// By default, this is `128`.
    pub const fn with_queue_size(mut self, size: u32) -> Self {
        assert!(
            size != 0 && (size & (size - 1)) == 0,
            "provided `size` value must be a power of 2"
        );
        self.queue_size = size;
        self
    }

    /// Enable/disable IO polling.
    ///
    /// Sets `IORING_SETUP_IOPOLL`
    ///
    /// <https://www.man7.org/linux/man-pages/man2/io_uring_setup.2.html>
    ///
    /// > Perform busy-waiting for an I/O completion, as opposed to
    /// > getting notifications via an asynchronous IRQ (Interrupt
    /// > Request).
    ///
    /// **WARNING: Enabling this option requires all file IO events to be O_DIRECT**
    ///
    /// By default, this is `disabled`.
    pub const fn with_io_polling(mut self, enable: bool) -> Self {
        self.io_poll = enable;
        self
    }

    /// Enables/disables submission queue polling by the kernel.
    ///
    /// Sets `IORING_SETUP_SQPOLL`
    ///
    /// <https://www.man7.org/linux/man-pages/man2/io_uring_setup.2.html>
    ///
    /// > When this flag is specified, a kernel thread is created to
    /// > perform submission queue polling.  An io_uring instance
    /// > configured in this way enables an application to issue I/O
    /// > without ever context switching into the kernel.  By using
    /// > the submission queue to fill in new submission queue
    /// > entries and watching for completions on the completion
    /// > queue, the application can submit and reap I/Os without
    /// > doing a single system call.
    ///
    /// By default, the system will use a `10ms` idle timeout, you can configure
    /// this value using [I2o2Builder::with_sqe_polling_timeout].
    pub const fn with_sqe_polling(mut self, enable: bool) -> Self {
        if enable {
            self.sqe_poll = Some(Duration::from_millis(2000));
        } else {
            self.sqe_poll = None;
        }
        self
    }

    /// Set the submission queue polling idle timeout.
    ///
    /// <https://www.man7.org/linux/man-pages/man2/io_uring_setup.2.html>
    ///
    /// This overwrites the default timeout value I2o2 sets of `10ms`.
    ///
    /// NOTE: `with_sqe_polling` must be enabled first before calling this method.
    pub const fn with_sqe_polling_timeout(mut self, timeout: Duration) -> Self {
        if self.sqe_poll.is_none() {
            panic!(
                "submission queue polling is not already enabled at the time of calling this method"
            );
        }
        assert!(
            timeout.as_secs_f32() <= 10.0,
            "timeout has gone beyond sane levels"
        );

        self.sqe_poll = Some(timeout);
        self
    }

    /// Set cpu core the polling thread should be pinned to.
    ///
    /// Sets `IORING_SETUP_SQ_AFF`
    ///
    /// <https://www.man7.org/linux/man-pages/man2/io_uring_setup.2.html>
    ///
    /// NOTE: `with_sqe_polling` must be enabled first before calling this method.
    pub const fn with_sqe_polling_pin_cpu(mut self, cpu: u32) -> Self {
        if self.sqe_poll.is_none() {
            panic!(
                "submission queue polling is not already enabled at the time of calling this method"
            );
        }
        self.sqe_poll_cpu = Some(cpu);
        self
    }

    /// Enables/disables submission queue polling by the kernel.
    ///
    /// Sets `IORING_SETUP_DEFER_TASKRUN`
    ///
    /// <https://www.man7.org/linux/man-pages/man2/io_uring_setup.2.html>
    ///
    /// > By default, io_uring will process all outstanding work at
    /// > the end of any system call or thread interrupt. This can
    /// > delay the application from making other progress.  Setting
    /// > this flag will hint to io_uring that it should defer work
    /// > until an io_uring_enter(2) call with the
    /// > IORING_ENTER_GETEVENTS flag set.
    ///
    /// By default, this is `disabled`.
    pub const fn with_defer_task_run(mut self, enable: bool) -> Self {
        self.defer_task_run = enable;
        self
    }

    /// Attempt to create the scheduler using the current configuration.
    pub fn try_create<G>(self) -> io::Result<(I2o2Scheduler<G>, I2o2Handle<G>)> {
        let (tx, rx) = flume::bounded(self.queue_size as usize);
        let waker = RingWaker::new()?;

        let ring = self.setup_io_ring()?;

        let handle = I2o2Handle {
            inner: tx,
            wake_on_drop: Arc::new(WakeOnDrop(waker.task_waker())),
        };

        let scheduler = I2o2Scheduler {
            ring,
            state: TrackedState::default(),
            self_waker: waker,
            incoming: rx,
            backlog: VecDeque::new(),
            _anti_send_ptr: std::ptr::null_mut(),
        };

        Ok((scheduler, handle))
    }

    /// Attempt to create the scheduler and run it in a background thread using the
    /// current configuration.
    pub fn try_spawn<G>(
        self,
    ) -> io::Result<(std::thread::JoinHandle<io::Result<()>>, I2o2Handle<G>)>
    where
        G: Send + 'static,
    {
        let (tx, rx) = flume::bounded(1);

        let task = move || {
            let (scheduler, handle) = self.try_create()?;

            if tx.send(handle).is_err() {
                return Ok(());
            }
            scheduler.run()?;
            Ok::<_, io::Error>(())
        };

        let scheduler_thread_handle = std::thread::Builder::new()
            .name("i2o2-scheduler-thread".to_string())
            .spawn(task)
            .expect("spawn background worker thread");

        if let Ok(handle) = rx.recv() {
            Ok((scheduler_thread_handle, handle))
        } else {
            let error = scheduler_thread_handle.join().unwrap().expect_err(
                "thread aborted before sending handle back but still returns Ok(())",
            );
            Err(error)
        }
    }

    fn setup_io_ring(&self) -> io::Result<IoUring> {
        let mut builder = IoUring::builder();
        builder.setup_single_issuer();
        builder.dontfork();

        if self.io_poll {
            builder.setup_iopoll();
        }

        if let Some(idle) = self.sqe_poll {
            builder.setup_sqpoll(idle.as_millis() as u32);

            if let Some(cpu) = self.sqe_poll_cpu {
                builder.setup_sqpoll_cpu(cpu);
            }
        } else {
            // This functionality effectively gets implicitly enabled by SQPOLL
            // we should only enable this if SQPOLL is disabled.
            builder.setup_coop_taskrun();
        }

        if self.defer_task_run {
            builder.setup_defer_taskrun();
        }

        builder.build(self.queue_size)
    }
}

/// The [I2o2Handle] allows you to interact with the [I2o2Scheduler] and
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
    /// - Obeys any additional safety constraints specified by the [i2o2::opcode](opcode).
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
        let message = Message::One(PackagedOp {
            entry,
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
    /// - Obeys any additional safety constraints specified by the [i2o2::opcode](opcode).
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
    /// - Obeys any additional safety constraints specified by the [i2o2::opcode](opcode).
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
        let message = Message::One(PackagedOp {
            entry,
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
    /// - Obeys any additional safety constraints specified by the [i2o2::opcode](opcode).
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
                .map_err(|_| SchedulerClosed)
                .await?;
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

        PackagedOp {
            entry,
            reply,
            guard,
        }
    });

    (Message::Many(SmallVec::from_iter(iter)), replies)
}

struct WakeOnDrop(std::task::Waker);

impl Drop for WakeOnDrop {
    fn drop(&mut self) {
        self.0.wake_by_ref();
    }
}

/// The [I2o2Scheduler] runs an io_uring ring in the current thread and submits
/// IO events from the handle into the ring.
///
/// Communication between the handles and the scheduler can be done both synchronously
/// and asynchronously.
pub struct I2o2Scheduler<G = DynamicGuard> {
    ring: IoUring,
    state: TrackedState<G>,
    /// A waker handle for triggering a completion event on `self`
    /// intern causing events to be processed.
    self_waker: RingWaker,
    /// A stream of incoming IO events to process.
    incoming: flume::Receiver<Message<G>>,
    /// A backlog of IO events to process once the queue has available space.
    ///
    /// The entries in this backlog have already had user data assigned to them
    /// and can be copied directly to the queue.
    backlog: VecDeque<Entry>,
    /// A null pointer used to prevent people from sending the scheduler across
    _anti_send_ptr: *mut u8,
}

impl<G> I2o2Scheduler<G> {
    /// Run the scheduler in the current thread until it is shut down.
    ///
    /// This will wait for all remaining tasks to complete.
    pub fn run(mut self) -> io::Result<()> {
        tracing::debug!("scheduler is running");

        let (submitter, sq, cq) = self.ring.split();
        let mut runner = RingRunner {
            submitter,
            sq,
            cq,
            state: &mut self.state,
            self_waker: &mut self.self_waker,
            backlog: &mut self.backlog,
            incoming: &self.incoming,
            pending_future: None,
            shutdown: false,
        };

        while !runner.shutdown {
            runner.run()?;
        }

        runner.wait_for_remaining()?;
        tracing::debug!("scheduler shutting down");

        Ok(())
    }
}

struct RingRunner<'ring, G> {
    submitter: Submitter<'ring>,
    sq: SubmissionQueue<'ring>,
    cq: CompletionQueue<'ring>,
    state: &'ring mut TrackedState<G>,
    self_waker: &'ring mut RingWaker,
    backlog: &'ring mut VecDeque<Entry>,
    incoming: &'ring flume::Receiver<Message<G>>,
    pending_future: Option<RecvFut<'ring, Message<G>>>,
    shutdown: bool,
}

impl<'ring, G> RingRunner<'ring, G> {
    /// Run a single cycle of the event loop.
    ///
    /// This executes steps in the order of:
    ///
    /// 1) Try and empty the backlog of submission events if applicable.
    /// 2) Ingest new events from `incoming` until the SQ is full or `incoming` is empty.
    /// 3) Process outstanding completion events.  
    /// 4) Re-register the EventFD listener if the system needs.
    /// 5) Submit outstanding submission events.
    /// 6) Wait for completion events if there is no outstanding work left.
    ///
    fn run(&mut self) -> io::Result<bool> {
        self.sq.sync();
        self.cq.sync();

        self.drain_backlog();
        self.ingest_from_incoming();
        self.drain_completion_events();
        self.maybe_register_waker();

        self.submit_and_maybe_wait()?;

        Ok(self.shutdown)
    }

    fn wait_for_remaining(&mut self) -> io::Result<()> {
        self.sq.sync();
        self.cq.sync();

        #[cfg(feature = "trace-hotpath")]
        tracing::debug!("scheduler is draining remaining events");

        while !self.backlog.is_empty() || self.state.remaining_tasks() > 0 {
            self.drain_backlog();
            self.ingest_from_incoming();
            self.drain_completion_events();
            self.submit_and_maybe_wait()?;
        }

        #[cfg(feature = "trace-hotpath")]
        tracing::debug!("scheduler has drained all events");

        Ok(())
    }

    /// Attempt to push outstanding backlog entries onto the submission queue.
    fn drain_backlog(&mut self) {
        if self.backlog.is_empty() {
            return;
        }

        #[cfg(feature = "trace-hotpath")]
        tracing::trace!(
            backlog_size = self.backlog.len(),
            "attempting to draining backlog"
        );

        while !self.sq.is_full() {
            let Some(entry) = self.backlog.pop_front() else {
                break;
            };

            // SAFETY: Responsibility about ensuring entry validity is pushed to the caller
            //         on the handle side.
            if unsafe { self.sq.push(&entry).is_err() } {
                self.backlog.push_front(entry);
                break;
            }
        }
        self.sq.sync();
    }

    /// Reads new entries from `incoming` until the submission queue is full.
    ///
    /// If multiple entries are included in a single message, the runner will
    /// add the events unable to be pushed to the SQ to the backlog where
    /// it will have priority when space is next available in the SQ.
    fn ingest_from_incoming(&mut self) {
        #[cfg(feature = "trace-hotpath")]
        tracing::trace!(
            incoming_len = self.incoming.len(),
            sq_len = self.sq.len(),
            sq_capacity = self.sq.capacity(),
            "ingesting new entries from incoming"
        );

        'ingest: while !self.sq.is_full() {
            if let Ok(message) = self.incoming.try_recv() {
                self.handle_message(message);
                continue;
            }

            // We must continue until we get either a disconnect or `pending` state
            // so we can be sure the waker is registered.
            loop {
                let mut context = self.self_waker.context();
                let future = self
                    .pending_future
                    .get_or_insert_with(|| self.incoming.recv_async());
                match future.poll_unpin(&mut context) {
                    Poll::Pending => break 'ingest,
                    Poll::Ready(Err(_)) => {
                        #[cfg(feature = "trace-hotpath")]
                        tracing::debug!("scheduler handle has been disconnected");
                        self.pending_future = None;
                        self.shutdown = true;
                        break 'ingest;
                    },
                    Poll::Ready(Ok(message)) => {
                        self.pending_future = None;
                        self.handle_message(message)
                    },
                }
            }
        }
        self.sq.sync();
    }

    fn drain_completion_events(&mut self) {
        for completion in &mut self.cq {
            let user_data = completion.user_data();

            if user_data == reserved_user_data::EVENT_FD_WAKER {
                self.self_waker.mark_unset();
            } else {
                let (reply_idx, guard_idx) = unpack_indexes(user_data);
                self.state.acknowledge_reply(reply_idx, completion.result());
                self.state.drop_guard_if_exists(guard_idx);

                #[cfg(feature = "trace-hotpath")]
                tracing::trace!(
                    task_id = reply_idx,
                    result = completion.result(),
                    "got completion"
                );
            }
        }
        self.cq.sync();
    }

    /// Register the EventFD waker if it is not already registered with a `read(2)` event.
    ///
    /// This is used to wake the scheduler when new `incoming` operations are available
    /// while waiting for completion events.
    fn maybe_register_waker(&mut self) {
        self.self_waker.maybe_submit_self(&mut self.sq);
        self.sq.sync();
    }

    /// Submit all new submission events to the kernel and wait
    /// for completion events to be ready if there is not anymore outstanding work.
    fn submit_and_maybe_wait(&self) -> io::Result<()> {
        if !self.has_outstanding_work() {
            #[cfg(feature = "trace-hotpath")]
            tracing::debug!("waiting for completion events");
            self.submit_and_wait()
        } else {
            #[cfg(feature = "trace-hotpath")]
            tracing::debug!("outstanding work ready, submitting without wait");
            self.submit_no_wait()
        }
    }

    fn submit_no_wait(&self) -> io::Result<()> {
        match self.submitter.submit() {
            Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => Ok(()),
            Err(other) => Err(other),
            Ok(_) => Ok(()),
        }
    }

    fn submit_and_wait(&self) -> io::Result<()> {
        match self.submitter.submit_and_wait(1) {
            Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => Ok(()),
            Err(other) => Err(other),
            Ok(_) => {
                #[cfg(feature = "trace-hotpath")]
                tracing::debug!("scheduler was woken");
                Ok(())
            },
        }
    }

    fn handle_message(&mut self, message: Message<G>) {
        match message {
            Message::Many(ops) => {
                for op in ops {
                    self.handle_op(op);
                }
            },
            Message::One(op) => self.handle_op(op),
        }
    }

    fn handle_op(&mut self, op: PackagedOp<G>) {
        let entry = self.state.register(op);
        self.push_entry(entry);
    }

    fn push_entry(&mut self, entry: Entry) {
        // SAFETY: Responsibility about ensuring entry validity is pushed to the caller
        //         on the handle side.
        if unsafe { self.sq.push(&entry).is_err() } {
            self.backlog.push_back(entry);
        }
    }

    fn has_outstanding_work(&self) -> bool {
        !self.incoming.is_empty()
            || self.incoming.is_disconnected()
            || !self.backlog.is_empty()
            || !self.cq.is_empty()
    }
}

struct TrackedState<G> {
    /// A set of guards that should be kept alive as long as the ring requires.
    guards: slab::Slab<G>,
    /// A slab of reply handles for scheduled tasks.
    replies: slab::Slab<reply::ReplyNotify>,
}

impl<G> Default for TrackedState<G> {
    fn default() -> Self {
        Self {
            guards: slab::Slab::default(),
            replies: slab::Slab::default(),
        }
    }
}

impl<G> TrackedState<G> {
    fn remaining_tasks(&self) -> usize {
        self.replies.len()
    }

    fn register(&mut self, op: PackagedOp<G>) -> Entry {
        let PackagedOp {
            entry,
            reply,
            guard,
        } = op;

        let reply_idx = self.replies.insert(reply);
        debug_assert!(reply_idx < u32::MAX as usize);

        let guard_idx = guard
            .map(|g| {
                let idx = self.guards.insert(g);
                debug_assert!(idx < u32::MAX as usize);
                idx as u32
            })
            .unwrap_or(u32::MAX);

        #[cfg(feature = "trace-hotpath")]
        tracing::trace!(task_id = reply_idx, "registered entry");

        let user_data = pack_indexes(reply_idx as u32, guard_idx);
        entry.user_data(user_data)
    }

    fn acknowledge_reply(&mut self, reply_idx: u32, result: i32) {
        let reply = self.replies.remove(reply_idx as usize);
        reply.set_result(result);
    }

    fn drop_guard_if_exists(&mut self, guard_idx: u32) {
        drop(self.guards.try_remove(guard_idx as usize));
    }
}

/// An operation to for the scheduler to process.
enum Message<G = DynamicGuard> {
    /// Submit many IO operations to the kernel.
    ///
    /// This can help avoid overhead with the channel communication.
    Many(SmallVec<[PackagedOp<G>; 3]>),
    /// Submit one IO operation to the kernel.
    One(PackagedOp<G>),
}

struct PackagedOp<G = DynamicGuard> {
    entry: Entry,
    reply: reply::ReplyNotify,
    guard: Option<G>,
}

const fn pack_indexes(reply_idx: u32, guard_idx: u32) -> u64 {
    ((reply_idx as u64) << 32) | (guard_idx as u64)
}

fn unpack_indexes(packed: u64) -> (u32, u32) {
    const MASK: u64 = 0xFFFF_FFFF;
    let reply_idx = (packed >> 32) as u32;
    let guard_idx = (packed & MASK) as u32;
    (reply_idx, guard_idx)
}

#[cfg(test)]
mod tests_packing {
    use super::*;

    #[rstest::rstest]
    #[case(0, 0)]
    #[case(1, 1)]
    #[case(4, 0)]
    #[case(0, 5)]
    #[case(9999, 12345)]
    #[case(u32::MAX, u32::MIN)]
    #[case(u32::MIN, u32::MAX)]
    #[case(u32::MIN, u32::MAX)]
    #[case(u32::MAX, 1234)]
    #[case(1234, u32::MAX)]
    fn test_pack_and_unpack_indexes(
        #[case] input_reply_idx: u32,
        #[case] guard_idx: u32,
    ) {
        let packed = pack_indexes(input_reply_idx, guard_idx);
        let (unpacked_reply, unpacked_guard) = unpack_indexes(packed);
        assert_eq!(unpacked_reply, input_reply_idx);
        assert_eq!(unpacked_guard, guard_idx);
    }
}
