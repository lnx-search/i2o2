mod reply;
mod wake;

use std::any::Any;
use std::io;
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

use futures_util::TryFutureExt;
pub use io_uring;
use io_uring::IoUring;
pub use io_uring::opcode;
pub use io_uring::squeue::Entry;
use smallvec::SmallVec;

use crate::wake::RingWaker;

/// A guard type that can be any object.
pub type DynamicGuard = Box<dyn Any>;
/// A submission result for the scheduler.
pub type SubmitResult<T> = Result<T, SchedulerClosed>;

#[derive(Debug, thiserror::Error)]
#[error("scheduler has closed")]
/// The scheduler has shutdown and is no longer accepting events.
pub struct SchedulerClosed;

/// Create a new [I2o2Scheduler] and [I2o2Handle] pair backed by io_uring.
///
/// This will use the default settings for the scheduler, you can optionally
/// use the [builder] to customise the ring behaviour.
///
/// ## Example
///
/// ```rust
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
///
/// let (scheduler, handle) = i2o2::create()?;
///
/// # Ok(())
/// # }
/// ```
pub fn create<G>() -> io::Result<(I2o2Scheduler<G>, I2o2Handle<G>)> {
    I2o2Builder::default().try_create()
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
///     .with_defer_task_run(true)
///     .with_io_polling(true)
///     .with_sqe_polling(true)
///     .with_sqe_polling_timeout(Duration::from_millis(100))
///     .try_create()?;
///
/// # Ok(())
/// # }
/// ```
pub fn builder<G>() -> I2o2Builder {
    I2o2Builder::default()
}

/// A set of configuration options for customising the [I2o2Scheduler] scheduler.
///
/// ## Example
///
/// ```rust
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::time::Duration;
///
/// let (scheduler, handle) = i2o2::I2o2Builder::default()
///     .with_defer_task_run(true)
///     .with_io_polling(true)
///     .with_sqe_polling(true)
///     .with_sqe_polling_timeout(Duration::from_millis(100))
///     .try_create()?;
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
        Self {
            queue_size: 128,
            io_poll: false,
            sqe_poll: None,
            sqe_poll_cpu: None,
            defer_task_run: false,
        }
    }
}

impl I2o2Builder {
    /// Set the queue size of the ring and handler buffer.
    ///
    /// The provided value should be a power of `2`.
    ///
    /// By default, this is `128`.
    pub fn with_queue_size(mut self, size: u32) -> Self {
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
    /// https://www.man7.org/linux/man-pages/man2/io_uring_setup.2.html
    ///
    /// > Perform busy-waiting for an I/O completion, as opposed to
    /// > getting notifications via an asynchronous IRQ (Interrupt
    /// > Request).
    ///    
    /// **WARNING: Enabling this option requires all file IO events to be O_DIRECT**
    ///
    /// By default, this is `disabled`.
    pub fn with_io_polling(mut self, enable: bool) -> Self {
        self.io_poll = enable;
        self
    }

    /// Enables/disables submission queue polling by the kernel.
    ///
    /// Sets `IORING_SETUP_SQPOLL`
    ///
    /// https://www.man7.org/linux/man-pages/man2/io_uring_setup.2.html
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
    pub fn with_sqe_polling(mut self, enable: bool) -> Self {
        if enable {
            self.sqe_poll = Some(Duration::from_millis(10));
        } else {
            self.sqe_poll = None;
        }
        self
    }

    /// Set the submission queue polling idle timeout.
    ///
    /// https://www.man7.org/linux/man-pages/man2/io_uring_setup.2.html
    ///
    /// This overwrites the default timeout value I2o2 sets of `10ms`.
    ///
    /// NOTE: `with_sqe_polling` must be enabled first before calling this method.
    pub fn with_sqe_polling_timeout(mut self, timeout: Duration) -> Self {
        if self.sqe_poll.is_none() {
            panic!(
                "submission queue polling is not already enabled at the time of calling this method"
            );
        }
        assert!(
            timeout <= Duration::from_secs(10),
            "timeout has gone beyond sane levels"
        );

        self.sqe_poll = Some(timeout);
        self
    }

    /// Set cpu core the polling thread should be pinned to.
    ///
    /// Sets `IORING_SETUP_SQ_AFF`
    ///
    /// https://www.man7.org/linux/man-pages/man2/io_uring_setup.2.html
    ///
    /// NOTE: `with_sqe_polling` must be enabled first before calling this method.
    pub fn with_sqe_polling_pinned_cpu(mut self, cpu: u32) -> Self {
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
    /// https://www.man7.org/linux/man-pages/man2/io_uring_setup.2.html
    ///
    /// > By default, io_uring will process all outstanding work at
    /// > the end of any system call or thread interrupt. This can
    /// > delay the application from making other progress.  Setting
    /// > this flag will hint to io_uring that it should defer work
    /// > until an io_uring_enter(2) call with the
    /// > IORING_ENTER_GETEVENTS flag set.
    ///
    /// By default, this is `disabled`.
    pub fn with_defer_task_run(mut self, enable: bool) -> Self {
        self.defer_task_run = enable;
        self
    }

    /// Attempt to create the scheduler using the current configuration.
    pub fn try_create<G>(self) -> io::Result<(I2o2Scheduler<G>, I2o2Handle<G>)> {
        let (tx, rx) = flume::bounded(self.queue_size as usize);
        let waker = RingWaker::new()?;

        let ring = self.setup_io_ring()?;

        let submitter = ring.submitter();
        submitter.register_eventfd(waker.event_fd())?;

        let scheduler = I2o2Scheduler {
            ring,
            state: TrackedState::default(),
            self_waker: waker,
            incoming: rx,
            backlog: Vec::new(),
            shutdown: false,
        };

        let handle = I2o2Handle { inner: tx };

        Ok((scheduler, handle))
    }

    fn setup_io_ring(&self) -> io::Result<IoUring> {
        let mut builder = IoUring::builder();
        builder.setup_coop_taskrun();
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
}

impl<G> Clone for I2o2Handle<G> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
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
    /// - Obeys any additional safety constraints specified by the [opcode].
    pub unsafe fn submit<O>(
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
    /// - Obeys any additional safety constraints specified by the [opcode].
    pub unsafe fn submit_many_entries<O>(
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
    /// - Obeys any additional safety constraints specified by the [opcode].
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
    /// - Obeys any additional safety constraints specified by the [opcode].
    pub unsafe fn submit_many_entries_async<O>(
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
    backlog: Vec<Entry>,
    /// A shutdown signal flag to close the scheduler.
    shutdown: bool,
}

impl<G> I2o2Scheduler<G> {
    /// Run the scheduler in the current thread until it is shut down.
    ///
    /// This will wait for all remaining tasks to complete.
    pub fn run(mut self) -> io::Result<()> {
        tracing::debug!("scheduler is running");

        while !self.shutdown {
            if let Err(e) = self.run_ops_cycle() {
                tracing::error!(error = ?e, "failed to complete io_uring cycle");
            }
        }

        tracing::debug!("scheduler shutting down");

        // Submit any remaining events.
        self.ring.submit()?;
        self.ring.submit_and_wait(self.state.remaining_tasks())?;

        let submitter = self.ring.submitter();
        submitter.unregister_eventfd()?;

        Ok(())
    }

    fn run_ops_cycle(&mut self) -> io::Result<()> {
        self.drain_completion_events();
        self.read_and_enqueue_events();

        let has_outstanding_writes = self.has_outstanding_ops();
        if !has_outstanding_writes {
            self.ring.submit_and_wait(1)?;
        } else {
            self.ring.submit()?;
        }

        Ok(())
    }

    fn has_outstanding_ops(&mut self) -> bool {
        !self.backlog.is_empty()
            || !self.incoming.is_empty()
            || !self.ring.completion().is_empty()
    }

    fn drain_completion_events(&mut self) {
        let mut completion = self.ring.completion();
        completion.sync();

        for completion in completion {
            let (reply_idx, guard_idx) = unpack_indexes(completion.user_data());
            self.state.acknowledge_reply(reply_idx, completion.result());
            self.state.drop_guard_if_exists(guard_idx);
        }
    }

    fn read_and_enqueue_events(&mut self) {
        // Prioritise draining the backlog before new ops.
        if self.try_drain_backlog().is_err() {
            return;
        }

        self.drain_incoming();
    }

    /// Ingest new messages coming from the `incoming` message channel.
    ///
    /// If the channel becomes empty, the system will register a waker in order
    /// to trigger ingestion again when new entries exist.
    ///
    /// A message can contain one or more operations to perform, if the operation could
    /// not be pushed onto the submission queue it will be added to the backlog and
    /// retried once the queue has space.
    fn drain_incoming(&mut self) {
        let mut submission = self.ring.submission();

        while let Ok(op) = self.incoming.try_recv() {
            let has_capacity =
                handle_message(&mut submission, &mut self.state, &mut self.backlog, op);

            if !has_capacity {
                break;
            }
        }

        if self.incoming.is_empty() {
            let mut ctx = self.self_waker.context();
            let mut future = pin!(self.incoming.recv_async());
            match future.as_mut().poll(&mut ctx) {
                Poll::Ready(Ok(op)) => {
                    handle_message(
                        &mut submission,
                        &mut self.state,
                        &mut self.backlog,
                        op,
                    );
                },
                // This only errors if all handles are dropped and the ring should shutdown.
                Poll::Ready(Err(_)) => {
                    self.shutdown = true;
                },
                Poll::Pending => {},
            }
        }

        submission.sync();
    }

    /// Attempts to submit all entries currently in the backlog queue.
    ///
    /// The queue is processed in a LIFO order to prioritise latency
    /// in the event the backlog grows too much, although in theory that should
    /// be minimal.
    ///
    /// Returns `Ok(())` if the backlog was cleared, otherwise `Err(())` is returned
    /// to signal entries still remain in the backlog and the submit queue is full.
    fn try_drain_backlog(&mut self) -> Result<(), ()> {
        let mut submission = self.ring.submission();

        unsafe {
            if submission.push_multiple(&self.backlog).is_ok() {
                self.backlog.clear();
            } else {
                while let Some(entry) = self.backlog.pop() {
                    if submission.push(&entry).is_err() {
                        self.backlog.push(entry);
                        submission.sync();
                        return Err(());
                    }
                }
            }
        }

        Ok(())
    }
}

fn handle_message<G>(
    submission: &mut io_uring::SubmissionQueue,
    state: &mut TrackedState<G>,
    backlog: &mut Vec<Entry>,
    op: Message<G>,
) -> bool {
    match op {
        Message::Many(ops) => {
            let mut entries = ops.into_iter().map(|op| state.register(op));

            while let Some(entry) = entries.next() {
                let result = unsafe { submission.push(&entry) };
                if result.is_err() {
                    backlog.extend(entries);
                    backlog.push(entry);
                    return false;
                }
            }
        },
        Message::One(op) => {
            let entry = state.register(op);
            let result = unsafe { submission.push(&entry) };
            if result.is_err() {
                backlog.push(entry);
                return false;
            }
        },
    }

    true
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

fn pack_indexes(reply_idx: u32, guard_idx: u32) -> u64 {
    ((reply_idx as u64) << 32) | (guard_idx as u64)
}

fn unpack_indexes(packed: u64) -> (u32, u32) {
    const MASK: u64 = 0xFFFF_FFFF;
    let reply_idx = (packed >> 32) as u32;
    let guard_idx = (packed & MASK) as u32;
    (reply_idx, guard_idx)
}

#[cfg(test)]
mod tests {
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
