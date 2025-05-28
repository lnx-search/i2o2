mod reply;
mod wake;

use std::any::Any;
use std::io;
use std::pin::pin;
use std::task::Poll;

use futures_util::TryFutureExt;
use io_uring::IoUring;
pub use io_uring::opcode;
pub use io_uring::squeue::Entry;
use smallvec::SmallVec;

use crate::wake::RingWaker;

/// A guard type that can be any object.
pub type DynamicGuard = Box<dyn Any>;
pub type SubmitResult<T> = Result<T, SchedulerClosed>;

#[derive(Debug, thiserror::Error)]
#[error("scheduler has closed")]
/// The scheduler has shutdown and is no longer accepting events.
pub struct SchedulerClosed;

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
    fn new() -> io::Result<(Self, flume::Sender<Message<G>>)> {
        let (tx, rx) = flume::bounded(128);

        let ring = IoUring::builder().setup_coop_taskrun().build(128)?;

        let waker = RingWaker::new()?;

        let submitter = ring.submitter();
        submitter.register_eventfd(waker.event_fd())?;

        let scheduler = Self {
            ring,
            state: TrackedState::default(),
            self_waker: waker,
            incoming: rx,
            backlog: Vec::new(),
            shutdown: false,
        };

        Ok((scheduler, tx))
    }

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
