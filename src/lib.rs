#![doc = include_str!("../README.md")]

mod builder;
mod handle;
mod reply;
#[cfg(test)]
mod tests;
mod wake;

use std::any::Any;
use std::collections::VecDeque;
use std::io;
use std::task::Poll;

use flume::r#async::RecvFut;
use futures_util::FutureExt;
use io_uring::squeue::Entry;
use io_uring::{CompletionQueue, IoUring, SubmissionQueue, Submitter};
pub use io_uring::{opcode, types};
use smallvec::SmallVec;

pub use crate::builder::I2o2Builder;
pub use crate::handle::I2o2Handle;
use crate::wake::RingWaker;

#[cfg(not(target_os = "linux"))]
compiler_error!(
    "I2o2 only supports linux based operating systems, and requires relatively new kernel versions"
);

/// A guard type that can be any object.
pub type DynamicGuard = Box<dyn Any>;
/// A submission result for the scheduler.
pub type SubmitResult<T> = Result<T, SchedulerClosed>;

mod flags {
    use std::process::abort;

    const FLAGS_MASK: u64 = 0xF000_0000_0000_0000;
    const NOT_SET: u64 = 0x0000_0000_0000_0000;
    pub const MAX_SAFE_IDX: u32 = 0x3FFF_FFFF;
    pub const EVENT_FD_WAKER: u64 = 0x1000_0000_0000_0000;
    pub const REGISTER_BUFFER: u64 = 0x2000_0000_0000_0000;
    pub const UNREGISTER_BUFFER: u64 = 0x3000_0000_0000_0000;
    pub const REGISTER_FILE: u64 = 0x4000_0000_0000_0000;
    pub const UNREGISTER_FILE: u64 = 0x5000_0000_0000_0000;
    pub const WITHOUT_GUARD: u64 = 0x6000_0000_0000_0000;

    #[repr(u64)]
    #[derive(Debug, Copy, Clone, Eq, PartialEq)]
    /// The possible flags that can be set.
    pub enum Flag {
        /// The event is coming from the event FD waker.
        EventFdWaker = EVENT_FD_WAKER,
        /// The event is coming from the register buffer event.
        RegisterBuffer = REGISTER_BUFFER,
        /// The event is coming from the unregister buffer event.
        UnregisterBuffer = UNREGISTER_BUFFER,
        /// The event is coming from the register file event.
        RegisterFile = REGISTER_FILE,
        /// The event is coming from the unregister file event.
        UnregisterFile = UNREGISTER_FILE,
        /// The entry has no guard value.
        WithoutGuard = WITHOUT_GUARD,
        /// No flag set, normally should never exist.
        NotSet = NOT_SET,
    }

    /// Packs the 4 bit `flag` with the 30 bit `reply_idx` and `guard_idx`.
    pub fn pack(flag: Flag, reply_idx: u32, guard_idx: u32) -> u64 {
        // If a program has *somehow* managed to enqueue 1,073,741,823
        // they are doing something *very* wrong, if the system is even still alive
        // we don't care to support that sort of behaviour so will abort to prevent
        // wraps or corrupting of the packed value.
        if reply_idx > MAX_SAFE_IDX || guard_idx > MAX_SAFE_IDX {
            abort_insane_program();
        }

        let reply_idx = (reply_idx as u64) << 30;
        let guard_idx = guard_idx as u64;
        let flag = flag as u64;
        flag | reply_idx | guard_idx
    }

    /// Unpacks the 4 bit `flag` and 30 bit `reply_idx` and `guard_idx` from
    /// the provided value.
    pub fn unpack(packed_value: u64) -> (Flag, u32, u32) {
        const REPLY_IDX_MASK: u64 = 0x0FFF_FFFF_C000_0000;
        const GUARD_IDX_MASK: u64 = 0x0000_0000_3FFF_FFFF;

        let guard_idx = (packed_value & GUARD_IDX_MASK) as u32;
        let reply_idx = ((packed_value & REPLY_IDX_MASK) >> 30) as u32;
        let flag = match packed_value & FLAGS_MASK {
            EVENT_FD_WAKER => Flag::EventFdWaker,
            REGISTER_BUFFER => Flag::RegisterBuffer,
            UNREGISTER_BUFFER => Flag::UnregisterBuffer,
            REGISTER_FILE => Flag::RegisterFile,
            UNREGISTER_FILE => Flag::UnregisterFile,
            _ => Flag::NotSet,
        };

        (flag, reply_idx, guard_idx)
    }

    #[inline(never)]
    fn abort_insane_program() {
        eprintln!(
            "billions of operations have been enqueued and not completed, program should abort"
        );
        abort();
    }
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
            let result = completion.result();
            let user_data = completion.user_data();

            let (flag, reply_idx, guard_idx) = flags::unpack(user_data);

            #[cfg(feature = "trace-hotpath")]
            tracing::trace!(flag = ?flag, task_id = reply_idx, result = result, "completion");

            match flag {
                flags::Flag::EventFdWaker => self.self_waker.mark_unset(),
                flags::Flag::RegisterBuffer => {},
                flags::Flag::UnregisterBuffer => {},
                flags::Flag::RegisterFile => {},
                flags::Flag::UnregisterFile => {},
                flags::Flag::WithoutGuard => {
                    self.state.acknowledge_reply(reply_idx, result);
                },
                flags::Flag::NotSet => {
                    self.state.acknowledge_reply(reply_idx, result);
                    self.state.drop_guard_if_exists(guard_idx);
                },
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

        let flag = if guard.is_none() {
            flags::Flag::WithoutGuard
        } else {
            flags::Flag::NotSet
        };

        let guard_idx = guard.map(|g| self.guards.insert(g) as u32).unwrap_or(0);

        #[cfg(feature = "trace-hotpath")]
        tracing::trace!(task_id = reply_idx, "registered entry");

        let user_data = flags::pack(flag, reply_idx as u32, guard_idx);
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

#[cfg(test)]
mod tests_packing {
    use super::*;

    #[rstest::rstest]
    #[case(flags::Flag::NotSet, 0, 0)]
    #[case(flags::Flag::NotSet, 1, 1)]
    #[case(flags::Flag::NotSet, 4, 0)]
    #[case(flags::Flag::NotSet, 0, 5)]
    #[case(flags::Flag::NotSet, 9999, 12345)]
    #[case(flags::Flag::NotSet, 0x3FFF_FFFF, u32::MIN)]
    #[case(flags::Flag::NotSet, u32::MIN, 0x3FFF_FFFF)]
    #[case(flags::Flag::NotSet, u32::MIN, 0x3FFF_FFFF)]
    #[case(flags::Flag::NotSet, 0x3FFF_FFFF, 1234)]
    #[case(flags::Flag::NotSet, 1234, 0x3FFF_FFFF)]
    fn test_pack_and_unpack_indexes(
        #[case] flag: flags::Flag,
        #[case] input_reply_idx: u32,
        #[case] guard_idx: u32,
    ) {
        let packed = flags::pack(flag, input_reply_idx, guard_idx);
        let (unpacked_flag, unpacked_reply, unpacked_guard) = flags::unpack(packed);
        assert_eq!(unpacked_flag, flag);
        assert_eq!(unpacked_reply, input_reply_idx);
        assert_eq!(unpacked_guard, guard_idx);
    }
}
