mod reply;

use std::any::Any;
use std::collections::VecDeque;
use std::io;
use std::io::empty;
use std::pin::pin;
use std::task::Context;

use io_uring::IoUring;
use io_uring::squeue::Flags;
use smallvec::SmallVec;

/// A guard type that can be any object.
pub type DynamicGuard = Box<dyn Any>;

pub struct I2o2Scheduler<G = DynamicGuard> {
    ring: IoUring,
    state: TrackedState<G>,
    /// A stream of incoming IO events to process.
    incoming: flume::Receiver<Message<G>>,
    /// A backlog of IO events to process once the queue has available space.
    ///
    /// The entries in this backlog have already had user data assigned to them
    /// and can be copied directly to the queue.
    backlog: Vec<io_uring::squeue::Entry>,
    /// A shutdown signal flag to close the scheduler.
    shutdown: bool,
}

impl<G> I2o2Scheduler<G> {
    fn run(&mut self) -> io::Result<()> {
        while !self.shutdown {
            if let Err(e) = self.run_ops_cycle() {
                tracing::error!(error = ?e, "failed to complete io_uring cycle");
            }
        }

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
        for completion in self.ring.completion() {
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
            match op {
                Message::Many(ops) => {
                    let mut entries = ops.into_iter()
                        .map(|op| self.state.register(op));

                    while let Some(entry) = entries.next() {
                        let result = unsafe { submission.push(&entry) };
                        if result.is_err() {
                            self.backlog.extend(entries);
                            self.backlog.push(entry);
                            break                            
                        }
                    }                    
                },
                Message::One(op) => {
                    let entry = self.state.register(op);
                    let result = unsafe { submission.push(&entry) };
                    if result.is_err() {
                        self.backlog.push(entry);                        
                    }                    
                },
            }
        }
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
                        return Err(());
                    }
                }
            }
        }
        
        Ok(())
    }
}

struct TrackedState<G> {
    /// A set of guards that should be kept alive as long as the ring requires.
    guards: slab::Slab<G>,
    /// A slab of reply handles for scheduled tasks.
    replies: slab::Slab<reply::ReplyNotify>,
}

impl<G> TrackedState<G> {
    fn register(&mut self, op: PackagedOp<G>) -> io_uring::squeue::Entry {
        let PackagedOp { entry, reply, guard } = op;
        
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
    One(PackagedOp<G>)
}

struct PackagedOp<G = DynamicGuard> {
    entry: io_uring::squeue::Entry,
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