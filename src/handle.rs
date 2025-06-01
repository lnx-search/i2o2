use std::io;
use std::sync::Arc;

use smallvec::SmallVec;

use crate::ops::RingOp;
use crate::reply::Cancelled;
use crate::{DynamicGuard, Message, Packaged, Resource, ResourceIndex, reply};

/// A submission result for the scheduler.
pub type SubmitResult<T> = Result<T, SchedulerClosed>;

#[derive(Debug, thiserror::Error)]
#[error("scheduler has closed")]
/// The scheduler has shutdown and is no longer accepting events.
pub struct SchedulerClosed;

#[derive(Debug, thiserror::Error)]
/// An error that prevent reregistration of a resource.
pub enum RegisterError {
    #[error("{0}")]
    /// The scheduler has shutdown and is no longer accepting events.
    SchedulerClosed(SchedulerClosed),
    #[error("{0}")]
    /// The scheduler cancelled the operation, this normally means the scheduler
    /// panicked in this situation.
    Cancelled(Cancelled),
    #[error("out of capacity")]
    /// The ring has no capacity left in order to register the resource.
    OutOfCapacity,
    #[error("{0}")]
    Io(io::Error),
}

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
    ///     let op = i2o2::opcode::Nop::new();
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
    pub unsafe fn submit<O: RingOp>(
        &self,
        op: O,
        guard: Option<G>,
    ) -> SubmitResult<reply::ReplyReceiver> {
        let (reply, rx) = reply::new();
        let message = Message::OpOne(Packaged {
            data: op.into_entry(),
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
    ///     let ops = std::iter::repeat_with(|| (i2o2::opcode::Nop::new(), None)).take(5);
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
    pub unsafe fn submit_many_entries<O: RingOp>(
        &self,
        pairs: impl IntoIterator<Item = (O, Option<G>)>,
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
    ///     let op = i2o2::opcode::Nop::new();
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
    pub unsafe fn submit_async<O: RingOp>(
        &self,
        op: O,
        guard: Option<G>,
    ) -> impl Future<Output = SubmitResult<reply::ReplyReceiver>> + '_ {
        use futures_util::TryFutureExt;

        let (reply, rx) = reply::new();
        let message = Message::OpOne(Packaged {
            data: op.into_entry(),
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
    ///     let ops = std::iter::repeat_with(|| (i2o2::opcode::Nop::new(), None)).take(5);
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
    pub unsafe fn submit_many_entries_async<O: RingOp>(
        &self,
        pairs: impl IntoIterator<Item = (O, Option<G>)>,
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

    /// Register a file with the ring returning a file index that is used
    /// to unregister the file at a later stage.
    ///
    /// Registering a file with the ring reduces the overhead of calls interacting
    /// with the target file by reducing the amount of work the kernel needs to do
    /// on every IO op.
    ///
    /// A `guard` value can be passed, which can be used to track when the registered
    /// file is free/unused after unregistering.
    ///
    /// You need to tell the scheduler up front how many files you plan on registering up
    /// front at creation time using the [I2o2Builder::with_num_registered_files](crate::I2o2Builder::with_num_registered_files)
    /// parameter.
    ///
    /// This method can error when attempting to register a file that is already registered
    /// or there are no free slots available on the ring.
    pub fn register_file(
        &self,
        fd: std::os::fd::RawFd,
        guard: Option<G>,
    ) -> Result<u32, RegisterError> {
        let (reply, rx) = reply::new();

        let message = Message::RegisterResource(Packaged {
            data: Resource::File(fd),
            reply,
            guard,
        });

        self.inner
            .send(message)
            .map_err(|_| RegisterError::SchedulerClosed(SchedulerClosed))?;
        let result = rx.wait().map_err(RegisterError::Cancelled)?;
        handle_register_resource_result(result)
    }

    /// Unregister a previously registered file with the ring using the `file_index`.
    ///
    /// If a guard was provided when registering, the guard will be dropped once all
    /// inflight operations using the registered file are complete.
    ///
    /// This method can error when attempting to unregister a file that does not exist.
    pub fn unregister_file(&self, file_index: u32) -> Result<(), RegisterError> {
        let (reply, rx) = reply::new();

        let message = Message::UnregisterResource(Packaged {
            data: ResourceIndex::File(file_index),
            reply,
            guard: None,
        });

        self.inner
            .send(message)
            .map_err(|_| RegisterError::SchedulerClosed(SchedulerClosed))?;
        let result = rx.wait().map_err(RegisterError::Cancelled)?;
        handle_unregister_resource_result(result)
    }

    /// Register a file with the ring asynchronously returning a file index that is used
    /// to unregister the file at a later stage.
    ///
    /// Registering a file with the ring reduces the overhead of calls interacting
    /// with the target file by reducing the amount of work the kernel needs to do
    /// on every IO op.
    ///
    /// A `guard` value can be passed, which can be used to track when the registered
    /// file is free/unused after unregistering.
    ///
    /// You need to tell the scheduler up front how many files you plan on registering up
    /// front at creation time using the [I2o2Builder::with_num_registered_files](crate::I2o2Builder::with_num_registered_files)
    /// parameter.
    ///
    /// This method can error when attempting to register a file that is already registered
    /// or there are no free slots available on the ring.
    pub async fn register_file_async(
        &self,
        fd: std::os::fd::RawFd,
        guard: Option<G>,
    ) -> Result<u32, RegisterError> {
        let (reply, rx) = reply::new();

        let message = Message::RegisterResource(Packaged {
            data: Resource::File(fd),
            reply,
            guard,
        });

        self.inner
            .send_async(message)
            .await
            .map_err(|_| RegisterError::SchedulerClosed(SchedulerClosed))?;
        let result = rx.await.map_err(RegisterError::Cancelled)?;
        handle_register_resource_result(result)
    }

    /// Unregister a previously registered file with the ring using the `file_index` asynchronously.
    ///
    /// If a guard was provided when registering, the guard will be dropped once all
    /// inflight operations using the registered file are complete.
    ///
    /// This method can error when attempting to unregister a file that does not exist.
    pub async fn unregister_file_async(
        &self,
        file_index: u32,
    ) -> Result<(), RegisterError> {
        let (reply, rx) = reply::new();

        let message = Message::UnregisterResource(Packaged {
            data: ResourceIndex::File(file_index),
            reply,
            guard: None,
        });

        self.inner
            .send_async(message)
            .await
            .map_err(|_| RegisterError::SchedulerClosed(SchedulerClosed))?;
        let result = rx.await.map_err(RegisterError::Cancelled)?;
        handle_unregister_resource_result(result)
    }

    /// Register a buffer with the ring returning a buffer index that can be
    /// used with `Fixed` operations.
    ///
    /// Registering a buffer with the ring reduces the overhead of calls interacting
    /// with the target buffer by reducing the amount of work the kernel needs to do
    /// on every IO op.
    ///
    /// A `guard` value can be passed, which can be used to ensure the buffer remains alive
    /// and valid for as long as the ring requires it.
    ///
    /// You need to tell the scheduler up front how many buffers you plan on registering up
    /// front at creation time using the [I2o2Builder::with_num_registered_buffers](crate::I2o2Builder::with_num_registered_buffers)
    /// parameter.
    ///
    /// This method can error when there are no free slots available on the ring.
    ///
    /// # Note
    ///
    /// There is no `unregister*` methods for buffers as we currently cannot make a populated
    /// entry in the ring sparse, so when you register the buffer you are effectively leaking
    /// the memory!
    ///
    /// # Safety
    ///
    /// It is the callers responsibility to ensure that the buffer is:
    /// - Safe to send across thread boundaries.
    /// - Is correctly aligned for any ops that use the buffer afterward.
    /// - Valid throughout the entire time the buffer is registered and in use by the scheduler.
    pub unsafe fn register_buffer(
        &self,
        ptr: *mut u8,
        len: usize,
        guard: Option<G>,
    ) -> Result<u32, RegisterError> {
        let (reply, rx) = reply::new();

        let message = Message::RegisterResource(Packaged {
            data: Resource::Buffer(libc::iovec {
                iov_base: ptr as *mut _,
                iov_len: len,
            }),
            reply,
            guard,
        });

        self.inner
            .send(message)
            .map_err(|_| RegisterError::SchedulerClosed(SchedulerClosed))?;
        let result = rx.wait().map_err(RegisterError::Cancelled)?;
        handle_register_resource_result(result)
    }

    /// Register a buffer with the ring asynchronously returning a buffer index that can be
    /// used with `Fixed` operations.
    ///
    /// Registering a buffer with the ring reduces the overhead of calls interacting
    /// with the target buffer by reducing the amount of work the kernel needs to do
    /// on every IO op.
    ///
    /// A `guard` value can be passed, which can be used to ensure the buffer remains alive
    /// and valid for as long as the ring requires it.
    ///
    /// You need to tell the scheduler up front how many buffers you plan on registering up
    /// front at creation time using the [I2o2Builder::with_num_registered_buffers](crate::I2o2Builder::with_num_registered_buffers)
    /// parameter.
    ///
    /// This method can error when there are no free slots available on the ring.
    ///
    /// # Note
    ///
    /// There is no `unregister*` methods for buffers as we currently cannot make a populated
    /// entry in the ring sparse, so when you register the buffer you are effectively leaking
    /// the memory!
    ///
    /// # Safety
    ///
    /// It is the callers responsibility to ensure that the buffer is:
    /// - Safe to send across thread boundaries.
    /// - Is correctly aligned for any ops that use the buffer afterward.
    /// - Valid throughout the entire time the buffer is registered and in use by the scheduler.
    pub async unsafe fn register_buffer_async(
        &self,
        ptr: *mut u8,
        len: usize,
        guard: Option<G>,
    ) -> Result<u32, RegisterError> {
        let (reply, rx) = reply::new();

        let message = Message::RegisterResource(Packaged {
            data: Resource::Buffer(libc::iovec {
                iov_base: ptr as *mut _,
                iov_len: len,
            }),
            reply,
            guard,
        });

        self.inner
            .send_async(message)
            .await
            .map_err(|_| RegisterError::SchedulerClosed(SchedulerClosed))?;
        let result = rx.await.map_err(RegisterError::Cancelled)?;
        handle_register_resource_result(result)
    }
}

fn prepare_many_entries<O: RingOp, G>(
    pairs: impl IntoIterator<Item = (O, Option<G>)>,
) -> (Message<G>, SmallVec<[reply::ReplyReceiver; 4]>) {
    let mut replies = SmallVec::<[reply::ReplyReceiver; 4]>::new();
    let iter = pairs.into_iter().map(|(op, guard)| {
        let (reply, rx) = reply::new();
        replies.push(rx);

        Packaged {
            data: op.into_entry(),
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

fn handle_register_resource_result(result: i32) -> Result<u32, RegisterError> {
    if result == crate::MAGIC_ERRNO_NO_CAPACITY {
        Err(RegisterError::OutOfCapacity)
    } else if result < 0 {
        Err(RegisterError::Io(io::Error::from_raw_os_error(-result)))
    } else {
        Ok(result as u32)
    }
}

fn handle_unregister_resource_result(result: i32) -> Result<(), RegisterError> {
    if result < 0 {
        Err(RegisterError::Io(io::Error::from_raw_os_error(-result)))
    } else {
        Ok(())
    }
}
