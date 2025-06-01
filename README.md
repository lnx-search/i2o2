# I2o2

A tiny scheduler for executing IO calls with an io_uring executor.

This project is designed for [lnx](https://github.com/lnx-search/lnx) as a replacement to glommio's file system API
and is a relatively low-level API.

In particular, the system only lightly attempts to prevent you messing stuff up, hence why almost all calls
are unsafe. The only thing it really protects against buffers being dropped early.

## FAQ

#### What is the minimum supported kernel version

Version `5.15`+ is supported by I2o2, we may advance this requirement in future releases.
It is recommended to use kernel version `5.19`+ in order to support most features, but realistically,
the newer, the better (and faster :))

#### Should I use this in my own project?

Probably not unless you are sure you need the performance and are willing to invest the time into ensuring your
system is implemented safely.

#### Why not use glommio or tokio-uring?

In our use case, our main runtime is the [tokio](https://tokio.rs/) multithreaded scheduler, the only thing we
need a separate scheduler for is performing heavy file IO calls, which is why we reached for io_uring.

However, we do not need any of the task scheduling these other libraries provide and just adds another 
layer of complexity to an already complex storage system. 

So we have effectively stripped out all task scheduling logic and in effect, are left with just an actor for
completing IO calls without blocking our main runtime.

#### Do I need multiple schedulers to handle my load?

Probably not, at least I haven't had a situation where that is the case. In the no-op benchmarks which effectively
just tests the overhead of the scheduler and io_uring, we can reach upto 3 million ops per second with 16 concurrent 
workers all pushing data to the scheduler.

In theory, you can increase this number even higher by using the `submit_many*` API for sending multiple IO ops
in a single channel message.

## Example

```rust
use std::io;
use std::sync::Arc;

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("creating our scheduler worker");

    // For most cases you can use `create_and_spawn` which uses a set of sane defaults.
    // The scheduler handle can be cheaply cloned and works in both sync and async contexts.
    let (thread_handle, scheduler_handle) = i2o2::create_and_spawn()?;

    // Now we can issue IO calls which would ordinarily be a syscall, like reading a file.
    // Now, this API is still unsafe, because you are responsible for ensuring the op is safe
    // to perform and buffers, etc... remain valid.

    let op = i2o2::opcode::Nop::new().build();

    // However, some utils are provided like the  `guard` parameter,  which will only be
    // dropped once the operation is complete and no longer needed by the kernel.
    let guard = Arc::new(());

    println!("submitting {op:?} to the scheduler");

    // We send the message to the scheduler asynchronously!
    let reply = unsafe {
        scheduler_handle
            .submit_async(op, Some(guard.clone()))
            .await
            .expect("submit op to scheduler")
    };
    println!("reply future created: {reply:?}");

    // We can synchronously or asynchronously wait for the reply, which will give us
    // back the result that the syscall equivalent of the operation would return.
    // I.e. `opcode::Write` would return the same value as `pwrite(2)`.
    let reply = reply.await;
    println!("our task completed with reply: {reply:?}");
    assert_eq!(reply, Ok(0));

    println!("shutting down scheduler");
    // The scheduler will shut down once all handles are dropped.
    // Any outstanding tasks will finish gracefully.
    drop(scheduler_handle);

    thread_handle
        .join()
        .expect("scheduler should never panic")?;

    println!("scheduler shutdown complete!");

    Ok(())
}
```

You can see more examples in the [example directory](/examples)


## Development

Anyone is welcome to contribute, just be aware I2o2 only wants to act as a slightly higher wrapper around io_uring
and just provide the async wrapper on top. We don't plan any support for making it more like a traditional runtime.

We also assume you have a very new kernel version for running tests, and by new I mean 6.1+ at least.