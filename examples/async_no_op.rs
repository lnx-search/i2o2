use std::io;
use std::sync::Arc;

#[cfg_attr(not(test), tokio::main)]
#[cfg_attr(test, tokio::test)]
async fn main() -> io::Result<()> {
    println!("creating our scheduler worker");

    // For most cases you can use `create_and_spawn` which uses a set of sane defaults.
    // The scheduler handle can be cheaply cloned and works in both sync and async contexts.
    let (thread_handle, scheduler_handle) = i2o2::create_and_spawn()?;

    // Now we can issue IO calls which would ordinarily be a syscall, like reading a file.
    // Now, this API is still unsafe, because you are responsible for ensuring the op is safe
    // to perform and buffers, etc... remain valid.

    let op = i2o2::opcode::Nop::new();

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
