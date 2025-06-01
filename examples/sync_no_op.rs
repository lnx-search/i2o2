use std::io;
use std::sync::Arc;

#[cfg_attr(test, test)]
fn main() -> io::Result<()> {
    println!("creating our scheduler worker");

    // For most cases you can use `create_and_spawn` which uses a set of sane defaults.
    // The scheduler handle can be cheaply cloned and works in both sync and async contexts.
    let (thread_handle, scheduler_handle) = i2o2::create_and_spawn()?;

    // Now we can issue IO calls which would ordinarily be a syscall, like reading a file.
    // Now, this API is still unsafe, because you are responsible for ensuring the op is safe
    // to perform and buffers, etc... remain valid.

    let timeout = i2o2::types::Timespec::new().nsec(50_000);
    let op = i2o2::opcode::Timeout::new(&timeout as *const _).build();

    // However, some utils are provided like the  `guard` parameter,  which will only be
    // dropped once the operation is complete and no longer needed by the kernel.
    let guard = Arc::new(());

    println!("submitting {op:?} to the scheduler");
    let reply = unsafe {
        scheduler_handle
            .submit(op, Some(guard.clone()))
            .expect("submit op to scheduler")
    };
    println!("reply future created: {reply:?}");

    // Our clone of `guard` should live until the operation completes.
    assert_eq!(Arc::strong_count(&guard), 2);

    // We can synchronously or asynchronously wait for the reply, which will give us
    // back the result that the syscall equivalent of the operation would return.
    // I.e. `opcode::Write` would return the same value as `pwrite(2)`.
    let reply = reply.wait();
    println!("our task completed with reply: {reply:?}");
    assert!(reply.is_ok());

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
