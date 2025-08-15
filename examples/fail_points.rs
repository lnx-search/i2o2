use std::io;
use std::sync::Arc;

#[cfg_attr(test, test)]
fn main() -> io::Result<()> {
    // Setup the fail scenario, you can find out more about this at
    // https://github.com/tikv/fail-rs
    let scenario = fail::FailScenario::setup();

    // Alternatively you can set `FAILPOINTS=i2o2::fail::try_get_result=return(-4)`
    // as an environment variable.
    fail::cfg("i2o2::fail::try_get_result", "return(-4)").unwrap();

    let (thread_handle, scheduler_handle) = i2o2::create_and_spawn()?;

    let op = i2o2::opcode::Nop::new();

    println!("submitting {op:?} to the scheduler");
    let reply = unsafe {
        scheduler_handle
            .submit(op, Option::<()>::None)
            .expect("submit op to scheduler")
    };
    println!("reply future created: {reply:?}");

    // Since our fail point is set, this is guaranteed to match what we've set.
    let reply = reply.try_get_result();
    println!("our task completed with reply: {reply:?}");
    assert_eq!(reply, Ok(-4));

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
