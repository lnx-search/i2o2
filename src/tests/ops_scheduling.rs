use std::os::fd::AsRawFd;
use std::{fs, io};

use io_uring::{IoUring, opcode, types};

#[test]
fn test_scheduler_noop() {
    super::try_init_logging();

    let (scheduler, handle) = crate::create_for_current_thread::<()>().unwrap();

    let op = opcode::Nop::new().build();
    eprintln!("built op");

    let reply = unsafe {
        handle
            .submit(op, None)
            .expect("scheduler should be running")
    };
    eprintln!("completed submit");
    drop(handle);

    scheduler.run().expect("run scheduler");

    let result = reply.wait().expect("operation should complete");
    eprintln!("completed result: {result}");
    if result != 0 {
        panic!(
            "operation errored: {:?}",
            io::Error::from_raw_os_error(result)
        );
    }
}
