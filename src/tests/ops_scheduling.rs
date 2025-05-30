use std::io;
use std::sync::Arc;

use io_uring::opcode;

#[test]
fn test_scheduler_noop() {
    super::try_init_logging();

    let (scheduler, handle) = crate::create_for_current_thread::<()>().unwrap();
    let handle2 = handle.clone();

    let op = opcode::Nop::new().build();
    eprintln!("built op");

    let reply = unsafe {
        handle2
            .submit(op, None)
            .expect("scheduler should be running")
    };
    eprintln!("completed submit");
    drop(handle);
    drop(handle2);

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

#[test]
fn test_scheduler_noop_with_guard() {
    super::try_init_logging();

    let (scheduler, handle) = crate::create_for_current_thread().unwrap();
    let guard = Arc::new(());

    let op = opcode::Nop::new().build();
    eprintln!("built op");

    let reply = unsafe {
        handle
            .submit(op, Some(guard.clone()))
            .expect("scheduler should be running")
    };
    assert_eq!(Arc::strong_count(&guard), 2);

    eprintln!("completed submit");
    drop(handle);

    scheduler.run().expect("run scheduler");
    assert_eq!(Arc::strong_count(&guard), 1);
    eprintln!("got reply: {reply:?}");

    let result = reply.wait().expect("operation should complete");
    eprintln!("completed result: {result}");
    if result != 0 {
        panic!(
            "operation errored: {:?}",
            io::Error::from_raw_os_error(result)
        );
    }
    assert_eq!(Arc::strong_count(&guard), 1);
}

#[test]
fn test_submit_many_sync() {
    super::try_init_logging();

    let (scheduler, handle) = crate::create_and_spawn::<()>().unwrap();

    eprintln!("built op");

    let replies = unsafe {
        handle
            .submit_many_entries(
                std::iter::repeat_with(|| {
                    let op = opcode::Nop::new().build();
                    (op, None)
                })
                .take(5),
            )
            .expect("scheduler should be running")
    };
    eprintln!("completed submit");

    for reply in replies {
        let result = reply.wait().expect("operation should complete");
        eprintln!("completed result: {result}");
        if result != 0 {
            panic!(
                "operation errored: {:?}",
                io::Error::from_raw_os_error(result)
            );
        }
    }

    drop(handle);
    scheduler.join().unwrap().unwrap();
}

#[tokio::test]
async fn test_submit_many_async() {
    super::try_init_logging();

    let (scheduler, handle) = crate::create_and_spawn::<()>().unwrap();

    eprintln!("built op");

    let replies = unsafe {
        handle
            .submit_many_entries_async(
                std::iter::repeat_with(|| {
                    let op = opcode::Nop::new().build();
                    (op, None)
                })
                .take(5),
            )
            .await
            .expect("scheduler should be running")
    };
    eprintln!("completed submit");

    for reply in replies {
        let result = reply.await.expect("operation should complete");
        eprintln!("completed result: {result}");
        if result != 0 {
            panic!(
                "operation errored: {:?}",
                io::Error::from_raw_os_error(result)
            );
        }
    }

    drop(handle);
    scheduler.join().unwrap().unwrap();
}
