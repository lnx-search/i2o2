use std::io;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;

use fail::FailScenario;

use crate::{SchedulerClosed, opcode, I2o2Builder};

#[rstest::rstest]
#[case::size64(crate::builder())]
#[case::size128(crate::builder().with_sqe_size128(true))]
fn test_scheduler_noop(#[case] builder: I2o2Builder) {
    super::try_init_logging();
    
    let (scheduler, handle) = builder
        .try_spawn::<()>()
        .unwrap();
    let handle2 = handle.clone();

    let op = opcode::Nop::new();
    eprintln!("built op");

    let reply = unsafe {
        handle2
            .submit(op, None)
            .expect("scheduler should be running")
    };
    eprintln!("completed submit");
    drop(handle);
    drop(handle2);

    let result = reply.wait().expect("operation should complete");
    eprintln!("completed result: {result}");
    if result != 0 {
        panic!(
            "operation errored: {:?}",
            io::Error::from_raw_os_error(result)
        );
    }
    
    scheduler.join().unwrap();
}

#[rstest::rstest]
#[case::size64(crate::builder())]
#[case::size128(crate::builder().with_sqe_size128(true))]
fn test_scheduler_noop_with_guard(#[case] builder: I2o2Builder) {
    super::try_init_logging();

    let (scheduler, handle) = builder
        .try_spawn()
        .unwrap();
    let guard = Arc::new(());

    let op = opcode::Nop::new();
    eprintln!("built op");

    let reply = unsafe {
        handle
            .submit(op, Some(guard.clone()))
            .expect("scheduler should be running")
    };
    assert_eq!(Arc::strong_count(&guard), 2);

    eprintln!("completed submit");
    drop(handle);

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
    
    scheduler.join().unwrap();
}

#[test]
fn failpoint_scheduler_create_fail_try_spawn() {
    let scenario = FailScenario::setup();
    let result = crate::create_and_spawn::<()>();
    match result {
        Err(e) => assert_eq!(e.kind(), ErrorKind::Other),
        Ok(_) => panic!("scheduler should fail to be created"),
    }
    scenario.teardown();
}

#[test]
fn failpoint_scheduler_run_fail_try_spawn() {
    let scenario = FailScenario::setup();
    let (scheduler, handle) = crate::create_and_spawn::<()>().unwrap();

    // Give it a moment to shut down...
    while !scheduler.is_running() {
        std::thread::sleep(Duration::from_millis(10));
    }

    let op = opcode::Nop::new();
    let err = unsafe { handle.submit(op, None).unwrap_err() };
    assert!(matches!(err, SchedulerClosed));

    let err = scheduler.join().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Other);
    scenario.teardown();
}
