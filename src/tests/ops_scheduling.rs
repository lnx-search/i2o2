use std::io;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;

use fail::FailScenario;

use crate::opcode::RingOp;
use crate::{I2o2Handle, I2o2Scheduler, SchedulerClosed, opcode};

#[rstest::rstest]
#[case::size64(crate::create_for_current_thread::<Arc<()>>().unwrap())]
#[case::size128(crate::builder().with_sqe_size128(true).try_create::<Arc<()>>().unwrap())]
fn test_scheduler_noop(#[case] pair: (I2o2Scheduler<Arc<()>>, I2o2Handle<Arc<()>>)) {
    super::try_init_logging();

    let (scheduler, handle) = pair;
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

#[rstest::rstest]
#[case::size64(crate::create_for_current_thread::<Arc<()>>().unwrap())]
#[case::size128(crate::builder().with_sqe_size128(true).try_create::<Arc<()>>().unwrap())]
fn test_scheduler_noop_with_guard(
    #[case] pair: (I2o2Scheduler<Arc<()>>, I2o2Handle<Arc<()>>),
) {
    super::try_init_logging();

    let (scheduler, handle) = pair;
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
    while !scheduler.is_finished() {
        std::thread::sleep(Duration::from_millis(10));
    }

    let op = opcode::Nop::new();
    let err = unsafe { handle.submit(op, None).unwrap_err() };
    assert!(matches!(err, SchedulerClosed));

    let err = scheduler.join().unwrap().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Other);
    scenario.teardown();
}

#[test]
fn test_io_prio() {
    super::try_init_logging();

    let (scheduler, handle) = crate::create_for_current_thread::<()>().unwrap();
    let handle2 = handle.clone();

    let mut op = opcode::Nop::new();
    op.set_io_priority(0);
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
            io::Error::from_raw_os_error(-result)
        );
    }
}

#[test]
fn test_will_block() {
    super::try_init_logging();

    let (scheduler, handle) = crate::create_for_current_thread::<()>().unwrap();
    let handle2 = handle.clone();

    let mut op = opcode::Nop::new();
    op.will_block();
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
            io::Error::from_raw_os_error(-result)
        );
    }
}
