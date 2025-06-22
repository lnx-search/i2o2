use std::io;
use std::os::fd::AsRawFd;

#[test]
fn test_madvise_op() {
    super::try_init_logging();

    let (scheduler, handle) = crate::create_for_current_thread::<()>().unwrap();

    let mut mem = memmap2::MmapOptions::new().len(128).map_anon().unwrap();

    let op = crate::opcode::Madvise::new(mem.as_mut_ptr(), 128, libc::MADV_RANDOM);

    let reply = unsafe { handle.submit(op, None).unwrap() };

    drop(handle);
    scheduler.run().unwrap();

    let result = reply.wait().unwrap();
    if result < 0 {
        panic!("got error: {}", io::Error::from_raw_os_error(-result))
    }
}

#[test]
fn test_fallocate_op() {
    super::try_init_logging();

    let (scheduler, handle) = crate::create_for_current_thread::<()>().unwrap();

    let file = tempfile::tempfile().unwrap();

    let op = crate::opcode::Fallocate::new(crate::types::Fd(file.as_raw_fd()), 0, 0, 64);

    let reply = unsafe { handle.submit(op, None).unwrap() };

    drop(handle);
    scheduler.run().unwrap();

    let result = reply.wait().unwrap();
    if result < 0 {
        panic!("got error: {}", io::Error::from_raw_os_error(-result))
    }
}

#[test]
fn test_ftruncate_op() {
    super::try_init_logging();

    let (scheduler, handle) = crate::create_for_current_thread::<()>().unwrap();

    let file = tempfile::tempfile().unwrap();

    let op = crate::opcode::Ftruncate::new(crate::types::Fd(file.as_raw_fd()), 64);

    let reply = unsafe { handle.submit(op, None).unwrap() };

    drop(handle);
    scheduler.run().unwrap();

    let result = reply.wait().unwrap();
    if result < 0 {
        panic!("got error: {}", io::Error::from_raw_os_error(-result))
    }
}

#[rstest::rstest]
#[case::fsync_mode_full(crate::opcode::FSyncMode::Full)]
#[case::fsync_mode_data(crate::opcode::FSyncMode::Data)]
fn test_fsync_op(#[case] mode: crate::opcode::FSyncMode) {
    super::try_init_logging();

    let (scheduler, handle) = crate::create_for_current_thread::<()>().unwrap();

    let file = tempfile::tempfile().unwrap();

    let op = crate::opcode::Fsync::new(crate::types::Fd(file.as_raw_fd()), mode);

    let reply = unsafe { handle.submit(op, None).unwrap() };

    drop(handle);
    scheduler.run().unwrap();

    let result = reply.wait().unwrap();
    if result < 0 {
        panic!("got error: {}", io::Error::from_raw_os_error(-result))
    }
}
