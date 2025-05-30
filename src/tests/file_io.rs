use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::time::Duration;

use io_uring::{opcode, types};

use crate::{I2o2Handle, I2o2Scheduler};

#[test]
fn test_sync_buffered_file_io_write() {
    super::try_init_logging();

    let (scheduler, handle) = crate::create_for_current_thread::<()>().unwrap();

    let tmp_file = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::File::options()
        .write(true)
        .read(true)
        .open(tmp_file.path())
        .unwrap();

    write_file(&file, 13, scheduler, handle);
}

#[test]
fn test_sync_direct_io_file_io_write() {
    super::try_init_logging();

    let (scheduler, handle) = crate::create_for_current_thread::<()>().unwrap();

    let tmp_file = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::File::options()
        .write(true)
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(tmp_file.path())
        .unwrap();

    write_file(&file, 4096, scheduler, handle);
}

#[tokio::test]
async fn test_async_buffered_file_io_write() {
    super::try_init_logging();

    let (scheduler, handle) = crate::create_and_spawn::<()>().unwrap();

    let tmp_file = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::File::options()
        .write(true)
        .read(true)
        .open(tmp_file.path())
        .unwrap();

    write_file_async(&file, 13, handle).await;

    scheduler.join().unwrap().unwrap();
}

#[tokio::test]
async fn test_async_direct_io_file_io_write() {
    super::try_init_logging();

    let (scheduler, handle) = crate::create_and_spawn::<()>().unwrap();

    let tmp_file = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::File::options()
        .write(true)
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(tmp_file.path())
        .unwrap();

    write_file_async(&file, 4096, handle).await;

    scheduler.join().unwrap().unwrap();
}

fn write_file(
    file: &std::fs::File,
    buffer_size: usize,
    scheduler: I2o2Scheduler<()>,
    handle: I2o2Handle<()>,
) {
    let sample = vec![1; buffer_size];

    let op = opcode::Write::new(
        types::Fd(file.as_raw_fd()),
        sample.as_ptr(),
        sample.len() as u32,
    )
    .build();
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
    if result < 0 {
        panic!(
            "operation errored: {:?}",
            io::Error::from_raw_os_error(result)
        );
    } else {
        eprintln!("wrote {result} bytes");
    }

    drop(sample);
}

async fn write_file_async(
    file: &std::fs::File,
    buffer_size: usize,
    handle: I2o2Handle<()>,
) {
    let sample = vec![1; buffer_size];

    let op = opcode::Write::new(
        types::Fd(file.as_raw_fd()),
        sample.as_ptr(),
        sample.len() as u32,
    )
    .build();
    eprintln!("built op");

    let reply = unsafe {
        handle
            .submit_async(op, None)
            .await
            .expect("scheduler should be running")
    };
    eprintln!("completed submit");

    let result = reply.await.expect("operation should complete");
    eprintln!("completed result: {result}");
    if result < 0 {
        panic!(
            "operation errored: {:?}",
            io::Error::from_raw_os_error(result)
        );
    } else {
        eprintln!("wrote {result} bytes");
    }

    drop(sample);
}
