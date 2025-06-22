use std::io;
use std::io::Write;
use std::os::fd::AsRawFd;

use crate::opcode::types;
use crate::{I2o2Handle, opcode};

#[test]
fn test_sync_io_write_fixed_size64() {
    super::try_init_logging();

    let (scheduler, handle) = crate::builder()
        .with_num_registered_files(1)
        .with_num_registered_buffers(1)
        .try_spawn::<()>()
        .unwrap();

    let tmp_file = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::File::options()
        .write(true)
        .read(true)
        .open(tmp_file.path())
        .unwrap();

    write_file(&file, 13, handle);

    scheduler.join().unwrap().unwrap();
}

#[test]
fn test_sync_io_read_fixed_size64() {
    super::try_init_logging();

    let (scheduler, handle) = crate::builder()
        .with_num_registered_files(1)
        .with_num_registered_buffers(1)
        .try_spawn::<()>()
        .unwrap();

    let tmp_file = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::File::options()
        .write(true)
        .read(true)
        .open(tmp_file.path())
        .unwrap();

    read_file(file, handle);

    scheduler.join().unwrap().unwrap();
}

fn read_file(mut file: std::fs::File, handle: I2o2Handle<()>) {
    file.write_all(b"hello, world!").unwrap();

    let mut sample = vec![1; 13];

    let file_id = handle.register_file(file.as_raw_fd(), None).unwrap();
    let buffer_id = unsafe {
        handle
            .register_buffer(sample.as_mut_ptr(), sample.len(), None)
            .unwrap()
    };

    let op = opcode::ReadFixed::new(
        types::Fixed(file_id),
        sample.as_mut_ptr(),
        sample.len(),
        buffer_id,
        0,
    );
    eprintln!("built op");

    let reply = unsafe {
        handle
            .submit(op, None)
            .expect("scheduler should be running")
    };
    eprintln!("completed submit");

    drop(handle);

    let result = reply.wait().expect("operation should complete");
    eprintln!("completed result: {result}");
    if result < 0 {
        panic!(
            "operation errored: {:?}",
            io::Error::from_raw_os_error(-result)
        );
    } else {
        eprintln!("read {result} bytes");
        assert_eq!(result, 13);
    }

    drop(sample);
}

fn write_file(file: &std::fs::File, buffer_size: usize, handle: I2o2Handle<()>) {
    let mut sample = vec![1; buffer_size];

    let file_id = handle.register_file(file.as_raw_fd(), None).unwrap();
    let buffer_id = unsafe {
        handle
            .register_buffer(sample.as_mut_ptr(), sample.len(), None)
            .unwrap()
    };

    let op = opcode::WriteFixed::new(
        types::Fixed(file_id),
        sample.as_ptr(),
        sample.len(),
        buffer_id,
        0,
    );
    eprintln!("built op");

    let reply = unsafe {
        handle
            .submit(op, None)
            .expect("scheduler should be running")
    };
    eprintln!("completed submit");

    drop(handle);

    let result = reply.wait().expect("operation should complete");
    eprintln!("completed result: {result}");
    if result < 0 {
        panic!(
            "operation errored: {:?}",
            io::Error::from_raw_os_error(-result)
        );
    } else {
        eprintln!("wrote {result} bytes");
    }

    drop(sample);
}
