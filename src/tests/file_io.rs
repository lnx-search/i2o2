use std::io;
use std::os::fd::AsRawFd;

use io_uring::{opcode, types};

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

    let sample = vec![1; 128];

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
    if result != -1 {
        panic!(
            "operation errored: {:?}",
            io::Error::from_raw_os_error(result)
        );
    }

    drop(file);
    drop(sample);
}
