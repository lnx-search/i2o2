use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::time::Duration;

use crate::RegisterError;

#[test]
fn test_register_buffers_sync() {
    super::try_init_logging();

    let (scheduler, handle) = crate::builder()
        .with_num_registered_buffers(1)
        .try_spawn::<()>()
        .unwrap();

    let mut buffer = vec![0; 1024];
    let id = unsafe {
        handle
            .register_buffer(buffer.as_mut_ptr(), buffer.len(), None)
            .expect("scheduler to accept buffer")
    };
    assert_eq!(id, 0);

    // ring should be out of capacity
    let err = unsafe {
        handle
            .register_buffer(buffer.as_mut_ptr(), buffer.len(), None)
            .expect_err("scheduler should return error")
    };
    assert_eq!(err.to_string(), RegisterError::OutOfCapacity.to_string());

    drop(handle);
    drop(buffer);

    scheduler.join().unwrap().unwrap();
}

#[tokio::test]
async fn test_register_buffers_async() {
    super::try_init_logging();

    let (scheduler, handle) = crate::builder()
        .with_num_registered_buffers(1)
        .try_spawn::<()>()
        .unwrap();

    let mut buffer = vec![0; 1024];
    let id = unsafe {
        handle
            .register_buffer_async(buffer.as_mut_ptr(), buffer.len(), None)
            .await
            .expect("scheduler to accept buffer")
    };
    assert_eq!(id, 0);

    // ring should be out of capacity
    let err = unsafe {
        handle
            .register_buffer_async(buffer.as_mut_ptr(), buffer.len(), None)
            .await
            .expect_err("scheduler should return error")
    };
    assert_eq!(err.to_string(), RegisterError::OutOfCapacity.to_string());

    drop(handle);
    drop(buffer);

    scheduler.join().unwrap().unwrap();
}

#[test]
fn test_register_files_sync() {
    super::try_init_logging();

    let (scheduler, handle) = crate::builder()
        .with_num_registered_files(1)
        .try_spawn()
        .unwrap();

    let file = tempfile::tempfile().unwrap();
    let guard = Arc::new(());

    let id = handle
        .register_file(file.as_raw_fd(), Some(guard.clone()))
        .expect("scheduler to accept file");
    assert_eq!(id, 0);
    assert_eq!(Arc::strong_count(&guard), 2);

    // ring should be out of capacity
    let err = handle
        .register_file(file.as_raw_fd(), None)
        .expect_err("scheduler should return error");
    assert_eq!(err.to_string(), RegisterError::OutOfCapacity.to_string());

    handle
        .unregister_file(id)
        .expect("scheduler to unregister file");
    assert_eq!(id, 0);

    // use a nop to run through a cycle and give io_uring a chance
    // to handle the file unregistering.
    unsafe {
        let nop = crate::opcode::Nop::new();
        handle.submit(nop, None).unwrap();
    }
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(Arc::strong_count(&guard), 1);

    drop(handle);
    drop(file);

    scheduler.join().unwrap().unwrap();
}

#[tokio::test]
async fn test_register_files_async() {
    super::try_init_logging();

    let (scheduler, handle) = crate::builder()
        .with_num_registered_files(1)
        .try_spawn()
        .unwrap();

    let file = tempfile::tempfile().unwrap();
    let guard = Arc::new(());

    let id = handle
        .register_file_async(file.as_raw_fd(), Some(guard.clone()))
        .await
        .expect("scheduler to accept file");
    assert_eq!(id, 0);
    assert_eq!(Arc::strong_count(&guard), 2);

    // ring should be out of capacity
    let err = handle
        .register_file_async(file.as_raw_fd(), None)
        .await
        .expect_err("scheduler should return error");
    assert_eq!(err.to_string(), RegisterError::OutOfCapacity.to_string());

    handle
        .unregister_file_async(id)
        .await
        .expect("scheduler to unregister file");
    assert_eq!(id, 0);

    // use a nop to run through a cycle and give io_uring a chance
    // to handle the file unregistering.
    unsafe {
        let nop = crate::opcode::Nop::new();
        handle.submit_async(nop, None).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(Arc::strong_count(&guard), 1);

    drop(handle);
    drop(file);

    scheduler.join().unwrap().unwrap();
}
