use std::time::Duration;

use crate::I2o2Builder;

#[rstest::rstest]
#[case::default(crate::builder())]
#[case::with_io_polling(
    crate::builder()
        .with_io_polling(true)
)]
#[case::with_sqe_polling_default_timeout(
    crate::builder()
        .with_sqe_polling(true)
)]
#[case::with_sqe_polling_custom_timeout(
    crate::builder()
        .with_sqe_polling(true)
        .with_sqe_polling_timeout(Duration::from_millis(50))
)]
#[case::with_sqe_polling_pin_cpu(
    crate::builder()
        .with_sqe_polling(true)
        .with_sqe_polling_pin_cpu(0)
)]
#[case::with_defer_task_run(
    crate::builder()
        .with_defer_task_run(true)
)]
fn test_scheduler_creation(#[case] builder: I2o2Builder) {
    let (_scheduler, _handle) = builder
        .try_create::<()>()
        .expect("builder should be produce valid scheduler");
}
