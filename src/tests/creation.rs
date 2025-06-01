use std::time::Duration;

use fail::FailScenario;

use crate::I2o2Builder;

#[rstest::rstest]
#[case::default(crate::builder())]
#[case::with_io_polling(
    crate::builder()
        .with_io_polling(true)
)]
#[case::with_io_polling_off(
    crate::builder()
        .with_io_polling(false)
)]
#[case::with_sqe_polling_default_timeout(
    crate::builder()
        .with_sqe_polling(true)
)]
#[case::with_sqe_polling_off(
    crate::builder()
        .with_sqe_polling(false)
)]
#[case::with_sqe_polling_custom_timeout(
    crate::builder()
        .with_sqe_polling(true)
        .with_sqe_polling_timeout(Duration::from_millis(50))
)]
#[should_panic]
#[case::with_sqe_polling_custom_timeout_sanity_check_fail(
    crate::builder()
        .with_sqe_polling(true)
        .with_sqe_polling_timeout(Duration::from_secs(100))
)]
#[should_panic]
#[case::with_sqe_polling_custom_timeout_before_enable(
    crate::builder()
        .with_sqe_polling_timeout(Duration::from_millis(50))
)]
#[case::with_sqe_polling_pin_cpu(
    crate::builder()
        .with_sqe_polling(true)
        .with_sqe_polling_pin_cpu(0)
)]
#[should_panic]
#[case::with_sqe_polling_pin_cpu_before_enable(
    crate::builder()
        .with_sqe_polling_pin_cpu(0)
)]
#[case::with_defer_task_run(
    crate::builder()
        .with_defer_task_run(true)
)]
#[case::with_queue_size(
    crate::builder()
        .with_queue_size(64)
)]
#[case::with_register_buffers(
    crate::builder()
        .with_num_registered_buffers(64)
)]
#[case::with_register_files(
    crate::builder()
        .with_num_registered_files(64)
)]
fn test_scheduler_creation(#[case] builder: I2o2Builder) {
    let (_scheduler, _handle) = builder
        .try_create::<()>()
        .expect("builder should be produce valid scheduler");
}

#[rstest::rstest]
#[case::failpoint_kernel_v5_13_default(crate::builder())]
#[case::failpoint_kernel_v5_15_default(crate::builder())]
#[case::failpoint_kernel_v5_19_default(crate::builder())]
#[case::failpoint_kernel_v6_0_default(crate::builder())]
#[case::failpoint_kernel_v6_1_default(crate::builder())]
#[case::failpoint_kernel_v5_13_io_polling(crate::builder().with_io_polling(true))]
#[case::failpoint_kernel_v5_15_io_polling(crate::builder().with_io_polling(true))]
#[case::failpoint_kernel_v5_19_io_polling(crate::builder().with_io_polling(true))]
#[case::failpoint_kernel_v6_0_io_polling(crate::builder().with_io_polling(true))]
#[case::failpoint_kernel_v6_1_io_polling(crate::builder().with_io_polling(true))]
#[case::failpoint_kernel_v5_13_sqe_polling(crate::builder().with_sqe_polling(true))]
#[case::failpoint_kernel_v5_15_sqe_polling(crate::builder().with_sqe_polling(true))]
#[case::failpoint_kernel_v5_19_sqe_polling(crate::builder().with_sqe_polling(true))]
#[case::failpoint_kernel_v6_0_sqe_polling(crate::builder().with_sqe_polling(true))]
#[case::failpoint_kernel_v6_1_sqe_polling(crate::builder().with_sqe_polling(true))]
#[should_panic]
#[case::failpoint_kernel_v5_13_defer_task_run(crate::builder().with_defer_task_run(true))]
#[should_panic]
#[case::failpoint_kernel_v5_15_defer_task_run(crate::builder().with_defer_task_run(true))]
#[should_panic]
#[case::failpoint_kernel_v5_19_defer_task_run(crate::builder().with_defer_task_run(true))]
#[should_panic]
#[case::failpoint_kernel_v6_0_defer_task_run(crate::builder().with_defer_task_run(true))]
#[case::failpoint_kernel_v6_1_defer_task_run(crate::builder().with_defer_task_run(true))]
#[case::failpoint_kernel_v5_13_registered_files(crate::builder().with_num_registered_files(8))]
#[case::failpoint_kernel_v5_15_registered_files(crate::builder().with_num_registered_files(8))]
#[case::failpoint_kernel_v5_19_registered_files(crate::builder().with_num_registered_files(8))]
#[case::failpoint_kernel_v6_0_registered_files(crate::builder().with_num_registered_files(8))]
#[case::failpoint_kernel_v6_1_registered_files(crate::builder().with_num_registered_files(8))]
#[should_panic]
#[case::failpoint_kernel_v5_13_registered_buffers(crate::builder().with_num_registered_buffers(8))]
#[should_panic]
#[case::failpoint_kernel_v5_15_registered_buffers(crate::builder().with_num_registered_buffers(8))]
#[case::failpoint_kernel_v5_19_registered_buffers(crate::builder().with_num_registered_buffers(8))]
#[case::failpoint_kernel_v6_0_registered_buffers(crate::builder().with_num_registered_buffers(8))]
#[case::failpoint_kernel_v6_1_registered_buffers(crate::builder().with_num_registered_buffers(8))]
fn failpoint_test_scheduler_creation(#[case] builder: I2o2Builder) {
    let scenario = FailScenario::setup();
    let (_scheduler, _handle) = builder
        .try_create::<()>()
        .expect("builder should be produce valid scheduler");
    scenario.teardown();
}
