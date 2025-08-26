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
#[case::with_queue_size(
    crate::builder()
        .with_queue_size(64)
)]
#[case::with_ring_depth(
    crate::builder()
        .with_ring_depth(64)
)]
#[case::with_register_buffers(
    crate::builder()
        .with_num_registered_buffers(64)
)]
#[case::with_register_files(
    crate::builder()
        .with_num_registered_files(64)
)]
// -- Begin size 128 tests
#[case::default_size128(
    crate::builder()
        .with_sqe_size128(true)
)]
#[case::with_size128_io_polling(
    crate::builder()
        .with_sqe_size128(true)
        .with_io_polling(true)
)]
#[case::with_size128_io_polling_off(
    crate::builder()
        .with_sqe_size128(true)
        .with_io_polling(false)
)]
#[case::with_size128_queue_size(
    crate::builder()
        .with_sqe_size128(true)
        .with_queue_size(64)
)]
#[case::with_size128_register_buffers(
    crate::builder()
        .with_sqe_size128(true)
        .with_num_registered_buffers(64)
)]
#[case::with_size128_register_files(
    crate::builder()
        .with_sqe_size128(true)
        .with_num_registered_files(64)
)]
#[case::with_size128_register_files(
    crate::builder()
        .skip_unsupported_features(true)
)]
fn test_scheduler_creation(#[case] builder: I2o2Builder) {
    let (_scheduler, _handle) = builder
        .try_create::<()>()
        .expect("builder should be produce valid scheduler");
}

#[rstest::rstest]
#[should_panic]
#[case::failpoint_kernel_v5_13_default(crate::builder())]
#[case::failpoint_kernel_v5_15_default(crate::builder())]
#[case::failpoint_kernel_v5_19_default(crate::builder())]
#[case::failpoint_kernel_v6_0_default(crate::builder())]
#[case::failpoint_kernel_v6_1_default(crate::builder())]
#[should_panic]
#[case::failpoint_kernel_v5_13_io_polling(crate::builder().with_io_polling(true))]
#[case::failpoint_kernel_v5_15_io_polling(crate::builder().with_io_polling(true))]
#[case::failpoint_kernel_v5_19_io_polling(crate::builder().with_io_polling(true))]
#[case::failpoint_kernel_v6_0_io_polling(crate::builder().with_io_polling(true))]
#[case::failpoint_kernel_v6_1_io_polling(crate::builder().with_io_polling(true))]
#[should_panic]
#[case::failpoint_kernel_v5_13_defer_task_run(crate::builder().with_coop_task_run(true))]
#[should_panic]
#[case::failpoint_kernel_v5_15_defer_task_run(crate::builder().with_coop_task_run(true))]
#[case::failpoint_kernel_v5_19_defer_task_run(crate::builder().with_coop_task_run(true))]
#[case::failpoint_kernel_v6_0_defer_task_run(crate::builder().with_coop_task_run(true))]
#[case::failpoint_kernel_v6_1_defer_task_run(crate::builder().with_coop_task_run(true))]
#[should_panic]
#[case::failpoint_kernel_v5_13_registered_files(crate::builder().with_num_registered_files(8))]
#[case::failpoint_kernel_v5_15_registered_files(crate::builder().with_num_registered_files(8))]
#[case::failpoint_kernel_v5_19_registered_files(crate::builder().with_num_registered_files(8))]
#[case::failpoint_kernel_v6_0_registered_files(crate::builder().with_num_registered_files(8))]
#[case::failpoint_kernel_v6_1_registered_files(crate::builder().with_num_registered_files(8))]
#[should_panic]
#[case::failpoint_kernel_v5_13_registered_buffers(crate::builder().with_num_registered_buffers(8))]
#[case::failpoint_kernel_v5_15_registered_buffers(crate::builder().with_num_registered_buffers(8))]
#[case::failpoint_kernel_v5_19_registered_buffers(crate::builder().with_num_registered_buffers(8))]
#[case::failpoint_kernel_v6_0_registered_buffers(crate::builder().with_num_registered_buffers(8))]
#[case::failpoint_kernel_v6_1_registered_buffers(crate::builder().with_num_registered_buffers(8))]
#[should_panic]
#[case::failpoint_kernel_v5_13_skip_unsupported_features(crate::builder().with_coop_task_run(true).skip_unsupported_features(true))]
#[case::failpoint_kernel_v5_15_skip_unsupported_features(crate::builder().with_coop_task_run(true).skip_unsupported_features(true))]
#[case::failpoint_kernel_v5_19_skip_unsupported_features(crate::builder().with_coop_task_run(true).skip_unsupported_features(true))]
#[case::failpoint_kernel_v6_0_unsupported_features(crate::builder().with_coop_task_run(true).skip_unsupported_features(true))]
#[case::failpoint_kernel_v6_1_unsupported_features(crate::builder().with_coop_task_run(true).skip_unsupported_features(true))]
// -- Begin size 128 tests
#[should_panic]
#[case::size128_failpoint_kernel_v5_13_default(crate::builder().with_sqe_size128(true))]
#[case::size128_failpoint_kernel_v5_15_default(crate::builder().with_sqe_size128(true))]
#[case::size128_failpoint_kernel_v5_19_default(crate::builder().with_sqe_size128(true))]
#[case::size128_failpoint_kernel_v6_0_default(crate::builder().with_sqe_size128(true))]
#[case::size128_failpoint_kernel_v6_1_default(crate::builder().with_sqe_size128(true))]
#[should_panic]
#[case::size128_failpoint_kernel_v5_13_io_polling(crate::builder().with_sqe_size128(true).with_io_polling(true))]
#[case::size128_failpoint_kernel_v5_15_io_polling(crate::builder().with_sqe_size128(true).with_io_polling(true))]
#[case::size128_failpoint_kernel_v5_19_io_polling(crate::builder().with_sqe_size128(true).with_io_polling(true))]
#[case::size128_failpoint_kernel_v6_0_io_polling(crate::builder().with_sqe_size128(true).with_io_polling(true))]
#[case::size128_failpoint_kernel_v6_1_io_polling(crate::builder().with_sqe_size128(true).with_io_polling(true))]
#[should_panic]
#[case::size128_failpoint_kernel_v5_13_coop_task_run(crate::builder().with_sqe_size128(true).with_coop_task_run(true))]
#[should_panic]
#[case::size128_failpoint_kernel_v5_15_coop_task_run(crate::builder().with_sqe_size128(true).with_coop_task_run(true))]
#[case::size128_failpoint_kernel_v5_19_coop_task_run(crate::builder().with_sqe_size128(true).with_coop_task_run(true))]
#[case::size128_failpoint_kernel_v6_0_coop_task_run(crate::builder().with_sqe_size128(true).with_coop_task_run(true))]
#[case::size128_failpoint_kernel_v6_1_coop_task_run(crate::builder().with_sqe_size128(true).with_coop_task_run(true))]
#[should_panic]
#[case::size128_failpoint_kernel_v5_13_registered_files(crate::builder().with_sqe_size128(true).with_num_registered_files(8))]
#[case::size128_failpoint_kernel_v5_15_registered_files(crate::builder().with_sqe_size128(true).with_num_registered_files(8))]
#[case::size128_failpoint_kernel_v5_19_registered_files(crate::builder().with_sqe_size128(true).with_num_registered_files(8))]
#[case::size128_failpoint_kernel_v6_0_registered_files(crate::builder().with_sqe_size128(true).with_num_registered_files(8))]
#[case::size128_failpoint_kernel_v6_1_registered_files(crate::builder().with_sqe_size128(true).with_num_registered_files(8))]
#[should_panic]
#[case::size128_failpoint_kernel_v5_13_registered_buffers(crate::builder().with_sqe_size128(true).with_num_registered_buffers(8))]
#[case::size128_failpoint_kernel_v5_15_registered_buffers(crate::builder().with_sqe_size128(true).with_num_registered_buffers(8))]
#[case::size128_failpoint_kernel_v5_19_registered_buffers(crate::builder().with_sqe_size128(true).with_num_registered_buffers(8))]
#[case::size128_failpoint_kernel_v6_0_registered_buffers(crate::builder().with_sqe_size128(true).with_num_registered_buffers(8))]
#[case::size128_failpoint_kernel_v6_1_registered_buffers(crate::builder().with_sqe_size128(true).with_num_registered_buffers(8))]
fn failpoint_test_scheduler_creation(#[case] builder: I2o2Builder) {
    let scenario = FailScenario::setup();
    let (_scheduler, _handle) = builder
        .try_create::<()>()
        .expect("builder should be produce valid scheduler");
    scenario.teardown();
}
