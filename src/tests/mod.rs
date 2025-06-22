mod creation;
mod fixed_file_io;
mod misc_ops;
mod ops_scheduling;
mod register;
mod write_file_io;

fn try_init_logging() {
    let _ = tracing_subscriber::fmt::try_init();
}
