mod creation;
mod file_io;
mod ops_scheduling;
mod register;

fn try_init_logging() {
    let _ = tracing_subscriber::fmt::try_init();
}
