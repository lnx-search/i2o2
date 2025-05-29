mod creation;
mod file_io;
mod ops_scheduling;

fn try_init_logging() {
    let _ = tracing_subscriber::fmt::try_init();
}
