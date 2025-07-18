//! Tests the throughput of the scheduler logic and io_uring using noop operations.

use std::io;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

mod no_op_shared;

const NUM_OPS_PER_WORKER: usize = 100_000;

fn main() -> io::Result<()> {
    tracing_subscriber::fmt::init();

    let concurrency_levels = [1usize, 8, 16, 32];

    let configs = [
        ("default config", i2o2::builder()),
        ("COOP task run", i2o2::builder().with_coop_task_run(true)),
    ];

    let mut results = no_op_shared::BenchmarkResults::default();

    for (name, config) in configs {
        eprintln!("running benchmark for config: {config:?}");
        for (run_id, num_workers) in concurrency_levels.iter().enumerate() {
            eprintln!("  {run_id} - run with {num_workers} workers");
            let (elapsed, total_ops, ops_per_sec) = bench_with_config(
                config.clone().with_queue_size(*num_workers as u32 * 8),
                *num_workers,
            )?;
            results.push(name, *num_workers, elapsed, total_ops, ops_per_sec);
        }
    }

    println!("{results}");

    Ok(())
}

fn bench_with_config(
    builder: i2o2::I2o2Builder,
    num_workers: usize,
) -> io::Result<(Duration, usize, f32)> {
    let (thread_handle, scheduler_handle) = builder.try_spawn::<()>()?;

    let barrier = Arc::new(Barrier::new(num_workers + 1));

    let mut worker_handles = Vec::with_capacity(num_workers);
    for _ in 0..num_workers {
        let barrier = barrier.clone();
        let handle = scheduler_handle.clone();

        let th = std::thread::spawn(move || {
            barrier.wait();

            for _ in 0..NUM_OPS_PER_WORKER {
                let op = i2o2::opcode::Nop::new();
                let reply = unsafe { handle.submit(op, None).unwrap() };
                let result = reply.wait();
                assert_eq!(result, Ok(0));
            }

            Ok::<_, io::Error>(0)
        });

        worker_handles.push(th);
    }

    barrier.wait();

    let start = Instant::now();
    for worker in worker_handles {
        worker.join().unwrap()?;
    }
    let elapsed = start.elapsed();
    let total_ops = num_workers * NUM_OPS_PER_WORKER;
    let ops_per_sec = total_ops as f32 / elapsed.as_secs_f32();
    drop(scheduler_handle);

    thread_handle.join().expect("executor panicked")?;

    Ok((elapsed, total_ops, ops_per_sec))
}
