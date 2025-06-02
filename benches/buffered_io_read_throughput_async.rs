//! Tests the throughput of the system when performing buffered IO.
//!
//! This is mostly to sanity check that we've done something really wrong,
//! because the main gain of us using io_uring is in the direct IO behaviour.
//!
//! Also, note from ChillFish8: Yes we could write bigger files to avoid the file cache
//! but on my machine that would mean writing 200GB+ of data every time, and I don't really
//! want that burning through my NVMEs!

use std::hint::black_box;
use std::io;
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

use crate::io_shared::{BenchmarkRandomReadResults, FileManager};

mod io_shared;

static BASE_PATH: &str = "./benchmark-data";
const BUFFER_SIZE: usize = 8 << 10;
const NUM_IOPS_PER_WORKER: usize = 10_0000;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let run_id = ulid::Ulid::new();
    let mut file_manger = FileManager::new(run_id, BASE_PATH.as_ref())?;
    let mut results = BenchmarkRandomReadResults::default();

    tracing::info!(run_id = %run_id, "starting benchmark");
    run_std_benches(&mut file_manger, &mut results).await?;
    run_ring_benches(&mut file_manger, &mut results).await?;

    tracing::info!("done!");

    println!("{results}");

    Ok(())
}

async fn run_std_benches(
    file_manger: &mut FileManager,
    results: &mut BenchmarkRandomReadResults,
) -> Result<()> {
    let mut buffer = vec![0; BUFFER_SIZE];
    fastrand::fill(&mut buffer);

    let file_1gb = file_manger.create_random_file(1 << 30).map(Arc::new)?;

    for concurrency in [1, 8, 32, 64, 256, 512] {
        tracing::info!(concurrency, "running benchmark 1gb");
        let iops =
            std_random_concurrent_read(file_1gb.clone(), 1 << 30, concurrency).await?;
        results.push("tokio::fs::File", 1 << 30, concurrency, BUFFER_SIZE, iops);
    }

    let file_10gb = file_manger.create_random_file(10 << 30).map(Arc::new)?;

    for concurrency in [1, 8, 32, 64, 256, 512] {
        tracing::info!(concurrency, "running benchmark 10gb");
        let iops =
            std_random_concurrent_read(file_10gb.clone(), 10 << 30, concurrency).await?;
        results.push("tokio::fs::File", 10 << 30, concurrency, BUFFER_SIZE, iops);
    }

    Ok(())
}

async fn run_ring_benches(
    file_manger: &mut FileManager,
    results: &mut BenchmarkRandomReadResults,
) -> Result<()> {
    let mut buffer = vec![0; BUFFER_SIZE];
    fastrand::fill(&mut buffer);

    let mut file_1gb = file_manger.create_random_file(1 << 30)?;

    let mut file_10gb = file_manger.create_random_file(10 << 30)?;

    Ok(())
}

async fn std_random_concurrent_read(
    file: Arc<std::fs::File>,
    file_len: usize,
    concurrency: usize,
) -> Result<f32> {
    let barrier = Arc::new(Barrier::new(concurrency));
    let mut set = JoinSet::new();

    for _ in 0..concurrency {
        let file = file.clone();
        let barrier = barrier.clone();

        set.spawn(async move {
            let _ = barrier.wait().await;

            let mut buffer = vec![0; BUFFER_SIZE];

            let start = Instant::now();
            for _ in 0..NUM_IOPS_PER_WORKER {
                let block_idx = fastrand::usize(0..file_len / BUFFER_SIZE);
                file.read_at(&mut buffer[..], (block_idx * BUFFER_SIZE) as u64)?;
                black_box(&buffer);
            }

            Ok::<_, io::Error>(start.elapsed())
        });
    }

    let timings = set.join_all().await;

    let mut total = Duration::default();
    for timing in timings {
        total += timing?;
    }

    let iops = NUM_IOPS_PER_WORKER as f32 / (total / concurrency as u32).as_secs_f32();

    Ok(iops)
}
