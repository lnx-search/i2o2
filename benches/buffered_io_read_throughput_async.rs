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
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use tokio::sync::Barrier;
use tokio::task::JoinSet;

use crate::io_shared::{BenchmarkRandomReadResults, FileManager};

mod io_shared;

static BASE_PATH: &str = "./benchmark-data";
const BUFFER_SIZE: usize = 8 << 10;
const NUM_IOPS_PER_WORKER: usize = 100 << 10;

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let run_id = ulid::Ulid::new();
    let mut file_manger = FileManager::new(run_id, BASE_PATH.as_ref())?;
    let mut results = BenchmarkRandomReadResults::default();

    tracing::info!(run_id = %run_id, "starting benchmark");
    // run_std_benches(&mut file_manger, &mut results)?;
    // run_glommio_benches(&mut file_manger, &mut results)?;
    run_ring_benches(&mut file_manger, &mut results)?;

    tracing::info!("done!");

    println!("{results}");

    Ok(())
}

#[tokio::main]
async fn run_std_benches(
    file_manger: &mut FileManager,
    results: &mut BenchmarkRandomReadResults,
) -> Result<()> {
    let mut buffer = vec![0; BUFFER_SIZE];
    fastrand::fill(&mut buffer);

    let file_1gb = file_manger.create_random_file(1 << 30, 0).map(Arc::new)?;

    for concurrency in [1, 8, 32, 64, 256] {
        tracing::info!(concurrency, "running tokio benchmark 1gb");
        let iops =
            std_random_concurrent_read(file_1gb.clone(), 1 << 30, concurrency).await?;
        results.push("tokio::fs::File", 1 << 30, concurrency, BUFFER_SIZE, iops);
    }

    let file_10gb = file_manger.create_random_file(10 << 30, 0).map(Arc::new)?;

    for concurrency in [1, 8, 32, 64, 256] {
        tracing::info!(concurrency, "running tokio benchmark 10gb");
        let iops =
            std_random_concurrent_read(file_10gb.clone(), 10 << 30, concurrency).await?;
        results.push("tokio::fs::File", 10 << 30, concurrency, BUFFER_SIZE, iops);
    }

    Ok(())
}

fn run_glommio_benches(
    file_manger: &mut FileManager,
    results: &mut BenchmarkRandomReadResults,
) -> Result<()> {
    let mut buffer = vec![0; BUFFER_SIZE];
    fastrand::fill(&mut buffer);

    let executor = glommio::LocalExecutorBuilder::default().make().unwrap();
    executor.run(async move {
        let fp = file_manger.get_random_file_path(1 << 30);
        let file_1gb = Rc::new(
            glommio::io::BufferedFile::open(fp)
                .await
                .map_err(io::Error::from)?,
        );

        for concurrency in [1, 8, 32, 64, 256, 512] {
            tracing::info!(concurrency, "running glommio buffered benchmark 1gb");
            let iops =
                glommio_random_concurrent_read(file_1gb.clone(), 1 << 30, concurrency)
                    .await?;
            results.push(
                "glommio::io::BufferedFile",
                1 << 30,
                concurrency,
                BUFFER_SIZE,
                iops,
            );
        }

        let fp = file_manger.get_random_file_path(10 << 30);
        let file_10gb = Rc::new(
            glommio::io::BufferedFile::open(fp)
                .await
                .map_err(io::Error::from)?,
        );

        for concurrency in [1, 8, 32, 64, 256, 512] {
            tracing::info!(concurrency, "running glommio buffered benchmark 10gb");
            let iops =
                glommio_random_concurrent_read(file_10gb.clone(), 10 << 30, concurrency)
                    .await?;
            results.push(
                "glommio::io::BufferedFile",
                10 << 30,
                concurrency,
                BUFFER_SIZE,
                iops,
            );
        }

        Ok(())
    })
}

#[tokio::main]
async fn run_ring_benches(
    file_manger: &mut FileManager,
    results: &mut BenchmarkRandomReadResults,
) -> Result<()> {
    let mut buffer = vec![0; BUFFER_SIZE];
    fastrand::fill(&mut buffer);

    let file_1gb = file_manger.create_random_file(1 << 30, 0).map(Arc::new)?;

    for concurrency in [256, 512] {
        tracing::info!(concurrency, "running i2o2 benchmark 1gb");
        let iops = i2o2_random_concurrent_read(
            &file_1gb,
            i2o2::builder(),
            1 << 30,
            concurrency,
        )
        .await?;
        results.push("i2o2 default", 1 << 30, concurrency, BUFFER_SIZE, iops);
    }

    // let file_10gb = file_manger.create_random_file(10 << 30, 0).map(Arc::new)?;
    //
    // for concurrency in [1, 8, 32, 64, 256, 512] {
    //     tracing::info!(concurrency, "running i2o2 benchmark 10gb");
    //     let iops = i2o2_random_concurrent_read(
    //         &file_10gb,
    //         i2o2::builder(),
    //         10 << 30,
    //         concurrency,
    //     )
    //     .await?;
    //     results.push("i2o2 default", 10 << 30, concurrency, BUFFER_SIZE, iops);
    // }

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

            let mut buffer = memmap2::MmapOptions::new().len(BUFFER_SIZE).map_anon()?;

            let start = Instant::now();
            for _ in 0..NUM_IOPS_PER_WORKER {
                let block_idx = fastrand::usize(0..file_len / BUFFER_SIZE);
                let n =
                    file.read_at(&mut buffer[..], (block_idx * BUFFER_SIZE) as u64)?;
                assert_eq!(n, BUFFER_SIZE);
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

async fn glommio_random_concurrent_read(
    file: Rc<glommio::io::BufferedFile>,
    file_len: usize,
    concurrency: usize,
) -> Result<f32> {
    let barrier = Rc::new(Barrier::new(concurrency));
    let mut tasks = Vec::new();

    for _ in 0..concurrency * 2 {
        let file = file.clone();
        let barrier = barrier.clone();

        let task = glommio::spawn_local(async move {
            let _ = barrier.wait().await;

            let start = Instant::now();
            for _ in 0..NUM_IOPS_PER_WORKER {
                let block_idx = fastrand::usize(0..file_len / BUFFER_SIZE);
                let result = file
                    .read_at((block_idx * BUFFER_SIZE) as u64, BUFFER_SIZE)
                    .await?;
                black_box(result);
            }

            Ok::<_, io::Error>(start.elapsed())
        });

        tasks.push(task);
    }

    let mut total = Duration::default();
    for timing in tasks {
        total += timing.await?;
    }

    let iops =
        NUM_IOPS_PER_WORKER as f32 / (total / (concurrency * 2) as u32).as_secs_f32();

    Ok(iops)
}

async fn i2o2_random_concurrent_read(
    file: &std::fs::File,
    builder: i2o2::I2o2Builder,
    file_len: usize,
    concurrency: usize,
) -> Result<f32> {
    let (scheduler_thread, handle) = builder
        .with_queue_size(128)
        // .with_io_polling(true)
        .with_sqe_polling(true)
        .with_sqe_polling_timeout(Duration::from_millis(150))
        // .with_sqe_polling_pin_cpu(8)
        .with_num_registered_files(1)
        .try_spawn::<()>()?;

    let fd = file.as_raw_fd();
    let id = handle.register_file_async(fd, None).await?;

    let barrier = Arc::new(Barrier::new(concurrency));
    let mut set = JoinSet::new();

    for _ in 0..concurrency {
        let handle = handle.clone();
        let barrier = barrier.clone();

        set.spawn(async move {
            let _ = barrier.wait().await;

            let start = Instant::now();

            for _ in 0..NUM_IOPS_PER_WORKER {
                let block_idx = fastrand::usize(0..file_len / BUFFER_SIZE);
                let mut buf = vec![0; BUFFER_SIZE];

                let op = i2o2::opcode::Read::new(
                    i2o2::types::Fixed(id),
                    buf.as_mut_ptr(),
                    buf.len() as u32,
                )
                .offset((block_idx * BUFFER_SIZE) as u64);

                let reply = unsafe { handle.submit_async(op, None).await? };
                let _ = reply.await;
                drop(buf);
            }

            Ok::<_, anyhow::Error>(start.elapsed())
        });
    }

    let timings = set.join_all().await;

    let mut total = Duration::default();
    for timing in timings {
        total += timing?;
    }

    let iops = NUM_IOPS_PER_WORKER as f32 / (total / concurrency as u32).as_secs_f32();

    drop(handle);
    scheduler_thread.join().unwrap()?;

    Ok(iops)
}
