//! Tests the throughput of the system when performing buffered IO.
//!
//! This is mostly to sanity check that we've done something really wrong,
//! because the main gain of us using io_uring is in the direct IO behaviour.
//!
//! Also, note from ChillFish8: Yes we could write bigger files to avoid the file cache
//! but on my machine that would mean writing 200GB+ of data every time, and I don't really
//! want that burning through my NVMEs!

use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use glommio::{CpuSet, Placement};
use humansize::DECIMAL;
use i2o2::DynamicGuard;
use memmap2::Advice;
use tokio::task::JoinSet;

use crate::io_shared::{BenchmarkRandomReadResults, FileManager};

mod io_shared;

static BASE_PATH: &str = "./benchmark-data/benchmark-xfs";
const BUFFER_SIZE: usize = 8 << 10;
const RUN_DURATION: Duration = Duration::from_secs(15);
const THREADED_CONCURRENCY_LEVELS: [usize; 4] = [1, 64, 256, 512];
const ASYNC_CONCURRENCY_LEVELS: [usize; 5] = [1, 64, 1024, 2048, 4096];

const PIN_SCHEDULER_THREAD_TO_CORE: u32 = 5;
const SIZE_100GB: usize = 100 << 30;

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let run_id = ulid::Ulid::new();
    let mut file_manger = FileManager::new(run_id, BASE_PATH.as_ref())?;
    let mut results = BenchmarkRandomReadResults::default();

    tracing::info!(run_id = %run_id, "starting benchmark");

    run_i2o2_benches(&mut file_manger, &mut results)?;
    std::thread::sleep(Duration::from_secs(20));

    // run_std_benches(&mut file_manger, &mut results)?;
    // std::thread::sleep(Duration::from_secs(20));
    // 
    // run_glommio_benches(&mut file_manger, &mut results)?;
    // std::thread::sleep(Duration::from_secs(20));

    tracing::info!("done!");

    println!("{results}");

    Ok(())
}

fn run_std_benches(
    file_manager: &mut FileManager,
    results: &mut BenchmarkRandomReadResults,
) -> Result<()> {
    let file_10gb = file_manager
        .create_random_file(SIZE_100GB, true)
        .map(Arc::new)?;
    for concurrency in THREADED_CONCURRENCY_LEVELS {
        execute_std_bench(file_10gb.clone(), results, concurrency, SIZE_100GB)?;
        std::thread::sleep(Duration::from_secs(5));
    }
    drop(file_10gb);

    Ok(())
}

fn run_glommio_benches(
    file_manager: &mut FileManager,
    results: &mut BenchmarkRandomReadResults,
) -> Result<()> {
    let executor = glommio::LocalExecutorBuilder::new(Placement::Fixed(4))
        .io_memory(128 << 20)
        .make()
        .unwrap();

    executor.run(async move {
        let fp = file_manager.get_random_file_path(SIZE_100GB);
        let file = glommio::io::DmaFile::open(fp)
            .await
            .map_err(io::Error::from)?;
        let file_10gb = Rc::new(file);
        for concurrency in ASYNC_CONCURRENCY_LEVELS {
            execute_glommio_bench(file_10gb.clone(), results, concurrency, SIZE_100GB)
                .await?;
            glommio::timer::sleep(Duration::from_secs(5)).await;
        }
        drop(file_10gb);

        Ok::<_, anyhow::Error>(())
    })?;

    Ok(())
}

#[tokio::main]
async fn run_i2o2_benches(
    file_manager: &mut FileManager,
    results: &mut BenchmarkRandomReadResults,
) -> Result<()> {
    let file_10gb = file_manager
        .create_random_file(SIZE_100GB, true)
        .map(Arc::new)?;
    for concurrency in ASYNC_CONCURRENCY_LEVELS {
        execute_i2o2_bench(file_10gb.clone(), results, concurrency, SIZE_100GB).await?;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    drop(file_10gb);

    Ok(())
}

fn execute_std_bench(
    file: Arc<std::fs::File>,
    results: &mut BenchmarkRandomReadResults,
    concurrency: usize,
    file_size: usize,
) -> Result<()> {
    tracing::info!(
        concurrency = concurrency,
        file_size = %humansize::format_size(file_size, DECIMAL),
        "starting std::fs::File run"
    );

    let num_blocks = file_size / BUFFER_SIZE;

    let total_op_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let total_op_count = total_op_count.clone();
        let file = file.clone();

        let handle = std::thread::spawn(move || {
            let mut buffer = memmap2::MmapOptions::new().len(BUFFER_SIZE).map_anon()?;
            buffer.advise(Advice::DontFork)?;
            assert_eq!(buffer.as_ptr() as u64 % 512, 0);

            let now = Instant::now();
            while now.elapsed() < RUN_DURATION {
                let block_id = fastrand::usize(0..num_blocks);
                let offset = block_id * BUFFER_SIZE;

                let n = file.read_at(&mut buffer, offset as u64)?;
                assert_eq!(n, BUFFER_SIZE);

                total_op_count.fetch_add(1, Ordering::Relaxed);
            }

            Ok::<_, anyhow::Error>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap()?;
    }

    let total_ops = total_op_count.load(Ordering::Relaxed);
    let iops = total_ops as f32 / RUN_DURATION.as_secs_f32();

    results.push("std::fs::File", file_size, concurrency, BUFFER_SIZE, iops);

    Ok(())
}

async fn execute_glommio_bench(
    file: Rc<glommio::io::DmaFile>,
    results: &mut BenchmarkRandomReadResults,
    concurrency: usize,
    file_size: usize,
) -> Result<()> {
    tracing::info!(
        concurrency = concurrency,
        file_size = %humansize::format_size(file_size, DECIMAL),
        "starting glommio::io::DmaFile run"
    );

    let num_blocks = file_size / BUFFER_SIZE;

    let total_op_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let total_op_count = total_op_count.clone();
        let file = file.clone();

        let handle = glommio::spawn_local(async move {
            let now = Instant::now();
            while now.elapsed() < RUN_DURATION {
                let block_id = fastrand::usize(0..num_blocks);
                let offset = block_id * BUFFER_SIZE;

                let buf = file.read_at_aligned(offset as u64, BUFFER_SIZE).await?;
                assert_eq!(buf.len(), BUFFER_SIZE);

                total_op_count.fetch_add(1, Ordering::Relaxed);
            }

            Ok::<_, io::Error>(())
        })
        .detach();
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap()?;
    }

    let total_ops = total_op_count.load(Ordering::Relaxed);
    let iops = total_ops as f32 / RUN_DURATION.as_secs_f32();

    results.push(
        "glommio::io::DmaFile",
        file_size,
        concurrency,
        BUFFER_SIZE,
        iops,
    );

    Ok(())
}

async fn execute_i2o2_bench(
    file: Arc<std::fs::File>,
    results: &mut BenchmarkRandomReadResults,
    concurrency: usize,
    file_size: usize,
) -> Result<()> {
    tracing::info!(
        concurrency = concurrency,
        file_size = %humansize::format_size(file_size, DECIMAL),
        "starting i2o2 run"
    );

    let num_blocks = file_size / BUFFER_SIZE;

    let mut cpu_set = i2o2::CpuSet::blank();
    cpu_set.set(8);
    
    let (scheduler_handle, handle) = i2o2::builder()
        .with_sq_polling(true)
        .with_sq_polling_timeout(Duration::from_millis(100))
        .with_queue_size((concurrency * 2) as u32)
        .with_num_registered_files(1)
        .with_num_registered_buffers(concurrency as u32)
        .try_spawn::<DynamicGuard>()?;

    let file_id = handle.register_file_async(file.as_raw_fd(), None).await?;

    let total_op_count = Arc::new(AtomicUsize::new(0));

    let mut buffers: Vec<(u32, BufRef)> = Vec::new();
    for _ in 0..concurrency {
        let mut buffer = memmap2::MmapOptions::new().len(BUFFER_SIZE).map_anon()?;
        buffer.advise(Advice::DontFork)?;
        assert_eq!(buffer.as_ptr() as u64 % 512, 0);

        let buf_ptr = buffer.as_mut_ptr();
        let buf_len = buffer.len();

        let id = unsafe {
            handle
                .register_buffer_async(
                    buf_ptr,
                    buf_len,
                    Some(Box::new(buffer) as DynamicGuard),
                )
                .await?
        };

        buffers.push((id, BufRef(buf_ptr, buf_len)));
    }

    let mut handles = JoinSet::new();
    for (buf_index, buf_ref) in buffers {
        let total_op_count = total_op_count.clone();
        let handle = handle.clone();

        handles.spawn(async move {
            let buf_ref = buf_ref;

            let now = Instant::now();
            while now.elapsed() < RUN_DURATION {
                let block_id = fastrand::usize(0..num_blocks);
                let offset = block_id * BUFFER_SIZE;

                let op = i2o2::opcode::ReadFixed::new(
                    i2o2::types::Fixed(file_id),
                    buf_ref.0,
                    buf_ref.1,
                    buf_index,
                    offset as u64,
                );

                let reply = unsafe { handle.submit_async(op, None).await? };
                let result = reply.await?;
                assert_eq!(result, BUFFER_SIZE as i32);

                total_op_count.fetch_add(1, Ordering::Relaxed);
            }

            Ok::<_, anyhow::Error>(())
        });
    }

    for handle in handles.join_all().await {
        handle?;
    }

    let total_ops = total_op_count.load(Ordering::Relaxed);
    let iops = total_ops as f32 / RUN_DURATION.as_secs_f32();

    results.push(
        "i2o2",
        file_size,
        concurrency,
        BUFFER_SIZE,
        iops,
    );

    drop(handle);
    scheduler_handle.join().unwrap()?;

    Ok(())
}

struct BufRef(*mut u8, usize);

unsafe impl Send for BufRef {}
