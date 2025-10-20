//! Tests the throughput of the system when performing buffered IO.
//!
//! This is mostly to sanity check that we've done something really wrong,
//! because the main gain of us using io_uring is in the direct IO behaviour.
//!
//! Also, note from ChillFish8: Yes we could write bigger files to avoid the file cache
//! but on my machine that would mean writing 200GB+ of data every time, and I don't really
//! want that burning through my NVMEs!

use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};
use std::{cmp, io};

use anyhow::{Result, bail};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::io_shared::{BenchmarkWriteResults, FileManager};

mod io_shared;

static BASE_PATH: &str = "./benchmark-data";

const BUFFER_SIZE: [usize; 5] = [8 << 10, 10 << 10, 32 << 10, 128 << 10, 512 << 10];

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let run_id = ulid::Ulid::new();
    let mut file_manger = FileManager::new(run_id, BASE_PATH.as_ref())?;
    let mut results = BenchmarkWriteResults::default();

    tracing::info!(run_id = %run_id, "starting benchmark");

    for buffer_size in BUFFER_SIZE {
        run_tokio_benches(&mut file_manger, &mut results, buffer_size).await?;
    }

    for buffer_size in BUFFER_SIZE {
        run_i2o2_benches(&mut file_manger, &mut results, buffer_size).await?;
    }

    tracing::info!("done!");

    println!("{results}");

    Ok(())
}

async fn run_tokio_benches(
    file_manger: &mut FileManager,
    results: &mut BenchmarkWriteResults,
    buffer_size: usize,
) -> Result<()> {
    let mut buffer = vec![0; buffer_size];
    fastrand::fill(&mut buffer);

    tracing::info!("running default file write 1GB");
    let mut file = file_manger.new_async_file().await?;
    let elapsed =
        sequential_write_repeating(file.as_file_mut(), &buffer, 1 << 30).await?;
    results.push("tokio::fs::File, 1GB", buffer_size, elapsed, 1 << 30);

    tracing::info!("running default file write 10GB");
    let mut file = file_manger.new_async_file().await?;
    let elapsed =
        sequential_write_repeating(file.as_file_mut(), &buffer, 10 << 30).await?;
    results.push("tokio::fs::File, 10GB", buffer_size, elapsed, 10 << 30);

    Ok(())
}

async fn run_i2o2_benches(
    file_manger: &mut FileManager,
    results: &mut BenchmarkWriteResults,
    buffer_size: usize,
) -> Result<()> {
    let mut buffer = vec![0; buffer_size];
    fastrand::fill(&mut buffer);

    tracing::info!("running ring file write 1GB");
    let file = file_manger.new_file()?;
    let elapsed = sequential_write_repeating_ring(
        file.as_file(),
        i2o2::builder(),
        &buffer,
        1 << 30,
    )
    .await?;
    results.push(
        "i2o2 buffered, default opts, 1GB",
        buffer_size,
        elapsed,
        1 << 30,
    );

    tracing::info!("running ring file write 10GB");
    let file = file_manger.new_file()?;
    let elapsed = sequential_write_repeating_ring(
        file.as_file(),
        i2o2::builder(),
        &buffer,
        10 << 30,
    )
    .await?;
    results.push(
        "i2o2 buffered, default opt, 10GB",
        buffer_size,
        elapsed,
        10 << 30,
    );

    Ok(())
}

/// Run a simple sequential write of a buffer for a given target file size.
async fn sequential_write_repeating<W: AsyncWrite + Unpin>(
    writer: &mut W,
    buffer: &[u8],
    target_file_size: usize,
) -> Result<Duration> {
    let now = Instant::now();

    let mut bytes_written = 0;
    while bytes_written < target_file_size {
        let remaining = target_file_size - bytes_written;
        let slice_at = cmp::min(buffer.len(), remaining);

        let n = writer.write(&buffer[..slice_at]).await?;
        if n == 0 {
            break;
        }

        bytes_written += n;
    }

    let elapsed = now.elapsed();

    if bytes_written != target_file_size {
        bail!(
            "system could not write file fully wrote: {bytes_written} expected: {target_file_size}"
        );
    }

    Ok(elapsed)
}

/// Run a simple sequential write of a buffer for a given target file size using io_uring.
///
/// We probably expect this to be slower than the default method, because the overhead
/// of the channel and submission and completion logic.
async fn sequential_write_repeating_ring(
    file: &std::fs::File,
    options: i2o2::I2o2Builder,
    buffer: &[u8],
    target_file_size: usize,
) -> Result<Duration> {
    let (scheduler_thread_handle, handle) = options.try_spawn::<()>()?;

    file.set_len(target_file_size as u64)?;

    let now = Instant::now();

    let mut bytes_written = 0;
    while bytes_written < target_file_size {
        let remaining = target_file_size - bytes_written;
        let len = cmp::min(buffer.len(), remaining);

        let op = i2o2::opcode::Write::new(
            i2o2::types::Fd(file.as_raw_fd()),
            buffer.as_ptr(),
            len,
            bytes_written as u64,
        );

        let reply = unsafe { handle.submit_async(op, None).await }?;
        let result = reply.await?;

        if result < 0 {
            bail!("got error: {}", io::Error::from_raw_os_error(-result));
        }

        if result == 0 {
            break;
        }

        bytes_written += result as usize;
    }

    let elapsed = now.elapsed();

    drop(handle);
    scheduler_thread_handle.join().unwrap()?;

    Ok(elapsed)
}
