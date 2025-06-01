//! Tests the throughput of the system when performing buffered IO.
//!
//! This is mostly to sanity check that we've done something really wrong,
//! because the main gain of us using io_uring is in the direct IO behaviour.
//!
//! Also, note from ChillFish8: Yes we could write bigger files to avoid the file cache
//! but on my machine that would mean writing 200GB+ of data every time, and I don't really
//! want that burning through my NVMEs!

use std::cmp;
use std::io::{BufWriter, Write};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};

use crate::io_shared::{BenchmarkResults, FileManager};

mod io_shared;

static BASE_PATH: &str = "./benchmark-data";
const BUFFER_SIZE: usize = 32 << 10;

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let run_id = ulid::Ulid::new();
    let base_path = "./benchmark-data";

    let mut file_manger = FileManager::new(run_id, base_path.as_ref())?;
    let mut results = BenchmarkResults::default();

    tracing::info!(run_id = %run_id, "starting benchmark");
    run_std_benches(&mut file_manger, &mut results)?;

    tracing::info!("done!");

    Ok(())
}

fn run_std_benches(
    file_manger: &mut FileManager,
    results: &mut BenchmarkResults,
) -> Result<()> {
    let mut buffer = vec![0; BUFFER_SIZE];
    fastrand::fill(&mut buffer);

    tracing::info!("running default file write 1GB");
    let mut file = file_manger.new_file()?;
    let elapsed = sequential_write_repeating(file.as_file_mut(), &buffer, 1 << 30)?;
    results.push("std::fs::File, 1GB", BUFFER_SIZE, elapsed, 1 << 30);

    tracing::info!("running default file write 10GB");
    let mut file = file_manger.new_file()?;
    let elapsed = sequential_write_repeating(file.as_file_mut(), &buffer, 10 << 30)?;
    results.push("std::fs::File, 1GB", BUFFER_SIZE, elapsed, 10 << 30);

    tracing::info!("running BufWriter file write 1GB");
    let mut file = file_manger.new_file()?;
    let mut writer = BufWriter::with_capacity(1 << 20, file.as_file_mut());
    let elapsed = sequential_write_repeating(&mut writer, &buffer, 10 << 30)?;
    results.push("std::fs::File, 1GB", BUFFER_SIZE, elapsed, 1 << 30);

    tracing::info!("running BufWriter file write 10GB");
    let mut file = file_manger.new_file()?;
    let mut writer = BufWriter::with_capacity(1 << 20, file.as_file_mut());
    let elapsed = sequential_write_repeating(&mut writer, &buffer, 10 << 30)?;
    results.push("std::fs::File, 1GB", BUFFER_SIZE, elapsed, 10 << 30);

    Ok(())
}

/// Run a simple sequential write of a buffer for a given target file size.
fn sequential_write_repeating<W: Write>(
    writer: &mut W,
    buffer: &[u8],
    target_file_size: usize,
) -> Result<Duration> {
    let now = Instant::now();

    let mut bytes_written = 0;
    while bytes_written < target_file_size {
        let remaining = target_file_size - bytes_written;
        let slice_at = cmp::min(buffer.len(), remaining);

        let n = writer.write(&buffer[..slice_at])?;
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
