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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use humansize::DECIMAL;
use crate::io_shared::{BenchmarkRandomReadResults, FileManager};

mod io_shared;

static BASE_PATH: &str = "./benchmark-data";
const BUFFER_SIZE: usize = 8 << 10;
const RUN_DURATION: Duration = Duration::from_secs(30);
const CONCURRENCY_LEVELS: [usize; 4] = [1, 64, 256, 512];

const SIZE_1GB: usize = 1 << 30;
const SIZE_10GB: usize = 10 << 30;

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let run_id = ulid::Ulid::new();
    let mut file_manger = FileManager::new(run_id, BASE_PATH.as_ref())?;
    let mut results = BenchmarkRandomReadResults::default();

    tracing::info!(run_id = %run_id, "starting benchmark");
    run_std_benches(&mut file_manger, &mut results)?;
    std::thread::sleep(Duration::from_secs(5));
    
    // run_glommio_benches(&mut file_manger, &mut results)?;
    // std::thread::sleep(Duration::from_secs(5));
    // 
    // run_ring_benches(&mut file_manger, &mut results)?;
    // std::thread::sleep(Duration::from_secs(5));

    tracing::info!("done!");
    
    println!("{results}");
    
    Ok(())
}


fn run_std_benches(
    file_manager: &mut FileManager, 
    results: &mut BenchmarkRandomReadResults,
) -> Result<()> {
    let file_1gb = file_manager.create_random_file(SIZE_1GB,  libc::O_DIRECT).map(Arc::new)?;
    for concurrency in CONCURRENCY_LEVELS {
        execute_std_bench(file_1gb.clone(), results, concurrency, SIZE_1GB)?;
    }
    drop(file_1gb);
    
    let file_10gb = file_manager.create_random_file(SIZE_10GB,  libc::O_DIRECT).map(Arc::new)?;
    for concurrency in CONCURRENCY_LEVELS {
        execute_std_bench(file_10gb.clone(), results, concurrency, SIZE_10GB)?;
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
        file_size = humansize::format_size(file_size, DECIMAL),
        "starting std::fs::File run"
    );
    
    let num_blocks = file_size / BUFFER_SIZE;
    
    let total_op_count = Arc::new(AtomicUsize::new(0));
    
    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let total_op_count = total_op_count.clone();
        let file = file.clone();
        
        let handle = std::thread::spawn(move || {
            let mut buffer = memmap2::MmapOptions::new()
                .len(BUFFER_SIZE)
                .map_anon()?;
            
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

