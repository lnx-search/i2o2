use std::fmt::{Display, Formatter};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::{cmp, io};

use anyhow::bail;
use humansize::DECIMAL;
use tabled::builder::Builder;
use tabled::settings::Style;
use tempfile::TempPath;

pub struct BenchmarkWriteResults {
    builder: Builder,
}

impl Default for BenchmarkWriteResults {
    fn default() -> Self {
        let mut builder = Builder::with_capacity(0, 4);
        builder.push_record(["Name", "Buffer Size", "Elapsed", "Bandwidth"]);

        Self { builder }
    }
}

impl BenchmarkWriteResults {
    #[allow(unused)]
    pub fn push(
        &mut self,
        name: &str,
        buffer_size: usize,
        elapsed: Duration,
        bytes_written: u64,
    ) {
        let bytes_per_sec = bytes_written as f32 / elapsed.as_secs_f32();

        self.builder.push_record([
            name.to_string(),
            humansize::format_size(buffer_size, DECIMAL),
            format_duration(elapsed),
            format!(
                "{}/sec",
                humansize::format_size(bytes_per_sec as u64, DECIMAL)
            ),
        ]);
    }
}

impl Display for BenchmarkWriteResults {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut table = self.builder.clone().build();
        table.with(Style::rounded());
        write!(f, "{table}")
    }
}

pub struct BenchmarkRandomReadResults {
    builder: Builder,
}

impl Default for BenchmarkRandomReadResults {
    fn default() -> Self {
        let mut builder = Builder::with_capacity(0, 4);
        builder.push_record([
            "Name",
            "File Size",
            "Concurrency",
            "IO Size",
            "IOPS",
            "Bandwidth",
        ]);

        Self { builder }
    }
}

impl BenchmarkRandomReadResults {
    pub fn push(
        &mut self,
        name: &str,
        file_size: usize,
        concurrency: usize,
        io_size: usize,
        iops: f32,
    ) {
        let mb_sec = (io_size as f32 * iops) as u64;
        self.builder.push_record([
            name.to_string(),
            humansize::format_size(file_size, DECIMAL),
            concurrency.to_string(),
            humansize::format_size(io_size, DECIMAL),
            format!("{} op/sec", format_total(iops)),
            format!("{}/sec", humansize::format_size(mb_sec, DECIMAL)),
        ]);
    }
}

impl Display for BenchmarkRandomReadResults {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut table = self.builder.clone().build();
        table.with(Style::rounded());
        write!(f, "{table}")
    }
}

#[allow(unused)]
fn format_duration(dur: Duration) -> String {
    if dur < Duration::from_secs(1) {
        let millis = dur.as_secs_f32() * 1000.0;
        format!("{millis:.2}ms")
    } else {
        let millis = dur.as_secs_f32();
        format!("{millis:.2}s")
    }
}

fn format_total(count: f32) -> String {
    const M: f32 = 1_000.0 * 1_000.0;
    const K: f32 = 1_000.0;

    if count / M > 1.0 {
        format!("{:.2}m", count / M)
    } else if count / K > 1.0 {
        format!("{:.2}k", count / K)
    } else {
        format!("{count:.2}")
    }
}

pub struct FileManager {
    base_path: PathBuf,
    core_path: PathBuf,
    sequence_id: usize,
}

impl FileManager {
    pub fn new(run_id: ulid::Ulid, base_path: &Path) -> io::Result<Self> {
        let path = base_path.join(format!("run-{run_id}"));

        std::fs::create_dir_all(&path)?;

        Ok(Self {
            base_path: path,
            core_path: base_path.to_path_buf(),
            sequence_id: 0,
        })
    }

    pub fn create_random_file(
        &mut self,
        target_size: usize,
        flags: i32,
    ) -> anyhow::Result<std::fs::File> {
        let fp = self.core_path.join(format!("rng-file-read-{target_size}"));

        if fp.exists() {
            let file = std::fs::File::open(&fp)?;
            return Ok(file);
        }

        let mut file = std::fs::File::options()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&fp)?;

        let mut buffer = vec![0; 32 << 10];
        fastrand::fill(&mut buffer);

        write_content(&mut file, &buffer, target_size)?;

        file.sync_all()?;

        let file = std::fs::File::options()
            .read(true)
            .custom_flags(flags)
            .open(&fp)?;

        Ok(file)
    }

    #[allow(unused)]
    pub fn new_file(&mut self) -> io::Result<tempfile::NamedTempFile> {
        self.sequence_id += 1;

        let path = self.base_path.join(format!("stage-{}", self.sequence_id));

        let file = std::fs::File::options()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path)?;

        Ok(tempfile::NamedTempFile::from_parts(
            file,
            TempPath::from_path(path),
        ))
    }

    #[allow(unused)]
    pub async fn new_async_file(
        &mut self,
    ) -> io::Result<tempfile::NamedTempFile<tokio::fs::File>> {
        self.sequence_id += 1;

        let path = self.base_path.join(format!("stage-{}", self.sequence_id));

        let file = tokio::fs::File::options()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path)
            .await?;

        Ok(tempfile::NamedTempFile::from_parts(
            file,
            TempPath::from_path(path),
        ))
    }
}

fn write_content<W: Write>(
    writer: &mut W,
    buffer: &[u8],
    target_file_size: usize,
) -> anyhow::Result<Duration> {
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
