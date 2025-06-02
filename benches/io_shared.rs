use std::fmt::{Display, Formatter};
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use humansize::DECIMAL;
use tabled::builder::Builder;
use tabled::settings::Style;
use tempfile::TempPath;

pub struct BenchmarkResults {
    builder: Builder,
}

impl Default for BenchmarkResults {
    fn default() -> Self {
        let mut builder = Builder::with_capacity(0, 4);
        builder.push_record(["Name", "Buffer Size", "Elapsed", "Bandwidth"]);

        Self { builder }
    }
}

impl BenchmarkResults {
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

impl Display for BenchmarkResults {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut table = self.builder.clone().build();
        table.with(Style::rounded());
        write!(f, "{table}")
    }
}

fn format_duration(dur: Duration) -> String {
    if dur < Duration::from_secs(1) {
        let millis = dur.as_secs_f32() * 1000.0;
        format!("{millis:.2}ms")
    } else {
        let millis = dur.as_secs_f32();
        format!("{millis:.2}s")
    }
}

pub struct FileManager {
    base_path: PathBuf,
    sequence_id: usize,
}

impl FileManager {
    pub fn new(run_id: ulid::Ulid, base_path: &Path) -> io::Result<Self> {
        let path = base_path.join(format!("run-{run_id}"));

        std::fs::create_dir_all(&path)?;

        Ok(Self {
            base_path: path,
            sequence_id: 0,
        })
    }

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
}
