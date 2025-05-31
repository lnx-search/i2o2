use std::fmt::{Display, Formatter};
use std::time::Duration;

use tabled::builder::Builder;
use tabled::settings::Style;

pub struct BenchmarkResults {
    builder: Builder,
}

impl Default for BenchmarkResults {
    fn default() -> Self {
        let mut builder = Builder::with_capacity(0, 4);
        builder.push_record(["Name", "Num Workers", "Elapsed", "Total Ops", "Ops/sec"]);

        Self { builder }
    }
}

impl BenchmarkResults {
    pub fn push(
        &mut self,
        name: &str,
        num_workers: usize,
        elapsed: Duration,
        total_ops: usize,
        ops_per_sec: f32,
    ) {
        self.builder.push_record([
            name.to_string(),
            format!("{num_workers}"),
            format_duration(elapsed),
            format_total(total_ops as f32),
            format_total(ops_per_sec),
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
