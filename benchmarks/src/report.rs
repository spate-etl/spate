//! The versioned record every benchmark binary emits.
//!
//! One JSON object per line, appended to the file named by `RESULTS`. A
//! single shape spans every rig: the arm under test goes in [`Report::variant`]
//! (an open map, so `kafka_topology`'s `mode`, `pipeline_synthetic`'s `threads`
//! and `ch_native_format`'s `format` coexist without a union type), and each
//! measured quantity goes in [`Report::metrics`].
//!
//! [`Metric`] carries its own `unit` and `higher_is_better`. That is
//! deliberate: a consumer plotting these records cannot silently draw a
//! lower-is-better quantity as a taller bar, because the direction travels with
//! the number rather than living in the plotting code.
//!
//! Top-level struct fields serialize in declaration order; `variant` and
//! `metrics` are `BTreeMap`s and so serialize with their keys sorted.

use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Schema version of the emitted records. Bump on any breaking field change.
pub const SCHEMA_VERSION: u32 = 1;

/// Whether a record reports a measurement or a decision derived from them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// An observed quantity for one arm of a benchmark.
    Measurement,
    /// A conclusion drawn across arms (for example a go/no-go gate).
    Verdict,
}

/// One measured quantity, carrying its unit and its direction of goodness.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    /// The measured value, in `unit`.
    pub value: f64,
    /// Unit of `value`, e.g. `ns`, `records/s`, `bytes`, `ms`.
    pub unit: String,
    /// `true` when a larger `value` is a better result.
    pub higher_is_better: bool,
    /// 95% confidence interval `(low, high)` when the rig took repetitions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ci95: Option<(f64, f64)>,
    /// Sample count behind `value` (repetitions, not inner iterations).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub n: Option<u64>,
}

impl Metric {
    /// A metric where more is better — throughput, rows written.
    pub fn maximize(value: f64, unit: impl Into<String>) -> Self {
        Self {
            value,
            unit: unit.into(),
            higher_is_better: true,
            ci95: None,
            n: None,
        }
    }

    /// A metric where less is better — latency, ns/record, bytes on the wire.
    pub fn minimize(value: f64, unit: impl Into<String>) -> Self {
        Self {
            value,
            unit: unit.into(),
            higher_is_better: false,
            ci95: None,
            n: None,
        }
    }

    /// A byte throughput, recorded as `MB/s` in the SI sense — 10^6 bytes, not
    /// 2^20.
    ///
    /// Rigs had drifted apart on this: two sites divided by `1024 * 1024` and
    /// two by `1e6`, all four emitting the same `MB/s` string, so the same
    /// physical throughput read 4.86% apart with nothing in the record to tell
    /// which convention produced it. Take the rate in bytes/s and let this pick
    /// the divisor, so a new rig cannot reintroduce the split.
    pub fn bytes_per_s(bytes_per_s: f64) -> Self {
        Self::maximize(bytes_per_s / 1e6, "MB/s")
    }

    /// Attaches a 95% confidence interval.
    #[must_use]
    pub fn with_ci(mut self, low: f64, high: f64) -> Self {
        self.ci95 = Some((low, high));
        self
    }

    /// Attaches the repetition count behind the value.
    #[must_use]
    pub fn with_n(mut self, n: u64) -> Self {
        self.n = Some(n);
        self
    }
}

/// Provenance for a run: when, where, and from which commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunMeta {
    /// Unix epoch milliseconds at which the record was built.
    pub ts_ms: u64,
    /// Short git commit of the working tree, when discoverable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    /// Host name of the machine that produced the record.
    pub host: String,
    /// CPU brand string, e.g. `Apple M5 Max`.
    pub cpu: String,
    /// Cores visible to the process.
    pub cores: usize,
    /// `os/arch`, e.g. `macos/aarch64`.
    pub os: String,
    /// Cargo profile the binary was built with.
    pub profile: String,
}

/// The static half of [`RunMeta`], resolved once per process.
struct StaticMeta {
    commit: Option<String>,
    host: String,
    cpu: String,
    cores: usize,
    os: String,
}

fn static_meta() -> &'static StaticMeta {
    static META: OnceLock<StaticMeta> = OnceLock::new();
    META.get_or_init(|| StaticMeta {
        commit: detect_commit(),
        host: detect_host(),
        cpu: detect_cpu(),
        cores: std::thread::available_parallelism().map_or(0, std::num::NonZero::get),
        os: format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH),
    })
}

fn trimmed_stdout(cmd: &str, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new(cmd).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    (!s.is_empty()).then_some(s)
}

fn detect_commit() -> Option<String> {
    if let Ok(c) = std::env::var("GIT_COMMIT")
        && !c.is_empty()
    {
        return Some(c);
    }
    trimmed_stdout("git", &["rev-parse", "--short=12", "HEAD"])
}

fn detect_host() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| trimmed_stdout("hostname", &[]))
        .unwrap_or_else(|| "unknown".to_owned())
}

fn detect_cpu() -> String {
    #[cfg(target_os = "macos")]
    if let Some(brand) = trimmed_stdout("sysctl", &["-n", "machdep.cpu.brand_string"]) {
        return brand;
    }
    #[cfg(target_os = "linux")]
    if let Ok(text) = std::fs::read_to_string("/proc/cpuinfo") {
        for line in text.lines() {
            if let Some((key, value)) = line.split_once(':')
                && key.trim() == "model name"
            {
                return value.trim().to_owned();
            }
        }
    }
    "unknown".to_owned()
}

impl RunMeta {
    /// Stamps the current time onto the process-wide static provenance.
    pub fn detect() -> Self {
        let meta = static_meta();
        Self {
            ts_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as u64),
            commit: meta.commit.clone(),
            host: meta.host.clone(),
            cpu: meta.cpu.clone(),
            cores: meta.cores,
            os: meta.os.clone(),
            profile: if cfg!(debug_assertions) {
                "debug".to_owned()
            } else {
                "release".to_owned()
            },
        }
    }
}

/// One emitted benchmark record.
///
/// ```no_run
/// use benchmarks::report::{Metric, Report};
///
/// Report::measurement("avro_pipeline")
///     .variant("deser", "fast_borrowed")
///     .variant("format", "native")
///     .metric("ns_per_event", Metric::minimize(54.0, "ns").with_n(15))
///     .metric("records_per_s", Metric::maximize(18_500_000.0, "records/s"))
///     .emit();
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Report {
    /// Schema version; always [`SCHEMA_VERSION`] on write.
    pub schema: u32,
    /// The rig that produced this record, e.g. `avro_pipeline`.
    pub bench: String,
    /// Measurement or verdict.
    pub kind: Kind,
    /// Provenance of the run.
    pub run: RunMeta,
    /// The arm under test, e.g. `{"deser": "fast_borrowed", "threads": 4}`.
    pub variant: BTreeMap<String, Value>,
    /// Measured quantities, keyed by metric name.
    pub metrics: BTreeMap<String, Metric>,
    /// Free-text caveat carried alongside the numbers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl Report {
    fn new(bench: impl Into<String>, kind: Kind) -> Self {
        Self {
            schema: SCHEMA_VERSION,
            bench: bench.into(),
            kind,
            run: RunMeta::detect(),
            variant: BTreeMap::new(),
            metrics: BTreeMap::new(),
            note: None,
        }
    }

    /// An observed quantity for one arm.
    pub fn measurement(bench: impl Into<String>) -> Self {
        Self::new(bench, Kind::Measurement)
    }

    /// A conclusion drawn across arms.
    pub fn verdict(bench: impl Into<String>) -> Self {
        Self::new(bench, Kind::Verdict)
    }

    /// Adds one dimension of the arm under test.
    #[must_use]
    pub fn variant(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.variant.insert(key.into(), value.into());
        self
    }

    /// Adds one measured quantity.
    #[must_use]
    pub fn metric(mut self, key: impl Into<String>, metric: Metric) -> Self {
        self.metrics.insert(key.into(), metric);
        self
    }

    /// Attaches a caveat that travels with the numbers.
    #[must_use]
    pub fn note(mut self, note: impl Into<String>) -> Self {
        self.note = Some(note.into());
        self
    }

    /// Prints the record to stdout and appends it to `RESULTS` when set.
    pub fn emit(&self) {
        crate::report(self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_json() {
        let rep = Report::measurement("avro_pipeline")
            .variant("deser", "fast_borrowed")
            .variant("threads", 4)
            .metric("ns_per_event", Metric::minimize(54.0, "ns").with_n(15))
            .metric(
                "records_per_s",
                Metric::maximize(18_500_000.0, "records/s").with_ci(18.0e6, 19.0e6),
            )
            .note("median of 15 reps");

        let line = serde_json::to_string(&rep).expect("serialize");
        assert!(!line.contains('\n'), "a record must be one JSON line");

        let back: Report = serde_json::from_str(&line).expect("deserialize");
        assert_eq!(back, rep);
        assert_eq!(back.schema, SCHEMA_VERSION);
        assert_eq!(back.kind, Kind::Measurement);
    }

    #[test]
    fn optional_fields_are_omitted_when_absent() {
        let line = serde_json::to_string(&Report::verdict("ch_native_format")).expect("serialize");
        assert!(!line.contains("note"), "{line}");
        assert!(line.contains(r#""kind":"verdict""#), "{line}");
    }

    #[test]
    fn byte_rates_are_si_megabytes() {
        // The whole point of the helper: one divisor, so rigs cannot drift onto
        // 2^20 while still labelling the result "MB/s".
        let m = Metric::bytes_per_s(1_048_576.0);
        assert_eq!(m.unit, "MB/s");
        assert!(m.higher_is_better);
        assert!(
            (m.value - 1.048576).abs() < 1e-12,
            "1 MiB/s must record as 1.048576 MB/s, got {}",
            m.value
        );
    }

    #[test]
    fn direction_travels_with_the_number() {
        assert!(Metric::maximize(1.0, "records/s").higher_is_better);
        assert!(!Metric::minimize(1.0, "ns").higher_is_better);
    }
}
