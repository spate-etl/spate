//! The versioned record a bench process emits, one per (case, replicate).
//!
//! One JSON object per line. A leg is a directory of `.jsonl` files and nothing
//! else — no index, no manifest — because records self-describe: each carries
//! its own case identity, its own iteration count, its own corpus digest and
//! its own build fingerprint. Two legs are compared by reading both directories
//! and pairing what is in them.
//!
//! # Absent is not zero
//!
//! Every metric is optional and several are conditional: `peak_rss_bytes` only
//! when running the case took the process above what its setup left resident
//! and the case did not batch its inputs, the allocation metrics only when the
//! counting allocator is installed, the throughput metrics only when the case
//! declared how much one iteration covers. A missing metric is
//! left out of the map and explained in [`Record::notes`]. Writing a zero
//! instead would compare as a real change of the whole quantity, which is the
//! failure this rule exists to prevent.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::fingerprint::{BuildFingerprint, Host};

/// Schema version of the emitted records. Bump on any breaking field change.
pub const SCHEMA_VERSION: u32 = 1;

/// Wall time inside the measured region, divided by the iteration count.
pub const WALL_NS_PER_ITER: &str = "wall_ns_per_iter";
/// CPU time (user + system) inside the region, divided by the iteration count.
pub const CPU_NS_PER_ITER: &str = "cpu_ns_per_iter";
/// Items processed per second, when the case declared how many an iteration
/// covers.
pub const RECORDS_PER_S: &str = "records_per_s";
/// Bytes processed per second, when the case declared how many an iteration
/// covers.
pub const BYTES_PER_S: &str = "bytes_per_s";
/// The process's peak resident set, present only when running the case set it.
pub const PEAK_RSS_BYTES: &str = "peak_rss_bytes";
/// Bytes allocated inside the region, divided by the iteration count.
pub const ALLOC_BYTES_PER_ITER: &str = "alloc_bytes_per_iter";
/// Allocations made inside the region, divided by the iteration count.
pub const ALLOC_COUNT_PER_ITER: &str = "alloc_count_per_iter";

/// Which case a record belongs to.
///
/// Three parts rather than one string, because the driver has to intersect two
/// legs' case lists and a package rename must not silently unpair every case in
/// it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CaseId {
    /// The package the bench target belongs to, e.g. `spate-json`.
    #[serde(rename = "crate")]
    pub krate: String,
    /// The bench target, e.g. `decode_wall`.
    pub target: String,
    /// The case within the target, e.g. `ndjson_1mib`.
    pub case: String,
}

impl std::fmt::Display for CaseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.krate, self.target, self.case)
    }
}

/// One measured quantity, carrying its unit and its direction of goodness.
///
/// The direction travels with the number rather than living in the renderer, so
/// a consumer cannot draw a lower-is-better quantity as an improvement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    /// The measured value, in `unit`.
    pub value: f64,
    /// Unit of `value`, e.g. `ns`, `records/s`, `bytes`.
    pub unit: String,
    /// `true` when a larger `value` is a better result.
    pub higher_is_better: bool,
}

impl Metric {
    /// A metric where more is better — throughput, records per second.
    #[must_use]
    pub fn maximize(value: f64, unit: impl Into<String>) -> Self {
        Self {
            value,
            unit: unit.into(),
            higher_is_better: true,
        }
    }

    /// A metric where less is better — latency, bytes allocated, resident set.
    #[must_use]
    pub fn minimize(value: f64, unit: impl Into<String>) -> Self {
        Self {
            value,
            unit: unit.into(),
            higher_is_better: false,
        }
    }
}

/// One (case, replicate) measurement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    /// [`SCHEMA_VERSION`] at the time of writing.
    pub schema: u32,
    /// Which case this measures.
    pub case: CaseId,
    /// Zero-based replicate index. Pairing is by this number, never by
    /// position in a file: a leg that lost one process would otherwise shift
    /// every later replicate against its partner and fabricate a difference.
    pub replicate: u32,
    /// Whether this is the discarded priming pass rather than a replicate.
    ///
    /// Written to the leg directory like any other record — a priming pass that
    /// vanished would be indistinguishable from one that never ran — and
    /// excluded by the comparator.
    pub priming: bool,
    /// Iterations of the case's routine inside the measured region.
    ///
    /// Decided by the driver, calibrated once on the base leg and pinned for
    /// both. A record whose `iters` differs from its partner's is not
    /// comparable.
    pub iters: u64,
    /// Whether the case declared itself noisy. An erratic case is reported and
    /// never flagged.
    pub erratic: bool,
    /// The seed the case's corpus was built from.
    pub seed: u64,
    /// Digest of everything the case absorbed into its [`crate::Corpus`].
    pub corpus_digest: String,
    /// The measured quantities. Absent is not zero — see the module docs.
    pub metrics: BTreeMap<String, Metric>,
    /// Why a metric is missing, or anything else about how the record was
    /// produced.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
    /// The build this record came from.
    pub build: BuildFingerprint,
    /// The machine this record came from.
    pub host: Host,
    /// Unix epoch milliseconds at which the record was written.
    pub ts_ms: u64,
}

impl Record {
    /// Milliseconds since the epoch, or zero if the clock is before it.
    #[must_use]
    pub fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }

    /// The record as one JSONL line, without the newline.
    ///
    /// # Errors
    ///
    /// When the record does not serialise. No arithmetic here can produce that
    /// today — `serde_json` writes a non-finite float as `null` rather than
    /// failing — so the error is carried rather than unwrapped, and a `null`
    /// would fail on the *read* side instead.
    pub fn to_line(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{CaseId, Metric, Record, SCHEMA_VERSION};
    use crate::fingerprint::{BuildFingerprint, Host};

    fn fixture() -> Record {
        Record {
            schema: SCHEMA_VERSION,
            case: CaseId {
                krate: "spate-bench".to_owned(),
                target: "selftest_wall".to_owned(),
                case: "sort_u64_16k".to_owned(),
            },
            replicate: 3,
            priming: false,
            iters: 64,
            erratic: false,
            seed: 12345,
            corpus_digest: "0123456789abcdef".to_owned(),
            metrics: BTreeMap::from([
                (
                    super::WALL_NS_PER_ITER.to_owned(),
                    Metric::minimize(1234.5, "ns"),
                ),
                (
                    super::RECORDS_PER_S.to_owned(),
                    Metric::maximize(987.0, "records/s"),
                ),
            ]),
            notes: vec!["peak_rss_bytes absent: the region never set the mark".to_owned()],
            build: BuildFingerprint {
                protocol: 1,
                leg: "head".to_owned(),
                rustc: Some("rustc 1.94.0".to_owned()),
                host_triple: Some("aarch64-apple-darwin".to_owned()),
                profile: Some("bench".to_owned()),
                codegen: Some("0f1e2d3c4b5a6978".to_owned()),
                features: vec!["simd".to_owned()],
                feature_args: vec!["--features".to_owned(), "simd".to_owned()],
                git_describe: Some("v0.1.0-3-gabc1234".to_owned()),
                dirty: true,
            },
            host: Host {
                os: "macos/aarch64".to_owned(),
                cpu: "Apple M5 Max".to_owned(),
                cores: 16,
                label: "local".to_owned(),
            },
            ts_ms: 1_754_000_000_000,
        }
    }

    /// The golden line. A renamed or reordered field is a test failure here
    /// rather than a leg that silently pairs with nothing: the comparator reads
    /// records written by *another checkout's* copy of this crate, so the wire
    /// shape is a contract between two versions of ourselves.
    #[test]
    fn the_record_line_is_the_shape_the_comparator_reads() {
        const GOLDEN: &str = concat!(
            r#"{"schema":1,"#,
            r#""case":{"crate":"spate-bench","target":"selftest_wall","case":"sort_u64_16k"},"#,
            r#""replicate":3,"priming":false,"iters":64,"erratic":false,"seed":12345,"#,
            r#""corpus_digest":"0123456789abcdef","#,
            r#""metrics":{"#,
            r#""records_per_s":{"value":987.0,"unit":"records/s","higher_is_better":true},"#,
            r#""wall_ns_per_iter":{"value":1234.5,"unit":"ns","higher_is_better":false}"#,
            r#"},"#,
            r#""notes":["peak_rss_bytes absent: the region never set the mark"],"#,
            r#""build":{"protocol":1,"leg":"head","rustc":"rustc 1.94.0","#,
            r#""host_triple":"aarch64-apple-darwin","profile":"bench","#,
            r#""codegen":"0f1e2d3c4b5a6978","features":["simd"],"#,
            r#""feature_args":["--features","simd"],"git_describe":"v0.1.0-3-gabc1234","dirty":true},"#,
            r#""host":{"os":"macos/aarch64","cpu":"Apple M5 Max","cores":16,"label":"local"},"#,
            r#""ts_ms":1754000000000}"#,
        );

        assert_eq!(fixture().to_line().expect("serialises"), GOLDEN);
    }

    #[test]
    fn a_record_survives_a_round_trip() {
        let original = fixture();
        let line = original.to_line().expect("serialises");
        let parsed: Record = serde_json::from_str(&line).expect("parses");
        assert_eq!(parsed, original);
    }

    /// The optional fields must vanish rather than serialise as `null`: a leg
    /// written by a checkout that had no notes has to parse identically to one
    /// written by a checkout that does.
    #[test]
    fn empty_optionals_are_omitted() {
        let mut record = fixture();
        record.notes.clear();
        record.build.features.clear();
        record.build.feature_args.clear();
        let line = record.to_line().expect("serialises");
        assert!(!line.contains("notes"), "{line}");
        assert!(!line.contains("features"), "{line}");

        let parsed: Record = serde_json::from_str(&line).expect("parses");
        assert_eq!(parsed, record);
    }

    #[test]
    fn the_case_id_renders_as_a_path() {
        assert_eq!(
            fixture().case.to_string(),
            "spate-bench/selftest_wall/sort_u64_16k"
        );
    }
}
