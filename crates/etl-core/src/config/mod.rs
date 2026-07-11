//! Pipeline configuration: typed framework sections plus opaque
//! per-component passthrough, loaded from YAML with `${VAR:-default}`
//! environment interpolation.
//!
//! The framework owns the typed sections (`pipeline`, `checkpoint`,
//! `backpressure`, `metrics`) and validates them strictly
//! (`deny_unknown_fields` at every level). The `source`, `deserializer`,
//! and `sink` sections are single-key mappings selecting a component type;
//! their bodies are opaque [`ComponentConfig`]s handed to the component's
//! factory, which deserializes its own typed config. This keeps connectors
//! fully decoupled from the framework schema.
//!
//! ```yaml
//! pipeline: { name: orders, threads: 4, io_threads: 2 }
//! checkpoint: { interval: 5s, max_pending_batches: 1024 }
//! backpressure: { max_inflight_bytes: 256MiB }
//! source:
//!   kafka:                                   # KafkaSourceConfig
//!     brokers: ${KAFKA_BROKERS:-localhost:9092}
//!     topic: orders
//!     group_id: orders-etl                   # required (no default)
//! deserializer:
//!   avro:                                    # AvroSettings (confluent mode)
//!     registry:
//!       url: ${SCHEMA_REGISTRY_URL:?schema registry required}
//! sink:
//!   clickhouse:                              # ClickHouseSinkConfig
//!     table: orders_local
//!     columns: [id, amount, ts]              # required; order is the wire contract
//!     shards:
//!       - { replicas: ["http://ch-0-0:8123", "http://ch-0-1:8123"] }
//! metrics: { exporter: prometheus, listen: 0.0.0.0:9090 }
//! ```
//!
//! Environment interpolation runs on the raw text before parsing — see
//! [`interpolate`](self::interpolate::interpolate_with) for the exact
//! semantics of `${VAR}`, `${VAR:-default}`, `${VAR:?message}`, and `$$`.
//!
//! # Example
//!
//! ```
//! use etl_core::config::PipelineConfig;
//!
//! let cfg = PipelineConfig::from_str(r#"
//! pipeline: { name: demo }
//! source: { memory: {} }
//! sink: { memory: {} }
//! "#).unwrap();
//!
//! assert_eq!(cfg.pipeline.name, "demo");
//! assert_eq!(cfg.pipeline.io_threads, 2);                    // default
//! assert_eq!(cfg.checkpoint.max_pending_batches, 1024);      // default
//! assert_eq!(cfg.source.type_tag(), "memory");
//! ```

mod component;
mod error;
mod interpolate;

pub use component::ComponentConfig;
pub use error::ConfigError;

/// Re-export of `serde_yaml::Value`, the opaque body type carried by a
/// [`ComponentConfig`]. `serde_yaml` is a 0.x dependency, so exposing its
/// `Value` directly in `etl-core`'s public API would tie our semver to
/// theirs; this alias is the documented exemption (mirroring the [`bytes`]
/// and `AvroValue` re-export pattern — see `docs/DESIGN.md` § Dependency
/// policy). A major bump of the YAML crate becomes a breaking change here,
/// and only here.
///
/// [`bytes`]: crate::bytes
pub use serde_yaml::Value as YamlValue;

use bytesize::ByteSize;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

/// Root of a pipeline's configuration file.
///
/// One process runs one pipeline; one file configures one process.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    /// Identity and thread budget.
    pub pipeline: PipelineSection,
    /// Watermark commit policy.
    #[serde(default)]
    pub checkpoint: CheckpointSection,
    /// In-flight budget and pause/resume hysteresis.
    #[serde(default)]
    pub backpressure: BackpressureSection,
    /// Exporter selection and observability knobs.
    #[serde(default)]
    pub metrics: MetricsSection,
    /// The source component (opaque body).
    pub source: ComponentConfig,
    /// Optional deserializer component (opaque body). Sources that emit
    /// ready-made records need none.
    #[serde(default)]
    pub deserializer: Option<ComponentConfig>,
    /// Single sink component (opaque body) — sugar for the common one-sink
    /// case, addressed as `"default"`. Mutually exclusive with `sinks`.
    /// Resolve via [`sink_config`](Self::sink_config).
    #[serde(default)]
    pub sink: Option<ComponentConfig>,
    /// Named sinks for a multi-sink split: a `name -> component` map, each an
    /// ordinary single-key component (`clickhouse: {...}`). Mutually exclusive
    /// with `sink`. Resolve via [`sink_config`](Self::sink_config).
    #[serde(default)]
    pub sinks: Option<BTreeMap<String, ComponentConfig>>,
}

/// `pipeline:` — identity and thread budget.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineSection {
    /// Pipeline name; the `pipeline` label on every metric.
    pub name: String,
    /// Pinned pipeline thread count. `None` derives from
    /// `available_parallelism` minus the I/O reserve at startup.
    #[serde(default)]
    pub threads: Option<usize>,
    /// Worker threads for the I/O runtime (sink workers, checkpointer,
    /// admin server).
    #[serde(default = "defaults::io_threads")]
    pub io_threads: usize,
    /// Core-pinning mode for pipeline threads.
    #[serde(default)]
    pub pinning: PinningMode,
}

/// How pipeline threads are pinned to cores.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PinningMode {
    /// No pinning (default). Correct choice unless the pod has exclusive
    /// cores (Kubernetes static CPU manager + Guaranteed QoS).
    #[default]
    Off,
    /// Pin thread *i* to core *i*.
    Compact,
}

/// `checkpoint:` — watermark commit policy.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CheckpointSection {
    /// How often committable watermarks are flushed to the source.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Maximum unacknowledged batches per partition before that partition
    /// is paused (doubles as a backpressure trigger).
    pub max_pending_batches: usize,
    /// Shutdown/rebalance drain budget. Must be comfortably below the pod's
    /// `terminationGracePeriodSeconds`.
    #[serde(with = "humantime_serde")]
    pub drain_timeout: Duration,
    /// A partition watermark stalled behind a failed batch for longer than
    /// this fails the pipeline. Failed batches only stall watermarks
    /// permanently (their data replays after restart), so this converts a
    /// permanent sink failure — a dropped table, revoked credentials — into
    /// a clean `Failed` exit and a restart instead of a process that runs on
    /// forever, consuming the source but committing nothing for that
    /// partition.
    #[serde(with = "humantime_serde")]
    pub stalled_fail_after: Duration,
}

impl Default for CheckpointSection {
    fn default() -> Self {
        CheckpointSection {
            interval: Duration::from_secs(5),
            max_pending_batches: 1024,
            drain_timeout: Duration::from_secs(25),
            stalled_fail_after: Duration::from_secs(120),
        }
    }
}

/// `backpressure:` — in-flight budget and hysteresis.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BackpressureSection {
    /// Global cap on bytes admitted into the pipeline but not yet durably
    /// written.
    pub max_inflight_bytes: ByteSize,
    /// Fraction of the budget at which sources are paused.
    pub high_ratio: f64,
    /// Fraction of the budget below which sources may resume.
    pub low_ratio: f64,
    /// Minimum pause duration before resuming (avoids flapping; pausing a
    /// Kafka partition purges its prefetch, so resume is not free).
    #[serde(with = "humantime_serde")]
    pub min_pause: Duration,
}

impl Default for BackpressureSection {
    fn default() -> Self {
        BackpressureSection {
            max_inflight_bytes: ByteSize::mib(256),
            high_ratio: 0.8,
            low_ratio: 0.5,
            min_pause: Duration::from_millis(500),
        }
    }
}

/// `metrics:` — exporter selection and observability knobs.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MetricsSection {
    /// Which exporter to install.
    pub exporter: MetricsExporter,
    /// Admin server bind address (`/metrics`, `/healthz`, `/readyz`).
    pub listen: SocketAddr,
    /// Emit per-partition gauge series (`partition` label). Off by default:
    /// cardinality grows with the assignment.
    pub per_partition_detail: bool,
    /// Time basis for `etl_e2e_latency_seconds`.
    pub e2e_basis: E2eBasis,
}

impl Default for MetricsSection {
    fn default() -> Self {
        MetricsSection {
            exporter: MetricsExporter::Prometheus,
            listen: SocketAddr::from(([0, 0, 0, 0], 9090)),
            per_partition_detail: false,
            e2e_basis: E2eBasis::Ingest,
        }
    }
}

/// Metrics exporter selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum MetricsExporter {
    /// Prometheus scrape endpoint on the admin server (default).
    #[default]
    Prometheus,
    /// No exporter (metrics recorded to a no-op recorder).
    None,
}

/// Which timestamp anchors end-to-end latency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum E2eBasis {
    /// Framework ingest time — immune to producer clock skew (default).
    #[default]
    Ingest,
    /// Record event time (e.g. Kafka message timestamp) — measures true
    /// pipeline lag but is sensitive to upstream clocks.
    Event,
}

mod defaults {
    pub(super) fn io_threads() -> usize {
        2
    }
}

impl PipelineConfig {
    /// Load from YAML text: interpolate `${VAR}` forms against the process
    /// environment, parse, and validate.
    // An inherent `from_str` (rather than `std::str::FromStr`) keeps the
    // call site `PipelineConfig::from_str(text)?` working without a trait
    // import, matching `from_path` beside it.
    #[expect(
        clippy::should_implement_trait,
        reason = "paired with from_path; no trait import required at call sites"
    )]
    pub fn from_str(text: &str) -> Result<Self, ConfigError> {
        let interpolated = interpolate::interpolate(text)?;
        Self::parse_interpolated(&interpolated)
    }

    /// Load from a YAML file (read, interpolate, parse, validate).
    pub fn from_path(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_owned(),
            source,
        })?;
        Self::from_str(&text)
    }

    fn parse_interpolated(text: &str) -> Result<Self, ConfigError> {
        let de = serde_yaml::Deserializer::from_str(text);
        let mut cfg: PipelineConfig =
            serde_path_to_error::deserialize(de).map_err(|e| ConfigError::Parse {
                path: e.path().to_string(),
                source: e.into_inner(),
            })?;
        cfg.source.set_section("source");
        if let Some(sink) = cfg.sink.as_mut() {
            sink.set_section("sink");
        }
        if let Some(sinks) = cfg.sinks.as_mut() {
            for sink in sinks.values_mut() {
                sink.set_section("sink");
            }
        }
        if let Some(deser) = cfg.deserializer.as_mut() {
            deser.set_section("deserializer");
        }
        cfg.validate()?;
        Ok(cfg)
    }

    /// Cross-field validation, run automatically by the loaders. Public so
    /// programmatically built configs (tests, `etl-test`) get the same
    /// checks.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let fail = |msg: String| Err(ConfigError::Validation(msg));

        if self.pipeline.name.trim().is_empty() {
            return fail("pipeline.name must not be empty".into());
        }
        if self.pipeline.io_threads == 0 {
            return fail("pipeline.io_threads must be at least 1".into());
        }
        if self.pipeline.threads == Some(0) {
            return fail("pipeline.threads must be at least 1 when set".into());
        }
        // The commit loop fires whenever `last_commit.elapsed() >= interval`,
        // so an interval below a poll cycle commits on nearly every loop.
        // A floor keeps sub-100ms intervals from hammering the source's
        // offset store for no durability gain (checkpointing is not the
        // durability boundary — the sink write is).
        const MIN_COMMIT_INTERVAL: Duration = Duration::from_millis(100);
        if self.checkpoint.interval < MIN_COMMIT_INTERVAL {
            return fail(format!(
                "checkpoint.interval must be at least 100ms (got {:?}): the commit \
                 loop fires every interval, so sub-100ms intervals hammer the \
                 source's offset store without improving durability",
                self.checkpoint.interval
            ));
        }
        if self.checkpoint.max_pending_batches == 0 {
            return fail("checkpoint.max_pending_batches must be at least 1".into());
        }
        if self.checkpoint.drain_timeout.is_zero() {
            return fail("checkpoint.drain_timeout must be greater than zero".into());
        }
        if self.checkpoint.stalled_fail_after.is_zero() {
            return fail("checkpoint.stalled_fail_after must be greater than zero".into());
        }
        if self.backpressure.max_inflight_bytes.as_u64() == 0 {
            return fail("backpressure.max_inflight_bytes must be greater than zero".into());
        }
        let (low, high) = (self.backpressure.low_ratio, self.backpressure.high_ratio);
        if !(low > 0.0 && low < high && high <= 1.0) {
            return fail(format!(
                "backpressure ratios must satisfy 0 < low_ratio < high_ratio <= 1 \
                 (got low_ratio={low}, high_ratio={high})"
            ));
        }
        match (&self.sink, &self.sinks) {
            (Some(_), Some(_)) => {
                return fail("set exactly one of `sink:` or `sinks:`, not both".into());
            }
            (None, None) => {
                return fail("a `sink:` or `sinks:` section is required".into());
            }
            (None, Some(map)) if map.is_empty() => {
                return fail("`sinks:` must declare at least one sink".into());
            }
            _ => {}
        }
        if let Some(sinks) = &self.sinks {
            for name in sinks.keys() {
                if name.is_empty() {
                    return fail("`sinks:` names must be non-empty".into());
                }
                // "default" maps to the historical component="sink" metric
                // label, so a sink literally named "sink" would merge its
                // series with the default's.
                if name == "sink" {
                    return fail(
                        "the sink name \"sink\" is reserved (it is the default \
                         sink's metric label); rename the `sinks:` entry"
                            .into(),
                    );
                }
            }
        }
        Ok(())
    }

    /// The component config for the sink named `name`. The single-sink `sink:`
    /// form is addressed as `"default"`. A connector factory calls this once
    /// per sink to build it.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Validation`] if no sink is configured under `name`.
    pub fn sink_config(&self, name: &str) -> Result<&ComponentConfig, ConfigError> {
        if let Some(sinks) = &self.sinks {
            sinks.get(name).ok_or_else(|| {
                let known: Vec<&str> = sinks.keys().map(String::as_str).collect();
                ConfigError::Validation(format!("no sink named {name:?} (configured: {known:?})"))
            })
        } else if name == "default" {
            self.sink
                .as_ref()
                .ok_or_else(|| ConfigError::Validation("no sink configured".into()))
        } else {
            Err(ConfigError::Validation(format!(
                "no sink named {name:?}: this pipeline configures a single `sink:` \
                 (address it as \"default\")"
            )))
        }
    }

    /// The configured sink names, sorted. A single-sink config reports
    /// `["default"]`.
    #[must_use]
    pub fn sink_names(&self) -> Vec<String> {
        match &self.sinks {
            Some(sinks) => sinks.keys().cloned().collect(),
            None => vec!["default".to_string()],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
pipeline: { name: demo }
source: { memory: {} }
sink: { memory: {} }
"#;

    #[test]
    fn minimal_config_applies_documented_defaults() {
        let cfg = PipelineConfig::from_str(MINIMAL).unwrap();
        assert_eq!(cfg.pipeline.name, "demo");
        assert_eq!(cfg.pipeline.threads, None);
        assert_eq!(cfg.pipeline.io_threads, 2);
        assert_eq!(cfg.pipeline.pinning, PinningMode::Off);
        assert_eq!(cfg.checkpoint.interval, Duration::from_secs(5));
        assert_eq!(cfg.checkpoint.max_pending_batches, 1024);
        assert_eq!(cfg.checkpoint.drain_timeout, Duration::from_secs(25));
        assert_eq!(cfg.checkpoint.stalled_fail_after, Duration::from_secs(120));
        assert_eq!(cfg.backpressure.max_inflight_bytes, ByteSize::mib(256));
        assert_eq!(cfg.backpressure.high_ratio, 0.8);
        assert_eq!(cfg.backpressure.low_ratio, 0.5);
        assert_eq!(cfg.backpressure.min_pause, Duration::from_millis(500));
        assert_eq!(cfg.metrics.exporter, MetricsExporter::Prometheus);
        assert_eq!(cfg.metrics.listen, SocketAddr::from(([0, 0, 0, 0], 9090)));
        assert!(!cfg.metrics.per_partition_detail);
        assert_eq!(cfg.metrics.e2e_basis, E2eBasis::Ingest);
        assert!(cfg.deserializer.is_none());
    }

    #[test]
    fn full_design_doc_example_parses() {
        // The connector bodies below mirror the module-doc example and use the
        // real connector field names. etl-core has no dependency on the
        // connector crates, so this test can only parse the framework layer;
        // the connector body shapes are verified by hand against
        // crates/etl-kafka/src/config.rs (brokers/topic/group_id),
        // crates/etl-avro/src/config.rs (registry.url), and
        // crates/etl-clickhouse/src/config.rs (table/columns/shards) — and
        // stay consistent with crates/etl/examples/kafka_avro_to_clickhouse.yaml.
        let yaml = r#"
pipeline: { name: orders, threads: 4, io_threads: 2 }
checkpoint: { interval: 5s, max_pending_batches: 1024 }
backpressure: { max_inflight_bytes: 256MiB }
source:
  kafka:
    brokers: ${KAFKA_BROKERS:-localhost:9092}
    topic: orders
    group_id: orders-etl
    rdkafka: { fetch.message.max.bytes: "1048576" }
deserializer:
  avro:
    mode: confluent
    registry:
      url: "${SCHEMA_REGISTRY_URL:-http://sr:8081}"
sink:
  clickhouse:
    table: orders_local
    columns: [id, amount, ts]
    shards:
      - { replicas: ["http://ch-0-0:8123", "http://ch-0-1:8123"] }
      - { replicas: ["http://ch-1-0:8123", "http://ch-1-1:8123"] }
    batch: { max_rows: 500000, max_bytes: 128MiB, linger: 1s }
    inflight: { max_per_shard: 2 }
    retry: { initial: 100ms, max: 10s, multiplier: 2.0 }
metrics: { exporter: prometheus, listen: 0.0.0.0:9090 }
"#;
        let cfg = PipelineConfig::from_str(yaml).unwrap();
        assert_eq!(cfg.pipeline.threads, Some(4));
        assert_eq!(cfg.source.type_tag(), "kafka");
        assert_eq!(cfg.deserializer.as_ref().unwrap().type_tag(), "avro");
        assert_eq!(cfg.sink_config("default").unwrap().type_tag(), "clickhouse");

        // Interpolated default landed inside the opaque body, and the kafka
        // body carries the required group_id.
        #[derive(Debug, serde::Deserialize)]
        struct KafkaProbe {
            brokers: String,
            group_id: String,
            #[serde(flatten)]
            _rest: serde_yaml::Value,
        }
        let kafka: KafkaProbe = cfg.source.deserialize_into().unwrap();
        assert_eq!(kafka.brokers, "localhost:9092");
        assert_eq!(kafka.group_id, "orders-etl");

        // The avro body uses the nested `registry.url` shape, and the
        // clickhouse body carries the required `columns`.
        #[derive(Debug, serde::Deserialize)]
        struct AvroProbe {
            registry: RegistryProbe,
        }
        #[derive(Debug, serde::Deserialize)]
        struct RegistryProbe {
            url: String,
        }
        let avro: AvroProbe = cfg
            .deserializer
            .as_ref()
            .unwrap()
            .deserialize_into()
            .unwrap();
        assert_eq!(avro.registry.url, "http://sr:8081");

        #[derive(Debug, serde::Deserialize)]
        struct ChProbe {
            columns: Vec<String>,
        }
        let ch: ChProbe = cfg
            .sink_config("default")
            .unwrap()
            .deserialize_into()
            .unwrap();
        assert_eq!(ch.columns, ["id", "amount", "ts"]);
    }

    #[test]
    fn single_sink_resolves_as_default() {
        let cfg = PipelineConfig::from_str(MINIMAL).unwrap();
        assert_eq!(cfg.sink_names(), vec!["default".to_string()]);
        assert_eq!(cfg.sink_config("default").unwrap().type_tag(), "memory");
        assert!(cfg.sink_config("other").is_err());
    }

    #[test]
    fn sinks_map_parses_and_resolves_by_name() {
        let yaml = r#"
pipeline: { name: demo }
source: { memory: {} }
sinks:
  type_a: { memory: {} }
  type_b: { memory: {} }
"#;
        let cfg = PipelineConfig::from_str(yaml).unwrap();
        assert_eq!(
            cfg.sink_names(),
            vec!["type_a".to_string(), "type_b".to_string()]
        );
        assert_eq!(cfg.sink_config("type_a").unwrap().type_tag(), "memory");
        assert_eq!(cfg.sink_config("type_b").unwrap().type_tag(), "memory");
        // The single-sink alias is not present in a `sinks:` config.
        assert!(cfg.sink_config("default").is_err());
    }

    #[test]
    fn sink_and_sinks_are_mutually_exclusive() {
        let both = r#"
pipeline: { name: demo }
source: { memory: {} }
sink: { memory: {} }
sinks:
  a: { memory: {} }
"#;
        assert!(matches!(
            PipelineConfig::from_str(both),
            Err(ConfigError::Validation(_))
        ));

        let neither = r#"
pipeline: { name: demo }
source: { memory: {} }
"#;
        assert!(matches!(
            PipelineConfig::from_str(neither),
            Err(ConfigError::Validation(_))
        ));

        let empty = r#"
pipeline: { name: demo }
source: { memory: {} }
sinks: {}
"#;
        assert!(matches!(
            PipelineConfig::from_str(empty),
            Err(ConfigError::Validation(_))
        ));
    }

    #[test]
    fn reserved_sink_names_are_rejected() {
        // "sink" is the default sink's metric label; a `sinks:` entry under
        // that name would merge its series with a default sink's.
        let reserved = r#"
pipeline: { name: demo }
source: { memory: {} }
sinks:
  sink: { memory: {} }
"#;
        assert!(matches!(
            PipelineConfig::from_str(reserved),
            Err(ConfigError::Validation(msg)) if msg.contains("reserved")
        ));

        let empty_name = r#"
pipeline: { name: demo }
source: { memory: {} }
sinks:
  "": { memory: {} }
"#;
        assert!(matches!(
            PipelineConfig::from_str(empty_name),
            Err(ConfigError::Validation(msg)) if msg.contains("non-empty")
        ));
    }

    #[test]
    fn unknown_fields_are_rejected_at_every_typed_level() {
        for (yaml, field) in [
            (
                "pipeline: { name: x, bogus: 1 }\nsource: { m: {} }\nsink: { m: {} }",
                "bogus",
            ),
            (
                "pipeline: { name: x }\ncheckpoint: { intervall: 5s }\nsource: { m: {} }\nsink: { m: {} }",
                "intervall",
            ),
            (
                "pipeline: { name: x }\nmetrics: { port: 9 }\nsource: { m: {} }\nsink: { m: {} }",
                "port",
            ),
            (
                "pipeline: { name: x }\nsource: { m: {} }\nsink: { m: {} }\nsinks: {}",
                "sinks",
            ),
        ] {
            let err = PipelineConfig::from_str(yaml).unwrap_err().to_string();
            assert!(err.contains(field), "expected `{field}` in error: {err}");
        }
    }

    #[test]
    fn parse_errors_carry_the_yaml_path() {
        let yaml = "pipeline: { name: x, io_threads: many }\nsource: { m: {} }\nsink: { m: {} }";
        let err = PipelineConfig::from_str(yaml).unwrap_err();
        let text = err.to_string();
        assert!(text.contains("pipeline.io_threads"), "{text}");
    }

    #[test]
    fn validation_rules() {
        let cases = [
            (
                "pipeline: { name: '  ' }\nsource: { m: {} }\nsink: { m: {} }",
                "pipeline.name",
            ),
            (
                "pipeline: { name: x, io_threads: 0 }\nsource: { m: {} }\nsink: { m: {} }",
                "io_threads",
            ),
            (
                "pipeline: { name: x, threads: 0 }\nsource: { m: {} }\nsink: { m: {} }",
                "threads",
            ),
            (
                "pipeline: { name: x }\ncheckpoint: { interval: 0s }\nsource: { m: {} }\nsink: { m: {} }",
                "interval",
            ),
            (
                "pipeline: { name: x }\ncheckpoint: { interval: 50ms }\nsource: { m: {} }\nsink: { m: {} }",
                "at least 100ms",
            ),
            (
                "pipeline: { name: x }\ncheckpoint: { max_pending_batches: 0 }\nsource: { m: {} }\nsink: { m: {} }",
                "max_pending_batches",
            ),
            (
                "pipeline: { name: x }\ncheckpoint: { drain_timeout: 0s }\nsource: { m: {} }\nsink: { m: {} }",
                "drain_timeout",
            ),
            (
                "pipeline: { name: x }\ncheckpoint: { stalled_fail_after: 0s }\nsource: { m: {} }\nsink: { m: {} }",
                "stalled_fail_after",
            ),
            (
                "pipeline: { name: x }\nbackpressure: { max_inflight_bytes: 0 }\nsource: { m: {} }\nsink: { m: {} }",
                "max_inflight_bytes",
            ),
            (
                "pipeline: { name: x }\nbackpressure: { low_ratio: 0.9, high_ratio: 0.8 }\nsource: { m: {} }\nsink: { m: {} }",
                "low_ratio",
            ),
            (
                "pipeline: { name: x }\nbackpressure: { high_ratio: 1.5 }\nsource: { m: {} }\nsink: { m: {} }",
                "high_ratio",
            ),
        ];
        for (yaml, needle) in cases {
            let err = PipelineConfig::from_str(yaml).unwrap_err().to_string();
            assert!(err.contains(needle), "expected `{needle}` in: {err}");
        }
    }

    #[test]
    fn missing_required_sections_error_clearly() {
        let err = PipelineConfig::from_str("pipeline: { name: x }\nsink: { m: {} }")
            .unwrap_err()
            .to_string();
        assert!(err.contains("source"), "{err}");
        let err = PipelineConfig::from_str("source: { m: {} }\nsink: { m: {} }")
            .unwrap_err()
            .to_string();
        assert!(err.contains("pipeline"), "{err}");
    }

    #[test]
    fn interpolation_failures_surface_with_position() {
        let err = PipelineConfig::from_str("pipeline:\n  name: ${UNSET_VAR_FOR_TEST}\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("UNSET_VAR_FOR_TEST"), "{err}");
        assert!(err.contains("line 2"), "{err}");
    }

    #[test]
    fn from_path_reads_interpolates_and_reports_io_errors() {
        use std::io::Write as _;
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write!(
            file,
            "pipeline: {{ name: ${{FILE_TEST_NAME:-from-file}} }}\nsource: {{ m: {{}} }}\nsink: {{ m: {{}} }}\n"
        )
        .unwrap();
        let cfg = PipelineConfig::from_path(file.path()).unwrap();
        assert_eq!(cfg.pipeline.name, "from-file");

        let err = PipelineConfig::from_path(Path::new("/nonexistent/etl.yaml")).unwrap_err();
        assert!(matches!(err, ConfigError::Io { .. }));
        assert!(err.to_string().contains("/nonexistent/etl.yaml"));
    }

    #[test]
    fn equal_inputs_parse_to_equal_configs() {
        let a = PipelineConfig::from_str(MINIMAL).unwrap();
        let b = PipelineConfig::from_str(MINIMAL).unwrap();
        assert_eq!(a, b);
        let c = PipelineConfig::from_str(
            "pipeline: { name: other }\nsource: { m: {} }\nsink: { m: {} }",
        )
        .unwrap();
        assert_ne!(a, c);
    }
}
