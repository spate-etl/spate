//! Pipeline configuration: typed framework sections plus opaque
//! per-component passthrough, loaded from YAML with `${VAR:-default}`
//! environment interpolation.
//!
//! The framework owns the typed sections (`admin`, `backpressure`,
//! `checkpoint`, `metrics`, `pipeline`) and validates them strictly
//! (`deny_unknown_fields` at every level). The `source`, `deserializer`,
//! and `sink` sections are single-key mappings selecting a component type;
//! their bodies are opaque [`ComponentConfig`]s handed to the component's
//! factory, which deserializes its own typed config.
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
//! admin: { listen: 0.0.0.0:9090 }           # /metrics, /healthz, /readyz
//! metrics: { exporter: prometheus }
//! ```
//!
//! Environment interpolation runs on the raw text before parsing. See
//! `interpolate_with` for the exact semantics of `${VAR}`,
//! `${VAR:-default}`, `${VAR:?message}`, and `$$`.
//!
//! Every framework section here is `#[non_exhaustive]`. Build a config with
//! [`PipelineConfig::new`] or [`PipelineConfig::new_multi_sink`], a
//! [`PipelineSection`] with [`PipelineSection::new`], and the optional
//! sections with `default()`, then assign the fields you are setting; a key
//! added later arrives as a new default and existing code keeps compiling.
//!
//! # Example
//!
//! ```
//! use spate_core::config::PipelineConfig;
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

mod chunk;
mod component;
mod error;
mod interpolate;

pub use component::ComponentConfig;
pub use error::ConfigError;

/// Re-export of `serde_yaml::Value`, the opaque body type carried by a
/// [`ComponentConfig`]. `serde_yaml` is a 0.x dependency, so exposing its
/// `Value` directly in `spate-core`'s public API would tie our semver to
/// theirs; this alias is the documented exemption (mirroring the [`bytes`]
/// and `AvroValue` re-export pattern, INV-6).
/// A major bump of the YAML crate becomes a breaking change here,
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
///
/// Construct with [`PipelineConfig::new`], or
/// [`PipelineConfig::new_multi_sink`] for a `sinks:` map, and set the optional
/// fields. The struct is `#[non_exhaustive]` so new sections can be added
/// without breaking callers.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct PipelineConfig {
    /// Identity and thread budget.
    pub pipeline: PipelineSection,
    /// The admin server carrying `/metrics`, `/healthz` and `/readyz`.
    #[serde(default)]
    pub admin: AdminSection,
    /// In-flight budget and pause/resume hysteresis.
    #[serde(default)]
    pub backpressure: BackpressureSection,
    /// Watermark commit policy.
    #[serde(default)]
    pub checkpoint: CheckpointSection,
    /// Exporter selection and observability knobs.
    #[serde(default)]
    pub metrics: MetricsSection,
    /// The source component (opaque body).
    pub source: ComponentConfig,
    /// Optional deserializer component (opaque body). Sources that emit
    /// ready-made records need none.
    #[serde(default)]
    pub deserializer: Option<ComponentConfig>,
    /// Single sink component (opaque body), sugar for the common one-sink
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

/// Identity and thread budget (`pipeline:`).
///
/// Construct with [`PipelineSection::new`] and set the optional fields. The
/// struct is `#[non_exhaustive]` so new knobs can be added without breaking
/// callers.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
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

impl PipelineSection {
    /// A section named `name`. Every other field starts at its YAML default.
    ///
    /// ```
    /// use spate_core::config::PipelineSection;
    ///
    /// let mut pipeline = PipelineSection::new("orders");
    /// pipeline.io_threads = 4;
    ///
    /// assert_eq!(pipeline.name, "orders");
    /// assert_eq!(pipeline.threads, None);
    /// ```
    #[must_use]
    pub fn new(name: impl Into<String>) -> PipelineSection {
        PipelineSection {
            name: name.into(),
            threads: None,
            io_threads: defaults::io_threads(),
            pinning: PinningMode::default(),
        }
    }
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

/// Watermark commit policy (`checkpoint:`).
///
/// Construct with [`CheckpointSection::default`] and set the fields. The
/// struct is `#[non_exhaustive]` so new knobs can be added without breaking
/// callers.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct CheckpointSection {
    /// How often committable watermarks are flushed to the source.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Hard per-partition ceiling on unacknowledged batches. A partition at
    /// the ceiling has its lanes skipped at the poll boundary until
    /// acknowledgments retire batches. No pause is involved, and other
    /// partitions are unaffected. Bounds tracker memory and the replay a
    /// stalled partition can accumulate.
    pub max_pending_batches: usize,
    /// Shutdown/rebalance drain budget. Must be comfortably below the pod's
    /// `terminationGracePeriodSeconds`.
    #[serde(with = "humantime_serde")]
    pub drain_timeout: Duration,
    /// A partition watermark stalled behind a failed batch for longer than
    /// this fails the pipeline. Failed batches only stall watermarks
    /// permanently (their data replays after restart), so this converts a
    /// permanent sink failure (a dropped table, revoked credentials) into a
    /// clean `Failed` exit and a restart instead of a process that runs on
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

/// In-flight budget and hysteresis (`backpressure:`).
///
/// Construct with [`BackpressureSection::default`] and set the fields. The
/// struct is `#[non_exhaustive]` so new knobs can be added without breaking
/// callers.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
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

/// The HTTP server carrying `/metrics`, `/healthz` and `/readyz` (`admin:`).
///
/// One server serves all three. The probes answer regardless of
/// [`MetricsSection::exporter`]. `/metrics` is served only where this
/// pipeline's own handle renders an exposition. `exporter: none` leaves it a
/// 404, and so does a recorder another library installed first, which this
/// pipeline records into but cannot render.
///
/// Construct with [`AdminSection::default`] and set the fields. The struct is
/// `#[non_exhaustive]` so new knobs can be added without breaking callers.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct AdminSection {
    /// Bind address, or `none` for no server at all.
    ///
    /// With no server the probes and the exposition are unreachable over
    /// HTTP. The exposition is still readable in-process through
    /// [`Pipeline::metrics`](crate::pipeline::Pipeline::metrics), which is
    /// what an embedding program mounting it on its own server uses.
    ///
    /// A port of `0` asks the kernel to pick a free one, and the start logs
    /// the address the server bound, at `INFO`. [Monitoring][mon] names the
    /// message it goes out under.
    ///
    /// [mon]: https://spate.kainth.dev/docs/user-guide/deployment/monitoring
    #[serde(deserialize_with = "listen_or_none")]
    pub listen: Option<SocketAddr>,
}

impl Default for AdminSection {
    fn default() -> Self {
        AdminSection {
            listen: Some(SocketAddr::from(([0, 0, 0, 0], 9090))),
        }
    }
}

/// Deserialize a bind address or the literal `none`.
///
/// Takes the value through a visitor rather than through `String` so that a
/// YAML null (`~`, `null`, or a bare `listen:`) reports the two accepted
/// spellings instead of a type mismatch against an intermediate this key does
/// not otherwise have.
fn listen_or_none<'de, D>(de: D) -> Result<Option<SocketAddr>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Visitor;

    const EXPECTING: &str = r#"a socket address or "none""#;

    impl serde::de::Visitor<'_> for Visitor {
        type Value = Option<SocketAddr>;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(EXPECTING)
        }

        fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<Self::Value, E> {
            if text == "none" {
                return Ok(None);
            }
            text.parse()
                .map(Some)
                .map_err(|_| E::invalid_value(serde::de::Unexpected::Str(text), &EXPECTING))
        }
    }

    de.deserialize_any(Visitor)
}

/// Exporter selection and observability knobs (`metrics:`).
///
/// Construct with [`MetricsSection::default`] and set the fields. The struct
/// is `#[non_exhaustive]` so new knobs can be added without breaking callers.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct MetricsSection {
    /// Which exporter to install.
    pub exporter: MetricsExporter,
    /// Emit per-partition gauge series (`partition` label). Off by default:
    /// cardinality grows with the assignment.
    pub per_partition_detail: bool,
    /// Time basis for `spate_e2e_latency_seconds`.
    pub e2e_basis: E2eBasis,
}

impl Default for MetricsSection {
    fn default() -> Self {
        MetricsSection {
            exporter: MetricsExporter::Prometheus,
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
    /// Framework ingest time, immune to producer clock skew (default).
    #[default]
    Ingest,
    /// Record event time (e.g. Kafka message timestamp). Measures true
    /// pipeline lag but is sensitive to upstream clocks.
    Event,
}

mod defaults {
    pub(super) fn io_threads() -> usize {
        2
    }
}

/// Tag a component with the section it sits in, so its error paths read the
/// same whether the config was parsed or built in code.
fn labelled(mut component: ComponentConfig, section: &'static str) -> ComponentConfig {
    component.set_section(section);
    component
}

impl PipelineConfig {
    /// A config for one source and one sink, the `sink:` form. Every optional
    /// section starts at its YAML default, and the sink is addressed as
    /// `"default"` by [`sink_config`](Self::sink_config).
    ///
    /// ```
    /// use spate_core::config::{ComponentConfig, PipelineConfig, PipelineSection, YamlValue};
    /// use std::time::Duration;
    ///
    /// let mut pipeline = PipelineSection::new("orders");
    /// pipeline.io_threads = 4;
    ///
    /// let mut cfg = PipelineConfig::new(
    ///     pipeline,
    ///     ComponentConfig::new("memory", YamlValue::Mapping(Default::default())),
    ///     ComponentConfig::new("memory", YamlValue::Mapping(Default::default())),
    /// );
    /// cfg.checkpoint.interval = Duration::from_secs(10);
    /// cfg.validate()?;
    ///
    /// assert_eq!(cfg.sink_config("default")?.type_tag(), "memory");
    /// # Ok::<(), spate_core::config::ConfigError>(())
    /// ```
    #[must_use]
    pub fn new(
        pipeline: PipelineSection,
        source: ComponentConfig,
        sink: ComponentConfig,
    ) -> PipelineConfig {
        Self::assemble(pipeline, source, Some(labelled(sink, "sink")), None)
    }

    /// A config for one source and a map of named sinks, the `sinks:` form.
    /// Every optional section starts at its YAML default, and each name
    /// addresses its sink through [`sink_config`](Self::sink_config).
    ///
    /// The map is not checked here. [`validate`](Self::validate) rejects an
    /// empty map, an empty name, and the reserved name `"sink"`.
    ///
    /// ```
    /// use spate_core::config::{ComponentConfig, PipelineConfig, PipelineSection, YamlValue};
    /// use std::collections::BTreeMap;
    ///
    /// let body = || YamlValue::Mapping(Default::default());
    /// let sinks = BTreeMap::from([
    ///     ("eu".to_owned(), ComponentConfig::new("memory", body())),
    ///     ("us".to_owned(), ComponentConfig::new("memory", body())),
    /// ]);
    ///
    /// let cfg = PipelineConfig::new_multi_sink(
    ///     PipelineSection::new("orders"),
    ///     ComponentConfig::new("memory", body()),
    ///     sinks,
    /// );
    /// cfg.validate()?;
    ///
    /// assert_eq!(cfg.sink_names(), ["eu", "us"]);
    /// # Ok::<(), spate_core::config::ConfigError>(())
    /// ```
    #[must_use]
    pub fn new_multi_sink(
        pipeline: PipelineSection,
        source: ComponentConfig,
        sinks: BTreeMap<String, ComponentConfig>,
    ) -> PipelineConfig {
        let sinks = sinks
            .into_iter()
            .map(|(name, sink)| (name, labelled(sink, "sink")))
            .collect();
        Self::assemble(pipeline, source, None, Some(sinks))
    }

    /// The same config with a deserializer component, tagged with its
    /// section the way the constructors tag the source and the sink.
    ///
    /// ```
    /// use spate_core::config::{ComponentConfig, PipelineConfig, PipelineSection, YamlValue};
    ///
    /// let body = || YamlValue::Mapping(Default::default());
    /// let cfg = PipelineConfig::new(
    ///     PipelineSection::new("orders"),
    ///     ComponentConfig::new("memory", body()),
    ///     ComponentConfig::new("memory", body()),
    /// )
    /// .with_deserializer(ComponentConfig::new("json", body()));
    ///
    /// assert_eq!(
    ///     cfg.deserializer.as_ref().map(ComponentConfig::type_tag),
    ///     Some("json")
    /// );
    /// ```
    #[must_use]
    pub fn with_deserializer(mut self, deserializer: ComponentConfig) -> PipelineConfig {
        self.deserializer = Some(labelled(deserializer, "deserializer"));
        self
    }

    /// The one place the field list lives, so a section added later reaches
    /// both constructors. Callers pass exactly one of `sink`/`sinks`.
    fn assemble(
        pipeline: PipelineSection,
        source: ComponentConfig,
        sink: Option<ComponentConfig>,
        sinks: Option<BTreeMap<String, ComponentConfig>>,
    ) -> PipelineConfig {
        PipelineConfig {
            pipeline,
            admin: AdminSection::default(),
            backpressure: BackpressureSection::default(),
            checkpoint: CheckpointSection::default(),
            metrics: MetricsSection::default(),
            source: labelled(source, "source"),
            deserializer: None,
            sink,
            sinks,
        }
    }

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
    /// programmatically built configs (tests, `spate-test`) get the same
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
        // offset store for no durability gain. The sink write is the
        // durability boundary, not the checkpoint.
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
        self.reject_stray_chunk()?;
        // Resolve every declared sink's `chunk:` block now, so a malformed
        // block is rejected at load, including for a `sinks:` entry this
        // binary never installs (nothing later would resolve it).
        if let Some(sink) = &self.sink {
            sink.resolved_chunk()?;
        }
        if let Some(sinks) = &self.sinks {
            for (name, sink) in sinks {
                sink.resolved_chunk()
                    .map_err(|e| name_sinks_entry_error(name, e))?;
            }
        }
        Ok(())
    }

    /// Reject the framework-reserved `chunk:` key on a non-sink section.
    /// `chunk:` configures the chain terminal, which only sinks have; the
    /// framework peels the key indiscriminately, so a stray `chunk:` under
    /// `source`/`deserializer` would otherwise be silently swallowed. Split
    /// out of [`validate`](Self::validate) so `Pipeline::from_config`, which
    /// skips full validation for minimal programmatic configs, can still
    /// enforce it.
    pub(crate) fn reject_stray_chunk(&self) -> Result<(), ConfigError> {
        if self.source.resolved_chunk()?.is_some() {
            return Err(ConfigError::Validation(
                "`chunk:` is only valid on a sink section, not `source`".into(),
            ));
        }
        if let Some(deser) = &self.deserializer
            && deser.resolved_chunk()?.is_some()
        {
            return Err(ConfigError::Validation(
                "`chunk:` is only valid on a sink section, not `deserializer`".into(),
            ));
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

/// Re-anchor a `sinks:` entry's chunk error onto its map key. The entry's
/// section prefix is the shared `sink.<type>`, which cannot distinguish two
/// entries of the same connector type. `sinks.<name>.<...>` can.
fn name_sinks_entry_error(name: &str, e: ConfigError) -> ConfigError {
    match e {
        ConfigError::Validation(m) => ConfigError::Validation(format!("sinks.{name}: {m}")),
        ConfigError::Component { context, message } => ConfigError::Component {
            context: match context.strip_prefix("sink.") {
                Some(rest) => format!("sinks.{name}.{rest}"),
                None => format!("sinks.{name}.{context}"),
            },
            message,
        },
        other => other,
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
        assert!(!cfg.metrics.per_partition_detail);
        assert_eq!(cfg.metrics.e2e_basis, E2eBasis::Ingest);
        assert_eq!(
            cfg.admin.listen,
            Some(SocketAddr::from(([0, 0, 0, 0], 9090)))
        );
        assert!(cfg.deserializer.is_none());
    }

    /// `new` and the YAML defaults are two spellings of one config, so a
    /// section added to the struct has to reach both.
    #[test]
    fn new_matches_the_yaml_defaults() {
        let body = || YamlValue::Mapping(Default::default());
        assert_eq!(
            PipelineConfig::new(
                PipelineSection::new("demo"),
                ComponentConfig::new("memory", body()),
                ComponentConfig::new("memory", body()),
            ),
            PipelineConfig::from_str(MINIMAL).unwrap()
        );
    }

    /// `new_multi_sink` and the YAML defaults are two spellings of one config,
    /// so a section added to the struct has to reach both.
    #[test]
    fn new_multi_sink_matches_the_yaml_defaults() {
        let body = || YamlValue::Mapping(Default::default());
        let sinks = BTreeMap::from([
            ("eu".to_owned(), ComponentConfig::new("memory", body())),
            ("us".to_owned(), ComponentConfig::new("memory", body())),
        ]);
        let yaml = "
pipeline: { name: demo }
source: { memory: {} }
sinks:
  eu: { memory: {} }
  us: { memory: {} }
";
        assert_eq!(
            PipelineConfig::new_multi_sink(
                PipelineSection::new("demo"),
                ComponentConfig::new("memory", body()),
                sinks,
            ),
            PipelineConfig::from_str(yaml).unwrap()
        );
    }

    /// A deserializer added through `with_deserializer` carries the section
    /// tag the loader gives one, so the two spellings report the same error
    /// paths.
    #[test]
    fn with_deserializer_matches_the_yaml_form() {
        let body = || YamlValue::Mapping(Default::default());
        let yaml = "
pipeline: { name: demo }
source: { memory: {} }
deserializer: { json: {} }
sink: { memory: {} }
";
        assert_eq!(
            PipelineConfig::new(
                PipelineSection::new("demo"),
                ComponentConfig::new("memory", body()),
                ComponentConfig::new("memory", body()),
            )
            .with_deserializer(ComponentConfig::new("json", body())),
            PipelineConfig::from_str(yaml).unwrap()
        );
    }

    /// A constructed config passes the validation the loaders run, so `new`
    /// cannot produce one `from_str` would have rejected.
    #[test]
    fn a_constructed_config_validates() {
        let body = || YamlValue::Mapping(Default::default());
        PipelineConfig::new(
            PipelineSection::new("demo"),
            ComponentConfig::new("memory", body()),
            ComponentConfig::new("memory", body()),
        )
        .validate()
        .expect("the single-sink form validates");

        PipelineConfig::new_multi_sink(
            PipelineSection::new("demo"),
            ComponentConfig::new("memory", body()),
            BTreeMap::from([("eu".to_owned(), ComponentConfig::new("memory", body()))]),
        )
        .validate()
        .expect("the multi-sink form validates");
    }

    #[test]
    fn admin_listen_takes_an_address_or_none() {
        let with = |admin: &str| {
            PipelineConfig::from_str(&format!(
                "pipeline: {{ name: demo }}\n{admin}\nsource: {{ memory: {{}} }}\n\
                 sink: {{ memory: {{}} }}\n"
            ))
        };

        let bound = with("admin: { listen: 127.0.0.1:7777 }").expect("an address parses");
        assert_eq!(
            bound.admin.listen,
            Some(SocketAddr::from(([127, 0, 0, 1], 7777)))
        );

        let off = with("admin: { listen: none }").expect("`none` parses");
        assert_eq!(off.admin.listen, None, "`none` asks for no server");

        // The error names the key and both accepted forms; without the path
        // a reader cannot tell which of several addresses in a file is wrong.
        let err = with("admin: { listen: 9090 }").expect_err("a bare port is not an address");
        let msg = err.to_string();
        assert!(msg.contains("admin.listen"), "{msg}");
        assert!(msg.contains(r#"a socket address or "none""#), "{msg}");

        // A YAML null is not a spelling of `none`. It has to say so in the
        // same terms as every other rejected value, or the one reader who
        // reaches for `~` gets a type error naming a type they never wrote.
        for null in [
            "admin: { listen: ~ }",
            "admin: { listen: null }",
            "admin:\n  listen:",
        ] {
            let msg = with(null)
                .expect_err("a null is not an address")
                .to_string();
            assert!(
                msg.contains(r#"a socket address or "none""#),
                "{null}: {msg}"
            );
        }
    }

    /// The bind address is `admin.listen`, and `metrics` carries no address
    /// of its own. A file placing one there fails to load rather than parsing
    /// into a pipeline whose server is somewhere else.
    #[test]
    fn the_metrics_section_takes_no_bind_address() {
        let err = PipelineConfig::from_str(
            "pipeline: { name: demo }\nmetrics: { listen: 0.0.0.0:9090 }\n\
             source: { memory: {} }\nsink: { memory: {} }\n",
        )
        .expect_err("metrics carries no bind address");
        let msg = err.to_string();
        assert!(msg.contains("listen"), "{msg}");
    }

    #[test]
    fn sink_chunk_block_parses_and_resolves() {
        let yaml = r#"
pipeline: { name: demo }
source: { memory: {} }
sink:
  memory:
    chunk: { target_bytes: 512KiB, encode_policy: fail }
"#;
        let cfg = PipelineConfig::from_str(yaml).unwrap();
        let chunk = cfg
            .sink_config("default")
            .unwrap()
            .resolved_chunk()
            .unwrap()
            .expect("chunk present");
        assert_eq!(chunk.target_bytes, 512 * 1024);
        assert_eq!(chunk.encode_policy, crate::error::ErrorPolicy::Fail);
    }

    #[test]
    fn chunk_on_a_source_section_is_rejected() {
        let yaml = r#"
pipeline: { name: demo }
source:
  memory:
    chunk: { target_bytes: 64KiB }
sink: { memory: {} }
"#;
        let err = PipelineConfig::from_str(yaml).unwrap_err().to_string();
        assert!(err.contains("chunk"), "{err}");
        assert!(err.contains("source"), "{err}");
    }

    #[test]
    fn zero_target_bytes_in_yaml_is_rejected_at_load() {
        let yaml = r#"
pipeline: { name: demo }
source: { memory: {} }
sink:
  memory:
    chunk: { target_bytes: 0B }
"#;
        // `validate` resolves every declared sink chunk, so the loader itself
        // rejects it. "Rejected at load" in the reference docs is literal.
        let err = PipelineConfig::from_str(yaml).unwrap_err().to_string();
        assert!(err.contains("chunk.target_bytes"), "{err}");
    }

    #[test]
    fn malformed_chunk_on_a_sinks_entry_is_rejected_at_load_naming_the_entry() {
        // Two entries of the same connector type share the dotted path
        // `sink.memory.…`, so the error must be re-anchored on the map key,
        // and an entry must fail at load even if no binary ever installs it.
        let yaml = r#"
pipeline: { name: demo }
source: { memory: {} }
sinks:
  hot: { memory: {} }
  cold:
    memory:
      chunk: { encode_policy: retry }
"#;
        let err = PipelineConfig::from_str(yaml).unwrap_err().to_string();
        assert!(err.contains("sinks.cold"), "{err}");
        let yaml = r#"
pipeline: { name: demo }
source: { memory: {} }
sinks:
  cold:
    memory:
      chunk: { target_bytes: 0B }
"#;
        let err = PipelineConfig::from_str(yaml).unwrap_err().to_string();
        assert!(err.contains("sinks.cold"), "{err}");
        assert!(err.contains("chunk.target_bytes"), "{err}");
    }

    #[test]
    fn full_design_doc_example_parses() {
        // The connector bodies below mirror the module-doc example and use the
        // real connector field names. spate-core has no dependency on the
        // connector crates, so this test only parses the framework layer.
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
admin: { listen: 0.0.0.0:9090 }
metrics: { exporter: prometheus }
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

        let err = PipelineConfig::from_path(Path::new("/nonexistent/spate.yaml")).unwrap_err();
        assert!(matches!(err, ConfigError::Io { .. }));
        assert!(err.to_string().contains("/nonexistent/spate.yaml"));
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
