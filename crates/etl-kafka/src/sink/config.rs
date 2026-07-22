//! Kafka sink configuration: typed fields, a validated raw librdkafka
//! passthrough, and the assembled [`KafkaSink`] bundle.
//!
//! ```yaml
//! sink:
//!   kafka:
//!     brokers: ${KAFKA_BROKERS:-localhost:9092}
//!     topic: orders-enriched
//!     delivery_timeout: 30s
//!     max_message_bytes: 1MB
//!     compression: lz4
//!     rdkafka:
//!       linger.ms: "20"
//! ```
//!
//! The sink builds exactly **one** producer per instance: librdkafka
//! already routes and batches per broker internally, and the connector's
//! statistics translation maps the client's cumulative counters
//! absolutely, which is only sound with a single client (see the source's
//! metrics module). The framework's `shards` are worker-side parallelism
//! over clones of that producer — one replica per shard, so replica
//! rotation is a no-op while the circuit breaker still provides
//! quarantine, backpressure, and the `etl_sink_shard_healthy` signal.
//!
//! Durability is not negotiable: `acks=all` and `enable.idempotence=true`
//! are forced (and denied in the passthrough), because the framework
//! treats a confirmed delivery report as a durable write and commits
//! source offsets past it. Weaker settings would silently turn
//! at-least-once into at-most-once.

use crate::sink::context::SinkContext;
use crate::sink::encoder::{
    DEFAULT_MAX_MESSAGE_BYTES, KafkaBytesEncoder, KafkaEncoder, KafkaJsonEncoder, MessageEncoder,
};
use crate::sink::writer::{KafkaEndpoint, KafkaWriter};
use bytesize::ByteSize;
use etl_core::config::{ComponentConfig, ConfigError};
use etl_core::deser::{Owned, RecFamily};
use etl_core::sink::{
    BatchConfig, BreakerConfig, InflightConfig, RetryConfig, SinkBundle, SinkParts, SinkPoolConfig,
    SinkProbeFn, endpoint_probe,
};
use rdkafka::producer::ThreadedProducer;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Properties the sink owns. Setting them through the passthrough is
/// rejected at load time with an explanation — overriding them would break
/// the durable-ack contract, the delivery-report countdown, or a typed
/// field's ownership.
///
/// As on the source side, every librdkafka alias of a reserved property is
/// denied alongside its canonical name (`_RK_C_ALIAS` entries write the
/// same underlying setting, and the passthrough and framework values are
/// distinct `ClientConfig` keys applied in unspecified order). The aliases
/// here: `metadata.broker.list` ↔ `bootstrap.servers`,
/// `delivery.timeout.ms` ↔ `message.timeout.ms`, and
/// `acks` ↔ `request.required.acks`.
const DENYLIST: &[(&str, &str)] = &[
    ("bootstrap.servers", "owned by the typed `brokers` field"),
    (
        "metadata.broker.list",
        "librdkafka alias of `bootstrap.servers`, owned by the typed \
         `brokers` field",
    ),
    (
        "statistics.interval.ms",
        "owned by the typed `statistics_interval` field",
    ),
    (
        "delivery.timeout.ms",
        "owned by the typed `delivery_timeout` field, which also bounds how \
         long a batch write awaits its delivery reports",
    ),
    (
        "message.timeout.ms",
        "librdkafka alias of `delivery.timeout.ms`, owned by the typed \
         `delivery_timeout` field",
    ),
    (
        "message.max.bytes",
        "owned by the typed `max_message_bytes` field, which keeps the \
         client-side limit aligned with the sink's encode-time guard",
    ),
    (
        "acks",
        "forced to `all`: the framework commits source offsets once a \
         delivery report confirms, so a report under weaker acks would \
         turn at-least-once into at-most-once",
    ),
    (
        "request.required.acks",
        "librdkafka alias of `acks`, which is forced to `all` (a confirmed \
         delivery report must mean a durable write)",
    ),
    (
        "enable.idempotence",
        "forced on: librdkafka's internal retries must not reorder or \
         duplicate within a session, and disabling it silently weakens the \
         delivery guarantees this sink documents",
    ),
    (
        "transactional.id",
        "transactions/exactly-once are not supported; the sink's ack model \
         is per-batch delivery confirmation, not a two-phase commit",
    ),
    (
        "delivery.report.only.error",
        "the sink counts every delivery report to acknowledge a batch; \
         suppressing success reports would hang every write",
    ),
    (
        "enable.gapless.guarantee",
        "raises librdkafka fatal errors on any gap, conflicting with the \
         framework's retry-and-replay model",
    ),
];

fn default_shards() -> usize {
    1
}

fn default_delivery_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_max_message_bytes() -> ByteSize {
    ByteSize(DEFAULT_MAX_MESSAGE_BYTES as u64)
}

fn default_statistics_interval() -> Duration {
    Duration::from_secs(5)
}

/// Producer compression codec, applied as librdkafka's
/// `compression.codec`. Codec availability depends on how librdkafka was
/// built — the bundled build always has `snappy` and `lz4`, `gzip` via
/// zlib; `zstd` requires an rdkafka cargo feature the workspace does not
/// enable today. An unavailable codec fails producer creation at startup
/// (surfaced as a config error), never mid-stream.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    /// No compression.
    None,
    /// Gzip (zlib).
    Gzip,
    /// Snappy.
    Snappy,
    /// LZ4.
    Lz4,
    /// Zstandard (requires an rdkafka build with zstd support).
    Zstd,
}

impl Compression {
    fn codec(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Gzip => "gzip",
            Compression::Snappy => "snappy",
            Compression::Lz4 => "lz4",
            Compression::Zstd => "zstd",
        }
    }
}

/// Configuration of a Kafka sink, deserialized from the pipeline's opaque
/// `sink: { kafka: ... }` section.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct KafkaSinkConfig {
    /// Comma-separated bootstrap servers.
    pub brokers: String,
    /// The topic to produce to. One topic per sink instance: multi-topic
    /// fan-out is the chain's `split` terminal with one sink per topic.
    pub topic: String,
    /// Framework shards (worker parallelism). All shards share the single
    /// underlying producer, so raising this adds concurrent writers contending
    /// on that one client — not producer parallelism. Keep the default of 1;
    /// the lever for producer throughput is batch size (see the Kafka sink
    /// benchmark).
    #[serde(default = "default_shards")]
    pub shards: usize,
    /// librdkafka `delivery.timeout.ms`, and the bound on how long a batch
    /// write awaits its delivery reports before failing retryably.
    #[serde(with = "humantime_serde", default = "default_delivery_timeout")]
    pub delivery_timeout: Duration,
    /// Per-message size limit (key + payload + headers), enforced at encode
    /// time as a record-level error (Skip/Fail policy) and applied to the
    /// producer as `message.max.bytes`. Keep aligned with the topic/broker
    /// limit.
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: ByteSize,
    /// librdkafka statistics emission interval, feeding the connector's
    /// `etl_kafka_sink_*` families. Zero disables statistics and those
    /// families with it.
    #[serde(with = "humantime_serde", default = "default_statistics_interval")]
    pub statistics_interval: Duration,
    /// Producer compression codec (librdkafka `compression.codec`).
    #[serde(default)]
    pub compression: Option<Compression>,
    /// Batch sealing thresholds.
    #[serde(default)]
    pub batch: BatchConfig,
    /// In-flight limits.
    #[serde(default)]
    pub inflight: InflightConfig,
    /// Write retry policy.
    #[serde(default)]
    pub retry: RetryConfig,
    /// Endpoint circuit breaker.
    #[serde(default)]
    pub breaker: BreakerConfig,
    /// Raw librdkafka producer properties, applied verbatim after
    /// validation. Sink-owned properties (see [`KafkaSinkConfig`] docs and
    /// the connector guide) are rejected; batching knobs like `linger.ms`
    /// and `batch.num.messages` may be tuned here.
    #[serde(default)]
    pub rdkafka: BTreeMap<String, String>,
}

impl KafkaSinkConfig {
    /// Cross-field validation, including the passthrough denylist.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let fail = |msg: String| Err(ConfigError::Validation(format!("sink.kafka: {msg}")));

        if self.brokers.trim().is_empty() {
            return fail("brokers must not be empty".into());
        }
        if self.topic.trim().is_empty() {
            return fail("topic must not be empty".into());
        }
        if self.shards == 0 {
            return fail("shards must be at least 1".into());
        }
        if self.delivery_timeout.is_zero() {
            return fail(
                "delivery_timeout must be positive (librdkafka treats 0 as \
                 infinite, which would let a batch write hang past every \
                 framework deadline)"
                    .into(),
            );
        }
        if self.delivery_timeout.as_millis() > i32::MAX as u128 {
            return fail(format!(
                "delivery_timeout {:?} exceeds librdkafka's maximum (~24.8 days)",
                self.delivery_timeout
            ));
        }
        // librdkafka's accepted range for `message.max.bytes`.
        let mmb = self.max_message_bytes.as_u64();
        if !(1_000..=1_000_000_000).contains(&mmb) {
            return fail(format!(
                "max_message_bytes must be in [1000, 1000000000] \
                 (librdkafka's `message.max.bytes` range), got {mmb}"
            ));
        }
        if self.batch.max_rows == 0 || self.batch.max_bytes == 0 {
            return fail("batch thresholds must be non-zero".into());
        }
        if self.inflight.max_per_shard == 0 {
            return fail("inflight.max_per_shard must be at least 1".into());
        }

        // Retry policy: a sub-1.0 multiplier shrinks the delay instead of
        // backing off, and a zero delay is not a backoff at all. The backoff
        // saturates rather than trusting these bounds, so this catches the
        // operator's intent at load, not a runtime hazard. The rules live in
        // the framework so every sink enforces the same ones.
        if let Err(why) = self.retry.validate() {
            return fail(why.to_string());
        }
        if self.breaker.failure_threshold == 0 {
            return fail("breaker.failure_threshold must be at least 1".into());
        }
        if self.breaker.half_open_probes == 0 {
            return fail("breaker.half_open_probes must be at least 1".into());
        }

        for (key, why) in DENYLIST {
            if self.rdkafka.contains_key(*key) {
                return fail(format!("rdkafka.\"{key}\" cannot be overridden: {why}"));
            }
        }
        crate::security::check_tls_feature(&self.rdkafka, "sink.kafka")?;
        if self.compression.is_some() {
            for key in ["compression.codec", "compression.type"] {
                if self.rdkafka.contains_key(key) {
                    return fail(format!(
                        "rdkafka.\"{key}\" conflicts with the typed \
                         `compression` field; set one, not both"
                    ));
                }
            }
        }
        Ok(())
    }

    /// Build the effective librdkafka producer configuration.
    pub(crate) fn client_config(&self) -> rdkafka::ClientConfig {
        self.client_config_impl(true)
    }

    /// The probe producer's configuration: identical, minus statistics —
    /// only the main producer's context translates statistics, so a second
    /// emitting client would be pure overhead.
    fn probe_client_config(&self) -> rdkafka::ClientConfig {
        self.client_config_impl(false)
    }

    fn client_config_impl(&self, with_statistics: bool) -> rdkafka::ClientConfig {
        let mut cc = rdkafka::ClientConfig::new();
        // User passthrough first: sink-owned settings below always win.
        for (k, v) in &self.rdkafka {
            cc.set(k, v);
        }
        if let Some(compression) = self.compression {
            cc.set("compression.codec", compression.codec());
        }
        cc.set("bootstrap.servers", &self.brokers);
        // The durable-ack contract (see the module docs).
        cc.set("enable.idempotence", "true");
        cc.set("acks", "all");
        cc.set(
            "message.timeout.ms",
            self.delivery_timeout.as_millis().to_string(),
        );
        cc.set(
            "message.max.bytes",
            self.max_message_bytes.as_u64().to_string(),
        );
        if with_statistics && !self.statistics_interval.is_zero() {
            cc.set(
                "statistics.interval.ms",
                self.statistics_interval.as_millis().to_string(),
            );
        }
        cc
    }
}

/// Everything the framework needs to run this sink. Build with
/// [`from_component_config`] or [`build`]; terminate a chain with one of
/// the `encoder_*` helpers (they bake the configured message-size guard
/// into the encoder).
#[derive(Debug)]
pub struct KafkaSink {
    /// The `ShardWriter` implementation.
    pub writer: KafkaWriter,
    /// Per-shard endpoints, one replica each — clones of the single
    /// shared producer.
    pub endpoints: Vec<Vec<KafkaEndpoint>>,
    /// Pool knobs mapped onto the framework's configuration.
    pub pool: SinkPoolConfig,
    /// A probe-only producer (no statistics), independent of the write
    /// path. One entry: every shard shares the producer, so probing once
    /// probes them all.
    probe_endpoints: Arc<Vec<Vec<KafkaEndpoint>>>,
    /// The configured encode-time size guard.
    max_message_bytes: usize,
}

impl KafkaSink {
    /// A payload-passthrough encoder for owned byte records, with this
    /// sink's `max_message_bytes` guard baked in.
    #[must_use]
    pub fn encoder_bytes(&self) -> KafkaEncoder<Owned<Vec<u8>>, KafkaBytesEncoder> {
        self.encoder_with(KafkaBytesEncoder::new())
    }

    /// A JSON encoder for any family whose records implement `Serialize`,
    /// with this sink's `max_message_bytes` guard baked in.
    #[must_use]
    pub fn encoder_json<F>(&self) -> KafkaEncoder<F, KafkaJsonEncoder<F>>
    where
        F: RecFamily,
        for<'b> F::Rec<'b>: serde::Serialize,
    {
        self.encoder_with(KafkaJsonEncoder::new())
    }

    /// Wrap a custom [`MessageEncoder`] (keys, headers, tombstones, other
    /// formats) with this sink's `max_message_bytes` guard.
    #[must_use]
    pub fn encoder_with<F: RecFamily, M: MessageEncoder<F>>(&self, inner: M) -> KafkaEncoder<F, M> {
        KafkaEncoder::with_max_message_bytes(inner, self.max_message_bytes)
    }

    /// A readiness probe over the sink's probe-only producer (topic
    /// metadata fetch — unknown topics fail fast). This is the probe
    /// [`SinkBundle::into_parts`] attaches; manual assemblies hand it to
    /// `SinkRuntime.probe` directly.
    #[must_use]
    pub fn probe_fn(&self) -> SinkProbeFn {
        endpoint_probe(self.writer.clone(), Arc::clone(&self.probe_endpoints))
    }
}

impl SinkBundle for KafkaSink {
    type Writer = KafkaWriter;

    fn into_parts(self) -> SinkParts<KafkaWriter> {
        let probe = self.probe_fn();
        let replica_labels = self
            .endpoints
            .iter()
            .map(|shard| shard.iter().map(|e| e.label().to_string()).collect())
            .collect();
        SinkParts::new(self.writer, self.endpoints, self.pool)
            .with_component_type("kafka")
            .with_replica_labels(replica_labels)
            .with_probe(probe)
    }
}

/// Build a [`KafkaSink`] from the opaque `sink: { kafka: ... }` component
/// section.
pub fn from_component_config(section: &ComponentConfig) -> Result<KafkaSink, ConfigError> {
    let cfg: KafkaSinkConfig = section.deserialize_into()?;
    build(cfg)
}

/// Build from an already-deserialized config (programmatic use).
///
/// Creates the producer eagerly: librdkafka validates property values and
/// idempotence compatibility (`max.in.flight` ≤ 5, `retries` > 0) at
/// creation, so a passthrough that conflicts with the forced durability
/// settings fails here — at startup — rather than on the first batch.
pub fn build(cfg: KafkaSinkConfig) -> Result<KafkaSink, ConfigError> {
    cfg.validate()?;

    // The statistics slot connects the writer (whose `attach_metrics`
    // fills it) to the main producer's context (whose `stats` callback
    // publishes through it).
    let stats_slot = Arc::new(Mutex::new(None));
    let producer: ThreadedProducer<SinkContext> = cfg
        .client_config()
        .create_with_context(SinkContext::new(Arc::clone(&stats_slot)))
        .map_err(|e| {
            ConfigError::Validation(format!("sink.kafka: producer creation failed: {e}"))
        })?;
    let label = format!("{}/{}", cfg.brokers, cfg.topic);
    let endpoint = KafkaEndpoint::new(producer, label.clone());
    let endpoints: Vec<Vec<KafkaEndpoint>> =
        (0..cfg.shards).map(|_| vec![endpoint.clone()]).collect();

    let probe_producer: ThreadedProducer<SinkContext> = cfg
        .probe_client_config()
        .create_with_context(SinkContext::detached())
        .map_err(|e| {
            ConfigError::Validation(format!("sink.kafka: probe producer creation failed: {e}"))
        })?;
    let probe_endpoints = Arc::new(vec![vec![KafkaEndpoint::new(probe_producer, label)]]);

    let pool = SinkPoolConfig {
        batch: cfg.batch,
        inflight: cfg.inflight,
        retry: cfg.retry,
        breaker: cfg.breaker,
    };

    Ok(KafkaSink {
        writer: KafkaWriter::new(
            cfg.topic,
            cfg.delivery_timeout,
            stats_slot,
            !cfg.statistics_interval.is_zero(),
        ),
        endpoints,
        pool,
        probe_endpoints,
        max_message_bytes: cfg.max_message_bytes.as_u64() as usize,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn section(body: &str) -> ComponentConfig {
        let yaml = format!("kafka:\n{body}");
        let value: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        ComponentConfig::new("kafka", value["kafka"].clone())
    }

    fn minimal() -> String {
        "  brokers: localhost:9092\n  topic: orders\n".to_string()
    }

    fn parse(body: &str) -> Result<KafkaSinkConfig, ConfigError> {
        let cfg: KafkaSinkConfig = section(body).deserialize_into()?;
        cfg.validate()?;
        Ok(cfg)
    }

    #[test]
    fn minimal_config_gets_documented_defaults() {
        let cfg = parse(&minimal()).unwrap();
        assert_eq!(cfg.shards, 1);
        assert_eq!(cfg.delivery_timeout, Duration::from_secs(30));
        assert_eq!(cfg.max_message_bytes.as_u64(), 1_000_000);
        assert_eq!(cfg.statistics_interval, Duration::from_secs(5));
        assert_eq!(cfg.compression, None);
        assert!(cfg.rdkafka.is_empty());
        assert_eq!(cfg.batch, BatchConfig::default());
        assert_eq!(cfg.retry, RetryConfig::default());
    }

    #[test]
    fn denylisted_properties_are_rejected_with_reasons() {
        for (key, _) in DENYLIST {
            let body = format!("{}  rdkafka:\n    \"{key}\": \"x\"\n", minimal());
            let err = parse(&body).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains(key), "error names the key: {msg}");
            assert!(msg.contains("sink.kafka"), "error names the section: {msg}");
        }
    }

    /// Regression: `message.timeout.ms` and `delivery.timeout.ms` are one
    /// underlying librdkafka setting; leaving either un-denied would let a
    /// passthrough race the typed `delivery_timeout` at client creation
    /// and silently unbound (or shrink) the write deadline.
    #[test]
    fn delivery_timeout_aliases_are_both_denied() {
        for key in ["delivery.timeout.ms", "message.timeout.ms"] {
            let body = format!("{}  rdkafka:\n    \"{key}\": \"1\"\n", minimal());
            let msg = parse(&body).unwrap_err().to_string();
            assert!(
                msg.contains("delivery_timeout"),
                "explains ownership: {msg}"
            );
        }
    }

    /// Regression: `acks` is the alias of `request.required.acks`; both
    /// must be denied or the forced `acks=all` could be overridden to `0`,
    /// making "delivered" mean "left the client" — silent data loss.
    #[test]
    fn acks_aliases_are_both_denied() {
        for key in ["acks", "request.required.acks"] {
            let body = format!("{}  rdkafka:\n    \"{key}\": \"0\"\n", minimal());
            let msg = parse(&body).unwrap_err().to_string();
            assert!(msg.contains(key), "error names the key: {msg}");
        }
    }

    #[test]
    fn forced_properties_win_over_passthrough() {
        let body = format!(
            "{}  rdkafka:\n    linger.ms: \"20\"\n    batch.num.messages: \"5000\"\n",
            minimal()
        );
        let cfg = parse(&body).unwrap();
        let cc = cfg.client_config();
        assert_eq!(cc.get("linger.ms"), Some("20"), "passthrough applies");
        assert_eq!(cc.get("batch.num.messages"), Some("5000"));
        assert_eq!(cc.get("bootstrap.servers"), Some("localhost:9092"));
        assert_eq!(cc.get("enable.idempotence"), Some("true"));
        assert_eq!(cc.get("acks"), Some("all"));
        assert_eq!(cc.get("message.timeout.ms"), Some("30000"));
        assert_eq!(cc.get("message.max.bytes"), Some("1000000"));
        assert_eq!(cc.get("statistics.interval.ms"), Some("5000"));
    }

    /// Sink counterpart to the source's TLS/SASL capability probe: a security
    /// passthrough is accepted (and the producer creates — which validates the
    /// build's SSL/SASL capability without connecting) only with the `tls`
    /// feature; otherwise it is rejected at load with an actionable message.
    #[test]
    fn tls_config_matches_build_capability() {
        for sec in [
            "    security.protocol: ssl\n",
            "    security.protocol: sasl_ssl\n    sasl.mechanism: SCRAM-SHA-256\n    \
             sasl.username: svc\n    sasl.password: secret\n",
        ] {
            let body = format!("{}  rdkafka:\n{sec}", minimal());
            let parsed = parse(&body);
            if cfg!(feature = "tls") {
                let cfg = parsed.expect("tls build accepts a security config");
                let cc = cfg.client_config();
                assert!(
                    cc.get("security.protocol").is_some(),
                    "security passthrough survives into the client (not denylisted)"
                );
                let producer: rdkafka::producer::BaseProducer = cc
                    .create()
                    .expect("SSL/SASL compiled in: producer creation succeeds");
                drop(producer);
            } else {
                let msg = parsed
                    .expect_err("non-tls build rejects a security config")
                    .to_string();
                assert!(msg.contains("kafka-tls"), "actionable: {msg}");
            }
        }
    }

    #[test]
    fn statistics_can_be_disabled_and_probe_config_never_emits() {
        let body = format!("{}  statistics_interval: 0s\n", minimal());
        let cfg = parse(&body).unwrap();
        assert_eq!(cfg.client_config().get("statistics.interval.ms"), None);
        let enabled = parse(&minimal()).unwrap();
        assert_eq!(
            enabled.probe_client_config().get("statistics.interval.ms"),
            None,
            "probe producers never emit statistics"
        );
    }

    #[test]
    fn compression_field_maps_and_conflicts_with_passthrough() {
        let body = format!("{}  compression: lz4\n", minimal());
        let cfg = parse(&body).unwrap();
        assert_eq!(cfg.client_config().get("compression.codec"), Some("lz4"));

        for key in ["compression.codec", "compression.type"] {
            let body = format!(
                "{}  compression: lz4\n  rdkafka:\n    \"{key}\": \"gzip\"\n",
                minimal()
            );
            let msg = parse(&body).unwrap_err().to_string();
            assert!(msg.contains("conflicts"), "conflict is explained: {msg}");
        }

        // Without the typed field, the passthrough key is allowed.
        let body = format!("{}  rdkafka:\n    compression.codec: \"gzip\"\n", minimal());
        assert!(parse(&body).is_ok());
    }

    #[test]
    fn zero_shards_rejected() {
        let body = format!("{}  shards: 0\n", minimal());
        assert!(parse(&body).unwrap_err().to_string().contains("shards"));
    }

    #[test]
    fn out_of_range_knobs_are_rejected() {
        for (body, needle) in [
            (
                format!("{}  delivery_timeout: 0s\n", minimal()),
                "delivery_timeout",
            ),
            (
                format!("{}  max_message_bytes: 10\n", minimal()),
                "max_message_bytes",
            ),
            (
                format!("{}  max_message_bytes: 2GB\n", minimal()),
                "max_message_bytes",
            ),
            (
                format!("{}  retry:\n    multiplier: 0.5\n", minimal()),
                "retry.multiplier",
            ),
            (
                format!("{}  retry:\n    jitter: 1.5\n", minimal()),
                "retry.jitter",
            ),
            (
                format!("{}  breaker:\n    failure_threshold: 0\n", minimal()),
                "breaker.failure_threshold",
            ),
            (
                format!("{}  inflight:\n    max_per_shard: 0\n", minimal()),
                "inflight.max_per_shard",
            ),
        ] {
            let msg = parse(&body).unwrap_err().to_string();
            assert!(msg.contains(needle), "`{needle}` in: {msg}");
        }
    }

    #[test]
    fn tuning_sections_map_to_pool_config() {
        let body = format!(
            "{}  shards: 3\n  batch:\n    max_rows: 1000\n    max_bytes: 1MiB\n    linger: 250ms\n  \
             inflight:\n    max_per_shard: 4\n  retry:\n    initial: 50ms\n    max: 5s\n  \
             breaker:\n    failure_threshold: 7\n",
            minimal()
        );
        let sink = build(parse(&body).unwrap()).unwrap();
        assert_eq!(sink.endpoints.len(), 3, "one worker per shard");
        assert!(
            sink.endpoints.iter().all(|shard| shard.len() == 1),
            "single-replica shards"
        );
        assert_eq!(sink.pool.batch.max_rows, 1000);
        assert_eq!(sink.pool.batch.max_bytes, 1024 * 1024);
        assert_eq!(sink.pool.batch.linger, Duration::from_millis(250));
        assert_eq!(sink.pool.inflight.max_per_shard, 4);
        assert_eq!(sink.pool.retry.initial, Duration::from_millis(50));
        assert_eq!(sink.pool.breaker.failure_threshold, 7);
    }

    #[test]
    fn bundle_exposes_kafka_component_type_and_labels() {
        let sink = build(parse(&minimal()).unwrap()).unwrap();
        let parts = sink.into_parts();
        assert_eq!(parts.component_type, "kafka");
        assert_eq!(
            parts.replica_labels,
            vec![vec!["localhost:9092/orders".to_string()]]
        );
        assert!(parts.probe.is_some(), "readiness probe attached");
    }

    /// The forced durability settings and an incompatible passthrough must
    /// fail at build time (librdkafka rejects the combination at producer
    /// creation), not on the first batch.
    #[test]
    fn idempotence_incompatible_passthrough_fails_at_build() {
        let body = format!("{}  rdkafka:\n    max.in.flight: \"10\"\n", minimal());
        let err = build(parse(&body).unwrap()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("producer creation failed"),
            "surfaced at startup: {msg}"
        );
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let body = format!("{}  topics: [a, b]\n", minimal());
        assert!(
            section(&body)
                .deserialize_into::<KafkaSinkConfig>()
                .is_err()
        );
    }

    #[test]
    fn empty_required_fields_error_clearly() {
        for body in [
            "  brokers: \"\"\n  topic: t\n",
            "  brokers: b\n  topic: \"\"\n",
        ] {
            assert!(parse(body).is_err());
        }
    }
}
