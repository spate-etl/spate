//! Kafka source configuration: typed fields plus a validated raw
//! librdkafka property passthrough.

use serde::Deserialize;
use spate_core::config::{ComponentConfig, ConfigError};
use std::collections::BTreeMap;
use std::time::Duration;

/// Properties the framework owns. Setting them through the passthrough is
/// rejected at load time with an explanation, because overriding them
/// silently breaks the framework's delivery guarantees or its threading
/// model.
///
/// librdkafka accepts several property names that write the same underlying
/// setting (`_RK_C_ALIAS` entries in `rdkafka_conf.c`). Because the
/// passthrough and the framework's own values are distinct `ClientConfig`
/// map keys applied in unspecified order, an alias of an owned property would
/// non-deterministically override it. Every alias of a reserved property is
/// therefore denied alongside its canonical name. Auditing the reserved set
/// against librdkafka's config table yields two alias names to add; the
/// other reserved properties (`group.id`, `enable.auto.offset.store`,
/// `auto.commit.interval.ms`, `enable.partition.eof`, `statistics.interval.ms`,
/// `group.protocol`, `partition.assignment.strategy`) have no alias.
const DENYLIST: &[(&str, &str)] = &[
    (
        "enable.auto.offset.store",
        "the framework stores offsets itself when checkpoint watermarks \
         advance; overriding this breaks at-least-once delivery",
    ),
    (
        "enable.auto.commit",
        "interval auto-commit of framework-stored offsets is the commit \
         mechanism; disabling it means nothing is ever committed",
    ),
    (
        // librdkafka's deprecated topic-level alias that `enable.auto.commit`
        // maps to; denied so auto-commit cannot be disabled by the back door.
        "auto.commit.enable",
        "deprecated librdkafka alias of `enable.auto.commit`; interval \
         auto-commit of framework-stored offsets is the commit mechanism, \
         disabling it means nothing is ever committed",
    ),
    (
        "auto.commit.interval.ms",
        "owned by the typed `commit_interval` field",
    ),
    (
        "enable.partition.eof",
        "EOF events would pollute the partition queues the pipeline polls",
    ),
    ("bootstrap.servers", "owned by the typed `brokers` field"),
    (
        // librdkafka's canonical name for `bootstrap.servers`; both write the
        // same broker list, so denying only one leaves the framework's
        // broker list overridable through the other.
        "metadata.broker.list",
        "librdkafka alias of `bootstrap.servers`, owned by the typed \
         `brokers` field",
    ),
    ("group.id", "owned by the typed `group_id` field"),
    (
        "statistics.interval.ms",
        "owned by the typed `statistics_interval` field",
    ),
    (
        "group.protocol",
        "only the classic consumer group protocol is supported today \
         (eager assignment is a framework invariant)",
    ),
];

fn default_commit_interval() -> Duration {
    Duration::from_secs(5)
}

fn default_startup_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_assignment_timeout() -> Duration {
    Duration::from_mins(5)
}

fn default_statistics_interval() -> Duration {
    Duration::from_secs(5)
}

/// Configuration of a [`KafkaSource`](crate::KafkaSource), deserialized
/// from the pipeline's opaque `source: { kafka: ... }` section.
///
/// Construct with [`KafkaSourceConfig::new`] and set the optional fields.
/// The struct is `#[non_exhaustive]` so new knobs can be added without
/// breaking callers.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct KafkaSourceConfig {
    /// Comma-separated bootstrap servers.
    pub brokers: String,
    /// The topic to consume. One topic per pipeline: the framework's
    /// `PartitionId` is the Kafka partition number, which is only unique
    /// within a single topic.
    pub topic: String,
    /// Consumer group id.
    pub group_id: String,
    /// How often stored offsets are auto-committed
    /// (librdkafka `auto.commit.interval.ms`).
    #[serde(with = "humantime_serde", default = "default_commit_interval")]
    pub commit_interval: Duration,
    /// How long to wait for the first partition assignment before the
    /// source reports a fatal startup error.
    #[serde(with = "humantime_serde", default = "default_startup_timeout")]
    pub startup_timeout: Duration,
    /// How long the source may run with no assignment, after having one,
    /// before it reports a fatal error. Measured from the moment ownership is
    /// released, cleared by the next accepted assignment, including an empty
    /// one. Zero disables the deadline.
    #[serde(with = "humantime_serde", default = "default_assignment_timeout")]
    pub assignment_timeout: Duration,
    /// librdkafka statistics emission interval, feeding the lag metrics and
    /// the connector's `spate_kafka_source_*` families (broker health,
    /// latency, queue saturation, group stability). Zero disables statistics
    /// and those families with it. [The metrics reference] tabulates them
    /// under Kafka source.
    ///
    /// [the metrics reference]: https://spate.kainth.dev/docs/METRICS
    #[serde(with = "humantime_serde", default = "default_statistics_interval")]
    pub statistics_interval: Duration,
    /// Raw librdkafka properties, applied verbatim after validation.
    /// Framework-owned properties (see crate docs) are rejected.
    ///
    /// The framework sets no prefetch cap of its own, so
    /// `queued.min.messages` (default 100000) and
    /// `queued.max.messages.kbytes` (default 65536) sit at librdkafka's
    /// defaults and bound memory *per assigned partition*, which is
    /// per-partition because this source splits partition queues; on an
    /// unsplit consumer the byte cap would apply once, to the whole client.
    ///
    /// librdkafka multiplies the kbytes figure by 1000, so the default is
    /// 65.5 MB per partition and a 100-partition assignment can hold up to
    /// ~6.6 GB of prefetched messages.
    ///
    /// Lowering them here caps that, at a throughput cost that is steep on a
    /// backlogged consumer. Note also that librdkafka clamps `fetch.max.bytes`
    /// to `queued.max.messages.kbytes * 1024` unless `fetch.max.bytes` is set
    /// explicitly, so capping the queue silently shrinks the fetch size too.
    /// See the Kafka source tuning guide.
    #[serde(default)]
    pub rdkafka: BTreeMap<String, String>,
}

impl KafkaSourceConfig {
    /// A config for `brokers`, `topic` and `group_id`. Every other field
    /// starts at its YAML default.
    #[must_use]
    pub fn new(
        brokers: impl Into<String>,
        topic: impl Into<String>,
        group_id: impl Into<String>,
    ) -> KafkaSourceConfig {
        KafkaSourceConfig {
            brokers: brokers.into(),
            topic: topic.into(),
            group_id: group_id.into(),
            commit_interval: default_commit_interval(),
            startup_timeout: default_startup_timeout(),
            assignment_timeout: default_assignment_timeout(),
            statistics_interval: default_statistics_interval(),
            rdkafka: BTreeMap::new(),
        }
    }

    /// Deserialize and validate from the pipeline's opaque component
    /// section.
    pub fn from_component_config(section: &ComponentConfig) -> Result<Self, ConfigError> {
        let cfg: KafkaSourceConfig = section.deserialize_into()?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Cross-field validation, including the passthrough denylist.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.brokers.trim().is_empty() {
            return Err(ConfigError::Validation(
                "source.kafka.brokers must not be empty".into(),
            ));
        }
        if self.topic.trim().is_empty() {
            return Err(ConfigError::Validation(
                "source.kafka.topic must not be empty".into(),
            ));
        }
        if self.group_id.trim().is_empty() {
            return Err(ConfigError::Validation(
                "source.kafka.group_id must not be empty".into(),
            ));
        }
        for (key, why) in DENYLIST {
            if self.rdkafka.contains_key(*key) {
                return Err(ConfigError::Validation(format!(
                    "source.kafka.rdkafka.\"{key}\" cannot be overridden: {why}"
                )));
            }
        }
        crate::security::check_tls_feature(&self.rdkafka, "source.kafka")?;
        if let Some(strategy) = self.rdkafka.get("partition.assignment.strategy")
            && strategy.contains("cooperative")
        {
            return Err(ConfigError::Validation(
                "source.kafka.rdkafka.\"partition.assignment.strategy\": \
                 cooperative assignment is not supported today; the framework \
                 relies on eager (full) rebalances"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Build the effective librdkafka client configuration.
    pub(crate) fn client_config(&self) -> rdkafka::ClientConfig {
        let mut cc = rdkafka::ClientConfig::new();
        // User passthrough first: framework-owned settings below always win.
        for (k, v) in &self.rdkafka {
            cc.set(k, v);
        }
        // Prefetch depth is deliberately left at librdkafka's own defaults
        // (`queued.min.messages` 100000, `queued.max.messages.kbytes` 65536)
        // so behavior is predictable and reasoning transfers from librdkafka's
        // documentation like it does for every other Kafka client.
        //
        // Forcing `queued.min.messages` to 1000 as a memory backstop cost,
        // measured on a backlogged consumer by a rig this repository no longer
        // carries: ~11x end-to-end (193k -> 2.11M msg/s through the full
        // pipeline) and 22-76x on the raw client path depending on message
        // size, because the local fetch queue sits empty and every driver
        // thread blocks on the broker. A memory cap that costs an order of
        // magnitude of throughput is the wrong default; deployments that need
        // the memory bounded set it through `rdkafka` below, where the trade
        // is visible.
        cc.set("bootstrap.servers", &self.brokers);
        cc.set("group.id", &self.group_id);
        // The framework's commit mechanism: offsets are stored explicitly
        // when checkpoint watermarks advance and committed on an interval.
        cc.set("enable.auto.offset.store", "false");
        cc.set("enable.auto.commit", "true");
        cc.set(
            "auto.commit.interval.ms",
            self.commit_interval.as_millis().to_string(),
        );
        cc.set("enable.partition.eof", "false");
        if !self.statistics_interval.is_zero() {
            cc.set(
                "statistics.interval.ms",
                self.statistics_interval.as_millis().to_string(),
            );
        }
        cc
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spate_core::config::ComponentConfig;

    fn section(body: &str) -> ComponentConfig {
        let yaml = format!("kafka:\n{body}");
        let value: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        ComponentConfig::new("kafka", value["kafka"].clone())
    }

    fn minimal() -> String {
        "  brokers: localhost:9092\n  topic: orders\n  group_id: spate\n".to_string()
    }

    #[test]
    fn minimal_config_gets_documented_defaults() {
        let cfg = KafkaSourceConfig::from_component_config(&section(&minimal())).unwrap();
        assert_eq!(cfg.commit_interval, Duration::from_secs(5));
        assert_eq!(cfg.startup_timeout, Duration::from_secs(30));
        assert_eq!(cfg.assignment_timeout, Duration::from_mins(5));
        assert_eq!(cfg.statistics_interval, Duration::from_secs(5));
        assert!(cfg.rdkafka.is_empty());
    }

    /// `new` and the YAML defaults are two spellings of one config, so a
    /// knob added to the struct has to reach both.
    #[test]
    fn new_matches_the_yaml_defaults() {
        assert_eq!(
            KafkaSourceConfig::new("localhost:9092", "orders", "spate"),
            KafkaSourceConfig::from_component_config(&section(&minimal())).unwrap()
        );
    }

    #[test]
    fn denylisted_properties_are_rejected_with_reasons() {
        for key in [
            "enable.auto.offset.store",
            "enable.auto.commit",
            "auto.commit.enable",
            "auto.commit.interval.ms",
            "enable.partition.eof",
            "bootstrap.servers",
            "metadata.broker.list",
            "group.id",
            "statistics.interval.ms",
            "group.protocol",
        ] {
            let body = format!("{}  rdkafka:\n    \"{key}\": \"x\"\n", minimal());
            let err = KafkaSourceConfig::from_component_config(&section(&body)).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains(key), "error names the key: {msg}");
        }
    }

    /// Regression: the librdkafka alias `metadata.broker.list` writes the same
    /// underlying broker list as `bootstrap.servers`. Left un-denied, a
    /// passthrough value would race the framework-owned broker list at client
    /// creation (`ClientConfig` applies its map in unspecified order), so some
    /// process restarts would silently consume from the wrong cluster.
    #[test]
    fn broker_list_alias_cannot_override_framework_brokers() {
        let body = format!(
            "{}  rdkafka:\n    metadata.broker.list: \"staging-kafka:9092\"\n",
            minimal()
        );
        let err = KafkaSourceConfig::from_component_config(&section(&body)).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("metadata.broker.list"),
            "error names the key: {msg}"
        );
        assert!(
            msg.contains("bootstrap.servers"),
            "error explains the alias: {msg}"
        );
    }

    #[test]
    fn cooperative_assignment_is_rejected() {
        let body = format!(
            "{}  rdkafka:\n    partition.assignment.strategy: cooperative-sticky\n",
            minimal()
        );
        let err = KafkaSourceConfig::from_component_config(&section(&body)).unwrap_err();
        assert!(err.to_string().contains("cooperative"));
    }

    #[test]
    fn passthrough_survives_and_framework_settings_win() {
        let body = format!(
            "{}  rdkafka:\n    fetch.message.max.bytes: \"1048576\"\n    queued.min.messages: \"5000\"\n",
            minimal()
        );
        let cfg = KafkaSourceConfig::from_component_config(&section(&body)).unwrap();
        let cc = cfg.client_config();
        assert_eq!(
            cc.get("fetch.message.max.bytes"),
            Some("1048576"),
            "passthrough applies"
        );
        assert_eq!(
            cc.get("queued.min.messages"),
            Some("5000"),
            "prefetch depth is tunable"
        );
        assert_eq!(cc.get("enable.auto.offset.store"), Some("false"));
        assert_eq!(cc.get("enable.auto.commit"), Some("true"));
        assert_eq!(cc.get("auto.commit.interval.ms"), Some("5000"));
        assert_eq!(cc.get("enable.partition.eof"), Some("false"));
    }

    /// The framework must not set a prefetch cap of its own. Forcing
    /// `queued.min.messages` down to 1000 cost ~11x end-to-end throughput on a
    /// backlogged consumer, because the local fetch queue stays empty and the
    /// driver threads block on the broker. Leaving the key unset means
    /// librdkafka's documented default (100000) applies and users can reason
    /// about it exactly as they would for any other Kafka client.
    ///
    /// Asserting `None` rather than a value: a default that equals
    /// librdkafka's would still be the framework's, and would silently
    /// diverge the day librdkafka changed its own.
    #[test]
    fn prefetch_is_left_at_the_librdkafka_default() {
        let cfg = KafkaSourceConfig::from_component_config(&section(&minimal())).unwrap();
        let cc = cfg.client_config();
        assert_eq!(
            cc.get("queued.min.messages"),
            None,
            "the framework must not pin prefetch depth"
        );
        assert_eq!(cc.get("queued.max.messages.kbytes"), None);
    }

    /// A TLS/SASL passthrough is accepted only when the `tls` feature compiled
    /// the transport into librdkafka. With the feature: the security keys
    /// survive into the client and creation succeeds, because client creation
    /// validates the build's capability (SSL present, and the SCRAM SASL
    /// provider present) without any network I/O. Without it: rejected at load
    /// with an actionable message. Default `cargo test -p spate-kafka` exercises
    /// the reject arm; `--features tls` / `--all-features` the accept arm.
    #[test]
    fn tls_config_matches_build_capability() {
        for sec in [
            "    security.protocol: ssl\n",
            "    security.protocol: sasl_ssl\n    sasl.mechanism: SCRAM-SHA-256\n    \
             sasl.username: svc\n    sasl.password: secret\n",
        ] {
            let body = format!("{}  rdkafka:\n{sec}", minimal());
            let parsed = KafkaSourceConfig::from_component_config(&section(&body));
            if cfg!(feature = "tls") {
                let cfg = parsed.expect("tls build accepts a security config");
                let cc = cfg.client_config();
                assert!(
                    cc.get("security.protocol").is_some(),
                    "security passthrough survives into the client (not denylisted)"
                );
                let consumer: rdkafka::consumer::BaseConsumer = cc
                    .create()
                    .expect("SSL/SASL compiled in: consumer creation succeeds");
                drop(consumer);
            } else {
                let err = parsed.expect_err("non-tls build rejects a security config");
                assert!(err.to_string().contains("kafka-tls"), "actionable: {err}");
            }
        }
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let body = format!("{}  topics: [a, b]\n", minimal());
        assert!(KafkaSourceConfig::from_component_config(&section(&body)).is_err());
    }

    #[test]
    fn empty_required_fields_error_clearly() {
        for body in [
            "  brokers: \"\"\n  topic: t\n  group_id: g\n",
            "  brokers: b\n  topic: \"\"\n  group_id: g\n",
            "  brokers: b\n  topic: t\n  group_id: \"\"\n",
        ] {
            assert!(KafkaSourceConfig::from_component_config(&section(body)).is_err());
        }
    }
}
