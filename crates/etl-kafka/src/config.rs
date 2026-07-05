//! Kafka source configuration: typed fields plus a validated raw
//! librdkafka property passthrough.

use etl_core::config::{ComponentConfig, ConfigError};
use serde::Deserialize;
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
/// against librdkafka's config table yields two alias names to add — the
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

fn default_statistics_interval() -> Duration {
    Duration::from_secs(5)
}

/// Configuration of a [`KafkaSource`](crate::KafkaSource), deserialized
/// from the pipeline's opaque `source: { kafka: ... }` section.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
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
    /// Statistics emission interval feeding lag metrics. Zero disables
    /// statistics.
    #[serde(with = "humantime_serde", default = "default_statistics_interval")]
    pub statistics_interval: Duration,
    /// Raw librdkafka properties, applied verbatim after validation.
    /// Framework-owned properties (see crate docs) are rejected;
    /// prefetch backstops (`queued.min.messages`,
    /// `queued.max.messages.kbytes`) may be tuned here.
    #[serde(default)]
    pub rdkafka: BTreeMap<String, String>,
}

impl KafkaSourceConfig {
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
        // Prefetch memory backstop, overridable through the passthrough.
        if !self.rdkafka.contains_key("queued.min.messages") {
            cc.set("queued.min.messages", "1000");
        }
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
    use etl_core::config::ComponentConfig;

    fn section(body: &str) -> ComponentConfig {
        let yaml = format!("kafka:\n{body}");
        let value: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        ComponentConfig::new("kafka", value["kafka"].clone())
    }

    fn minimal() -> String {
        "  brokers: localhost:9092\n  topic: orders\n  group_id: etl\n".to_string()
    }

    #[test]
    fn minimal_config_gets_documented_defaults() {
        let cfg = KafkaSourceConfig::from_component_config(&section(&minimal())).unwrap();
        assert_eq!(cfg.commit_interval, Duration::from_secs(5));
        assert_eq!(cfg.startup_timeout, Duration::from_secs(30));
        assert_eq!(cfg.statistics_interval, Duration::from_secs(5));
        assert!(cfg.rdkafka.is_empty());
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
            "backstop is overridable"
        );
        assert_eq!(cc.get("enable.auto.offset.store"), Some("false"));
        assert_eq!(cc.get("enable.auto.commit"), Some("true"));
        assert_eq!(cc.get("auto.commit.interval.ms"), Some("5000"));
        assert_eq!(cc.get("enable.partition.eof"), Some("false"));
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
