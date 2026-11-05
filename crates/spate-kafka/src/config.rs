//! Kafka source configuration: typed fields plus a validated raw
//! librdkafka property passthrough.

use crate::client::{Base, Keys, Role};
use serde::Deserialize;
use spate_core::config::{ComponentConfig, ConfigError, DurationBounds, check_duration, redact};
use std::collections::BTreeMap;
use std::time::Duration;

/// One of the source's consumers, and the properties it owns beyond the base
/// in [`crate::client`]. Setting one through the passthrough is rejected at
/// load time with the reason given here, because overriding it silently
/// breaks the framework's delivery guarantees or its threading model.
///
/// The second field is the consumer's index when the source runs more than
/// one, and suffixes its `client.id`.
pub(crate) struct Consumer<'a>(pub(crate) &'a KafkaSourceConfig, pub(crate) Option<usize>);

impl<'a> Consumer<'a> {
    /// Consumer `k` of `config`'s [`KafkaSourceConfig::consumers`], indexed
    /// only when there is more than one.
    pub(crate) fn member(config: &'a KafkaSourceConfig, k: usize) -> Consumer<'a> {
        Consumer(config, (config.consumers > 1).then_some(k))
    }
}

impl Role for Consumer<'_> {
    fn name(&self) -> &'static str {
        "consumer"
    }

    fn statistics(&self) -> bool {
        true
    }

    fn instance(&self) -> Option<usize> {
        self.1
    }

    fn defaults(&self) -> &'static [(&'static str, &'static str)] {
        // Each partition queue is split, so librdkafka's queue thresholds apply
        // per partition, and a partition that reaches one is not fetched again
        // for `fetch.queue.backoff.ms`. A lane drains a full queue well inside
        // librdkafka's 1000 ms, and its pipeline thread then waits on an empty
        // queue for the rest of the backoff.
        &[("fetch.queue.backoff.ms", "10")]
    }

    fn declare(&self, keys: &mut Keys<'_>) {
        keys.own(
            "group.id",
            &self.0.group_id,
            "owned by the typed `group_id` field",
        );
        // The framework's commit mechanism: offsets are stored explicitly
        // when checkpoint watermarks advance and committed on an interval.
        keys.own(
            "enable.auto.offset.store",
            "false",
            "the framework stores offsets itself when checkpoint watermarks \
             advance; overriding this breaks at-least-once delivery",
        );
        keys.own(
            "enable.auto.commit",
            "true",
            "interval auto-commit of framework-stored offsets is the commit \
             mechanism; disabling it means nothing is ever committed",
        );
        keys.own_if(
            true,
            "auto.commit.interval.ms",
            || self.0.commit_interval.as_millis().to_string(),
            "owned by the typed `commit_interval` field",
        );
        keys.own(
            "enable.partition.eof",
            "false",
            "EOF events would pollute the partition queues the pipeline polls",
        );
        keys.forbid(
            "auto.commit.enable",
            "a deprecated librdkafka topic property the consumer does not \
             read; the framework commits through `enable.auto.commit`, which \
             it owns",
        );
        keys.forbid(
            "group.protocol",
            "only the classic consumer group protocol is supported today \
             (eager assignment is a framework invariant)",
        );
    }
}

fn default_consumers() -> usize {
    1
}

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
#[derive(Clone, Deserialize, PartialEq)]
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
    /// How many consumers this process runs, at least 1. Each is a separate
    /// member of `group_id` that owns its own share of the topic's
    /// partitions, and puts one more Fetch in flight on each broker that
    /// leads one of them. Each also adds its own librdkafka threads, a
    /// connection per broker, a group coordinator connection and, under SASL,
    /// its own session. Consumers beyond the partition count hold nothing.
    ///
    /// Above 1, consumer `k` sends `client.id` with a `-<k>` suffix, so a
    /// broker quota on the unsuffixed id does not apply to it, and its
    /// statistics carry a `client="consumer-<k>"` label. Static membership
    /// (`group.instance.id`) requires 1.
    #[serde(default = "default_consumers")]
    pub consumers: usize,
    /// How often stored offsets are auto-committed
    /// (librdkafka `auto.commit.interval.ms`). librdkafka reads the value in
    /// whole milliseconds up to 24h and turns interval commits off at 0, so
    /// [`validate`](Self::validate) refuses a value under 1ms or above 24h.
    #[serde(with = "humantime_serde", default = "default_commit_interval")]
    pub commit_interval: Duration,
    /// How long to wait for the first partition assignment before the
    /// source reports a fatal startup error, which names the last consumer
    /// error and how long ago it arrived. Zero disables the deadline, and
    /// the source waits for as long as the group takes.
    #[serde(with = "humantime_serde", default = "default_startup_timeout")]
    pub startup_timeout: Duration,
    /// How long the source may run with no assignment, after having had one,
    /// before it reports a fatal error, which names the last consumer error
    /// and how long ago it arrived. Measured from the moment ownership is
    /// released, cleared by the next accepted assignment, including an empty
    /// one. Zero disables the deadline.
    #[serde(with = "humantime_serde", default = "default_assignment_timeout")]
    pub assignment_timeout: Duration,
    /// librdkafka statistics emission interval, feeding the lag metrics and
    /// the connector's `spate_kafka_source_*` families (broker health,
    /// latency, queue saturation, group stability). Zero disables statistics
    /// and those families with it. librdkafka reads the value in whole
    /// milliseconds up to 24h, so [`validate`](Self::validate) refuses a
    /// non-zero value under 1ms or above 24h. [The metrics reference]
    /// tabulates them under Kafka source.
    ///
    /// [the metrics reference]: https://spate.kainth.dev/docs/METRICS
    #[serde(with = "humantime_serde", default = "default_statistics_interval")]
    pub statistics_interval: Duration,
    /// Raw librdkafka properties, applied verbatim after validation.
    /// Framework-owned properties (see crate docs) are rejected. With the
    /// `tls` feature and vendored OpenSSL, a map naming neither
    /// `ssl.ca.location` nor `ssl.ca.pem` gets `ssl.ca.location: probe` unless
    /// `SSL_CERT_FILE` or `SSL_CERT_DIR` is set; a build linked against the
    /// system OpenSSL keeps librdkafka's native CA default.
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
    ///
    /// A partition whose queue reaches either threshold is not fetched again
    /// for `fetch.queue.backoff.ms`, which the source sets to 10 ms unless
    /// this map sets it.
    #[serde(default)]
    pub rdkafka: BTreeMap<String, String>,
}

// Hand-written: the `rdkafka` map carries credentials. The destructure lists
// every field so a new one cannot reach `Debug` unredacted.
impl std::fmt::Debug for KafkaSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let KafkaSourceConfig {
            brokers,
            topic,
            group_id,
            consumers,
            commit_interval,
            startup_timeout,
            assignment_timeout,
            statistics_interval,
            rdkafka,
        } = self;
        f.debug_struct("KafkaSourceConfig")
            .field("brokers", brokers)
            .field("topic", topic)
            .field("group_id", group_id)
            .field("consumers", consumers)
            .field("commit_interval", commit_interval)
            .field("startup_timeout", startup_timeout)
            .field("assignment_timeout", assignment_timeout)
            .field("statistics_interval", statistics_interval)
            .field("rdkafka", &redact::map(rdkafka.keys()))
            .finish()
    }
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
            consumers: default_consumers(),
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
        if self.consumers == 0 {
            return Err(ConfigError::Validation(
                "source.kafka.consumers must be at least 1".into(),
            ));
        }
        // Consumers of one process share the passthrough, so they would all
        // claim one static member id and fence each other.
        if self.consumers > 1
            && let Some((key, _)) = self
                .rdkafka
                .iter()
                .find(|(k, v)| crate::client::canonical(k) == "group.instance.id" && !v.is_empty())
        {
            return Err(ConfigError::Validation(format!(
                "source.kafka.consumers above 1 cannot be combined with \
                 source.kafka.rdkafka.\"{key}\": every consumer would join as \
                 the same static member and fence the others; set consumers: 1 \
                 or remove the static member id"
            )));
        }
        if let Err(why) = self.base(false).check_statistics_interval() {
            return Err(ConfigError::Validation(format!(
                "source.kafka.statistics_interval {why}"
            )));
        }
        // librdkafka's `auto.commit.interval.ms` range is 0 to 24h, and 0
        // turns the commit timer off.
        const COMMIT_INTERVAL: DurationBounds =
            DurationBounds::at_least(Duration::from_millis(1)).at_most(Duration::from_hours(24));
        if let Err(e) = check_duration(self.commit_interval, COMMIT_INTERVAL) {
            return Err(ConfigError::Validation(format!(
                "source.kafka.commit_interval {e}: librdkafka reads the interval in whole \
                 milliseconds up to 24h and turns interval commits off at 0"
            )));
        }
        if let Err(refusal) = self.base(false).denied(&[&Consumer(self, None)]) {
            return Err(ConfigError::Validation(format!("source.kafka.{refusal}")));
        }
        if self.rdkafka.contains_key("auto.offset.reset")
            && self.rdkafka.contains_key("topic.auto.offset.reset")
        {
            return Err(ConfigError::Validation(
                "source.kafka.rdkafka sets both \"auto.offset.reset\" and \
                 \"topic.auto.offset.reset\": librdkafka reads them as one \
                 property and applies them in no set order; set one"
                    .into(),
            ));
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

    /// Where a partition with no committed offset starts under
    /// `auto.offset.reset`. `None` for `error`, which only librdkafka applies.
    /// `End` is resolved and committed once, by the source.
    /// librdkafka reads `topic.auto.offset.reset` as the same property.
    pub(crate) fn uncommitted_start(&self) -> Option<rdkafka::Offset> {
        let policy = self
            .rdkafka
            .get("auto.offset.reset")
            .or_else(|| self.rdkafka.get("topic.auto.offset.reset"))
            .map_or("latest", String::as_str);
        // librdkafka skips leading whitespace in the value.
        match policy.trim_start().to_ascii_lowercase().as_str() {
            "smallest" | "earliest" | "beginning" => Some(rdkafka::Offset::Beginning),
            "largest" | "latest" | "end" => Some(rdkafka::Offset::End),
            _ => None,
        }
    }

    /// Whether librdkafka measures lag to the last stable offset rather than
    /// the high watermark (`isolation.level`, `read_committed` by default).
    pub(crate) fn reads_committed(&self) -> bool {
        self.rdkafka
            .get("isolation.level")
            .is_none_or(|level| level.eq_ignore_ascii_case("read_committed"))
    }

    /// What the source's consumer shares with every Kafka client.
    pub(crate) fn base(&self, openssl_env: bool) -> Base<'_> {
        Base {
            section: "source.kafka",
            brokers: &self.brokers,
            rdkafka: &self.rdkafka,
            statistics_interval: self.statistics_interval,
            openssl_env,
        }
    }

    /// The consumer's librdkafka configuration.
    ///
    /// Prefetch depth is left at librdkafka's own defaults
    /// (`queued.min.messages` 100000, `queued.max.messages.kbytes` 65536), so
    /// behavior is predictable and reasoning transfers from librdkafka's
    /// documentation like it does for every other Kafka client. Forcing
    /// `queued.min.messages` to 1000 as a memory backstop cost ~11x end to end
    /// on a backlogged consumer, because the local fetch queue sits empty and
    /// every driver thread blocks on the broker; deployments that need memory
    /// bounded set it through `rdkafka`, where the trade is visible.
    #[cfg(test)]
    pub(crate) fn client_config(&self) -> rdkafka::ClientConfig {
        self.client_config_with(false)
    }

    #[cfg(test)]
    pub(crate) fn client_config_with(&self, openssl_env: bool) -> rdkafka::ClientConfig {
        self.base(openssl_env).config(&Consumer(self, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::testing::Raw as _;
    use spate_core::config::ComponentConfig;

    fn section(body: &str) -> ComponentConfig {
        spate_test::component_config("kafka", body)
    }

    fn minimal() -> String {
        "  brokers: localhost:9092\n  topic: orders\n  group_id: spate\n".to_string()
    }

    fn with_rdkafka(pairs: &[(&str, &str)]) -> KafkaSourceConfig {
        let mut cfg = KafkaSourceConfig::new("localhost:9092", "orders", "spate");
        cfg.rdkafka = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        cfg
    }

    /// `Debug` shows every `rdkafka` key and none of the values.
    #[test]
    fn debug_never_prints_rdkafka_values() {
        let body = format!("{}  rdkafka:\n    sasl.password: hunter2\n", minimal());
        let cfg: KafkaSourceConfig = section(&body).deserialize_into().unwrap();
        let printed = format!("{cfg:?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        assert!(
            printed.contains("\"sasl.password\": <redacted>"),
            "{printed}"
        );
    }

    /// Every spelling librdkafka accepts maps to the same start, and an unset
    /// policy starts at the end, as librdkafka's default does.
    #[test]
    fn the_reset_policy_maps_to_a_start_offset() {
        use rdkafka::Offset;
        for (policy, start) in [
            ("smallest", Some(Offset::Beginning)),
            ("Earliest", Some(Offset::Beginning)),
            ("beginning", Some(Offset::Beginning)),
            ("largest", Some(Offset::End)),
            ("LATEST", Some(Offset::End)),
            ("end", Some(Offset::End)),
            ("error", None),
        ] {
            let cfg = with_rdkafka(&[("auto.offset.reset", policy)]);
            assert_eq!(cfg.uncommitted_start(), start, "{policy}");
        }
        assert_eq!(with_rdkafka(&[]).uncommitted_start(), Some(Offset::End));
    }

    /// librdkafka reads `topic.auto.offset.reset` as `auto.offset.reset`, so
    /// the prefixed key gives the same start. Regression for #1255.
    #[test]
    fn the_topic_prefixed_reset_policy_maps_to_the_same_start() {
        use rdkafka::Offset;
        for (policy, start) in [
            ("smallest", Some(Offset::Beginning)),
            ("Earliest", Some(Offset::Beginning)),
            ("beginning", Some(Offset::Beginning)),
            ("largest", Some(Offset::End)),
            ("LATEST", Some(Offset::End)),
            ("end", Some(Offset::End)),
            ("error", None),
        ] {
            let cfg = with_rdkafka(&[("topic.auto.offset.reset", policy)]);
            assert_eq!(cfg.uncommitted_start(), start, "{policy}");
        }
    }

    /// Both spellings reach librdkafka as one property in no set order, so a
    /// config that sets both fails at load.
    #[test]
    fn both_spellings_of_the_reset_policy_are_refused() {
        let body = format!(
            "{}  rdkafka:\n    auto.offset.reset: earliest\n    topic.auto.offset.reset: earliest\n",
            minimal()
        );
        let err = KafkaSourceConfig::from_component_config(&section(&body)).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("\"auto.offset.reset\""), "{msg}");
        assert!(msg.contains("\"topic.auto.offset.reset\""), "{msg}");
    }

    #[test]
    fn lag_reads_the_stable_offset_unless_uncommitted_reads_are_set() {
        assert!(with_rdkafka(&[]).reads_committed());
        assert!(with_rdkafka(&[("isolation.level", "read_committed")]).reads_committed());
        assert!(!with_rdkafka(&[("isolation.level", "read_uncommitted")]).reads_committed());
    }

    #[test]
    fn minimal_config_gets_documented_defaults() {
        let cfg = KafkaSourceConfig::from_component_config(&section(&minimal())).unwrap();
        assert_eq!(cfg.commit_interval, Duration::from_secs(5));
        assert_eq!(cfg.startup_timeout, Duration::from_secs(30));
        assert_eq!(cfg.assignment_timeout, Duration::from_mins(5));
        assert_eq!(cfg.statistics_interval, Duration::from_secs(5));
        assert_eq!(cfg.consumers, 1);
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
        // Written out: a loop over the consumer's declarations would follow
        // an entry removed from them.
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
            "allow.auto.create.topics",
        ] {
            for stats in ["0s", "5s"] {
                for written in [key.to_owned(), format!("topic.{key}")] {
                    let body = format!(
                        "{}  statistics_interval: {stats}\n  rdkafka:\n    \"{written}\": \"x\"\n",
                        minimal()
                    );
                    let msg = KafkaSourceConfig::from_component_config(&section(&body))
                        .unwrap_err()
                        .to_string();
                    assert!(
                        msg.contains(&format!(
                            "source.kafka.rdkafka.\"{written}\" cannot be overridden"
                        )),
                        "{msg}"
                    );
                }
            }
        }
    }

    /// A global librdkafka property whose name starts with `topic.` is not a
    /// topic-prefixed spelling of an owned property.
    #[test]
    fn a_global_topic_property_passes_validation() {
        let body = format!(
            "{}  rdkafka:\n    topic.metadata.refresh.interval.ms: \"1000\"\n",
            minimal()
        );
        KafkaSourceConfig::from_component_config(&section(&body)).expect("not owned");
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
        assert_eq!(cc.get("allow.auto.create.topics"), Some("false"));
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
    /// validates the build's capability (SSL present, and the PLAIN and SCRAM
    /// SASL providers present) without any network I/O. Without it: rejected at
    /// load with an actionable message. Default `cargo test -p spate-kafka`
    /// exercises the reject arm; `--features tls` / `--all-features` the accept
    /// arm.
    #[test]
    fn tls_config_matches_build_capability() {
        for sec in [
            "    security.protocol: ssl\n",
            "    security.protocol: sasl_ssl\n    sasl.mechanism: PLAIN\n    \
             sasl.username: svc\n    sasl.password: secret\n",
            "    security.protocol: sasl_ssl\n    sasl.mechanism: SCRAM-SHA-256\n    \
             sasl.username: svc\n    sasl.password: secret\n",
            "    security.protocol: sasl_ssl\n    sasl.mechanism: SCRAM-SHA-512\n    \
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
                    .build()
                    .expect("SSL/SASL compiled in: consumer creation succeeds");
                drop(consumer);
            } else {
                let msg = parsed
                    .expect_err("non-tls build rejects a security config")
                    .to_string();
                assert!(msg.contains("kafka-tls"), "actionable: {msg}");
                assert!(msg.contains("source.kafka.rdkafka"), "scoped: {msg}");
            }
        }
    }

    /// The `tls` build compiles in neither GSSAPI nor the OAUTHBEARER OIDC
    /// method, so consumer creation fails for each, and for an omitted
    /// mechanism, which defaults to GSSAPI.
    #[cfg(feature = "tls")]
    #[test]
    fn tls_build_rejects_gssapi_and_oidc() {
        for (sasl, expected) in [
            ("    sasl.mechanism: GSSAPI\n", "GSSAPI"),
            ("", "GSSAPI"),
            (
                "    sasl.mechanism: OAUTHBEARER\n    sasl.oauthbearer.method: oidc\n    \
                 sasl.oauthbearer.client.id: svc\n    sasl.oauthbearer.client.secret: secret\n    \
                 sasl.oauthbearer.token.endpoint.url: https://idp.invalid/token\n",
                "not supported in this build",
            ),
        ] {
            let body = format!(
                "{}  rdkafka:\n    security.protocol: sasl_ssl\n{sasl}",
                minimal()
            );
            let cfg = KafkaSourceConfig::from_component_config(&section(&body))
                .expect("config load does not check the mechanism");
            let Err(err) = cfg
                .client_config()
                .build::<rdkafka::consumer::BaseConsumer>()
            else {
                panic!("consumer creation succeeded with {sasl:?}");
            };
            assert!(err.to_string().contains(expected), "{err}");
        }
    }

    /// The `tls` build supports every `builtin.features` flag the guard
    /// rejects without it.
    #[cfg(feature = "tls")]
    #[test]
    fn tls_build_supports_guarded_builtin_features() {
        let body = format!(
            "{}  rdkafka:\n    builtin.features: ssl,sasl_scram,sasl_oauthbearer\n",
            minimal()
        );
        let cfg = KafkaSourceConfig::from_component_config(&section(&body))
            .expect("tls build accepts the flags at load");
        let consumer: rdkafka::consumer::BaseConsumer = cfg
            .client_config()
            .build()
            .expect("OpenSSL compiled in: consumer creation succeeds");
        drop(consumer);
    }

    /// A vendored-OpenSSL `tls` build gets `probe` when the passthrough names no
    /// CA and no OpenSSL env override is set, and keeps a CA it names.
    #[cfg(feature = "tls")]
    #[test]
    fn tls_build_ca_default_yields_to_env_and_named_ca() {
        let cfg = KafkaSourceConfig::from_component_config(&section(&minimal())).unwrap();
        assert_eq!(
            cfg.client_config_with(false).get("ssl.ca.location"),
            cfg!(spate_openssl_vendored).then_some("probe")
        );
        assert_eq!(cfg.client_config_with(true).get("ssl.ca.location"), None);

        let body = format!(
            "{}  rdkafka:\n    ssl.ca.location: /etc/kafka/ca.pem\n",
            minimal()
        );
        let cfg = KafkaSourceConfig::from_component_config(&section(&body)).unwrap();
        assert_eq!(
            cfg.client_config_with(false).get("ssl.ca.location"),
            Some("/etc/kafka/ca.pem")
        );
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

    fn with_consumers(count: &str, rdkafka: &str) -> Result<KafkaSourceConfig, ConfigError> {
        let body = format!("{}  consumers: {count}\n{rdkafka}", minimal());
        KafkaSourceConfig::from_component_config(&section(&body))
    }

    /// A consumer count of zero is refused, naming the key and the bound.
    #[test]
    fn zero_consumers_are_rejected() {
        let err = with_consumers("0", "").unwrap_err().to_string();
        assert!(
            err.contains("source.kafka.consumers must be at least 1"),
            "{err}"
        );
    }

    /// Any count from one up is accepted.
    #[test]
    fn two_consumers_are_accepted() {
        assert_eq!(with_consumers("2", "").unwrap().consumers, 2);
        assert_eq!(with_consumers("1", "").unwrap().consumers, 1);
    }

    /// A count that is not a non-negative integer is refused as a type error
    /// naming the key.
    #[test]
    fn a_non_integer_consumer_count_names_the_key() {
        for count in ["many", "-1"] {
            let err = with_consumers(count, "").unwrap_err().to_string();
            assert!(err.contains("consumers"), "{count}: {err}");
            assert!(err.contains("expected usize"), "{count}: {err}");
        }
    }

    /// The consumer count reaches `Debug`.
    #[test]
    fn debug_prints_the_consumer_count() {
        let cfg = with_consumers("2", "").unwrap();
        assert!(format!("{cfg:?}").contains("consumers: 2"), "{cfg:?}");
    }

    /// Several consumers would all join as one static member and fence each
    /// other, so a static member id requires one consumer. The refusal names
    /// both keys and never the id.
    #[test]
    fn static_membership_with_several_consumers_is_refused() {
        let id = "  rdkafka:\n    group.instance.id: w-secret\n";
        let err = with_consumers("2", id).unwrap_err().to_string();
        assert!(err.contains("source.kafka.consumers"), "{err}");
        assert!(err.contains("group.instance.id"), "{err}");
        assert!(!err.contains("w-secret"), "{err}");
        with_consumers("1", id).expect("one consumer may be a static member");
        with_consumers("2", "  rdkafka:\n    group.instance.id: \"\"\n")
            .expect("an empty id is no static membership");
    }

    /// Pins that a non-zero `statistics_interval` under 1ms or above 24h is
    /// refused at load ahead of the passthrough denylist, that the refusal
    /// names the key and its reason and no passthrough value, and that `0s`,
    /// `1ms` and `24h` load with the value written in milliseconds.
    /// Regression for #1528.
    #[test]
    fn a_statistics_interval_outside_1ms_to_24h_is_refused() {
        for interval in [
            "1ns",
            "500us",
            "999us",
            "999999ns",
            "24h 1ns",
            "24h 1ms",
            "4294967296ms",
        ] {
            let body = format!(
                "{}  statistics_interval: {interval}\n  rdkafka:\n    client.id: hunter2\n",
                minimal()
            );
            let msg = KafkaSourceConfig::from_component_config(&section(&body))
                .expect_err(interval)
                .to_string();
            assert!(
                msg.contains("source.kafka.statistics_interval"),
                "{interval}: {msg}"
            );
            assert!(!msg.contains("hunter2"), "{interval}: {msg}");
        }
        let body = format!(
            "{}  statistics_interval: 500us\n  rdkafka:\n    \"statistics.interval.ms\": \"x\"\n",
            minimal()
        );
        let msg = KafkaSourceConfig::from_component_config(&section(&body))
            .unwrap_err()
            .to_string();
        assert_eq!(
            msg,
            "invalid configuration: source.kafka.statistics_interval must be 0s or from 1ms \
             to 1day; found 500us: 0s disables statistics, and librdkafka reads whole \
             milliseconds up to 24h"
        );
        for (interval, written) in [
            ("0s", None),
            ("1ms", Some("1")),
            ("1ms 500us", Some("1")),
            ("24h", Some("86400000")),
        ] {
            let body = format!("{}  statistics_interval: {interval}\n", minimal());
            let cfg = KafkaSourceConfig::from_component_config(&section(&body))
                .unwrap_or_else(|err| panic!("{interval}: {err}"));
            assert_eq!(
                cfg.client_config().get("statistics.interval.ms"),
                written,
                "{interval}"
            );
        }
    }

    /// The refusal `validate` gives for a `commit_interval` of `found`.
    fn commit_refusal(found: &str) -> String {
        format!(
            "invalid configuration: source.kafka.commit_interval must be from 1ms to 1day; \
             found {found}: librdkafka reads the interval in whole milliseconds up to 24h and \
             turns interval commits off at 0"
        )
    }

    /// Pins that a `commit_interval` under 1ms, `0s` included, or above 24h
    /// is refused at load ahead of the passthrough denylist, that the refusal
    /// names the key and no passthrough value, and that `1ms` and `24h` load
    /// with the value written in milliseconds. Regression for #1661.
    #[test]
    fn a_commit_interval_outside_1ms_to_24h_is_refused() {
        for (interval, found) in [
            ("0s", "0s"),
            ("1ns", "1ns"),
            ("500us", "500us"),
            ("999us", "999us"),
            ("999999ns", "999us 999ns"),
            ("24h 1ns", "1day 1ns"),
            ("24h 500us", "1day 500us"),
            ("24h 1ms", "1day 1ms"),
            ("4294967296ms", "1month 19days 6h 29m 11s 296ms"),
        ] {
            let body = format!(
                "{}  commit_interval: {interval}\n  rdkafka:\n    client.id: hunter2\n",
                minimal()
            );
            let msg = KafkaSourceConfig::from_component_config(&section(&body))
                .expect_err(interval)
                .to_string();
            assert_eq!(msg, commit_refusal(found), "{interval}");
        }
        let body = format!(
            "{}  commit_interval: 500us\n  rdkafka:\n    \"auto.commit.interval.ms\": \"x\"\n",
            minimal()
        );
        let msg = KafkaSourceConfig::from_component_config(&section(&body))
            .unwrap_err()
            .to_string();
        assert_eq!(msg, commit_refusal("500us"));
        for (interval, written) in [("1ms", "1"), ("1ms 500us", "1"), ("24h", "86400000")] {
            let body = format!("{}  commit_interval: {interval}\n", minimal());
            let cfg = KafkaSourceConfig::from_component_config(&section(&body))
                .unwrap_or_else(|err| panic!("{interval}: {err}"));
            assert_eq!(
                cfg.client_config().get("auto.commit.interval.ms"),
                Some(written),
                "{interval}"
            );
        }
        let cfg = KafkaSourceConfig::from_component_config(&section(&minimal())).unwrap();
        assert_eq!(
            cfg.client_config().get("auto.commit.interval.ms"),
            Some("5000")
        );
    }
}
