//! The `kafka:` body of a `feed:` section, decoded and checked.

use serde::Deserialize;
use spate_core::config::{ConfigError, FeedSection, redact};
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::time::Duration;

/// The key that names this backend in a `feed:` section.
pub(crate) const BACKEND: &str = "kafka";

/// Deliveries after which a failed unit is dead-lettered, unless the section
/// sets `attempt_cap`.
const ATTEMPT_CAP: NonZeroU32 = NonZeroU32::new(10).expect("non-zero");

/// Properties the feed owns, refused in the `rdkafka:` passthrough with the
/// reason. One passthrough map feeds the feed's consumer and its producer,
/// so the list is the union of what either client owns.
///
/// librdkafka accepts several names for one setting, and the passthrough
/// and the feed's own values are separate `ClientConfig` keys applied in no
/// set order, so every alias of an owned property is refused beside it.
/// librdkafka also reads `topic.<name>` as the topic property `<name>`, so a
/// key is checked with that prefix removed.
const DENYLIST: &[(&str, &str)] = &[
    ("group.id", "owned by the typed `group_id` field"),
    (
        "group.protocol",
        "the feed runs the classic consumer group protocol with eager \
         assignment",
    ),
    (
        "partition.assignment.strategy",
        "the feed relies on eager assignment, under which a rebalance revokes \
         every partition before it assigns any",
    ),
    (
        "auto.offset.reset",
        "a partition with no committed offset starts at its earliest offset, \
         so a group whose offsets expired reads the topic from its start",
    ),
    (
        "enable.auto.commit",
        "the feed commits each partition up to its first unfinished unit \
         itself",
    ),
    (
        "auto.commit.enable",
        "librdkafka's legacy topic-level auto-commit property; the feed \
         commits each partition up to its first unfinished unit itself",
    ),
    (
        "enable.auto.offset.store",
        "the feed decides which offsets may be committed",
    ),
    (
        "enable.partition.eof",
        "the feed reads only messages from the consumer",
    ),
    (
        "acks",
        "forced to `all`: an offset completes once the delivery report \
         confirms its retry or dead-letter write, so a report under weaker \
         acks could lose the unit",
    ),
    (
        "request.required.acks",
        "`acks` is the librdkafka alias of this property, which is forced to \
         `all`",
    ),
    (
        "enable.idempotence",
        "forced on, so librdkafka's own retries neither reorder nor duplicate \
         a write",
    ),
    (
        "transactional.id",
        "the feed does not use transactions; a write is confirmed by its \
         delivery report",
    ),
    (
        "message.timeout.ms",
        "owned by the typed `delivery_timeout` field, which also bounds how \
         long the feed waits for a delivery report",
    ),
    (
        "delivery.timeout.ms",
        "librdkafka alias of `message.timeout.ms`, owned by the typed \
         `delivery_timeout` field",
    ),
    (
        "allow.auto.create.topics",
        "forced off: a write to a missing topic must fail, not create the \
         topic with the broker's defaults",
    ),
    (
        "statistics.interval.ms",
        "owned by the typed `statistics_interval` field",
    ),
    ("bootstrap.servers", "owned by the typed `brokers` field"),
    (
        "metadata.broker.list",
        "librdkafka's own name for `bootstrap.servers`, owned by the typed \
         `brokers` field",
    ),
    (
        "delivery.report.only.error",
        "a retry or dead-letter write finishes its offset only once its \
         delivery report confirms it, so suppressing success reports would \
         stop the partition's commits",
    ),
];

fn default_delivery_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_statistics_interval() -> Duration {
    Duration::from_secs(5)
}

/// The keys under `feed: { kafka: … }`, as written.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Body {
    brokers: String,
    group_id: String,
    topic: String,
    retry_topic: String,
    dead_letter_topic: String,
    #[serde(with = "humantime_serde", default = "default_delivery_timeout")]
    delivery_timeout: Duration,
    #[serde(with = "humantime_serde", default = "default_statistics_interval")]
    statistics_interval: Duration,
    #[serde(default)]
    rdkafka: BTreeMap<String, String>,
    /// Read only to refuse it with a pointer to the section key.
    #[serde(default)]
    attempt_cap: Option<serde::de::IgnoredAny>,
}

/// A `feed: { kafka: … }` section, decoded and checked.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct KafkaFeedConfig {
    /// Comma-separated bootstrap servers.
    pub(crate) brokers: String,
    /// The consumer group every worker of the feed joins.
    pub(crate) group_id: String,
    /// The work topic: where a publish writes a unit.
    pub(crate) topic: String,
    /// Where a unit failed below the attempt cap is written again.
    pub(crate) retry_topic: String,
    /// Where a unit goes at the attempt cap, or a message that names no unit.
    pub(crate) dead_letter_topic: String,
    /// The bound on every write's wait for its delivery report, and the
    /// producer's `message.timeout.ms`.
    pub(crate) delivery_timeout: Duration,
    /// librdkafka's statistics interval. Zero disables statistics.
    pub(crate) statistics_interval: Duration,
    /// Raw librdkafka properties for every feed client.
    pub(crate) rdkafka: BTreeMap<String, String>,
    /// The delivery on which a failed unit moves to the dead-letter topic,
    /// from the section's `attempt_cap`.
    pub(crate) attempt_cap: NonZeroU32,
}

// Hand-written: the `rdkafka` map carries credentials. The destructure lists
// every field so a new one cannot reach `Debug` unredacted.
impl std::fmt::Debug for KafkaFeedConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let KafkaFeedConfig {
            brokers,
            group_id,
            topic,
            retry_topic,
            dead_letter_topic,
            delivery_timeout,
            statistics_interval,
            rdkafka,
            attempt_cap,
        } = self;
        f.debug_struct("KafkaFeedConfig")
            .field("brokers", brokers)
            .field("group_id", group_id)
            .field("topic", topic)
            .field("retry_topic", retry_topic)
            .field("dead_letter_topic", dead_letter_topic)
            .field("delivery_timeout", delivery_timeout)
            .field("statistics_interval", statistics_interval)
            .field("rdkafka", &redact::map(rdkafka.keys()))
            .field("attempt_cap", attempt_cap)
            .finish()
    }
}

impl KafkaFeedConfig {
    /// Decode and check the `kafka:` body of `section`, with the section's
    /// `attempt_cap`, else 10.
    pub(crate) fn from_section(section: &FeedSection) -> Result<KafkaFeedConfig, ConfigError> {
        let backend = section.backend();
        if backend.type_tag() != BACKEND {
            return Err(error(format!(
                "the Kafka feed is built from `feed: {{ {BACKEND}: … }}`, but the section names `{}`",
                backend.type_tag()
            )));
        }
        let body: Body = backend.deserialize_into()?;
        if body.attempt_cap.is_some() {
            return Err(error(
                "`attempt_cap` is a key of `feed:` itself, beside `kafka:`".to_owned(),
            ));
        }
        let config = KafkaFeedConfig {
            brokers: body.brokers,
            group_id: body.group_id,
            topic: body.topic,
            retry_topic: body.retry_topic,
            dead_letter_topic: body.dead_letter_topic,
            delivery_timeout: body.delivery_timeout,
            statistics_interval: body.statistics_interval,
            rdkafka: body.rdkafka,
            attempt_cap: section.attempt_cap.unwrap_or(ATTEMPT_CAP),
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        for (key, value) in [
            ("brokers", &self.brokers),
            ("group_id", &self.group_id),
            ("topic", &self.topic),
            ("retry_topic", &self.retry_topic),
            ("dead_letter_topic", &self.dead_letter_topic),
        ] {
            if value.trim().is_empty() {
                return Err(error(format!("`{key}` must not be empty")));
            }
        }
        if self.delivery_timeout.as_millis() == 0 {
            return Err(error(
                "`delivery_timeout` must be at least 1ms: librdkafka reads 0 as no timeout, and \
                 a write could then wait for ever"
                    .to_owned(),
            ));
        }
        if self.delivery_timeout.as_millis() > i32::MAX as u128 {
            return Err(error(format!(
                "`delivery_timeout` {:?} exceeds librdkafka's maximum (about 24.8 days)",
                self.delivery_timeout
            )));
        }
        // librdkafka reads 0 as off, so a positive interval under a
        // millisecond would silently disable statistics.
        if !self.statistics_interval.is_zero() && self.statistics_interval.as_millis() == 0 {
            return Err(error(
                "`statistics_interval` must be 0s, which disables statistics, or at least 1ms"
                    .to_owned(),
            ));
        }
        for key in self.rdkafka.keys() {
            let name = key.strip_prefix("topic.").unwrap_or(key);
            if let Some((_, why)) = DENYLIST.iter().find(|(owned, _)| *owned == name) {
                return Err(error(format!(
                    "rdkafka.\"{key}\" cannot be overridden: {why}"
                )));
            }
        }
        crate::security::check_tls_feature(&self.rdkafka, "feed.kafka")
    }

    /// The consumer's librdkafka configuration: the passthrough, then the
    /// properties the feed owns, which win.
    ///
    /// Takes the OpenSSL env check as an argument: on Linux, cargo exports
    /// `SSL_CERT_FILE` and `SSL_CERT_DIR` to every process it runs, so a test
    /// reading them always sees them set.
    pub(crate) fn consumer_config(&self, openssl_env: bool) -> rdkafka::ClientConfig {
        let mut cc = self.client_config(openssl_env);
        cc.set("group.id", &self.group_id);
        cc.set("group.protocol", "classic");
        // Eager: a rebalance revokes every partition before it assigns any,
        // and the tracker relies on that.
        cc.set("partition.assignment.strategy", "range,roundrobin");
        cc.set("auto.offset.reset", "earliest");
        cc.set("enable.auto.commit", "false");
        cc.set("enable.auto.offset.store", "false");
        cc.set("enable.partition.eof", "false");
        cc
    }

    /// The producer's librdkafka configuration: the passthrough, then the
    /// properties the feed owns, which win.
    pub(crate) fn producer_config(&self, openssl_env: bool) -> rdkafka::ClientConfig {
        let mut cc = self.client_config(openssl_env);
        cc.set("acks", "all");
        cc.set("enable.idempotence", "true");
        cc.set(
            "message.timeout.ms",
            self.delivery_timeout.as_millis().to_string(),
        );
        cc
    }

    /// The configuration of the clients the startup checks read through:
    /// the passthrough, then the shared properties the feed owns, with no
    /// statistics, which nothing reads from them.
    pub(crate) fn admin_config(&self, openssl_env: bool) -> rdkafka::ClientConfig {
        let mut cc = self.client_config(openssl_env);
        cc.remove("statistics.interval.ms");
        cc
    }

    fn client_config(&self, openssl_env: bool) -> rdkafka::ClientConfig {
        let mut cc = rdkafka::ClientConfig::new();
        // rdkafka reads the log facade by default; tracing filters these events.
        cc.set_log_level(rdkafka::config::RDKafkaLogLevel::Debug);
        for (k, v) in &self.rdkafka {
            cc.set(k, v);
        }
        crate::security::apply_ca_default(&mut cc, &self.rdkafka, openssl_env);
        cc.set("bootstrap.servers", &self.brokers);
        // A write to, or a metadata read of, a missing topic must fail, and
        // never create the topic with the broker's defaults.
        cc.set("allow.auto.create.topics", "false");
        if !self.statistics_interval.is_zero() {
            cc.set(
                "statistics.interval.ms",
                self.statistics_interval.as_millis().to_string(),
            );
        }
        cc
    }
}

fn error(message: String) -> ConfigError {
    ConfigError::Component {
        context: format!("feed.{BACKEND}"),
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = "kafka:\n  brokers: localhost:9092\n  group_id: workers\n  \
                           topic: units\n  retry_topic: units-retry\n  \
                           dead_letter_topic: units-dead\n";

    fn section(yaml: &str) -> FeedSection {
        serde_yaml::from_str(yaml).unwrap()
    }

    fn parse(yaml: &str) -> Result<KafkaFeedConfig, ConfigError> {
        KafkaFeedConfig::from_section(&section(yaml))
    }

    fn with_rdkafka(key: &str, value: &str) -> Result<KafkaFeedConfig, ConfigError> {
        parse(&format!(
            "{MINIMAL}  rdkafka:\n    \"{key}\": \"{value}\"\n"
        ))
    }

    /// Pins every key read and the defaults of the optional ones.
    #[test]
    fn the_body_is_read_with_its_defaults() {
        let config = parse(MINIMAL).unwrap();
        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.group_id, "workers");
        assert_eq!(config.topic, "units");
        assert_eq!(config.retry_topic, "units-retry");
        assert_eq!(config.dead_letter_topic, "units-dead");
        assert_eq!(config.delivery_timeout, Duration::from_secs(30));
        assert_eq!(config.statistics_interval, Duration::from_secs(5));
        assert!(config.rdkafka.is_empty());
        assert_eq!(config.attempt_cap.get(), 10);

        let config = parse(&format!(
            "{MINIMAL}  delivery_timeout: 10s\n  statistics_interval: 0s\n  \
             rdkafka:\n    max.poll.interval.ms: \"60000\"\n"
        ))
        .unwrap();
        assert_eq!(config.delivery_timeout, Duration::from_secs(10));
        assert_eq!(config.statistics_interval, Duration::ZERO);
        assert_eq!(config.rdkafka["max.poll.interval.ms"], "60000");
    }

    /// Pins that every owned property and every alias is refused by name,
    /// bare and with the `topic.` prefix librdkafka strips.
    #[test]
    fn every_owned_property_and_alias_is_refused() {
        // Written out: a loop over `DENYLIST` would follow an entry removed
        // from it.
        for key in [
            "group.id",
            "group.protocol",
            "partition.assignment.strategy",
            "auto.offset.reset",
            "enable.auto.commit",
            "auto.commit.enable",
            "enable.auto.offset.store",
            "enable.partition.eof",
            "acks",
            "request.required.acks",
            "enable.idempotence",
            "transactional.id",
            "message.timeout.ms",
            "delivery.timeout.ms",
            "allow.auto.create.topics",
            "statistics.interval.ms",
            "bootstrap.servers",
            "metadata.broker.list",
            "delivery.report.only.error",
        ] {
            for written in [key.to_owned(), format!("topic.{key}")] {
                let err = with_rdkafka(&written, "x").unwrap_err().to_string();
                assert!(
                    err.contains(&format!("\"{written}\" cannot be overridden")),
                    "{err}"
                );
                assert!(err.contains("feed.kafka"), "{err}");
            }
        }
    }

    /// Pins that an eager assignment strategy is refused too: the feed owns
    /// the strategy whatever its value.
    #[test]
    fn an_eager_assignment_strategy_override_is_refused() {
        for strategy in ["roundrobin", "range", "range,roundrobin"] {
            let err = with_rdkafka("partition.assignment.strategy", strategy)
                .unwrap_err()
                .to_string();
            assert!(err.contains("partition.assignment.strategy"), "{err}");
        }
    }

    /// Pins that the attempt cap is read from the section, and refused in
    /// the `kafka:` body.
    #[test]
    fn the_attempt_cap_is_a_section_key() {
        let config = parse(&format!("attempt_cap: 3\n{MINIMAL}")).unwrap();
        assert_eq!(config.attempt_cap.get(), 3);

        let err = parse(&format!("{MINIMAL}  attempt_cap: 3\n"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("`attempt_cap` is a key of `feed:`"), "{err}");
    }

    /// Pins the refusals: another backend's tag, a missing, empty or blank
    /// topic, an unknown key and a delivery timeout outside librdkafka's
    /// range, with the accepted values at each end of it.
    #[test]
    fn a_section_the_kafka_feed_cannot_read_is_refused() {
        let err = parse("memory: {}").unwrap_err().to_string();
        assert!(err.contains("names `memory`"), "{err}");

        let err = parse(&MINIMAL.replace("  dead_letter_topic: units-dead\n", ""))
            .unwrap_err()
            .to_string();
        assert!(err.contains("dead_letter_topic"), "{err}");

        for empty in ["\"\"", "\"   \""] {
            let err = parse(&MINIMAL.replace("units-retry", empty))
                .unwrap_err()
                .to_string();
            assert!(err.contains("`retry_topic` must not be empty"), "{err}");
        }

        let err = parse(&format!("{MINIMAL}  topics: [a]\n"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("feed.kafka"), "{err}");

        // librdkafka takes whole milliseconds from 1 to `i32::MAX`.
        for timeout in ["0s", "500us", "2147483648ms", "25d"] {
            let err = parse(&format!("{MINIMAL}  delivery_timeout: {timeout}\n"))
                .unwrap_err()
                .to_string();
            assert!(err.contains("delivery_timeout"), "{timeout}: {err}");
        }
        for timeout in ["1ms", "24d", "2147483647ms"] {
            parse(&format!("{MINIMAL}  delivery_timeout: {timeout}\n"))
                .unwrap_or_else(|err| panic!("{timeout}: {err}"));
        }
    }

    /// Pins that a statistics interval under a millisecond is refused, since
    /// librdkafka would read it as off, and that 0s and 1ms are accepted.
    #[test]
    fn a_sub_millisecond_statistics_interval_is_refused() {
        for interval in ["500us", "999us"] {
            let err = parse(&format!("{MINIMAL}  statistics_interval: {interval}\n"))
                .unwrap_err()
                .to_string();
            assert!(err.contains("statistics_interval"), "{interval}: {err}");
        }
        for interval in ["0s", "1ms"] {
            parse(&format!("{MINIMAL}  statistics_interval: {interval}\n"))
                .unwrap_or_else(|err| panic!("{interval}: {err}"));
        }
    }

    /// Pins the properties each client is given: the passthrough, and the
    /// feed's own settings over it.
    #[test]
    fn each_client_gets_the_settings_the_feed_owns() {
        let config = parse(&format!(
            "{MINIMAL}  delivery_timeout: 7s\n  statistics_interval: 2s\n  \
             rdkafka:\n    session.timeout.ms: \"6000\"\n"
        ))
        .unwrap();
        let consumer = config.consumer_config(true);
        for (key, value) in [
            ("bootstrap.servers", "localhost:9092"),
            ("group.id", "workers"),
            ("group.protocol", "classic"),
            ("partition.assignment.strategy", "range,roundrobin"),
            ("auto.offset.reset", "earliest"),
            ("enable.auto.commit", "false"),
            ("enable.auto.offset.store", "false"),
            ("enable.partition.eof", "false"),
            ("allow.auto.create.topics", "false"),
            ("statistics.interval.ms", "2000"),
            ("session.timeout.ms", "6000"),
        ] {
            assert_eq!(consumer.get(key), Some(value), "consumer {key}");
        }
        assert_eq!(consumer.get("acks"), None);

        let producer = config.producer_config(true);
        for (key, value) in [
            ("bootstrap.servers", "localhost:9092"),
            ("acks", "all"),
            ("enable.idempotence", "true"),
            ("message.timeout.ms", "7000"),
            ("allow.auto.create.topics", "false"),
            ("statistics.interval.ms", "2000"),
        ] {
            assert_eq!(producer.get(key), Some(value), "producer {key}");
        }
        assert_eq!(producer.get("group.id"), None);

        let admin = config.admin_config(true);
        for (key, value) in [
            ("bootstrap.servers", "localhost:9092"),
            ("allow.auto.create.topics", "false"),
            ("session.timeout.ms", "6000"),
        ] {
            assert_eq!(admin.get(key), Some(value), "admin {key}");
        }
        assert_eq!(admin.get("statistics.interval.ms"), None);

        let quiet = parse(&format!("{MINIMAL}  statistics_interval: 0s\n")).unwrap();
        assert_eq!(
            quiet.consumer_config(true).get("statistics.interval.ms"),
            None
        );
        assert_eq!(
            quiet.producer_config(true).get("statistics.interval.ms"),
            None
        );
    }

    /// Pins that `Debug` shows every `rdkafka` key and none of the values.
    #[test]
    fn debug_never_prints_rdkafka_values() {
        let config = with_rdkafka("sasl.password", "hunter2");
        // A build without `tls` refuses the key; the redaction is checked on
        // a config built by hand there.
        let config = config.unwrap_or_else(|_| {
            let mut config = parse(MINIMAL).unwrap();
            config
                .rdkafka
                .insert("sasl.password".to_owned(), "hunter2".to_owned());
            config
        });
        let printed = format!("{config:?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        assert!(
            printed.contains("\"sasl.password\": <redacted>"),
            "{printed}"
        );
    }

    /// Pins that a TLS or SASL passthrough is refused only in a build
    /// without the `tls` feature, scoped to `feed.kafka`.
    #[test]
    fn a_security_passthrough_follows_the_build() {
        let got = with_rdkafka("security.protocol", "ssl");
        if cfg!(feature = "tls") {
            got.unwrap();
        } else {
            let err = got.unwrap_err().to_string();
            assert!(err.contains("kafka-tls"), "{err}");
            assert!(err.contains("feed.kafka.rdkafka"), "{err}");
        }
    }
}
