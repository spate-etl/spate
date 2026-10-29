//! The `kafka:` body of a `feed:` section, decoded and checked.

use crate::client::{Base, Keys, Role};
use serde::Deserialize;
use spate_core::config::{ConfigError, FeedSection, redact};
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::time::Duration;

/// The key that names this backend in a `feed:` section.
pub(crate) const BACKEND: &str = "kafka";

/// Attempts at which a unit goes to the dead-letter topic, read or unread,
/// unless the section sets `attempt_cap`.
const ATTEMPT_CAP: NonZeroU32 = NonZeroU32::new(10).expect("non-zero");

/// One of the feed's six clients.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FeedRole {
    /// The group member that reads the work and retry topics.
    Consumer,
    /// The producer for attempt, retry, dead-letter and tombstone writes.
    Producer,
    /// The depth reader: the group's committed offsets, never subscribed.
    Depth,
    /// The startup checks' admin client.
    Admin,
    /// The startup checks' metadata reader.
    Reader,
    /// The attempt follower, which assigns its own partitions.
    Follower,
}

impl FeedRole {
    pub(crate) const ALL: [FeedRole; 6] = [
        FeedRole::Consumer,
        FeedRole::Producer,
        FeedRole::Depth,
        FeedRole::Admin,
        FeedRole::Reader,
        FeedRole::Follower,
    ];
}

/// A feed client, and the properties it owns beyond the base in
/// [`crate::client`]. One passthrough map feeds all six clients, so the
/// passthrough refuses the union of what any of them owns, each with the
/// reason given here.
pub(crate) struct Client<'a> {
    pub(crate) config: &'a KafkaFeedConfig,
    pub(crate) role: FeedRole,
}

impl Role for Client<'_> {
    fn name(&self) -> &'static str {
        match self.role {
            FeedRole::Consumer => "consumer",
            FeedRole::Producer => "producer",
            FeedRole::Depth => "depth",
            FeedRole::Admin => "admin",
            FeedRole::Reader => "reader",
            FeedRole::Follower => "follower",
        }
    }

    fn statistics(&self) -> bool {
        matches!(self.role, FeedRole::Consumer | FeedRole::Producer)
    }

    fn declare(&self, keys: &mut Keys<'_>) {
        let cfg = self.config;
        match self.role {
            FeedRole::Consumer | FeedRole::Depth => {
                group(keys, cfg);
                keys.own(
                    "group.protocol",
                    "classic",
                    "the feed runs the classic consumer group protocol with \
                     eager assignment",
                );
                // Eager: a rebalance revokes every partition before it
                // assigns any, and the tracker relies on that.
                keys.own(
                    "partition.assignment.strategy",
                    "range,roundrobin",
                    "the feed relies on eager assignment, under which a \
                     rebalance revokes every partition before it assigns any",
                );
            }
            // The follower joins the feed's group by `group.id` only:
            // librdkafka refuses `assign` on a consumer with no group. It
            // assigns its partitions itself, so it never takes part in a
            // rebalance, and it commits nothing.
            FeedRole::Follower => group(keys, cfg),
            FeedRole::Producer => {
                keys.own(
                    "acks",
                    "all",
                    "forced to `all`: an offset completes once the delivery \
                     report confirms its tombstone or retry write, and a unit \
                     is read once its attempt write is confirmed, so a report \
                     under weaker acks could lose the unit or its count",
                );
                keys.own(
                    "enable.idempotence",
                    "true",
                    "forced on, so librdkafka's own retries neither reorder \
                     nor duplicate a write",
                );
                keys.own_if(
                    true,
                    "message.timeout.ms",
                    || cfg.delivery_timeout.as_millis().to_string(),
                    "owned by the typed `delivery_timeout` field, which also \
                     bounds how long the feed waits for a delivery report",
                );
            }
            FeedRole::Admin | FeedRole::Reader => {}
        }
        keys.forbid(
            "group.instance.id",
            "a static member sends no LeaveGroup when it closes, so its \
             partitions wait out `session.timeout.ms` before another member \
             reads them",
        );
        keys.forbid(
            "auto.commit.enable",
            "librdkafka's legacy topic-level auto-commit property; the feed \
             commits each partition up to its first unfinished unit itself",
        );
        keys.forbid(
            "transactional.id",
            "the feed does not use transactions; a write is confirmed by its \
             delivery report",
        );
        keys.forbid(
            "delivery.report.only.error",
            "a unit is read only once its attempt write is confirmed, and its \
             offset finishes only once its tombstone or retry write is, so \
             suppressing success reports would stop the partition's reads and \
             commits",
        );
        keys.forbid(
            "enable.gapless.guarantee",
            "raises a fatal producer error on any gap, which stops the feed",
        );
    }
}

/// The group settings of a feed consumer that never commits through
/// librdkafka.
fn group(keys: &mut Keys<'_>, cfg: &KafkaFeedConfig) {
    keys.own(
        "group.id",
        &cfg.group_id,
        "owned by the typed `group_id` field",
    );
    keys.own(
        "auto.offset.reset",
        "earliest",
        "a partition with no committed offset starts at its earliest offset, \
         so a group whose offsets expired reads the topic from its start",
    );
    keys.own(
        "enable.auto.commit",
        "false",
        "the feed commits each partition up to its first unfinished unit \
         itself",
    );
    keys.own(
        "enable.auto.offset.store",
        "false",
        "the feed decides which offsets may be committed",
    );
    keys.own(
        "enable.partition.eof",
        "false",
        "the feed reads only messages from the consumer",
    );
}

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
    attempt_topic: String,
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
    /// The compacted topic each unit's attempt is recorded on before it is
    /// read.
    pub(crate) attempt_topic: String,
    /// The bound on every write's wait for its delivery report, the attempt
    /// and tombstone writes beside the retry and dead-letter ones, and the
    /// producer's `message.timeout.ms`. It also bounds one pass of the gate,
    /// which can run up to 2 s past it.
    pub(crate) delivery_timeout: Duration,
    /// librdkafka's statistics interval. Zero disables statistics.
    pub(crate) statistics_interval: Duration,
    /// Raw librdkafka properties for every feed client.
    pub(crate) rdkafka: BTreeMap<String, String>,
    /// The attempt at which a unit moves to the dead-letter topic: a unit
    /// failed on this attempt or later, or a unit whose recorded attempts
    /// have reached it, unread. From the section's `attempt_cap`.
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
            attempt_topic,
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
            .field("attempt_topic", attempt_topic)
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
            attempt_topic: body.attempt_topic,
            delivery_timeout: body.delivery_timeout,
            statistics_interval: body.statistics_interval,
            rdkafka: body.rdkafka,
            attempt_cap: section.attempt_cap.unwrap_or(ATTEMPT_CAP),
        };
        config.validate()?;
        Ok(config)
    }

    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        for (key, value) in [
            ("brokers", &self.brokers),
            ("group_id", &self.group_id),
            ("topic", &self.topic),
            ("retry_topic", &self.retry_topic),
            ("dead_letter_topic", &self.dead_letter_topic),
            ("attempt_topic", &self.attempt_topic),
        ] {
            if value.trim().is_empty() {
                return Err(error(format!("`{key}` must not be empty")));
            }
        }
        // A message read from the work topic counts under its own
        // coordinates, so a retry copy written there would start a new count.
        if self.retry_topic == self.topic {
            return Err(error("`retry_topic` must differ from `topic`".to_owned()));
        }
        // The group reads the work and retry topics, so a dead-letter write
        // to either is read and written again without end.
        for (key, value) in [("topic", &self.topic), ("retry_topic", &self.retry_topic)] {
            if self.dead_letter_topic == *value {
                return Err(error(format!(
                    "`dead_letter_topic` must differ from `{key}`"
                )));
            }
        }
        // The attempt topic is compacted and the others must not be. Startup
        // skips that check for a topic that reports no `cleanup.policy`, so it
        // cannot be relied on to refuse a shared name.
        for (key, value) in [
            ("topic", &self.topic),
            ("retry_topic", &self.retry_topic),
            ("dead_letter_topic", &self.dead_letter_topic),
        ] {
            if self.attempt_topic == *value {
                return Err(error(format!("`attempt_topic` must differ from `{key}`")));
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
        let clients = FeedRole::ALL.map(|role| Client { config: self, role });
        let roles: Vec<&dyn Role> = clients.iter().map(|c| c as &dyn Role).collect();
        if let Err(refusal) = self.base(false).denied(&roles) {
            return Err(error(refusal.to_string()));
        }
        crate::security::check_tls_feature(&self.rdkafka, "feed.kafka")
    }

    /// What the feed's clients share with every Kafka client.
    ///
    /// Takes the OpenSSL env check as an argument: on Linux, cargo exports
    /// `SSL_CERT_FILE` and `SSL_CERT_DIR` to every process it runs, so a test
    /// reading them always sees them set.
    pub(crate) fn base(&self, openssl_env: bool) -> Base<'_> {
        Base {
            section: "feed.kafka",
            brokers: &self.brokers,
            rdkafka: &self.rdkafka,
            statistics_interval: self.statistics_interval,
            openssl_env,
        }
    }

    /// `role`'s client over this section.
    pub(crate) fn client(&self, role: FeedRole) -> Client<'_> {
        Client { config: self, role }
    }

    /// `role`'s librdkafka configuration.
    #[cfg(test)]
    pub(crate) fn config(&self, role: FeedRole, openssl_env: bool) -> rdkafka::ClientConfig {
        self.base(openssl_env).config(&self.client(role))
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
                           dead_letter_topic: units-dead\n  \
                           attempt_topic: units-attempts\n";

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
        assert_eq!(config.attempt_topic, "units-attempts");
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
        // Written out: a loop over the roles' declarations would follow an entry removed
        // from it.
        for key in [
            "group.id",
            "group.instance.id",
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
            "enable.gapless.guarantee",
        ] {
            for stats in ["0s", "5s"] {
                for written in [key.to_owned(), format!("topic.{key}")] {
                    let err = parse(&format!(
                        "{MINIMAL}  statistics_interval: {stats}\n  rdkafka:\n    \"{written}\": \"x\"\n"
                    ))
                    .unwrap_err()
                    .to_string();
                    assert!(
                        err.contains(&format!("rdkafka.\"{written}\" cannot be overridden")),
                        "{err}"
                    );
                    assert!(err.contains("feed.kafka"), "{err}");
                }
            }
        }
    }

    /// A refused key's value never reaches the error.
    #[test]
    fn rejection_never_prints_the_value() {
        let err = with_rdkafka("acks", "secret-marker")
            .unwrap_err()
            .to_string();
        assert!(err.contains("\"acks\""), "{err}");
        assert!(!err.contains("secret-marker"), "{err}");
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

        for (line, key) in [
            ("  dead_letter_topic: units-dead\n", "dead_letter_topic"),
            ("  attempt_topic: units-attempts\n", "attempt_topic"),
        ] {
            let err = parse(&MINIMAL.replace(line, "")).unwrap_err().to_string();
            assert!(err.contains(key), "{err}");
        }

        for empty in ["\"\"", "\"   \""] {
            for (value, key) in [
                ("units-retry", "retry_topic"),
                ("units-attempts", "attempt_topic"),
            ] {
                let err = parse(&MINIMAL.replace(value, empty))
                    .unwrap_err()
                    .to_string();
                assert!(err.contains(&format!("`{key}` must not be empty")), "{err}");
            }
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

    /// Pins that a retry topic named like the work topic is refused: a retry
    /// copy read from the work topic would count under its own coordinates.
    #[test]
    fn a_retry_topic_that_is_the_work_topic_is_refused() {
        let err = parse(&MINIMAL.replace("units-retry", "units"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("`retry_topic` must differ from `topic`"),
            "{err}"
        );
    }

    /// Pins that a dead-letter topic equal to the work topic or the retry
    /// topic is refused, naming both keys.
    #[test]
    fn a_dead_letter_topic_that_is_the_work_topic_is_refused() {
        let err =
            parse(&MINIMAL.replace("dead_letter_topic: units-dead", "dead_letter_topic: units"))
                .unwrap_err()
                .to_string();
        assert!(
            err.contains("`dead_letter_topic` must differ from `topic`"),
            "{err}"
        );
    }

    /// Pins that a dead-letter topic equal to the retry topic is refused,
    /// naming both keys.
    #[test]
    fn a_dead_letter_topic_that_is_the_retry_topic_is_refused() {
        let err = parse(&MINIMAL.replace(
            "dead_letter_topic: units-dead",
            "dead_letter_topic: units-retry",
        ))
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("`dead_letter_topic` must differ from `retry_topic`"),
            "{err}"
        );
    }

    /// Pins that an attempt topic equal to the work, retry or dead-letter
    /// topic is refused, naming both keys.
    #[test]
    fn an_attempt_topic_named_like_another_topic_is_refused() {
        for (key, other) in [
            ("topic", "units"),
            ("retry_topic", "units-retry"),
            ("dead_letter_topic", "units-dead"),
        ] {
            let yaml = MINIMAL.replace(
                "attempt_topic: units-attempts",
                &format!("attempt_topic: {other}"),
            );
            let err = parse(&yaml)
                .expect_err(&format!("attempt_topic {other} accepted"))
                .to_string();
            assert!(
                err.contains(&format!("`attempt_topic` must differ from `{key}`")),
                "{err}"
            );
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
        let consumer = config.config(FeedRole::Consumer, true);
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

        let producer = config.config(FeedRole::Producer, true);
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

        let admin = config.config(FeedRole::Admin, true);
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
            quiet
                .config(FeedRole::Consumer, true)
                .get("statistics.interval.ms"),
            None
        );
        assert_eq!(
            quiet
                .config(FeedRole::Producer, true)
                .get("statistics.interval.ms"),
            None
        );
    }

    /// Pins the follower's settings: the feed's group, the four keys that
    /// keep it a reader that commits nothing, over a passthrough that sets
    /// them otherwise, and no statistics.
    #[test]
    fn the_follower_config_joins_the_feeds_group_and_only_reads() {
        let mut config = parse(&format!(
            "{MINIMAL}  statistics_interval: 5s\n  \
             rdkafka:\n    session.timeout.ms: \"6000\"\n"
        ))
        .unwrap();
        // Built by hand: validation refuses these keys in the passthrough.
        for (key, value) in [
            ("group.id", "other"),
            ("auto.offset.reset", "latest"),
            ("enable.auto.commit", "true"),
            ("enable.auto.offset.store", "true"),
            ("enable.partition.eof", "true"),
        ] {
            config.rdkafka.insert(key.to_owned(), value.to_owned());
        }
        let follower = config.config(FeedRole::Follower, true);
        for (key, value) in [
            ("bootstrap.servers", "localhost:9092"),
            ("group.id", "workers"),
            ("auto.offset.reset", "earliest"),
            ("enable.auto.commit", "false"),
            ("enable.auto.offset.store", "false"),
            ("enable.partition.eof", "false"),
            ("allow.auto.create.topics", "false"),
            ("session.timeout.ms", "6000"),
        ] {
            assert_eq!(follower.get(key), Some(value), "follower {key}");
        }
        assert_eq!(follower.get("statistics.interval.ms"), None);
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
