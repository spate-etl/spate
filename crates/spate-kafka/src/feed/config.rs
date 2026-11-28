//! The Kafka feed's config, decoded from a `feed:` section or built in code,
//! and the holder that carries its CA bundle to every client.

use crate::client::{Base, COMPONENT_TYPE, Keys, Role, check_message_timeout};
use serde::Deserialize;
use spate_core::config::{ConfigError, FeedSection, SectionPath, redact};
use spate_core::error::ConnectError;
use spate_core::tls::TlsSection;
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::time::Duration;

/// The backend's name in the text a person reads.
pub(crate) const NAME: &str = "Kafka";

/// The component a startup error names, as in "the Kafka feed cannot start".
pub(crate) const COMPONENT: &str = "Kafka feed";

/// The role of a feed client, each of which owns its own properties.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FeedRole {
    /// The group member that reads the work and retry topics.
    Consumer,
    /// The producer for attempt, retry, dead-letter and tombstone writes.
    Producer,
    /// The depth reader: the group's committed offsets and each topic's high
    /// watermark, never subscribed.
    Depth,
    /// The startup checks' admin client.
    Admin,
    /// The startup checks' metadata reader.
    Reader,
    /// The attempt follower, which assigns its own partitions.
    Follower,
    /// The gate's reader of the attempt topic's ends, never assigned.
    Gate,
}

impl FeedRole {
    pub(crate) const ALL: [FeedRole; 7] = [
        FeedRole::Consumer,
        FeedRole::Producer,
        FeedRole::Depth,
        FeedRole::Admin,
        FeedRole::Reader,
        FeedRole::Follower,
        FeedRole::Gate,
    ];
}

/// A feed client, and the properties it owns beyond the base in
/// [`crate::client`]. One passthrough map feeds every client, so the
/// passthrough refuses the union of what any of them owns, each with the
/// reason given here.
pub(crate) struct Client<'a> {
    pub(crate) config: &'a KafkaFeedConfig,
    pub(crate) role: FeedRole,
}

/// Why the feed owns `isolation.level`, one text for every role that sets it,
/// since a refusal names the first owner it finds.
const ISOLATION_LEVEL: &str = "the feed sets it: the depth reader lists ends at the high \
     watermark so a confirmed write behind an open transaction counts, the attempt \
     follower and the gate read past another writer's open transaction so a unit's \
     count keeps up with the feed's own attempt writes, and the group member reads \
     committed records so a commit cannot pass that transaction";

impl Role for Client<'_> {
    fn name(&self) -> &'static str {
        match self.role {
            FeedRole::Consumer => "consumer",
            FeedRole::Producer => "producer",
            FeedRole::Depth => "depth",
            FeedRole::Admin => "admin",
            FeedRole::Reader => "reader",
            FeedRole::Follower => "follower",
            FeedRole::Gate => "gate",
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
                keys.own_if(
                    self.role == FeedRole::Depth,
                    "isolation.level",
                    || "read_uncommitted".to_owned(),
                    ISOLATION_LEVEL,
                );
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
            // rebalance, and it commits nothing. The gate's reader takes the
            // same settings, so the two read at one isolation level.
            FeedRole::Follower | FeedRole::Gate => {
                group(keys, cfg);
                keys.own("isolation.level", "read_uncommitted", ISOLATION_LEVEL);
            }
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
        "a partition with no committed offset, or a committed offset outside its \
         log, starts at its earliest offset, so a group whose offsets expired, or \
         whose partition was truncated below its commit, reads it from its start",
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
    #[serde(default)]
    tls: TlsSection,
    /// Read only to refuse it with a pointer to the section key.
    #[serde(default)]
    attempt_cap: Option<serde::de::IgnoredAny>,
}

/// The keys under `feed: { kafka: … }`, with the section's `attempt_cap`:
/// the configuration [`KafkaFeed::connect`](crate::feed::KafkaFeed::connect)
/// and [`KafkaFeedPublisher::connect`](crate::feed::KafkaFeedPublisher::connect)
/// build from.
///
/// Build one in code with [`new`](Self::new), or from a pipeline's `feed:`
/// section with [`from_section`](Self::from_section).
///
/// ```
/// use spate_kafka::KafkaFeedConfig;
/// use std::time::Duration;
///
/// let mut config = KafkaFeedConfig::new(
///     "localhost:9092",
///     "workers",
///     "units",
///     "units-retry",
///     "units-dead",
///     "units-attempts",
/// );
/// config.delivery_timeout = Duration::from_secs(60);
/// config.validate()?;
/// # Ok::<(), spate_core::config::ConfigError>(())
/// ```
#[derive(Clone, PartialEq)]
#[non_exhaustive]
pub struct KafkaFeedConfig {
    /// Comma-separated bootstrap servers.
    pub brokers: String,
    /// The consumer group every worker of the feed joins.
    pub group_id: String,
    /// The work topic: where a publish writes a unit.
    pub topic: String,
    /// Where a unit failed below the attempt cap is written again. Differs
    /// from `topic`.
    pub retry_topic: String,
    /// Where a unit goes at the attempt cap, or a message that names no unit.
    /// Differs from `topic` and `retry_topic`.
    pub dead_letter_topic: String,
    /// The compacted topic each unit's attempt is recorded on before it is
    /// read. Differs from the other three topics.
    pub attempt_topic: String,
    /// The bound on every write's wait for its delivery report, the attempt
    /// and tombstone writes beside the retry and dead-letter ones, and the
    /// producer's `message.timeout.ms`. It also bounds one pass of the gate,
    /// which can run up to 2 s past it. Defaults to 30 s.
    pub delivery_timeout: Duration,
    /// librdkafka's statistics interval. Zero disables statistics. Defaults
    /// to 5 s.
    pub statistics_interval: Duration,
    /// Raw librdkafka properties for every feed client.
    pub rdkafka: BTreeMap<String, String>,
    /// The roots every feed client verifies brokers against: the system
    /// roots, and the certificates in `root_ca` beside them.
    pub tls: TlsSection,
    /// The attempt at which a unit moves to the dead-letter topic: a unit
    /// failed on this attempt or later, or a unit whose recorded attempts
    /// have reached it, unread. [`from_section`](Self::from_section) reads it
    /// from `feed.attempt_cap`, beside `kafka:`. Defaults to
    /// [`DEFAULT_ATTEMPT_CAP`](spate_core::feed::DEFAULT_ATTEMPT_CAP).
    pub attempt_cap: NonZeroU32,
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
            tls,
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
            .field("tls", tls)
            .field("attempt_cap", attempt_cap)
            .finish()
    }
}

impl KafkaFeedConfig {
    /// A config for the feed over `brokers` whose workers join `group_id`,
    /// with its four topics in the order the YAML lists them: the work topic,
    /// then the retry, dead-letter and attempt topics. Every other field
    /// starts at its YAML default.
    #[must_use]
    pub fn new(
        brokers: impl Into<String>,
        group_id: impl Into<String>,
        topic: impl Into<String>,
        retry_topic: impl Into<String>,
        dead_letter_topic: impl Into<String>,
        attempt_topic: impl Into<String>,
    ) -> KafkaFeedConfig {
        KafkaFeedConfig {
            brokers: brokers.into(),
            group_id: group_id.into(),
            topic: topic.into(),
            retry_topic: retry_topic.into(),
            dead_letter_topic: dead_letter_topic.into(),
            attempt_topic: attempt_topic.into(),
            delivery_timeout: default_delivery_timeout(),
            statistics_interval: default_statistics_interval(),
            rdkafka: BTreeMap::new(),
            tls: TlsSection::default(),
            attempt_cap: spate_core::feed::DEFAULT_ATTEMPT_CAP,
        }
    }

    /// Decode and check the `kafka:` body of `section`, with the section's
    /// `attempt_cap`, else
    /// [`DEFAULT_ATTEMPT_CAP`](spate_core::feed::DEFAULT_ATTEMPT_CAP).
    ///
    /// # Errors
    ///
    /// [`ConfigError::Component`] at `feed` for a section that names another
    /// backend. [`ConfigError::Component`], starting at `feed.kafka`, for a
    /// body that does not decode, such as one missing a required key, holding
    /// an unknown key or a value of the wrong type, for an `attempt_cap` under
    /// `kafka:`, and for a body [`validate`](Self::validate) refuses.
    pub fn from_section(section: &FeedSection) -> Result<KafkaFeedConfig, ConfigError> {
        let body: Body = section.backend_body(COMPONENT_TYPE, NAME)?;
        if body.attempt_cap.is_some() {
            return Err(SectionPath::feed(COMPONENT_TYPE)
                .invalid("attempt_cap", "is a key of `feed:` itself, beside `kafka:`"));
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
            tls: body.tls,
            attempt_cap: section.attempt_cap_or_default(),
        };
        config.validate()?;
        Ok(config)
    }

    /// Check the config's values against each other and against what
    /// librdkafka accepts.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Component`], starting at `feed.kafka`, for an empty
    /// broker list, group or topic name, a `retry_topic` equal to `topic`, a
    /// `dead_letter_topic` equal to either, an `attempt_topic` equal to any of
    /// the three, a `delivery_timeout` librdkafka refuses as
    /// `message.timeout.ms`, a `statistics_interval` it refuses, an `rdkafka`
    /// property the feed owns or forbids, a security setting that a build
    /// without the `tls` feature cannot honor, and a `tls.root_ca` without
    /// `rdkafka."security.protocol"` set to `ssl` or `sasl_ssl`.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let at = SectionPath::feed(COMPONENT_TYPE);
        for (key, value) in [
            ("brokers", &self.brokers),
            ("group_id", &self.group_id),
            ("topic", &self.topic),
            ("retry_topic", &self.retry_topic),
            ("dead_letter_topic", &self.dead_letter_topic),
            ("attempt_topic", &self.attempt_topic),
        ] {
            if value.trim().is_empty() {
                return Err(at.invalid(key, "must not be empty"));
            }
        }
        // A message read from the work topic counts under its own
        // coordinates, so a retry copy written there would start a new count.
        if self.retry_topic == self.topic {
            return Err(at.invalid("retry_topic", "must differ from `topic`"));
        }
        // The group reads the work and retry topics, so a dead-letter write
        // to either is read and written again without end.
        for (key, value) in [("topic", &self.topic), ("retry_topic", &self.retry_topic)] {
            if self.dead_letter_topic == *value {
                return Err(at.invalid("dead_letter_topic", format!("must differ from `{key}`")));
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
                return Err(at.invalid("attempt_topic", format!("must differ from `{key}`")));
            }
        }
        if let Err(why) = check_message_timeout(self.delivery_timeout) {
            return Err(at.invalid("delivery_timeout", why));
        }
        if let Err(why) = self.base(false).check_statistics_interval() {
            return Err(at.invalid("statistics_interval", why));
        }
        let clients = FeedRole::ALL.map(|role| Client { config: self, role });
        let roles: Vec<&dyn Role> = clients.iter().map(|c| c as &dyn Role).collect();
        if let Err(refusal) = self.base(false).denied(&roles) {
            return Err(at.error(refusal.to_string()));
        }
        crate::security::check_tls_feature(&self.rdkafka, &self.tls, &at)
    }

    /// What the feed's clients share with every Kafka client, without the
    /// bundle `tls.root_ca` builds: for the checks [`validate`](Self::validate)
    /// runs. A client takes [`Trusted::base`].
    ///
    /// Takes the OpenSSL env check as an argument: on Linux, cargo exports
    /// `SSL_CERT_FILE` and `SSL_CERT_DIR` to every process it runs, so a test
    /// reading them always sees them set.
    fn base(&self, openssl_env: bool) -> Base<'_> {
        Base {
            section: "feed.kafka",
            brokers: &self.brokers,
            rdkafka: &self.rdkafka,
            statistics_interval: self.statistics_interval,
            openssl_env,
            root_ca: self.tls.root_ca.is_some(),
            ca_pem: None,
        }
    }

    /// `role`'s client over this config.
    pub(crate) fn client(&self, role: FeedRole) -> Client<'_> {
        Client { config: self, role }
    }
}

/// A [`KafkaFeedConfig`] with the bundle its `tls.root_ca` builds, read once
/// for every client the feed or the publisher creates. Its
/// [`base`](Self::base) is the one way to a feed client's [`Base`], so no
/// client is created without the bundle.
pub(crate) struct Trusted {
    config: KafkaFeedConfig,
    ca_pem: Option<String>,
}

impl Trusted {
    /// `config` with the bundle its `tls.root_ca` builds.
    ///
    /// # Errors
    ///
    /// [`ConnectErrorKind::Config`](spate_core::error::ConnectErrorKind::Config)
    /// at `feed.kafka.tls.root_ca` when `root_ca` cannot be read or holds no
    /// certificate.
    pub(crate) fn new(config: KafkaFeedConfig) -> Result<Trusted, ConnectError> {
        Self::with_bundle(config, crate::security::ca_bundle)
    }

    /// `config` with the bundle `bundle` builds from its `tls`.
    fn with_bundle(
        config: KafkaFeedConfig,
        bundle: impl FnOnce(&TlsSection) -> Result<Option<String>, String>,
    ) -> Result<Trusted, ConnectError> {
        let ca_pem = bundle(&config.tls)
            .map_err(|why| SectionPath::feed(COMPONENT_TYPE).invalid("tls.root_ca", why))?;
        Ok(Trusted { config, ca_pem })
    }

    /// What the feed's clients share with every Kafka client, the bundle
    /// included.
    pub(crate) fn base(&self, openssl_env: bool) -> Base<'_> {
        self.config
            .base(openssl_env)
            .trusting(self.ca_pem.as_deref())
    }

    /// The config decoded from `section`, with no bundle, for a test that
    /// creates feed clients.
    #[cfg(test)]
    pub(crate) fn from_section(section: &FeedSection) -> Result<Trusted, ConfigError> {
        Ok(Self::without_bundle(KafkaFeedConfig::from_section(
            section,
        )?))
    }

    /// `config` with no bundle, for a test that creates feed clients.
    #[cfg(test)]
    pub(crate) fn without_bundle(config: KafkaFeedConfig) -> Trusted {
        Trusted {
            config,
            ca_pem: None,
        }
    }

    /// `role`'s librdkafka configuration.
    #[cfg(test)]
    pub(crate) fn config(&self, role: FeedRole, openssl_env: bool) -> rdkafka::ClientConfig {
        self.base(openssl_env).config(&self.client(role))
    }
}

impl std::ops::Deref for Trusted {
    type Target = KafkaFeedConfig;

    fn deref(&self) -> &KafkaFeedConfig {
        &self.config
    }
}

// A test changes a setting after decoding; production code never changes a
// config whose bundle it has read.
#[cfg(test)]
impl std::ops::DerefMut for Trusted {
    fn deref_mut(&mut self) -> &mut KafkaFeedConfig {
        &mut self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spate_core::error::ConnectErrorKind;

    const MINIMAL: &str = "kafka:\n  brokers: localhost:9092\n  group_id: workers\n  \
                           topic: units\n  retry_topic: units-retry\n  \
                           dead_letter_topic: units-dead\n  \
                           attempt_topic: units-attempts\n";

    /// The bundle a feed resolves reaches every role's client as
    /// `ssl.ca.pem`, and a bundle that cannot be built is a config error at
    /// `feed.kafka.tls.root_ca`.
    #[test]
    fn every_feed_role_takes_the_resolved_bundle() {
        const BUNDLE: &str = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";
        let mut config = KafkaFeedConfig::from_section(&section(MINIMAL)).unwrap();
        config.tls.root_ca = Some("/etc/spate/private-ca.pem".into());
        let Err(refused) = Trusted::with_bundle(config.clone(), |_| Err("unreadable".to_owned()))
        else {
            panic!("an unreadable bundle is refused");
        };
        assert_eq!(refused.kind(), ConnectErrorKind::Config, "{refused}");
        assert_eq!(refused.to_string(), "feed.kafka.tls.root_ca: unreadable");
        let Ok(config) = Trusted::with_bundle(config, |tls| {
            assert_eq!(tls.root_ca, Some("/etc/spate/private-ca.pem".into()));
            Ok(Some(BUNDLE.to_owned()))
        }) else {
            panic!("a readable bundle is accepted");
        };
        for role in FeedRole::ALL {
            assert_eq!(
                config.config(role, false).get("ssl.ca.pem"),
                Some(BUNDLE),
                "{role:?}"
            );
        }
    }

    fn section(yaml: &str) -> FeedSection {
        serde_yaml::from_str(yaml).unwrap()
    }

    fn parse(yaml: &str) -> Result<KafkaFeedConfig, ConfigError> {
        KafkaFeedConfig::from_section(&section(yaml))
    }

    /// The config `yaml` decodes to, with no bundle, to read its clients'
    /// settings.
    fn trusted(yaml: &str) -> Trusted {
        Trusted::without_bundle(parse(yaml).unwrap())
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

    /// Pins that only the depth client, the attempt follower and the gate's
    /// reader read at `read_uncommitted`: every other client keeps
    /// librdkafka's default, so the group member cannot read past an open
    /// transaction. Regression for #1763 and #1765.
    #[test]
    fn only_the_depth_client_and_the_attempt_readers_read_uncommitted() {
        let config = trusted(MINIMAL);
        for role in FeedRole::ALL {
            let client = config.config(role, true);
            let expected = matches!(role, FeedRole::Depth | FeedRole::Follower | FeedRole::Gate)
                .then_some("read_uncommitted");
            assert_eq!(client.get("isolation.level"), expected, "{role:?}");
        }
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
            "isolation.level",
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
                        err.starts_with(&format!(
                            "feed.kafka: rdkafka.\"{written}\" cannot be overridden"
                        )),
                        "{err}"
                    );
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

    /// Pins that a config built in code starts at the defaults the YAML
    /// path gives a section holding only the required keys, the attempt cap
    /// included, and differs from a capped section only in its cap.
    #[test]
    fn new_matches_a_section_holding_only_the_required_keys() {
        let mut built = KafkaFeedConfig::new(
            "localhost:9092",
            "workers",
            "units",
            "units-retry",
            "units-dead",
            "units-attempts",
        );
        assert_eq!(built, parse(MINIMAL).unwrap());
        built.attempt_cap = NonZeroU32::new(3).unwrap();
        assert_eq!(built, parse(&format!("attempt_cap: 3\n{MINIMAL}")).unwrap());
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
        assert_eq!(
            err,
            "feed.kafka.attempt_cap: is a key of `feed:` itself, beside `kafka:`"
        );
    }

    /// Pins the refusals: another backend's tag, reported at the section, a
    /// missing, empty or blank topic, an unknown key and a delivery timeout
    /// outside librdkafka's range, with the accepted values at each end of it.
    #[test]
    fn a_section_the_kafka_feed_cannot_read_is_refused() {
        let err = parse("memory: {}").unwrap_err().to_string();
        assert!(
            err.starts_with("feed: ") && err.contains("names `memory`"),
            "{err}"
        );

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
                assert_eq!(err, format!("feed.kafka.{key}: must not be empty"));
            }
        }

        let err = parse(&format!("{MINIMAL}  topics: [a]\n"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("feed.kafka"), "{err}");

        // librdkafka takes whole milliseconds from 1 to `i32::MAX`.
        for timeout in ["0s", "500us", "2147483648ms", "2147483647ms 1ns", "25d"] {
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
        assert_eq!(err, "feed.kafka.retry_topic: must differ from `topic`");
    }

    /// Pins that a dead-letter topic equal to the work topic or the retry
    /// topic is refused, naming both keys.
    #[test]
    fn a_dead_letter_topic_that_is_the_work_topic_is_refused() {
        let err =
            parse(&MINIMAL.replace("dead_letter_topic: units-dead", "dead_letter_topic: units"))
                .unwrap_err()
                .to_string();
        assert_eq!(
            err,
            "feed.kafka.dead_letter_topic: must differ from `topic`"
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
        assert_eq!(
            err,
            "feed.kafka.dead_letter_topic: must differ from `retry_topic`"
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
            assert_eq!(
                err,
                format!("feed.kafka.attempt_topic: must differ from `{key}`")
            );
        }
    }

    /// Pins that a non-zero statistics interval under 1ms or above 24h is
    /// refused, since librdkafka would read it as off or reject it, ahead of
    /// a denied `statistics.interval.ms` passthrough, and that 0s, 1ms and
    /// 24h are accepted.
    #[test]
    fn a_statistics_interval_outside_1ms_to_24h_is_refused() {
        for interval in ["500us", "999us", "24h 1ms"] {
            let err = parse(&format!("{MINIMAL}  statistics_interval: {interval}\n"))
                .unwrap_err()
                .to_string();
            assert!(
                err.starts_with("feed.kafka.statistics_interval: must be"),
                "{interval}: {err}"
            );
        }
        let err = parse(&format!(
            "{MINIMAL}  statistics_interval: 500us\n  rdkafka:\n    \"statistics.interval.ms\": \"x\"\n"
        ))
        .unwrap_err()
        .to_string();
        assert!(
            err.starts_with("feed.kafka.statistics_interval: must be"),
            "{err}"
        );
        for interval in ["0s", "1ms", "24h"] {
            parse(&format!("{MINIMAL}  statistics_interval: {interval}\n"))
                .unwrap_or_else(|err| panic!("{interval}: {err}"));
        }
    }

    /// Pins the properties each client is given: the passthrough, and the
    /// feed's own settings over it.
    #[test]
    fn each_client_gets_the_settings_the_feed_owns() {
        let config = trusted(&format!(
            "{MINIMAL}  delivery_timeout: 7s\n  statistics_interval: 2s\n  \
             rdkafka:\n    session.timeout.ms: \"6000\"\n"
        ));
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

        let quiet = trusted(&format!("{MINIMAL}  statistics_interval: 0s\n"));
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
        let mut config = trusted(&format!(
            "{MINIMAL}  statistics_interval: 5s\n  \
             rdkafka:\n    session.timeout.ms: \"6000\"\n"
        ));
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

    /// Pins which of the feed's clients emit statistics: the consumer and
    /// the producer, whose snapshots the feed reads, and no other.
    #[test]
    fn only_the_consumer_and_producer_emit_statistics() {
        let config = trusted(&format!("{MINIMAL}  statistics_interval: 2s\n"));
        for role in FeedRole::ALL {
            let expected =
                matches!(role, FeedRole::Consumer | FeedRole::Producer).then_some("2000");
            assert_eq!(
                config.config(role, true).get("statistics.interval.ms"),
                expected,
                "{role:?}"
            );
        }
    }
}
