//! [`KafkaFeedPublisher`]: a handle that writes units to the Kafka feed's
//! work topic and reads none, for a process that only publishes, such as a
//! lister run.
//!
//! It shares the worker feed's section parse, startup checks, producer
//! settings and publish path, and holds no consumer, gate or follower.

use super::config::{BACKEND, KafkaFeedConfig};
use super::write::Producer;
use super::{Core, KafkaFeedError, check_topics, gone, producer_parts, publish_unit, startup};
use spate_core::config::FeedSection;
use spate_core::feed::{
    Confirmation, Delivery, Feed, FeedError, FeedErrorKind, FeedWaker, Receipt, Unit,
};
use std::sync::Arc;
use std::time::Duration;

/// A publish-only handle on the Kafka feed, built from the workers' own
/// `feed: { kafka: … }` section.
///
/// It writes each unit to the work topic through one producer with the
/// settings the feed owns, `acks=all` and idempotence included, and a
/// publish's [`Confirmation`] resolves on the write's delivery report. It
/// joins no group, starts none of the feed's consumer, gate and follower
/// threads, and writes no attempt record, so it takes no partition from the workers and charges no unit an
/// attempt. No method waits on I/O.
///
/// It reads no unit: [`receive`](Feed::receive) fails as Fatal, and
/// [`renew`](Feed::renew), [`acknowledge`](Feed::acknowledge) and
/// [`fail`](Feed::fail) name no delivery. Passed to `Pipeline::feed` as a
/// worker's feed, it fails the worker at its first read.
///
/// A credential a broker rejects, or a TLS handshake either side rejects,
/// after connecting, or a fatal producer error such as a refused idempotent
/// producer id, stops the handle with the reason, and every later publish
/// fails with it. Dropping the handle reports each publish it has not
/// confirmed to its caller as retryable; a unit published again after a
/// retryable rejection can reach the work topic twice.
pub struct KafkaFeedPublisher {
    core: Arc<Core>,
    producer: Producer,
}

impl std::fmt::Debug for KafkaFeedPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaFeedPublisher")
            .field("topic", &self.core.topics().work)
            .finish_non_exhaustive()
    }
}

impl KafkaFeedPublisher {
    /// Build the publisher a `feed: { kafka: … }` section names and check its
    /// topics.
    ///
    /// It reads the section a worker reads and refuses any section a worker
    /// refuses. Its writes use `brokers`, `topic`, `delivery_timeout`,
    /// `statistics_interval` and the `rdkafka` passthrough; the other three
    /// topic names serve its checks. Connecting runs the checks
    /// [`KafkaFeed::connect`](super::KafkaFeed::connect) runs, on the runtime
    /// `io`, in at most 30 seconds, then builds the producer.
    ///
    /// # Errors
    ///
    /// As [`KafkaFeed::connect`](super::KafkaFeed::connect), less the
    /// failure to start the feed's own threads: [`KafkaFeedError::Config`] for a section the feed
    /// cannot read, [`KafkaFeedError::Refused`] for a client configuration
    /// librdkafka refuses, a rejected credential or TLS handshake, a refused
    /// topic read and topics the feed cannot run on, and
    /// [`KafkaFeedError::Unavailable`] when a read of the topics fails
    /// otherwise or the checks take longer than 30 seconds.
    ///
    /// # Panics
    ///
    /// When called from inside an async context, as
    /// [`Handle::block_on`](tokio::runtime::Handle::block_on) does.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use spate_core::config::FeedSection;
    /// use spate_core::feed::{Feed as _, Unit};
    /// use spate_kafka::feed::KafkaFeedPublisher;
    ///
    /// # fn run(section: &FeedSection, io: &tokio::runtime::Handle) -> Result<(), Box<dyn std::error::Error>> {
    /// let publisher = KafkaFeedPublisher::connect(section, io)?;
    /// let confirmation = publisher.publish(Unit::new("unit-1", b"body".to_vec()));
    /// io.block_on(confirmation)?;
    /// # Ok(()) }
    /// ```
    pub fn connect(
        section: &FeedSection,
        io: &tokio::runtime::Handle,
    ) -> Result<KafkaFeedPublisher, KafkaFeedError> {
        Self::connect_with(section, io, startup::Scope::All, startup::CONNECT_BOUND)
    }

    /// [`connect`](Self::connect) with only the checks that the topics
    /// exist, for rdkafka's MockCluster, which answers no request for a
    /// topic's configuration.
    ///
    /// # Errors
    ///
    /// As [`connect`](Self::connect), less the configuration checks.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub fn connect_without_config_checks(
        section: &FeedSection,
        io: &tokio::runtime::Handle,
    ) -> Result<KafkaFeedPublisher, KafkaFeedError> {
        Self::connect_with(
            section,
            io,
            startup::Scope::Existence,
            startup::CONNECT_BOUND,
        )
    }

    pub(crate) fn connect_with(
        section: &FeedSection,
        io: &tokio::runtime::Handle,
        scope: startup::Scope,
        bound: Duration,
    ) -> Result<KafkaFeedPublisher, KafkaFeedError> {
        let config = KafkaFeedConfig::from_section(section)?;
        let checked = check_topics(&config, io, scope, bound)?;
        let (core, producer) = producer_parts(&config, checked.attempt_partitions)?;
        log_started(&config, &checked);
        Ok(KafkaFeedPublisher { core, producer })
    }

    #[cfg(test)]
    pub(crate) fn core(&self) -> &Arc<Core> {
        &self.core
    }

    #[cfg(test)]
    pub(crate) fn producer(&self) -> &Producer {
        &self.producer
    }

    #[cfg(test)]
    pub(crate) fn from_parts(core: Arc<Core>, producer: Producer) -> KafkaFeedPublisher {
        KafkaFeedPublisher { core, producer }
    }
}

/// Log the INFO line a publisher that connected writes: its work topic and
/// what the startup checks read of it, `unreported` where the broker gave no
/// value. It names no group, since the publisher joins none.
fn log_started(config: &KafkaFeedConfig, checked: &startup::Checked) {
    let reported = |v: &Option<String>| v.clone().unwrap_or_else(|| "unreported".to_owned());
    tracing::info!(
        topic = %config.topic,
        cleanup_policy = %reported(&checked.cleanup_policy),
        timestamp_type = %reported(&checked.timestamp_type),
        "the Kafka feed's publisher started"
    );
}

impl Feed for KafkaFeedPublisher {
    fn backend(&self) -> &str {
        BACKEND
    }

    fn set_waker(&self, _waker: FeedWaker) {}

    fn receive(&self, _max: usize) -> Result<Vec<Delivery>, FeedError> {
        Err(FeedError::new(
            FeedErrorKind::Fatal,
            "a KafkaFeedPublisher only publishes and delivers no unit; a worker reads a feed \
             built with KafkaFeed::connect",
        ))
    }

    fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
        Err(gone(receipt))
    }

    fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
        Err(gone(receipt))
    }

    fn fail(&self, receipt: Receipt, _reason: &str) -> Result<(), FeedError> {
        Err(gone(receipt))
    }

    fn publish(&self, unit: Unit) -> Confirmation {
        publish_unit(&self.core, &self.producer, &unit)
    }
}
