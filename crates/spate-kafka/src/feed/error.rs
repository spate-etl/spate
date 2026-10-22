//! Why the Kafka feed could not be built.

use spate_core::config::ConfigError;

/// Why [`KafkaFeed::connect`](crate::feed::KafkaFeed::connect) could not
/// build a feed, or
/// [`KafkaFeedPublisher::connect`](crate::feed::KafkaFeedPublisher::connect)
/// a publisher.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum KafkaFeedError {
    /// The `feed:` section is not one the Kafka feed can read.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// The feed cannot start, and starting again fails the same way: a
    /// client could not be built from the configuration, as for a property
    /// value librdkafka does not accept or a TLS context that fails; a
    /// broker rejected the client's credential, or either side rejected the
    /// TLS handshake; the cluster refused a read of a topic as unauthorized
    /// or for an invalid topic name, or described no configuration entry for
    /// one; or the topics failed one of the feed's startup checks.
    #[error("the Kafka feed cannot start: {0}")]
    Refused(String),
    /// The feed could not start for a reason that may pass: a read of the
    /// topics failed otherwise, the startup checks took longer than their
    /// bound, or the system could not start a thread. Starting again later
    /// may succeed.
    #[error("Kafka is unavailable: {0}")]
    Unavailable(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use spate_core::config::FeedSection;

    /// Pins that a config error converts with its text unchanged, and the
    /// prefix each startup failure carries.
    #[test]
    fn each_error_renders_its_cause() {
        let section: FeedSection = serde_yaml::from_str("memory: {}").unwrap();
        let config = crate::feed::config::KafkaFeedConfig::from_section(&section).unwrap_err();
        let text = config.to_string();
        let err: KafkaFeedError = config.into();
        assert!(matches!(err, KafkaFeedError::Config(_)));
        assert_eq!(err.to_string(), text);

        assert_eq!(
            KafkaFeedError::Refused("no such topic".into()).to_string(),
            "the Kafka feed cannot start: no such topic"
        );
        assert_eq!(
            KafkaFeedError::Unavailable("timed out".into()).to_string(),
            "Kafka is unavailable: timed out"
        );
    }
}
