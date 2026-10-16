//! Why the Kafka feed could not be built.

use spate_core::config::ConfigError;

/// Why the Kafka feed could not be built.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub(crate) enum KafkaFeedError {
    /// The `feed:` section is not one the Kafka feed can read.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// The feed cannot start, and starting again fails the same way: the
    /// brokers refused the credentials, a topic, or the TLS handshake; a
    /// client could not be built from the configuration, as when its TLS
    /// context fails; or a topic failed one of the feed's startup checks.
    #[error("the Kafka feed cannot start: {0}")]
    Refused(String),
    /// The brokers did not answer in time, or kept failing transiently.
    /// Starting again later may succeed.
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
