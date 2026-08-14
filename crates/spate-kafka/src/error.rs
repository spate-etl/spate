//! Classification of librdkafka consumer errors into framework error
//! classes.
//!
//! librdkafka surfaces transient and permanent conditions through the same
//! `poll` return value. Treating every poll error as [`ErrorClass::Retryable`]
//! means a permanent, operator-actionable failure (an ACL revoked underneath
//! a running consumer, a deleted or invalid topic, an unsupported protocol)
//! is retried forever while the health probe stays green and the pipeline
//! silently delivers nothing. This module maps clearly-permanent rdkafka
//! error codes to [`ErrorClass::Fatal`] so the driver fails fast, while
//! keeping transient codes (transport hiccups, leader elections,
//! coordinator churn) retryable.

use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use spate_core::error::ErrorClass;

/// Classify a single librdkafka consumer/queue error code.
///
/// `after_startup` gates [`RDKafkaErrorCode::UnknownTopicOrPartition`]:
/// before the first partition assignment it is a benign
/// metadata-propagation race (the source's startup deadline guards that
/// window), but once partitions are owned it means the topic was deleted
/// under a live consumer and can never recover on retry.
pub(crate) fn classify_consumer_error(code: RDKafkaErrorCode, after_startup: bool) -> ErrorClass {
    use RDKafkaErrorCode as C;
    match code {
        // Authorization / authentication: an ACL was revoked or the
        // credentials are wrong. Retrying the identical request can never
        // start succeeding without operator action.
        C::TopicAuthorizationFailed
        | C::GroupAuthorizationFailed
        | C::ClusterAuthorizationFailed
        | C::TransactionalIdAuthorizationFailed
        | C::Authentication
        | C::SaslAuthenticationFailed
        | C::UnsupportedSASLMechanism
        | C::IllegalSASLState
        | C::SecurityDisabled
        // The request is structurally impossible to serve as configured:
        // an invalid topic name, or a broker/protocol mismatch.
        | C::InvalidTopic
        | C::UnsupportedVersion
        | C::UnsupportedForMessageFormat => ErrorClass::Fatal,
        // A partition that vanished after we already owned it: the topic was
        // deleted. Before startup this is just metadata catching up.
        C::UnknownTopicOrPartition if after_startup => ErrorClass::Fatal,
        // Everything else (transport, leader elections, coordinator churn,
        // offset-reset conditions handled elsewhere) is transient.
        _ => ErrorClass::Retryable,
    }
}

/// Classify a [`KafkaError`] returned by a consumer/queue poll, defaulting to
/// [`ErrorClass::Retryable`] when the error carries no librdkafka code.
pub(crate) fn classify_poll_error(err: &KafkaError, after_startup: bool) -> ErrorClass {
    match err.rdkafka_error_code() {
        Some(code) => classify_consumer_error(code, after_startup),
        None => ErrorClass::Retryable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::error::RDKafkaErrorCode as C;

    /// Permanent broker-side conditions must fail the pipeline fast, in both
    /// the pre- and post-startup windows.
    #[test]
    fn permanent_codes_classify_fatal() {
        for code in [
            C::TopicAuthorizationFailed,
            C::GroupAuthorizationFailed,
            C::ClusterAuthorizationFailed,
            C::TransactionalIdAuthorizationFailed,
            C::Authentication,
            C::SaslAuthenticationFailed,
            C::UnsupportedSASLMechanism,
            C::IllegalSASLState,
            C::SecurityDisabled,
            C::InvalidTopic,
            C::UnsupportedVersion,
            C::UnsupportedForMessageFormat,
        ] {
            assert_eq!(
                classify_consumer_error(code, true),
                ErrorClass::Fatal,
                "{code:?} after startup"
            );
            assert_eq!(
                classify_consumer_error(code, false),
                ErrorClass::Fatal,
                "{code:?} before startup"
            );
        }
    }

    /// Transient conditions keep retrying.
    #[test]
    fn transient_codes_classify_retryable() {
        for code in [
            C::BrokerNotAvailable,
            C::LeaderNotAvailable,
            C::NotLeaderForPartition,
            C::RequestTimedOut,
            C::NetworkException,
            C::CoordinatorNotAvailable,
            C::CoordinatorLoadInProgress,
            C::NotCoordinator,
            C::RebalanceInProgress,
            C::OffsetOutOfRange,
        ] {
            assert_eq!(
                classify_consumer_error(code, true),
                ErrorClass::Retryable,
                "{code:?}"
            );
        }
    }

    /// A missing topic/partition is only fatal once we have already been
    /// assigned partitions (topic deleted under a live consumer); before
    /// that it is a metadata race the startup deadline covers.
    #[test]
    fn unknown_topic_is_fatal_only_after_startup() {
        assert_eq!(
            classify_consumer_error(C::UnknownTopicOrPartition, false),
            ErrorClass::Retryable,
        );
        assert_eq!(
            classify_consumer_error(C::UnknownTopicOrPartition, true),
            ErrorClass::Fatal,
        );
    }
}
