//! Classification of librdkafka consumer errors into framework error
//! classes.
//!
//! librdkafka surfaces transient and permanent conditions through the same
//! `poll` return value. Treating every poll error as [`ErrorClass::Retryable`]
//! means a permanent, operator-actionable failure (an ACL revoked underneath
//! a running consumer, a deleted or invalid topic, an unsupported protocol)
//! is retried forever while the health probe stays green and the pipeline
//! silently delivers nothing. This module maps clearly-permanent rdkafka
//! error codes, and a TLS rejection named in librdkafka's error text, to
//! [`ErrorClass::Fatal`] so the driver fails fast, while keeping transient
//! codes (transport hiccups, leader elections, coordinator churn) retryable.

use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use spate_core::error::{ErrorClass, TLS_REJECTION_ALERTS};

/// Render a client-creation error without any rejected property value.
///
/// Config rejections name the property and librdkafka's result code, and
/// creation failures carry no detail text; neither can quote a value.
pub(crate) fn redacted_client_error(e: &KafkaError) -> String {
    match e {
        KafkaError::ClientConfig(res, _, key, _) => {
            format!("Client config error: librdkafka rejected the value of {key} ({res:?})")
        }
        KafkaError::ClientCreation(_) => "Client creation error".to_string(),
        other => other.to_string(),
    }
}

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

/// Whether an error callback's `code` and `text` report a TLS handshake the
/// client or the broker rejected.
///
/// The code is `SSL` or `BrokerTransportFailure`, and the text carries
/// OpenSSL's `certificate verify failed` or `SSL alert number N` with `N` in
/// [`TLS_REJECTION_ALERTS`].
pub(crate) fn tls_rejection(code: RDKafkaErrorCode, text: &str) -> bool {
    const ALERT: &str = "SSL alert number ";
    matches!(
        code,
        RDKafkaErrorCode::SSL | RDKafkaErrorCode::BrokerTransportFailure
    ) && (text.contains("certificate verify failed")
        || text.match_indices(ALERT).any(|(at, _)| {
            let rest = &text[at + ALERT.len()..];
            let digits = rest
                .find(|c: char| !c.is_ascii_digit())
                .map_or(rest, |end| &rest[..end]);
            digits
                .parse::<u8>()
                .is_ok_and(|alert| TLS_REJECTION_ALERTS.contains(&alert))
        }))
}

/// Classify a [`KafkaError`] returned by a consumer/queue poll, defaulting to
/// [`ErrorClass::Retryable`] when the error carries no librdkafka code.
///
/// [`KafkaError::MessageConsumptionFatal`] is [`ErrorClass::Fatal`] whatever
/// its code: librdkafka has failed the client. `text` is the error callback's
/// text for the same event, when known; a [`tls_rejection`] in it is
/// [`ErrorClass::Fatal`].
pub(crate) fn classify_poll_error(
    err: &KafkaError,
    after_startup: bool,
    text: Option<&str>,
) -> ErrorClass {
    if let KafkaError::MessageConsumptionFatal(_) = err {
        return ErrorClass::Fatal;
    }
    match err.rdkafka_error_code() {
        Some(code) if text.is_some_and(|text| tls_rejection(code, text)) => ErrorClass::Fatal,
        Some(code) => classify_consumer_error(code, after_startup),
        None => ErrorClass::Retryable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::error::RDKafkaErrorCode as C;

    /// Rejected values never reach the rendered error, whatever shape
    /// librdkafka quotes them in, and the property key stays.
    #[test]
    fn client_error_omits_the_value() {
        use rdkafka::ClientConfig;
        use rdkafka::consumer::BaseConsumer;

        let long = format!("hunter2{}", "x".repeat(600));
        let cases = [
            ("auto.offset.reset", "hunter2".to_string()),
            ("auto.offset.reset", " hunter2".to_string()),
            ("auto.offset.reset", long),
            ("debug", "all,hunter2".to_string()),
            ("partition.assignment.strategy", "hunter2".to_string()),
        ];
        for (key, value) in cases {
            let err = ClientConfig::new()
                .set("bootstrap.servers", "localhost:1")
                .set(key, &value)
                .create::<BaseConsumer>()
                .err()
                .expect("value is rejected");
            let msg = redacted_client_error(&err);
            assert!(!msg.contains("unter2"), "{key}: {msg}");
        }
    }

    #[test]
    fn client_config_error_names_the_key() {
        use rdkafka::ClientConfig;
        use rdkafka::consumer::BaseConsumer;

        let err = ClientConfig::new()
            .set("bootstrap.servers", "localhost:1")
            .set("auto.offset.reset", "hunter2")
            .create::<BaseConsumer>()
            .err()
            .expect("value is rejected");
        assert!(
            redacted_client_error(&err).contains("auto.offset.reset"),
            "{err}"
        );
    }

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

    /// librdkafka's error text for a handshake the broker rejected with
    /// alert 40, at the default log level.
    const ALERT_40: &str = "ssl://127.0.0.1:54436/bootstrap: SSL handshake failed: \
        error:0A000410:SSL routines::ssl/tls alert handshake failure: SSL alert number 40 \
        (after 0ms in state SSL_HANDSHAKE)";

    /// A rejection is matched in each form librdkafka reports it: a listed
    /// alert during or after the handshake, and a broker certificate the
    /// client failed to verify.
    #[test]
    fn tls_rejection_matches_a_rejected_handshake() {
        for (code, text) in [
            (C::SSL, ALERT_40),
            (
                C::SSL,
                "ssl://127.0.0.1:54436/bootstrap: SSL handshake failed: error:0A000410:SSL \
                 routines::ssl/tls alert handshake failure: SSL alert number 40 (after 0ms in \
                 state SSL_HANDSHAKE, 1 identical error(s) suppressed)",
            ),
            // Alert 40 with no cipher suite in common, from a debug build of
            // OpenSSL that prefixes the source location.
            (
                C::SSL,
                "ssl://127.0.0.1:47104/bootstrap: SSL handshake failed: \
                 ssl/record/rec_layer_s3.c:918:ssl3_read_bytes error:0A000410:SSL \
                 routines::ssl/tls alert handshake failure: SSL alert number 40",
            ),
            (
                C::SSL,
                "ssl://127.0.0.1:54440/bootstrap: SSL handshake failed: error:0A00042E:SSL \
                 routines::tlsv1 alert protocol version: SSL alert number 70 (after 0ms in \
                 state SSL_HANDSHAKE)",
            ),
            (
                C::SSL,
                "ssl://127.0.0.1:47105/bootstrap: SSL handshake failed: \
                 ssl/statem/statem_clnt.c:2126:tls_post_process_server_certificate \
                 error:0A000086:SSL routines::certificate verify failed: broker certificate \
                 could not be verified, verify that ssl.ca.location is correctly configured \
                 or root CA certificates are installed (brew install openssl)",
            ),
            // TLS 1.3 refuses a missing client certificate after the
            // handshake, on the first read.
            (
                C::BrokerTransportFailure,
                "ssl://127.0.0.1:47101/bootstrap: Receive failed: \
                 ssl/record/rec_layer_s3.c:918:ssl3_read_bytes error:0A00045C:SSL \
                 routines::tlsv13 alert certificate required: SSL alert number 116",
            ),
        ] {
            assert!(tls_rejection(code, text), "{code:?}: {text}");
        }
    }

    /// A reset, a refused connection, a malformed record, an unlisted alert,
    /// a number outside the alert range and a listed alert under an
    /// unrelated code are not rejections.
    #[test]
    fn tls_rejection_leaves_other_failures_alone() {
        for (code, text) in [
            (
                C::BrokerTransportFailure,
                "127.0.0.1:1/bootstrap: Connect to ipv4#127.0.0.1:1 failed: Connection refused \
                 (after 0ms in state CONNECT)",
            ),
            (
                C::BrokerTransportFailure,
                "ssl://127.0.0.1:9093/bootstrap: SSL handshake failed: Disconnected: \
                 connection reset by peer (after 2ms in state SSL_HANDSHAKE)",
            ),
            (
                C::SSL,
                "ssl://127.0.0.1:9093/bootstrap: SSL handshake failed: error:0A00010B:SSL \
                 routines::wrong version number (after 0ms in state SSL_HANDSHAKE)",
            ),
            (
                C::SSL,
                "ssl://127.0.0.1:54448/bootstrap: SSL handshake failed: error:0A00041A:SSL \
                 routines::tlsv1 alert decode error: SSL alert number 50 (after 0ms in state \
                 SSL_HANDSHAKE)",
            ),
            (
                C::SSL,
                "ssl://127.0.0.1:54452/bootstrap: SSL handshake failed: error:0A000438:SSL \
                 routines::tlsv1 alert internal error: SSL alert number 80 (after 0ms in state \
                 SSL_HANDSHAKE)",
            ),
            (C::SSL, "SSL handshake failed: SSL alert number 400"),
            (C::SSL, "SSL handshake failed: SSL alert number 4"),
            (C::SSL, "SSL handshake failed: SSL alert number "),
            (C::AllBrokersDown, ALERT_40),
            (C::MessageTimedOut, ALERT_40),
        ] {
            assert!(!tls_rejection(code, text), "{code:?}: {text}");
        }
    }

    /// A poll error librdkafka marks fatal is fatal, whatever its code.
    /// Regression for #727.
    #[test]
    fn a_fatal_poll_error_is_fatal() {
        let err = KafkaError::MessageConsumptionFatal(C::FencedInstanceId);
        assert_eq!(classify_poll_error(&err, true, None), ErrorClass::Fatal);
        let err = KafkaError::MessageConsumption(C::FencedInstanceId);
        assert_eq!(classify_poll_error(&err, true, None), ErrorClass::Retryable);
    }

    /// A poll error is fatal on a rejection in its callback text, and keeps
    /// its code's class without one.
    #[test]
    fn a_poll_error_with_a_rejection_in_its_text_is_fatal() {
        let err = KafkaError::MessageConsumption(C::SSL);
        assert_eq!(
            classify_poll_error(&err, false, Some(ALERT_40)),
            ErrorClass::Fatal
        );
        assert_eq!(
            classify_poll_error(&err, false, None),
            ErrorClass::Retryable
        );
    }
}
