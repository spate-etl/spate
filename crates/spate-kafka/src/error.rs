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
use spate_core::error::{ErrorClass, SourceError, TLS_REJECTION_ALERTS};

/// Render a client-creation error without any rejected property value.
///
/// A config rejection names the property and librdkafka's result code. A
/// creation failure reports that librdkafka's detail text is withheld.
pub(crate) fn redacted_client_error(e: &KafkaError) -> String {
    match e {
        KafkaError::ClientConfig(res, _, key, _) => {
            format!("Client config error: librdkafka rejected the value of {key} ({res:?})")
        }
        KafkaError::ClientCreation(_) => {
            "Client creation error (librdkafka detail withheld)".to_string()
        }
        other => other.to_string(),
    }
}

/// What a failure to settle a partition's start position does to the offer it
/// belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StartFailure {
    /// The group has moved past this member or waits for it to rejoin, so the
    /// offer is discarded.
    Discard,
    /// The offer is kept and tried again.
    Retry,
    /// The run stops.
    Fatal,
}

/// Classify an error from reading, listing or committing a start position.
///
/// Reads as before startup: a broker can answer `UnknownTopicOrPartition` for a
/// partition just added to the topic, and a deleted topic fails the source at
/// its next poll or at the assignment deadline.
pub(crate) fn classify_start_error(code: RDKafkaErrorCode) -> StartFailure {
    match code {
        // A rebalance can wait on this member, which rejoins only once its offer
        // is answered, so retrying while holding the offer can never clear it.
        RDKafkaErrorCode::IllegalGeneration
        | RDKafkaErrorCode::UnknownMemberId
        | RDKafkaErrorCode::RebalanceInProgress => StartFailure::Discard,
        code => match classify_consumer_error(code, false) {
            ErrorClass::Fatal => StartFailure::Fatal,
            _ => StartFailure::Retry,
        },
    }
}

/// [`classify_consumer_error`] for a client error. One that carries no code
/// is Retryable.
pub(crate) fn classify_kafka_error(e: &KafkaError, after_startup: bool) -> ErrorClass {
    e.rdkafka_error_code()
        .map_or(ErrorClass::Retryable, |code| {
            classify_consumer_error(code, after_startup)
        })
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
        | C::UnsupportedForMessageFormat
        // A fetched batch this build cannot read: a compression codec
        // librdkafka was built without, or an unknown message format. A retry
        // refetches the same batch, or librdkafka skips the message.
        | C::NotImplemented
        // A fetched batch whose codec is known but whose payload does not
        // decompress. A retry refetches the same bytes.
        | C::BadCompression
        // A partition with no committed offset, or a fetch out of range, under
        // `auto.offset.reset: error`. librdkafka stops fetching the partition
        // once it reports this.
        | C::AutoOffsetReset => ErrorClass::Fatal,
        // A partition that vanished after we already owned it: the topic was
        // deleted. Before startup this is just metadata catching up.
        C::UnknownTopicOrPartition if after_startup => ErrorClass::Fatal,
        // Everything else (transport, leader elections, coordinator churn)
        // is transient.
        _ => ErrorClass::Retryable,
    }
}

/// Whether `code` ends an idempotent producer for good: librdkafka's own
/// fatal error, or a cause it makes fatal for such a producer. rdkafka
/// reports a fatal error under its cause's code, `Fenced` among them, the
/// code librdkafka gives a producer id refused for an old epoch.
pub(crate) fn ends_idempotent_producer(code: RDKafkaErrorCode) -> bool {
    use RDKafkaErrorCode as C;
    matches!(
        code,
        C::Fatal
            | C::OutOfOrderSequenceNumber
            | C::InvalidProducerEpoch
            | C::InvalidProducerIdMapping
            | C::UnknownProducerId
            | C::Fenced
            | C::Inconsistent
            | C::GaplessGuarantee
    )
}

/// Whether an error callback's `code` and `text` report a rejection: a SASL
/// authentication failure or a [`tls_rejection`]. An `Authentication` error
/// whose text says the SASL mechanism handshake failed on a local error, such
/// as the connection dropping, is no rejection.
pub(crate) fn rejection(code: RDKafkaErrorCode, text: &str) -> bool {
    match code {
        // librdkafka reports a SASL mechanism handshake that ended on a
        // local transport failure or timeout under this code too.
        RDKafkaErrorCode::Authentication => !text.contains("mechanism handshake failed: Local: "),
        RDKafkaErrorCode::SaslAuthenticationFailed => true,
        _ => tls_rejection(code, text),
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

/// The reason a consumer/queue poll error reports: `prefix`, the error, and
/// the error callback's `text` when known.
///
/// An error for a fetched batch this build cannot read or decompress also
/// names its likely cause, since the code reads only as "Not implemented" or
/// "Invalid compressed data".
pub(crate) fn poll_reason(prefix: &str, err: &KafkaError, text: Option<&str>) -> String {
    let note = match err.rdkafka_error_code() {
        Some(RDKafkaErrorCode::NotImplemented) => {
            " (a fetched batch uses a compression codec or message format this build cannot read)"
        }
        // librdkafka stops a zstd output buffer at the receive cap and
        // reports the batch as invalid compressed data.
        Some(RDKafkaErrorCode::BadCompression) => {
            " (a fetched batch does not decompress, or a zstd batch decompresses past \
             `receive.message.max.bytes`)"
        }
        _ => "",
    };
    match text {
        Some(text) => format!("{prefix}: {err}: {text}{note}"),
        None => format!("{prefix}: {err}{note}"),
    }
}

/// The source error for a consumer/queue poll error: the class
/// [`classify_poll_error`] gives it and the reason [`poll_reason`] writes.
pub(crate) fn poll_error(
    prefix: &str,
    err: &KafkaError,
    after_startup: bool,
    text: Option<&str>,
) -> SourceError {
    SourceError::new(
        classify_poll_error(err, after_startup, text),
        poll_reason(prefix, err, text),
    )
}

/// Classify a [`KafkaError`] returned by a consumer/queue poll, defaulting to
/// [`ErrorClass::Retryable`] when the error carries no librdkafka code.
///
/// [`KafkaError::MessageConsumptionFatal`] is [`ErrorClass::Fatal`] whatever
/// its code: librdkafka has failed the client. `text` is the error callback's
/// text for the same event, when known; a [`tls_rejection`] in it is
/// [`ErrorClass::Fatal`], and an `Authentication` error whose text is no
/// [`rejection`] is [`ErrorClass::Retryable`]. With no text, `Authentication`
/// is [`ErrorClass::Fatal`].
///
/// [`RDKafkaErrorCode::BadMessage`], a message librdkafka reports as corrupt,
/// is [`ErrorClass::Fatal`]. librdkafka does not stop on it, and reports it
/// again on each fetch that reads it. For a message in format v0 or v1 it skips
/// the offset when the same fetch also returns a later readable message. For a
/// record batch in format v2 it either fetches the batch again or moves its
/// fetch position to an offset read from the wrong bytes, where
/// `auto.offset.reset` decides what is read next. A later delivery from the
/// partition could then commit past a record nobody read.
pub(crate) fn classify_poll_error(
    err: &KafkaError,
    after_startup: bool,
    text: Option<&str>,
) -> ErrorClass {
    if let KafkaError::MessageConsumptionFatal(_) = err {
        return ErrorClass::Fatal;
    }
    match err.rdkafka_error_code() {
        Some(RDKafkaErrorCode::BadMessage) => ErrorClass::Fatal,
        Some(code) if text.is_some_and(|text| tls_rejection(code, text)) => ErrorClass::Fatal,
        // A SASL handshake that ended on a local transport failure or timeout
        // refused nothing, and the next connection can pass.
        Some(code @ RDKafkaErrorCode::Authentication)
            if text.is_some_and(|text| !rejection(code, text)) =>
        {
            ErrorClass::Retryable
        }
        Some(code) => classify_consumer_error(code, after_startup),
        None => ErrorClass::Retryable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::testing::{Raw as _, raw_config};
    use rdkafka::error::RDKafkaErrorCode as C;

    /// A start the group has moved past is discarded, a permanent code stops
    /// the run, and anything else is kept and retried.
    #[test]
    fn classify_start_error_sorts_each_code() {
        for (code, class) in [
            (C::IllegalGeneration, StartFailure::Discard),
            (C::UnknownMemberId, StartFailure::Discard),
            (C::RebalanceInProgress, StartFailure::Discard),
            (C::NotCoordinator, StartFailure::Retry),
            (C::CoordinatorNotAvailable, StartFailure::Retry),
            (C::CoordinatorLoadInProgress, StartFailure::Retry),
            (C::RequestTimedOut, StartFailure::Retry),
            (C::OperationTimedOut, StartFailure::Retry),
            (C::BrokerTransportFailure, StartFailure::Retry),
            (C::WaitingForCoordinator, StartFailure::Retry),
            (C::LeaderNotAvailable, StartFailure::Retry),
            (C::NotLeaderForPartition, StartFailure::Retry),
            (C::OffsetNotAvailable, StartFailure::Retry),
            (C::NoOffset, StartFailure::Retry),
            (C::GroupAuthorizationFailed, StartFailure::Fatal),
            (C::TopicAuthorizationFailed, StartFailure::Fatal),
            (C::UnsupportedVersion, StartFailure::Fatal),
            (C::UnknownTopicOrPartition, StartFailure::Retry),
        ] {
            assert_eq!(classify_start_error(code), class, "{code:?}");
        }
    }

    /// Rejected values never reach the rendered error, whatever shape
    /// librdkafka quotes them in, and the property key stays.
    #[test]
    fn client_error_omits_the_value() {
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
            let err = raw_config()
                .set("bootstrap.servers", "localhost:1")
                .set(key, &value)
                .build::<BaseConsumer>()
                .err()
                .expect("value is rejected");
            let msg = redacted_client_error(&err);
            assert!(!msg.contains("unter2"), "{key}: {msg}");
        }
    }

    #[test]
    fn client_config_error_names_the_key() {
        use rdkafka::consumer::BaseConsumer;

        let err = raw_config()
            .set("bootstrap.servers", "localhost:1")
            .set("auto.offset.reset", "hunter2")
            .build::<BaseConsumer>()
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
            C::NotImplemented,
            C::BadCompression,
            C::AutoOffsetReset,
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

    /// A SASL handshake the connection dropped under is retryable, while a
    /// refused login under the same code, and the code with no text, stay
    /// fatal. Regression for #1269.
    #[test]
    fn a_sasl_handshake_dropped_locally_is_retryable() {
        const DROPPED: &str = "sasl_ssl://127.0.0.1:9093/bootstrap: SASL SCRAM-SHA-256 mechanism \
            handshake failed: Local: Broker transport failure: broker's supported mechanisms: \
            (n/a) (after 0ms in state DOWN)";
        const REFUSED: &str = "sasl_ssl://127.0.0.1:9093/bootstrap: SASL SCRAM-SHA-256 mechanism \
            handshake failed: Broker: Unsupported SASL mechanism: broker's supported mechanisms: \
            PLAIN (after 3ms in state AUTH_HANDSHAKE)";
        let err = KafkaError::MessageConsumption(C::Authentication);
        for after_startup in [false, true] {
            assert_eq!(
                classify_poll_error(&err, after_startup, Some(DROPPED)),
                ErrorClass::Retryable
            );
            assert_eq!(
                classify_poll_error(&err, after_startup, Some(REFUSED)),
                ErrorClass::Fatal
            );
            assert_eq!(
                classify_poll_error(&err, after_startup, None),
                ErrorClass::Fatal
            );
        }
        let failed = KafkaError::MessageConsumption(C::SaslAuthenticationFailed);
        assert_eq!(
            classify_poll_error(&failed, true, Some(DROPPED)),
            ErrorClass::Fatal
        );
    }

    /// Regression for #727.
    #[test]
    fn a_fatal_poll_error_is_fatal() {
        let err = KafkaError::MessageConsumptionFatal(C::FencedInstanceId);
        assert_eq!(classify_poll_error(&err, true, None), ErrorClass::Fatal);
        let err = KafkaError::MessageConsumption(C::FencedInstanceId);
        assert_eq!(classify_poll_error(&err, true, None), ErrorClass::Retryable);
    }

    /// A batch the build cannot decompress fails the poll as fatal.
    /// Regression for #1253.
    #[test]
    fn an_unreadable_batch_is_fatal() {
        let err = KafkaError::MessageConsumption(C::NotImplemented);
        assert_eq!(classify_poll_error(&err, true, None), ErrorClass::Fatal);
    }

    /// A poll reason carries the callback text when known, and only an
    /// unreadable or undecompressable batch adds a note.
    #[test]
    fn a_poll_reason_names_an_unreadable_batch() {
        let unreadable = KafkaError::MessageConsumption(C::NotImplemented);
        assert_eq!(
            poll_reason(
                "poll",
                &unreadable,
                Some("Decompression (codec 0x5) failed")
            ),
            "poll: Message consumption error: NotImplemented (Local: Not implemented): \
             Decompression (codec 0x5) failed (a fetched batch uses a compression codec or \
             message format this build cannot read)"
        );
        assert_eq!(
            poll_reason(
                "poll",
                &KafkaError::MessageConsumption(C::BadCompression),
                None
            ),
            "poll: Message consumption error: BadCompression (Local: Invalid compressed data) \
             (a fetched batch does not decompress, or a zstd batch decompresses past \
             `receive.message.max.bytes`)"
        );
        for err in [
            KafkaError::MessageConsumption(C::BadMessage),
            KafkaError::MessageConsumptionFatal(C::FencedInstanceId),
        ] {
            assert_eq!(poll_reason("poll", &err, None), format!("poll: {err}"));
        }
    }

    /// A message librdkafka reports as bad is fatal before and after the
    /// first assignment, and a transport error is not. Regression for #1257.
    #[test]
    fn a_bad_message_poll_error_is_fatal() {
        let bad = KafkaError::MessageConsumption(C::BadMessage);
        assert_eq!(classify_poll_error(&bad, true, None), ErrorClass::Fatal);
        assert_eq!(classify_poll_error(&bad, false, None), ErrorClass::Fatal);
        let down = KafkaError::MessageConsumption(C::AllBrokersDown);
        assert_eq!(
            classify_poll_error(&down, true, None),
            ErrorClass::Retryable
        );
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
