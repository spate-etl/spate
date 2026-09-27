//! Error taxonomy and per-stage error policies.
//!
//! A pipeline failure falls into one of three classes (ADR-0010):
//! *retryable* (transient I/O, handled by the sink retry layer),
//! *record-level* (a bad payload or failed user code, subject to
//! [`ErrorPolicy`]), and *fatal* (invariant violations; the pipeline
//! stops). Record-level policies are limited to `Skip` and `Fail`; every
//! skip is surfaced through metrics.

/// What to do when a record fails in a stage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorPolicy {
    /// Drop the record, count it in `spate_*_dropped_total{reason}`, and
    /// continue. Default for deserializers.
    Skip,
    /// Fail the batch and stop the pipeline. An operator stage counts it in
    /// `spate_operator_errors_total{error_type="fatal"}`. Default for
    /// operators.
    #[default]
    Fail,
}

/// Broad classification used in metrics labels (`error_type`) and by the
/// retry layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorClass {
    /// Transient; retrying the same operation may succeed.
    Retryable,
    /// Specific to one record; retrying the same record cannot succeed.
    RecordLevel,
    /// The component or pipeline is broken; processing must stop.
    Fatal,
}

/// An unrecoverable pipeline failure: an invariant was violated or a
/// `Fail`-policy stage tripped. The pipeline transitions to `Failed`, the
/// partition watermarks stop advancing, and the process exits non-zero.
#[derive(Debug, thiserror::Error)]
#[error("fatal error in {component}: {reason}")]
pub struct FatalError {
    /// Component id where the failure originated.
    pub component: String,
    /// Human-readable cause.
    pub reason: String,
}

/// A payload could not be deserialized.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DeserError {
    /// The payload bytes do not match the expected format.
    #[error("malformed payload: {reason}")]
    Malformed {
        /// Human-readable cause, for logs and dead-record metrics.
        reason: String,
    },
    /// A schema required to decode the payload is not available.
    #[error("schema unavailable: {reason}")]
    SchemaUnavailable {
        /// Human-readable cause.
        reason: String,
    },
    /// The payload cannot be decoded *yet*. A required resource (typically
    /// a schema fetched from a registry) is still being obtained, and the
    /// deserializer has already triggered the asynchronous work that will
    /// make it available. The chain reports the batch `Blocked` at this
    /// payload and the driver's retry loop re-pushes it. The record is
    /// neither dropped nor counted as an error, and the stage's
    /// [`ErrorPolicy`] does not apply.
    ///
    /// Contract: a deserializer must return this **before emitting any
    /// record** for the payload; records emitted ahead of a `NotReady`
    /// would be duplicated when the payload is replayed.
    #[error("not ready: {reason}")]
    NotReady {
        /// What is being waited for.
        reason: String,
    },
    /// The deserializer can decode no further payload, for example because
    /// its schema registry rejected the credentials. The chain stops the
    /// pipeline whatever the stage's [`ErrorPolicy`], and counts the payload
    /// as an error but not as dropped.
    ///
    /// Contract: as with `NotReady`, return this before emitting any record
    /// for the payload.
    #[error("deserializer failed: {reason}")]
    Fatal {
        /// Human-readable cause.
        reason: String,
    },
}

/// A source failed to poll, commit, or manage its assignment.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SourceError {
    /// Underlying client error.
    #[error("source error ({class:?}): {reason}")]
    Client {
        /// Retryable vs fatal, as judged by the connector.
        class: ErrorClass,
        /// Human-readable cause.
        reason: String,
    },
}

/// A sink failed to write a batch.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SinkError {
    /// Underlying client error.
    #[error("sink error ({class:?}): {reason}")]
    Client {
        /// Retryable (will be retried on another replica) vs fatal.
        class: ErrorClass,
        /// Human-readable cause.
        reason: String,
    },
}

/// The TLS alert descriptions (RFC 8446 numbering) with which a server
/// rejects the client's certificate, credential or negotiated parameters.
///
/// A fatal alert from this list is a rejection that retrying cannot cure, so
/// a connector classes it [`ErrorClass::Fatal`]. Any other alert, such as
/// `decode_error` (50) or `internal_error` (80), stays retryable.
pub const TLS_REJECTION_ALERTS: &[u8] = &[
    40,  // handshake_failure
    42,  // bad_certificate
    43,  // unsupported_certificate
    44,  // certificate_revoked
    45,  // certificate_expired
    46,  // certificate_unknown
    48,  // unknown_ca
    49,  // access_denied
    51,  // decrypt_error
    70,  // protocol_version
    71,  // insufficient_security
    109, // missing_extension
    110, // unsupported_extension
    112, // unrecognized_name
    116, // certificate_required
    120, // no_application_protocol
];

/// The first `T` in `err`'s source chain, `err` included.
///
/// At each [`std::io::Error`] it also follows the error the `io::Error`
/// wraps, which `io::Error::source` skips.
#[must_use]
pub fn find_source<'a, T: std::error::Error + 'static>(
    err: &'a (dyn std::error::Error + 'static),
) -> Option<&'a T> {
    let mut pending = vec![err];
    while let Some(e) = pending.pop() {
        if let Some(found) = e.downcast_ref() {
            return Some(found);
        }
        if let Some(inner) = e
            .downcast_ref::<std::io::Error>()
            .and_then(|io| io.get_ref())
        {
            pending.push(inner);
        }
        pending.extend(e.source());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_documented_policy() {
        assert_eq!(ErrorPolicy::default(), ErrorPolicy::Fail);
    }

    #[test]
    fn errors_render_reasons() {
        let e = DeserError::Malformed {
            reason: "truncated header".into(),
        };
        assert!(e.to_string().contains("truncated header"));
    }

    #[derive(Debug, thiserror::Error)]
    #[error("target {0}")]
    struct Target(u8);

    #[derive(Debug, thiserror::Error)]
    #[error("wrapper")]
    struct Wrapper(#[source] std::io::Error);

    /// A `T` inside an `io::Error` is found, though `io::Error::source` skips
    /// it.
    #[test]
    fn find_source_reaches_the_error_an_io_error_wraps() {
        let err = Wrapper(std::io::Error::other(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            Target(7),
        )));
        assert_eq!(find_source::<Target>(&err).map(|t| t.0), Some(7));
        assert!(find_source::<FatalError>(&err).is_none());
    }
}
