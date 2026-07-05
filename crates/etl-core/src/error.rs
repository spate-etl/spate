//! Error taxonomy and per-stage error policies.
//!
//! Three classes of failure exist in a pipeline (see `docs/DESIGN.md`):
//! *retryable* (transient I/O — handled by the sink retry layer),
//! *record-level* (a bad payload or failed user code — subject to
//! [`ErrorPolicy`]), and *fatal* (invariant violations — the pipeline
//! stops). Record-level policies are deliberately limited to `Skip` and
//! `Fail`; every skip is surfaced through metrics.

/// What to do when a record fails in a stage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorPolicy {
    /// Drop the record, count it in `etl_*_dropped_total{reason}`, and
    /// continue. Default for deserializers.
    Skip,
    /// Fail the batch and stop the pipeline. Default for operators.
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
}
