//! [`ConnectError`]: why a feed backend could not connect.

use crate::config::ConfigError;
use crate::error::ErrorClass;

/// Why a feed backend's `connect` could not build a feed or a publisher.
///
/// `backend` names the backend for a person to read, such as `"Kafka"`, and
/// appears in the rendered text. A backend outside this crate builds the
/// error through [`refused`](Self::refused),
/// [`unavailable`](Self::unavailable) or [`from_class`](Self::from_class),
/// and a caller matches a variant with `{ reason, .. }`.
///
/// # Examples
///
/// ```
/// use spate_core::error::ErrorClass;
/// use spate_core::feed::ConnectError;
///
/// let err = ConnectError::from_class("Queue", ErrorClass::Retryable, "timed out");
/// assert!(matches!(&err, ConnectError::Unavailable { reason, .. } if reason == "timed out"));
/// assert_eq!(err.to_string(), "Queue is unavailable: timed out");
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectError {
    /// The `feed:` section is not one the backend can read.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// The backend cannot start, and starting again fails the same way.
    #[error("the {backend} feed cannot start: {reason}")]
    #[non_exhaustive]
    Refused {
        /// The backend's name, for a person to read.
        backend: &'static str,
        /// Why it cannot start.
        reason: String,
    },
    /// The backend could not start for a reason that may pass. Starting
    /// again later may succeed.
    #[error("{backend} is unavailable: {reason}")]
    #[non_exhaustive]
    Unavailable {
        /// The backend's name, for a person to read.
        backend: &'static str,
        /// What failed.
        reason: String,
    },
}

impl ConnectError {
    /// A [`Refused`](Self::Refused) error for `backend`.
    #[must_use]
    pub fn refused(backend: &'static str, reason: impl Into<String>) -> ConnectError {
        ConnectError::Refused {
            backend,
            reason: reason.into(),
        }
    }

    /// An [`Unavailable`](Self::Unavailable) error for `backend`.
    #[must_use]
    pub fn unavailable(backend: &'static str, reason: impl Into<String>) -> ConnectError {
        ConnectError::Unavailable {
            backend,
            reason: reason.into(),
        }
    }

    /// The error for a failure of `class`:
    /// [`Unavailable`](Self::Unavailable) for
    /// [`Retryable`](ErrorClass::Retryable), and [`Refused`](Self::Refused)
    /// for [`Fatal`](ErrorClass::Fatal) and
    /// [`RecordLevel`](ErrorClass::RecordLevel), since connecting again
    /// fails the same way for both.
    #[must_use]
    pub fn from_class(
        backend: &'static str,
        class: ErrorClass,
        reason: impl Into<String>,
    ) -> ConnectError {
        match class {
            ErrorClass::Retryable => ConnectError::unavailable(backend, reason),
            ErrorClass::Fatal | ErrorClass::RecordLevel => ConnectError::refused(backend, reason),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the text each startup failure renders, with the backend's name
    /// in it.
    #[test]
    fn each_failure_renders_its_backend_and_reason() {
        assert_eq!(
            ConnectError::refused("Kafka", "no such topic").to_string(),
            "the Kafka feed cannot start: no such topic"
        );
        assert_eq!(
            ConnectError::unavailable("SQS", "timed out").to_string(),
            "SQS is unavailable: timed out"
        );
    }

    /// Pins that a retryable class is unavailable, and a fatal or
    /// record-level class is refused.
    #[test]
    fn a_class_picks_the_failure() {
        let kind = |class| match ConnectError::from_class("Queue", class, "x") {
            ConnectError::Refused { backend, reason } => ("refused", backend, reason),
            ConnectError::Unavailable { backend, reason } => ("unavailable", backend, reason),
            ConnectError::Config(e) => panic!("{e}"),
        };
        assert_eq!(
            kind(ErrorClass::Retryable),
            ("unavailable", "Queue", "x".to_owned())
        );
        assert_eq!(
            kind(ErrorClass::Fatal),
            ("refused", "Queue", "x".to_owned())
        );
        assert_eq!(
            kind(ErrorClass::RecordLevel),
            ("refused", "Queue", "x".to_owned())
        );
    }

    /// Pins that a config error converts through `?` with its text
    /// unchanged.
    #[test]
    fn a_config_error_keeps_its_text() {
        fn connect() -> Result<(), ConnectError> {
            Err(ConfigError::Validation("bad".into()))?
        }
        let err = connect().unwrap_err();
        assert!(matches!(err, ConnectError::Config(_)));
        assert_eq!(err.to_string(), "invalid configuration: bad");
    }
}
