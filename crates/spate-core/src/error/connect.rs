//! [`ConnectError`]: why a constructor that does I/O could not build its
//! component.

use crate::config::ConfigError;
use crate::error::{ErrorClass, Source, role_error_accessors};

/// How a caller must react to a [`ConnectError`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectErrorKind {
    /// The configuration is not one the component can use;
    /// [`ConnectError::config_error`] returns it.
    Config,
    /// The component cannot start, and starting again fails the same way.
    Refused,
    /// The component could not start for a reason that may pass. Starting
    /// again later may succeed.
    Unavailable,
}

impl ConnectErrorKind {
    /// [`Unavailable`](Self::Unavailable) is retryable, and
    /// [`Config`](Self::Config) and [`Refused`](Self::Refused) are fatal.
    fn class(self) -> ErrorClass {
        match self {
            ConnectErrorKind::Unavailable => ErrorClass::Retryable,
            ConnectErrorKind::Config | ConnectErrorKind::Refused => ErrorClass::Fatal,
        }
    }
}

/// Why a constructor that does I/O, such as a feed backend's `connect`, could
/// not build its component.
///
/// `component` names what was being built for a person to read, such as
/// `"Kafka feed"`, and appears in the rendered text. A component outside this
/// crate builds the error through [`refused`](Self::refused),
/// [`unavailable`](Self::unavailable), [`from_class`](Self::from_class) or
/// `From<ConfigError>`, so `?` converts a configuration error.
///
/// A [`Config`](ConnectErrorKind::Config) error renders as the
/// [`ConfigError`] it holds, and [`source`](std::error::Error::source)
/// returns that error's own source.
///
/// # Examples
///
/// ```
/// use spate_core::error::{ConnectError, ConnectErrorKind, ErrorClass};
///
/// let err = ConnectError::from_class("queue feed", ErrorClass::Retryable, "timed out");
/// assert_eq!(err.kind(), ConnectErrorKind::Unavailable);
/// assert_eq!(err.component(), Some("queue feed"));
/// assert_eq!(err.to_string(), "the queue feed is unavailable: timed out");
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub struct ConnectError {
    kind: ConnectErrorKind,
    component: Option<&'static str>,
    reason: String,
    source: Option<Source>,
    config: Option<ConfigError>,
}

role_error_accessors!(ConnectError, ConnectErrorKind);

impl ConnectError {
    fn failed(kind: ConnectErrorKind, component: &'static str, reason: String) -> ConnectError {
        ConnectError {
            kind,
            component: Some(component),
            reason,
            source: None,
            config: None,
        }
    }

    /// A [`Refused`](ConnectErrorKind::Refused) error for `component`.
    #[must_use]
    pub fn refused(component: &'static str, reason: impl Into<String>) -> ConnectError {
        ConnectError::failed(ConnectErrorKind::Refused, component, reason.into())
    }

    /// An [`Unavailable`](ConnectErrorKind::Unavailable) error for
    /// `component`.
    #[must_use]
    pub fn unavailable(component: &'static str, reason: impl Into<String>) -> ConnectError {
        ConnectError::failed(ConnectErrorKind::Unavailable, component, reason.into())
    }

    /// The error for a failure of `class`:
    /// [`Unavailable`](ConnectErrorKind::Unavailable) for
    /// [`Retryable`](ErrorClass::Retryable), and
    /// [`Refused`](ConnectErrorKind::Refused) for [`Fatal`](ErrorClass::Fatal)
    /// and [`RecordLevel`](ErrorClass::RecordLevel), since connecting again
    /// fails the same way for both.
    #[must_use]
    pub fn from_class(
        component: &'static str,
        class: ErrorClass,
        reason: impl Into<String>,
    ) -> ConnectError {
        match class {
            ErrorClass::Retryable => ConnectError::unavailable(component, reason),
            ErrorClass::Fatal | ErrorClass::RecordLevel => ConnectError::refused(component, reason),
        }
    }

    /// The component that could not start, or `None` for a
    /// [`Config`](ConnectErrorKind::Config) error, whose [`ConfigError`]
    /// names its own section.
    #[must_use]
    pub fn component(&self) -> Option<&'static str> {
        self.component
    }

    /// The configuration error a [`Config`](ConnectErrorKind::Config) error
    /// holds, or `None` for any other kind.
    #[must_use]
    pub fn config_error(&self) -> Option<&ConfigError> {
        self.config.as_ref()
    }
}

impl From<ConfigError> for ConnectError {
    fn from(err: ConfigError) -> ConnectError {
        ConnectError {
            kind: ConnectErrorKind::Config,
            component: None,
            reason: err.to_string(),
            source: None,
            config: Some(err),
        }
    }
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let component = self.component.unwrap_or("component");
        match self.kind {
            ConnectErrorKind::Config => f.write_str(&self.reason),
            ConnectErrorKind::Refused => {
                write!(f, "the {component} cannot start: {}", self.reason)
            }
            ConnectErrorKind::Unavailable => {
                write!(f, "the {component} is unavailable: {}", self.reason)
            }
        }
    }
}

impl std::error::Error for ConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let Some(source) = self.source.as_deref() {
            return Some(source as &(dyn std::error::Error + 'static));
        }
        self.config.as_ref().and_then(std::error::Error::source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::find_source;

    #[derive(Debug, thiserror::Error)]
    #[error("client error")]
    struct ClientError;

    /// Pins the text each startup failure renders, with the component's
    /// name in it.
    #[test]
    fn each_failure_renders_its_component_and_reason() {
        let refused = ConnectError::refused("Kafka feed", "no such topic");
        assert_eq!(
            refused.to_string(),
            "the Kafka feed cannot start: no such topic"
        );
        assert_eq!(
            (refused.component(), refused.reason()),
            (Some("Kafka feed"), "no such topic")
        );
        assert!(refused.config_error().is_none());
        let unavailable = ConnectError::unavailable("SQS feed", "timed out");
        assert_eq!(
            unavailable.to_string(),
            "the SQS feed is unavailable: timed out"
        );
        assert!(unavailable.config_error().is_none());
    }

    /// Pins each kind's class, and that a retryable class is unavailable
    /// while a fatal or record-level class is refused.
    #[test]
    fn a_class_picks_the_kind() {
        let cases = [
            (ErrorClass::Retryable, ConnectErrorKind::Unavailable),
            (ErrorClass::Fatal, ConnectErrorKind::Refused),
            (ErrorClass::RecordLevel, ConnectErrorKind::Refused),
        ];
        for (class, kind) in cases {
            let err = ConnectError::from_class("queue feed", class, "x");
            assert_eq!(
                (err.kind(), err.component(), err.reason()),
                (kind, Some("queue feed"), "x")
            );
        }
        assert_eq!(
            ConnectError::unavailable("q", "x").class(),
            ErrorClass::Retryable
        );
        assert_eq!(ConnectError::refused("q", "x").class(), ErrorClass::Fatal);
        let config = ConnectError::from(ConfigError::Validation("bad".into()));
        assert_eq!(config.class(), ErrorClass::Fatal);
    }

    /// Pins that a config error converts through `?` transparently: its
    /// text, its own source, no component, and the error itself returned
    /// by `config_error`.
    #[test]
    fn a_config_error_is_transparent() {
        fn connect() -> Result<(), ConnectError> {
            Err(ConfigError::Validation("bad".into()))?
        }
        let err = connect().unwrap_err();
        assert_eq!(err.kind(), ConnectErrorKind::Config);
        assert_eq!(err.to_string(), "invalid configuration: bad");
        assert_eq!(err.reason(), "invalid configuration: bad");
        assert_eq!(err.component(), None);
        assert!(matches!(
            err.config_error(),
            Some(ConfigError::Validation(message)) if message == "bad"
        ));
        assert!(std::error::Error::source(&err).is_none());

        let wrapped = ConfigError::Io {
            path: "spate.yaml".into(),
            source: std::io::Error::other("disk"),
        };
        let want = wrapped.to_string();
        let err = ConnectError::from(wrapped);
        assert_eq!(err.to_string(), want);
        let source = std::error::Error::source(&err).expect("the config error's source");
        assert_eq!(source.to_string(), "disk");
    }

    /// An attached source is what `source()` returns, and `find_source`
    /// reaches it.
    #[test]
    fn with_source_is_the_source_of_a_connect_error() {
        let err = ConnectError::refused("Kafka feed", "denied").with_source(ClientError);
        let source = std::error::Error::source(&err).expect("the attached source");
        assert_eq!(source.to_string(), "client error");
        assert!(find_source::<ClientError>(&err).is_some());
    }
}
