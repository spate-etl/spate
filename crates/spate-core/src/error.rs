//! Error taxonomy and per-stage error policies.
//!
//! A pipeline failure falls into one of three classes (ADR-0010):
//! *retryable* (transient I/O, handled by the sink retry layer),
//! *record-level* (a bad payload or failed user code, subject to
//! [`ErrorPolicy`]), and *fatal* (invariant violations; the pipeline
//! stops). Record-level policies are limited to `Skip` and `Fail`; every
//! skip is surfaced through metrics.

#[cfg(not(loom))]
mod connect;
#[cfg(not(loom))]
pub use connect::{ConnectError, ConnectErrorKind};

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
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("fatal error in {component}: {reason}")]
#[non_exhaustive]
pub struct FatalError {
    /// Component id where the failure originated.
    pub component: String,
    /// Human-readable cause.
    pub reason: String,
}

impl FatalError {
    /// A failure in `component` with a human-readable `reason`. Code outside
    /// this crate builds one here, as the struct is `#[non_exhaustive]`.
    ///
    /// # Examples
    ///
    /// ```
    /// use spate_core::error::FatalError;
    ///
    /// let e = FatalError::new("sink", "schema mismatch");
    /// assert_eq!(e.to_string(), "fatal error in sink: schema mismatch");
    /// ```
    #[must_use]
    pub fn new(component: impl Into<String>, reason: impl Into<String>) -> FatalError {
        FatalError {
            component: component.into(),
            reason: reason.into(),
        }
    }
}

/// The client error a role error carries, behind an `Arc` so the error stays
/// cheap to clone.
pub(crate) type Source = std::sync::Arc<dyn std::error::Error + Send + Sync + 'static>;

/// The accessors every role error shares: `kind`, `class`, `reason` and
/// `with_source`. Each type is a struct with private `kind`, `reason` and
/// `source` fields, and its kind enum has a `class` method.
macro_rules! role_error_accessors {
    ($ty:ident, $kind:ident) => {
        impl $ty {
            /// How the caller must react.
            #[must_use]
            pub fn kind(&self) -> $kind {
                self.kind
            }

            /// The [`ErrorClass`](crate::error::ErrorClass) the kind maps to,
            /// which answers "retry or stop" the same way for every role.
            #[must_use]
            pub fn class(&self) -> $crate::error::ErrorClass {
                self.kind.class()
            }

            /// The human-readable cause.
            #[must_use]
            pub fn reason(&self) -> &str {
                &self.reason
            }

            /// This error with `source` as the error that caused it, which
            /// [`source`](std::error::Error::source) returns.
            ///
            /// The source's text and `Debug` follow the rules for the reason:
            /// no credential, and no record or payload content. The source is
            /// for logging through [`chain`](crate::error::chain); decide by
            /// `kind()` or `class()`, since the source's type can change when
            /// Spate updates a client crate.
            #[must_use]
            pub fn with_source(
                mut self,
                source: impl std::error::Error + Send + Sync + 'static,
            ) -> Self {
                self.source = Some(std::sync::Arc::new(source));
                self
            }
        }
    };
}
pub(crate) use role_error_accessors;

/// The methods of a run-time role error: `SourceError`, `SinkError`,
/// `FeedError` and `DeserError`. Beyond the accessors, each has
/// `new(kind, reason)`, and `source()` returns the attached error.
macro_rules! role_error_methods {
    ($ty:ident, $kind:ident) => {
        impl $ty {
            #[doc = concat!("An error of `kind` with a human-readable `reason`.")]
            #[must_use]
            pub fn new(kind: $kind, reason: impl Into<String>) -> Self {
                Self {
                    kind,
                    reason: reason.into(),
                    source: None,
                }
            }
        }

        $crate::error::role_error_accessors!($ty, $kind);

        impl std::error::Error for $ty {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.source
                    .as_deref()
                    .map(|source| source as &(dyn std::error::Error + 'static))
            }
        }
    };
}
pub(crate) use role_error_methods;

/// How the chain must react to a [`DeserError`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeserErrorKind {
    /// The payload bytes do not match the expected format.
    Malformed,
    /// A schema required to decode the payload is not available.
    SchemaUnavailable,
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
    NotReady,
    /// The deserializer can decode no further payload, for example because
    /// its schema registry rejected the credentials. The chain stops the
    /// pipeline whatever the stage's [`ErrorPolicy`], and counts the payload
    /// as an error but not as dropped.
    ///
    /// Contract: as with `NotReady`, return this before emitting any record
    /// for the payload.
    Fatal,
}

impl DeserErrorKind {
    /// [`Malformed`](Self::Malformed) and
    /// [`SchemaUnavailable`](Self::SchemaUnavailable) are record-level, as the
    /// stage's [`ErrorPolicy`] decides them, and
    /// [`NotReady`](Self::NotReady) is retryable. The chain acts on the kind
    /// itself.
    pub(crate) fn class(self) -> ErrorClass {
        match self {
            DeserErrorKind::Malformed | DeserErrorKind::SchemaUnavailable => {
                ErrorClass::RecordLevel
            }
            DeserErrorKind::NotReady => ErrorClass::Retryable,
            DeserErrorKind::Fatal => ErrorClass::Fatal,
        }
    }
}

/// A payload could not be deserialized.
///
/// The chain logs the reason for a skipped payload and uses it as the
/// pipeline's failure reason, so a deserializer keeps the payload's content
/// out of the reason and out of any source it attaches.
///
/// # Examples
///
/// ```
/// use spate_core::error::{DeserError, DeserErrorKind, ErrorClass};
///
/// let e = DeserError::malformed("truncated header");
/// assert_eq!(e.kind(), DeserErrorKind::Malformed);
/// assert_eq!(e.class(), ErrorClass::RecordLevel);
/// assert_eq!(e.to_string(), "malformed payload: truncated header");
/// ```
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DeserError {
    kind: DeserErrorKind,
    reason: String,
    source: Option<Source>,
}

role_error_methods!(DeserError, DeserErrorKind);

impl DeserError {
    /// A [`Malformed`](DeserErrorKind::Malformed) error.
    #[must_use]
    pub fn malformed(reason: impl Into<String>) -> DeserError {
        DeserError::new(DeserErrorKind::Malformed, reason)
    }

    /// A [`SchemaUnavailable`](DeserErrorKind::SchemaUnavailable) error.
    #[must_use]
    pub fn schema_unavailable(reason: impl Into<String>) -> DeserError {
        DeserError::new(DeserErrorKind::SchemaUnavailable, reason)
    }

    /// A [`NotReady`](DeserErrorKind::NotReady) error; `reason` says what is
    /// being waited for.
    #[must_use]
    pub fn not_ready(reason: impl Into<String>) -> DeserError {
        DeserError::new(DeserErrorKind::NotReady, reason)
    }

    /// A [`Fatal`](DeserErrorKind::Fatal) error.
    #[must_use]
    pub fn fatal(reason: impl Into<String>) -> DeserError {
        DeserError::new(DeserErrorKind::Fatal, reason)
    }
}

impl std::fmt::Display for DeserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let what = match self.kind {
            DeserErrorKind::Malformed => "malformed payload",
            DeserErrorKind::SchemaUnavailable => "schema unavailable",
            DeserErrorKind::NotReady => "not ready",
            DeserErrorKind::Fatal => "deserializer failed",
        };
        write!(f, "{what}: {}", self.reason)
    }
}

/// The constructors `SourceError` and `SinkError` share beyond the role
/// methods, for a class the producer picks.
macro_rules! client_error_methods {
    ($ty:ident, $kind:ident, $what:literal, $fatal:literal) => {
        impl $kind {
            fn class(self) -> ErrorClass {
                match self {
                    $kind::Retryable => ErrorClass::Retryable,
                    $kind::RecordLevel => ErrorClass::RecordLevel,
                    $kind::Fatal => ErrorClass::Fatal,
                }
            }

            fn from_class(class: ErrorClass) -> $kind {
                match class {
                    ErrorClass::Retryable => $kind::Retryable,
                    ErrorClass::RecordLevel => $kind::RecordLevel,
                    ErrorClass::Fatal => $kind::Fatal,
                }
            }
        }

        role_error_methods!($ty, $kind);

        impl $ty {
            #[doc = concat!("A ", $what, " error of `class` with a human-readable `reason`.")]
            ///
            /// # Examples
            ///
            /// ```
            #[doc = concat!("use spate_core::error::{ErrorClass, ", stringify!($ty), ", ", stringify!($kind), "};")]
            ///
            #[doc = concat!("let e = ", stringify!($ty), "::from_class(ErrorClass::RecordLevel, \"bad row\");")]
            #[doc = concat!("assert_eq!(e.kind(), ", stringify!($kind), "::RecordLevel);")]
            /// assert_eq!(e.reason(), "bad row");
            /// ```
            #[must_use]
            pub fn from_class(class: ErrorClass, reason: impl Into<String>) -> Self {
                Self::new($kind::from_class(class), reason)
            }

            #[doc = $fatal]
            ///
            /// # Examples
            ///
            /// ```
            #[doc = concat!("use spate_core::error::{ErrorClass, ", stringify!($ty), "};")]
            ///
            #[doc = concat!("let e = ", stringify!($ty), "::fatal(\"credentials rejected\");")]
            /// assert_eq!(e.class(), ErrorClass::Fatal);
            #[doc = concat!("assert_eq!(e.to_string(), \"", $what, " error (Fatal): credentials rejected\");")]
            /// ```
            #[must_use]
            pub fn fatal(reason: impl Into<String>) -> Self {
                Self::new($kind::Fatal, reason)
            }

            /// A transient error; retrying the same operation may succeed.
            #[must_use]
            pub fn retryable(reason: impl Into<String>) -> Self {
                Self::new($kind::Retryable, reason)
            }

            /// An error specific to one record, which retrying cannot fix.
            #[must_use]
            pub fn record_level(reason: impl Into<String>) -> Self {
                Self::new($kind::RecordLevel, reason)
            }

            /// This error with the kind for `class`, and its reason and
            /// source kept.
            #[must_use]
            pub fn with_class(mut self, class: ErrorClass) -> Self {
                self.kind = $kind::from_class(class);
                self
            }

            /// This error with `f` applied to its reason, and its kind and
            /// source kept.
            #[must_use]
            pub fn map_reason(mut self, f: impl FnOnce(String) -> String) -> Self {
                self.reason = f(self.reason);
                self
            }
        }

        impl std::fmt::Display for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, concat!($what, " error ({:?}): {}"), self.kind, self.reason)
            }
        }
    };
}

/// How the pipeline reacts to a [`SourceError`], as judged by the connector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SourceErrorKind {
    /// Transient; retrying the same operation may succeed.
    Retryable,
    /// Specific to one record; retrying the same record cannot succeed.
    RecordLevel,
    /// The source is broken; processing must stop.
    Fatal,
}

/// A source failed to poll, commit, or manage its assignment.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SourceError {
    kind: SourceErrorKind,
    reason: String,
    source: Option<Source>,
}

client_error_methods!(
    SourceError,
    SourceErrorKind,
    "source",
    "An error that stops the pipeline when [`Source::open`](crate::source::Source::open), \
     [`Source::poll_events`](crate::source::Source::poll_events), \
     [`Source::commit`](crate::source::Source::commit) or \
     [`SourceLane::poll`](crate::source::SourceLane::poll) returns it. An error \
     of any class from `pause`, `resume`, `stop_reading` or a revocation's \
     `flush_commits` is logged and the run goes on."
);

/// How the framework reacts to a [`SinkError`].
///
/// From a write, `Retryable` is retried, rotating across healthy replicas,
/// and any other kind abandons the batch. From
/// [`RowEncoder::encode`](crate::sink::RowEncoder::encode), `Fatal` stops the
/// pipeline and any other kind follows the stage's [`ErrorPolicy`]. An error
/// of any kind from
/// [`RowEncoder::finish_chunk`](crate::sink::RowEncoder::finish_chunk) or a
/// [`FrameCodec`](crate::sink::FrameCodec) stops the pipeline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SinkErrorKind {
    /// Transient; retrying the same operation may succeed.
    Retryable,
    /// Specific to one record; retrying the same record cannot succeed.
    RecordLevel,
    /// The sink is broken; processing must stop.
    Fatal,
}

/// A sink failed to write a batch.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SinkError {
    kind: SinkErrorKind,
    reason: String,
    source: Option<Source>,
}

client_error_methods!(
    SinkError,
    SinkErrorKind,
    "sink",
    "An error the framework does not retry. A write that returns it abandons \
     its batch, and an encoder or frame codec that returns it stops the \
     pipeline."
);

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

/// `err` and each error in its source chain, joined by `": "`.
///
/// It walks [`source`](std::error::Error::source) alone. An
/// [`std::io::Error`]'s `Display` already prints the error it wraps, so
/// following that error as well would print its text twice.
///
/// A link whose text the link before it already ends with is left out, since
/// many client errors print their source in their own text.
///
/// ```
/// use spate_core::error::{SourceError, chain};
///
/// let err = SourceError::retryable("listing").with_source(std::io::Error::other("connection reset"));
/// assert_eq!(chain(&err), "source error (Retryable): listing: connection reset");
/// ```
#[must_use]
pub fn chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut rendered = err.to_string();
    let mut previous = rendered.clone();
    let mut next = err.source();
    while let Some(source) = next {
        let text = source.to_string();
        if !previous.ends_with(&text) {
            rendered.push_str(": ");
            rendered.push_str(&text);
        }
        previous = text;
        next = source.source();
    }
    rendered
}

/// The TLS rejection in an error's source chain, as an
/// `Option<&rustls::Error>`: a certificate the client failed to verify, an
/// alert in [`TLS_REJECTION_ALERTS`](crate::error::TLS_REJECTION_ALERTS), or a
/// peer that shares no protocol version, cipher suite or other handshake
/// parameter with the client (`PeerIncompatible`).
///
/// The first argument is the path to the caller's `rustls` crate, such as
/// `rustls`, or the path a crate re-exports it under, and the second an
/// `&(dyn Error + 'static)`. It expands against the caller's rustls, so
/// spate-core itself does not depend on rustls.
///
/// ```ignore
/// let fatal = spate::tls_rejection!(rustls, &err).is_some();
/// ```
#[macro_export]
macro_rules! tls_rejection {
    ($($rustls:ident)::+, $err:expr) => {
        $crate::error::find_source::<$($rustls)::+::Error>($err).filter(|tls| match tls {
            $($rustls)::+::Error::InvalidCertificate(_)
            | $($rustls)::+::Error::PeerIncompatible(_) => true,
            $($rustls)::+::Error::AlertReceived(alert) => {
                $crate::error::TLS_REJECTION_ALERTS.contains(&u8::from(*alert))
            }
            _ => false,
        })
    };
}

/// The reason text for a TLS error, as a `String`: the error's own text, and
/// for a certificate from an issuer the client does not trust, how to trust
/// it.
///
/// The first argument is the path to the caller's `rustls` crate, as for
/// [`tls_rejection!`], the second an `&rustls::Error`, and the third an
/// `Option<&str>` naming the config key that adds a root CA. Pass `None` when
/// the connector would refuse that key, as it does with no `https://`
/// endpoint.
///
/// ```ignore
/// let reason = spate::tls_reason!(rustls, tls, Some("tls.root_ca"));
/// ```
#[macro_export]
macro_rules! tls_reason {
    ($($rustls:ident)::+, $tls:expr, $key:expr) => {{
        let tls: &$($rustls)::+::Error = $tls;
        let unknown_issuer = matches!(
            tls,
            $($rustls)::+::Error::InvalidCertificate(
                $($rustls)::+::CertificateError::UnknownIssuer
            )
        );
        $crate::error::tls_reason_text(tls, unknown_issuer, $key)
    }};
}

/// How to trust the issuer of a certificate the client rejected, as an
/// `Option<String>`: `Some` for a certificate from an issuer the client does
/// not trust, and `None` for any other TLS error.
///
/// A connector puts the hint in its error's reason and attaches the client's
/// error as the source, which already carries the TLS error's text. The
/// arguments are those of [`tls_reason!`].
///
/// ```ignore
/// let hint = spate::tls_hint!(rustls, tls, Some("tls.root_ca"));
/// ```
#[macro_export]
macro_rules! tls_hint {
    ($($rustls:ident)::+, $tls:expr, $key:expr) => {{
        let tls: &$($rustls)::+::Error = $tls;
        let unknown_issuer = matches!(
            tls,
            $($rustls)::+::Error::InvalidCertificate(
                $($rustls)::+::CertificateError::UnknownIssuer
            )
        );
        $crate::error::tls_hint_text(unknown_issuer, $key)
    }};
}

/// The text [`tls_hint!`] returns, given whether the error is an unknown
/// issuer.
#[doc(hidden)]
#[must_use]
pub fn tls_hint_text(unknown_issuer: bool, key: Option<&str>) -> Option<String> {
    unknown_issuer.then(|| match key {
        Some(key) => format!("add the issuing CA to the system trust store or `{key}`"),
        None => "add the issuing CA to the system trust store".to_owned(),
    })
}

/// The text [`tls_reason!`] returns, given whether the error is an unknown
/// issuer.
#[doc(hidden)]
#[must_use]
pub fn tls_reason_text(
    tls: &dyn std::fmt::Display,
    unknown_issuer: bool,
    key: Option<&str>,
) -> String {
    match tls_hint_text(unknown_issuer, key) {
        Some(hint) => format!("{tls}; {hint}"),
        None => tls.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An unknown issuer names the system trust store, and the key when there
    /// is one; any other TLS error is its own text.
    #[test]
    fn tls_reason_hints_only_for_an_unknown_issuer() {
        use rustls::{CertificateError, Error as Tls};
        let unknown = Tls::InvalidCertificate(CertificateError::UnknownIssuer);
        assert_eq!(
            crate::tls_reason!(rustls, &unknown, Some("tls.root_ca")),
            "invalid peer certificate: UnknownIssuer; add the issuing CA to the system \
             trust store or `tls.root_ca`"
        );
        assert_eq!(
            crate::tls_reason!(rustls, &unknown, None),
            "invalid peer certificate: UnknownIssuer; add the issuing CA to the system \
             trust store"
        );
        let expired = Tls::InvalidCertificate(CertificateError::Expired);
        assert_eq!(
            crate::tls_reason!(rustls, &expired, Some("tls.root_ca")),
            "invalid peer certificate: Expired"
        );
        let general = Tls::General("reset".into());
        assert_eq!(
            crate::tls_reason!(rustls, &general, Some("tls.root_ca")),
            "unexpected error: reset"
        );
    }

    /// An unknown issuer gives a hint naming the system trust store, and the
    /// key when there is one; any other TLS error gives none.
    #[test]
    fn tls_hint_only_for_an_unknown_issuer() {
        use rustls::{CertificateError, Error as Tls};
        let unknown = Tls::InvalidCertificate(CertificateError::UnknownIssuer);
        assert_eq!(
            crate::tls_hint!(rustls, &unknown, Some("tls.root_ca")).as_deref(),
            Some("add the issuing CA to the system trust store or `tls.root_ca`")
        );
        assert_eq!(
            crate::tls_hint!(rustls, &unknown, None).as_deref(),
            Some("add the issuing CA to the system trust store")
        );
        let expired = Tls::InvalidCertificate(CertificateError::Expired);
        assert_eq!(
            crate::tls_hint!(rustls, &expired, Some("tls.root_ca")),
            None
        );
        let general = Tls::General("reset".into());
        assert_eq!(
            crate::tls_hint!(rustls, &general, Some("tls.root_ca")),
            None
        );
    }

    /// A certificate failure and an incompatible peer are rejections behind an
    /// `io::Error`, and other TLS errors are not. Each alert rustls names is a
    /// rejection as its row says, and no number rustls leaves unnamed is one,
    /// so every alert value is pinned.
    #[test]
    fn tls_rejection_matches_the_rejection_classes() {
        use rustls::{AlertDescription as A, CertificateError, Error as Tls};
        let rejects = |tls: Tls| {
            let err = std::io::Error::new(std::io::ErrorKind::InvalidData, tls);
            crate::tls_rejection!(rustls, &err).is_some()
        };
        for (name, tls, rejected) in [
            (
                "InvalidCertificate",
                Tls::InvalidCertificate(CertificateError::UnknownIssuer),
                true,
            ),
            (
                "PeerIncompatible",
                Tls::PeerIncompatible(rustls::PeerIncompatible::NoCipherSuitesInCommon),
                true,
            ),
            ("General", Tls::General("reset".into()), false),
        ] {
            assert_eq!(rejects(tls), rejected, "{name}");
        }
        for (alert, rejected) in [
            (A::CloseNotify, false),
            (A::UnexpectedMessage, false),
            (A::BadRecordMac, false),
            (A::DecryptionFailed, false),
            (A::RecordOverflow, false),
            (A::DecompressionFailure, false),
            (A::HandshakeFailure, true),
            (A::NoCertificate, false),
            (A::BadCertificate, true),
            (A::UnsupportedCertificate, true),
            (A::CertificateRevoked, true),
            (A::CertificateExpired, true),
            (A::CertificateUnknown, true),
            (A::IllegalParameter, false),
            (A::UnknownCA, true),
            (A::AccessDenied, true),
            (A::DecodeError, false),
            (A::DecryptError, true),
            (A::ExportRestriction, false),
            (A::ProtocolVersion, true),
            (A::InsufficientSecurity, true),
            (A::InternalError, false),
            (A::InappropriateFallback, false),
            (A::UserCanceled, false),
            (A::NoRenegotiation, false),
            (A::MissingExtension, true),
            (A::UnsupportedExtension, true),
            (A::CertificateUnobtainable, false),
            (A::UnrecognisedName, true),
            (A::BadCertificateStatusResponse, false),
            (A::BadCertificateHashValue, false),
            (A::UnknownPSKIdentity, false),
            (A::CertificateRequired, true),
            (A::NoApplicationProtocol, true),
            (A::EncryptedClientHelloRequired, false),
        ] {
            assert_eq!(rejects(Tls::AlertReceived(alert)), rejected, "{alert:?}");
        }
        for n in 0..=u8::MAX {
            let alert = A::from(n);
            if matches!(alert, A::Unknown(_)) {
                assert!(!rejects(Tls::AlertReceived(alert)), "{alert:?}");
            }
        }
    }

    /// Each constructor builds the kind it names, which maps to its class,
    /// with the reason given, and both types render it in their `Display`
    /// text.
    #[test]
    fn source_and_sink_constructors_carry_their_kind_and_class() {
        use ErrorClass::{Fatal, RecordLevel, Retryable};
        let sources = [
            (SourceError::fatal("broken"), SourceErrorKind::Fatal, Fatal),
            (
                SourceError::retryable("broken"),
                SourceErrorKind::Retryable,
                Retryable,
            ),
            (
                SourceError::record_level("broken"),
                SourceErrorKind::RecordLevel,
                RecordLevel,
            ),
            (
                SourceError::from_class(Fatal, "broken"),
                SourceErrorKind::Fatal,
                Fatal,
            ),
            (
                SourceError::from_class(Retryable, "broken"),
                SourceErrorKind::Retryable,
                Retryable,
            ),
            (
                SourceError::from_class(RecordLevel, "broken"),
                SourceErrorKind::RecordLevel,
                RecordLevel,
            ),
            (
                SourceError::new(SourceErrorKind::Fatal, "broken"),
                SourceErrorKind::Fatal,
                Fatal,
            ),
        ];
        for (e, kind, class) in sources {
            assert_eq!((e.kind(), e.class(), e.reason()), (kind, class, "broken"));
            assert_eq!(e.to_string(), format!("source error ({kind:?}): broken"));
        }
        let sinks = [
            (SinkError::fatal("broken"), SinkErrorKind::Fatal, Fatal),
            (
                SinkError::retryable("broken"),
                SinkErrorKind::Retryable,
                Retryable,
            ),
            (
                SinkError::record_level("broken"),
                SinkErrorKind::RecordLevel,
                RecordLevel,
            ),
            (
                SinkError::from_class(Fatal, "broken"),
                SinkErrorKind::Fatal,
                Fatal,
            ),
            (
                SinkError::from_class(Retryable, "broken"),
                SinkErrorKind::Retryable,
                Retryable,
            ),
            (
                SinkError::from_class(RecordLevel, "broken"),
                SinkErrorKind::RecordLevel,
                RecordLevel,
            ),
            (
                SinkError::new(SinkErrorKind::Retryable, "broken"),
                SinkErrorKind::Retryable,
                Retryable,
            ),
        ];
        for (e, kind, class) in sinks {
            assert_eq!((e.kind(), e.class(), e.reason()), (kind, class, "broken"));
            assert_eq!(e.to_string(), format!("sink error ({kind:?}): broken"));
        }
    }

    /// Each deserializer constructor builds the kind it names, with its class
    /// and its own `Display` prefix.
    #[test]
    fn deser_constructors_carry_their_kind_class_and_text() {
        let cases = [
            (
                DeserError::malformed("x"),
                DeserErrorKind::Malformed,
                ErrorClass::RecordLevel,
                "malformed payload: x",
            ),
            (
                DeserError::schema_unavailable("x"),
                DeserErrorKind::SchemaUnavailable,
                ErrorClass::RecordLevel,
                "schema unavailable: x",
            ),
            (
                DeserError::not_ready("x"),
                DeserErrorKind::NotReady,
                ErrorClass::Retryable,
                "not ready: x",
            ),
            (
                DeserError::fatal("x"),
                DeserErrorKind::Fatal,
                ErrorClass::Fatal,
                "deserializer failed: x",
            ),
        ];
        for (e, kind, class, text) in cases {
            assert_eq!((e.kind(), e.class(), e.reason()), (kind, class, "x"));
            assert_eq!(e.to_string(), text);
            assert_eq!(DeserError::new(kind, "x").to_string(), text);
        }
    }

    /// An attached source is what `source()` returns, and `find_source`
    /// reaches it through every role error defined here. Without one,
    /// `source()` is none. The feed module tests `FeedError`.
    #[test]
    fn with_source_is_the_source_of_every_role_error() {
        let errors: [Box<dyn std::error::Error>; 3] = [
            Box::new(SourceError::fatal("a").with_source(Target(1))),
            Box::new(SinkError::fatal("a").with_source(Target(2))),
            Box::new(DeserError::fatal("a").with_source(Target(3))),
        ];
        for (want, err) in (1..).zip(&errors) {
            let source = err.source().expect("the attached source");
            assert_eq!(source.to_string(), format!("target {want}"));
            assert_eq!(find_source::<Target>(err.as_ref()).map(|t| t.0), Some(want));
        }
        assert!(std::error::Error::source(&SourceError::fatal("a")).is_none());
        assert!(std::error::Error::source(&DeserError::fatal("a")).is_none());
    }

    /// `with_class` keeps the reason and the source, and `map_reason` keeps
    /// the kind and the source.
    #[test]
    fn with_class_and_map_reason_keep_the_rest() {
        let sink = SinkError::fatal("a").with_class(ErrorClass::Retryable);
        assert_eq!(sink.to_string(), "sink error (Retryable): a");
        let source = SourceError::retryable("a").map_reason(|r| format!("{r}; b"));
        assert_eq!(source.to_string(), "source error (Retryable): a; b");
        let sink = SinkError::record_level("a").map_reason(|r| format!("{r}; b"));
        assert_eq!(sink.to_string(), "sink error (RecordLevel): a; b");
        let source = SourceError::fatal("a").with_class(ErrorClass::RecordLevel);
        assert_eq!(source.to_string(), "source error (RecordLevel): a");

        let kept = |e: &(dyn std::error::Error + 'static)| find_source::<Target>(e).map(|t| t.0);
        let source = SourceError::fatal("a").with_source(Target(5));
        assert_eq!(
            kept(&source.clone().with_class(ErrorClass::Retryable)),
            Some(5)
        );
        assert_eq!(kept(&source.map_reason(|r| r + "; b")), Some(5));
        let sink = SinkError::fatal("a").with_source(Target(6));
        assert_eq!(
            kept(&sink.clone().with_class(ErrorClass::Retryable)),
            Some(6)
        );
        assert_eq!(kept(&sink.map_reason(|r| r + "; b")), Some(6));
    }

    #[test]
    fn defaults_match_documented_policy() {
        assert_eq!(ErrorPolicy::default(), ErrorPolicy::Fail);
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

    #[derive(Debug, thiserror::Error)]
    #[error("outer")]
    struct Outer(#[source] Middle);

    #[derive(Debug, thiserror::Error)]
    #[error("middle")]
    struct Middle(#[source] Target);

    #[test]
    fn chain_joins_every_link() {
        assert_eq!(chain(&Outer(Middle(Target(3)))), "outer: middle: target 3");
    }

    /// An error with fixed text and an optional source.
    #[derive(Debug)]
    struct Link(&'static str, Option<Box<Link>>);

    impl std::fmt::Display for Link {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for Link {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1
                .as_deref()
                .map(|l| l as &(dyn std::error::Error + 'static))
        }
    }

    fn links(texts: &[&'static str]) -> Link {
        texts
            .iter()
            .rev()
            .fold(None, |source, text| Some(Link(text, source.map(Box::new))))
            .expect("at least one link")
    }

    /// A source whose text its parent already ends with is printed once, and
    /// the links after it still follow.
    #[test]
    fn chain_prints_a_repeated_source_once() {
        let err = links(&["outer: middle", "middle", "target"]);
        assert_eq!(chain(&err), "outer: middle: target");
    }

    /// Two adjacent links with the same text print once.
    #[test]
    fn chain_collapses_equal_links() {
        assert_eq!(chain(&links(&["same", "same", "target"])), "same: target");
    }

    /// A source whose text appears in its parent's text, though not at its
    /// end, is printed.
    #[test]
    fn chain_keeps_a_source_its_parent_only_mentions() {
        let err = links(&["x middle y", "middle"]);
        assert_eq!(chain(&err), "x middle y: middle");
    }

    /// The error an `io::Error` wraps renders once, through the `io::Error`'s
    /// own text, and its source follows.
    #[test]
    fn chain_renders_the_error_an_io_error_wraps_once() {
        let err = Wrapper(std::io::Error::other(Middle(Target(9))));
        assert_eq!(chain(&err), "wrapper: middle: target 9");
    }
}
