//! Mapping `object_store` failures into the framework's error taxonomy.

use crate::unit::Origin;
use spate_core::error::ErrorClass;

/// Classify an `object_store` error for a **data** operation (list / GET).
///
/// [`ErrorClass::Fatal`] here means *not worth retrying in place*, which is
/// a narrower claim than "kill the pipeline". How far a non-retryable
/// failure reaches is [`is_pipeline_fatal`]'s decision, by scope:
///
/// - `NotFound` / `Precondition` and friends mean the planned key set moved
///   under the backfill (a deleted key, an overwritten object failing its
///   `if_match`). Retrying the same read cannot help, but the fact is about
///   **one object**, so it becomes split poison rather than a fleet-wide
///   failure. A failed precondition on a notification unit that names no
///   version is an overwrite instead, which [`is_overwrite`] decides.
/// - Authentication, permission, and configuration errors hold for every
///   object on every instance: non-retryable *and* pipeline-fatal. A listing
///   the store answers 401 or 403 is non-retryable, and the planner fails the
///   pipeline on it.
/// - A TLS rejection ([`tls_rejection!`](spate_core::tls_rejection)) by the
///   store or the credential endpoint is non-retryable and pipeline-fatal.
/// - Everything else (other `Generic` failures, timeouts, 5xx) is retryable.
pub(crate) fn classify(e: &object_store::Error) -> ErrorClass {
    use object_store::Error as E;
    match e {
        _ if list_rejected(e)
            || delete_rejected(e)
            || spate_core::tls_rejection!(rustls, e).is_some() =>
        {
            ErrorClass::Fatal
        }
        E::NotFound { .. }
        | E::Precondition { .. }
        | E::NotModified { .. }
        | E::AlreadyExists { .. }
        | E::InvalidPath { .. }
        | E::NotSupported { .. }
        | E::NotImplemented { .. }
        | E::PermissionDenied { .. }
        | E::Unauthenticated { .. }
        | E::UnknownConfigurationKey { .. } => ErrorClass::Fatal,
        _ => ErrorClass::Retryable,
    }
}

/// Whether a non-retryable **data-read** failure condemns the whole
/// pipeline rather than just the object being read.
///
/// Credentials, permissions, TLS rejections and client misconfiguration hold
/// for every object on every instance, and no peer will fare better, so they
/// stay pipeline-fatal. The rest of the non-retryable classes (`NotFound`,
/// `Precondition`, `NotModified`, `AlreadyExists`) are facts about **one
/// object** (deleted after planning, overwritten under its ETag pin) and
/// are handled as split poison: the split is handed back, retried
/// elsewhere, and quarantined at the attempt cap instead of killing a
/// fleet-wide backfill.
pub(crate) fn is_pipeline_fatal(e: &object_store::Error) -> bool {
    use object_store::Error as E;
    spate_core::tls_rejection!(rustls, e).is_some()
        || matches!(
            e,
            E::InvalidPath { .. }
                | E::NotSupported { .. }
                | E::NotImplemented { .. }
                | E::PermissionDenied { .. }
                | E::Unauthenticated { .. }
                | E::UnknownConfigurationKey { .. }
        )
}

/// Whether `e` is a listing the store answered with 401 or 403.
///
/// object_store reports a failed listing as `Generic` and keeps the error types
/// in its chain private, so this matches each link by its message prefix. The
/// tests against a local server fail if that text changes.
fn list_rejected(e: &object_store::Error) -> bool {
    let object_store::Error::Generic { source, .. } = e else {
        return false;
    };
    source
        .to_string()
        .starts_with("Error performing list request: ")
        && source
            .source()
            .and_then(|request| request.source())
            .is_some_and(|status| {
                status
                    .to_string()
                    .strip_prefix("Server returned non-2xx status code: ")
                    .is_some_and(|code| code.starts_with("401 ") || code.starts_with("403 "))
            })
}

/// Whether `e` is a `DeleteObjects` entry the store refused with
/// `AccessDenied`. The request itself answers 200, and `object_store` reports
/// the entry as `Generic` with a private error type, so this matches its
/// message.
fn delete_rejected(e: &object_store::Error) -> bool {
    let object_store::Error::Generic { source, .. } = e else {
        return false;
    };
    let text = source.to_string();
    text.starts_with("DeleteObjects request failed for key ")
        && text.ends_with("(code: AccessDenied)")
}

/// `e`'s message, with the TLS rejection behind it appended.
pub(crate) fn reason(e: &object_store::Error) -> String {
    match spate_core::tls_rejection!(rustls, e) {
        Some(tls) => format!("{e}: {tls}"),
        None => e.to_string(),
    }
}

/// Whether `e`, failing a read of a part from `origin`, means the key was
/// overwritten under its ETag pin: a failed precondition, or a range the
/// store cannot satisfy, on a notification unit that names no version.
///
/// RustFS answers a ranged GET that starts past the end of a smaller new
/// object with 416 before it checks `If-Match`. The read then re-enqueues
/// the key at its current ETag. A version-named read and a lister's unit keep
/// [`classify`]'s and [`poison_kind`]'s classification.
pub(crate) fn is_overwrite(e: &object_store::Error, origin: &Origin) -> bool {
    (matches!(e, object_store::Error::Precondition { .. }) || range_not_satisfiable(e))
        && matches!(origin, Origin::Notification { version: None, .. })
}

/// Whether `e` is a request the store answered with 416.
///
/// object_store reports it as `Generic` and keeps the error type that holds
/// the status private, so this matches a link in the chain by its message.
/// The tests against a local server fail if that text changes.
fn range_not_satisfiable(e: &object_store::Error) -> bool {
    let mut link: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(error) = link {
        if error
            .to_string()
            .contains("Server returned non-2xx status code: 416 ")
        {
            return true;
        }
        link = error.source();
    }
    false
}

/// Classify an object-level (non-pipeline-fatal, non-retryable) failure
/// into the bounded poison taxonomy.
pub(crate) fn poison_kind(e: &object_store::Error) -> crate::split_ctx::PoisonKind {
    use crate::split_ctx::PoisonKind;
    use object_store::Error as E;
    match e {
        E::NotFound { .. } => PoisonKind::NotFound,
        // Conditional-request failures: the content moved under its pin.
        E::Precondition { .. } | E::NotModified { .. } | E::AlreadyExists { .. } => {
            PoisonKind::EtagDrift
        }
        _ => PoisonKind::Undecodable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins that only a `DeleteObjects` entry refused with `AccessDenied` is
    /// fatal: another code on a delete, or `AccessDenied` on another request,
    /// stays retryable.
    #[test]
    fn only_a_delete_entry_refused_with_access_denied_is_fatal() {
        let generic = |text: &str| object_store::Error::Generic {
            store: "S3",
            source: text.to_owned().into(),
        };
        let cases = [
            (
                "DeleteObjects request failed for key k: Access Denied (code: AccessDenied)",
                ErrorClass::Fatal,
            ),
            (
                "DeleteObjects request failed for key k: We encountered an internal error \
                 (code: InternalError)",
                ErrorClass::Retryable,
            ),
            (
                "Error performing put request k: Access Denied (code: AccessDenied)",
                ErrorClass::Retryable,
            ),
        ];
        for (text, want) in cases {
            assert_eq!(classify(&generic(text)), want, "{text}");
        }
    }

    #[test]
    fn object_level_failures_classify_as_poison_not_pipeline_fatal() {
        use object_store::Error as E;
        let src = || "boom".into();
        // (error, non-retryable?, pipeline-fatal?)
        let table: Vec<(E, bool, bool)> = vec![
            // One object's fact: deleted after planning / overwritten
            // under the pin. Poison, never pipeline-fatal.
            (
                E::NotFound {
                    path: "k".into(),
                    source: src(),
                },
                true,
                false,
            ),
            (
                E::Precondition {
                    path: "k".into(),
                    source: src(),
                },
                true,
                false,
            ),
            // Holds for every object on every instance: pipeline-fatal.
            (
                E::PermissionDenied {
                    path: "k".into(),
                    source: src(),
                },
                true,
                true,
            ),
            (
                E::Unauthenticated {
                    path: "k".into(),
                    source: src(),
                },
                true,
                true,
            ),
            (
                E::UnknownConfigurationKey {
                    store: "s3",
                    key: "nope".into(),
                },
                true,
                true,
            ),
            // Transient transport trouble: retried in place, escalating
            // to poison only when the budget runs out.
            (
                E::Generic {
                    store: "s3",
                    source: src(),
                },
                false,
                false,
            ),
        ];
        for (e, non_retryable, fatal) in &table {
            assert_eq!(
                classify(e) != crate::error::ErrorClass::Retryable,
                *non_retryable,
                "classify({e})"
            );
            assert_eq!(is_pipeline_fatal(e), *fatal, "is_pipeline_fatal({e})");
        }
    }

    /// A listing the store answers 401 or 403 is `Fatal`; a listing answered
    /// 500 stays `Retryable`. A rejected listing that arrives typed fails the
    /// test, since `list_rejected` then matches nothing.
    #[tokio::test]
    async fn a_rejected_listing_is_fatal() {
        use futures_util::StreamExt as _;
        use object_store::ObjectStore as _;
        for (status, expected) in [
            ("403 Forbidden", ErrorClass::Fatal),
            ("401 Unauthorized", ErrorClass::Fatal),
            ("500 Internal Server Error", ErrorClass::Retryable),
        ] {
            let store = crate::test_servers::store_at(&crate::test_servers::status_server(status));
            let e = store.list(None).next().await.unwrap().unwrap_err();
            assert!(e.to_string().contains(status), "{e}");
            if expected == ErrorClass::Fatal {
                assert!(
                    matches!(e, object_store::Error::Generic { .. }),
                    "object_store reports a rejected listing as a typed error, so \
                     `list_rejected` is dead and can be deleted: {e:?}"
                );
            }
            assert_eq!(classify(&e), expected, "{e}");
        }
    }

    /// An object read the store answers 403 or 401 arrives typed, names the
    /// status, and is `Fatal` and pipeline-fatal.
    #[tokio::test]
    async fn a_rejected_read_is_fatal() {
        use object_store::ObjectStore as _;
        for status in ["403 Forbidden", "401 Unauthorized"] {
            let store = crate::test_servers::store_at(&crate::test_servers::status_server(status));
            let e = store
                .get_opts(&"k".into(), object_store::GetOptions::default())
                .await
                .unwrap_err();
            assert!(
                matches!(
                    e,
                    object_store::Error::PermissionDenied { .. }
                        | object_store::Error::Unauthenticated { .. }
                ),
                "{e:?}"
            );
            assert!(e.to_string().contains(status), "{e}");
            assert_eq!(classify(&e), ErrorClass::Fatal, "{e}");
            assert!(is_pipeline_fatal(&e), "{e}");
        }
    }

    /// A `Generic` error whose text carries a 403 is `Retryable` unless its
    /// source chain is a listing answered with that status.
    #[test]
    fn a_403_outside_a_listing_is_retryable() {
        for text in [
            "Error performing PUT http://127.0.0.1/latest/api/token in 1ms - \
             Server returned non-2xx status code: 403 Forbidden: denied",
            "Error performing list request: Server returned non-2xx status code: 403 Forbidden: ",
        ] {
            let e = object_store::Error::Generic {
                store: "S3",
                source: text.into(),
            };
            assert_eq!(classify(&e), ErrorClass::Retryable, "{e}");
            assert!(!is_pipeline_fatal(&e), "{e}");
        }
    }

    /// An error with the listing's three-link shape and a 403 is `Fatal` only
    /// when its first link is the list request.
    #[test]
    fn a_403_chain_is_fatal_only_under_a_list_request() {
        #[derive(Debug)]
        struct Link(String, Option<Box<Link>>);
        impl std::fmt::Display for Link {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
        impl std::error::Error for Link {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.1.as_deref().map(|l| l as _)
            }
        }
        let status = "Server returned non-2xx status code: 403 Forbidden: denied";
        let chain = |top: &str| {
            let request = format!("Error performing POST http://127.0.0.1/b in 1ms - {status}");
            object_store::Error::Generic {
                store: "S3",
                source: Box::new(Link(
                    format!("{top}: {request}"),
                    Some(Box::new(Link(
                        request,
                        Some(Box::new(Link(status.to_owned(), None))),
                    ))),
                )),
            }
        };
        for (top, expected) in [
            ("Error performing list request", ErrorClass::Fatal),
            (
                "Error performing CreateSession request",
                ErrorClass::Retryable,
            ),
        ] {
            let e = chain(top);
            assert_eq!(classify(&e), expected, "{e}");
        }
    }

    async fn list_error(store: &object_store::aws::AmazonS3) -> object_store::Error {
        use futures_util::StreamExt as _;
        use object_store::ObjectStore as _;
        store.list(None).next().await.unwrap().unwrap_err()
    }

    async fn get_error(store: &object_store::aws::AmazonS3) -> object_store::Error {
        use object_store::ObjectStore as _;
        store
            .get_opts(&"k".into(), object_store::GetOptions::default())
            .await
            .unwrap_err()
    }

    /// A TLS alert that rejects the handshake is `Fatal` and pipeline-fatal on
    /// a listing and a read, and the reason names it; `decode_error` stays
    /// `Retryable`.
    #[tokio::test]
    async fn a_rejecting_tls_alert_is_fatal() {
        use rustls::AlertDescription as A;
        let ca = crate::test_servers::TestCa::new("any");
        for (alert, expected) in [
            (A::HandshakeFailure, ErrorClass::Fatal),
            (A::ProtocolVersion, ErrorClass::Fatal),
            (A::DecodeError, ErrorClass::Retryable),
        ] {
            let name = format!("{alert:?}");
            let addr = spate_test::tls_alert_server(b"", u8::from(alert));
            let store = crate::test_servers::tls_store_at(&format!("https://{addr}"), &ca);
            for e in [list_error(&store).await, get_error(&store).await] {
                assert!(format!("{e:?}").contains(&name), "{e:?}");
                assert_eq!(classify(&e), expected, "{e:?}");
                assert_eq!(is_pipeline_fatal(&e), expected == ErrorClass::Fatal, "{e}");
                if expected == ErrorClass::Fatal {
                    assert!(reason(&e).contains(&name), "{}", reason(&e));
                }
            }
        }
    }

    /// A server certificate from a CA the client does not trust is `Fatal` and
    /// pipeline-fatal on a listing and a read, and the reason names the
    /// verification failure.
    #[tokio::test]
    async fn an_untrusted_certificate_is_fatal() {
        let url =
            crate::test_servers::serve(&crate::test_servers::TestCa::new("server"), None).await;
        let store =
            crate::test_servers::tls_store_at(&url, &crate::test_servers::TestCa::new("other"));
        for e in [list_error(&store).await, get_error(&store).await] {
            assert_eq!(classify(&e), ErrorClass::Fatal, "{e:?}");
            assert!(is_pipeline_fatal(&e), "{e}");
            assert!(
                matches!(
                    spate_core::tls_rejection!(rustls, &e),
                    Some(rustls::Error::InvalidCertificate(_))
                ),
                "{e:?}"
            );
            assert!(reason(&e).contains("UnknownIssuer"), "{}", reason(&e));
        }
    }

    /// A TLS 1.3 server that refuses the client for presenting no certificate
    /// fails a listing as `Fatal`, and the reason names `CertificateRequired`.
    #[tokio::test]
    async fn a_refused_client_certificate_is_fatal() {
        let server = crate::test_servers::TestCa::new("server");
        let clients = crate::test_servers::TestCa::new("clients");
        let url = crate::test_servers::serve(&server, Some(&clients)).await;
        let e = list_error(&crate::test_servers::tls_store_at(&url, &server)).await;
        assert_eq!(classify(&e), ErrorClass::Fatal, "{e:?}");
        assert!(reason(&e).contains("CertificateRequired"), "{}", reason(&e));
    }

    fn notification(version: Option<&str>) -> Origin {
        Origin::Notification {
            version: version.map(str::to_owned),
            re_enqueues: 0,
        }
    }

    fn not_found() -> object_store::Error {
        object_store::Error::NotFound {
            path: "k".into(),
            source: "missing".into(),
        }
    }

    fn denied() -> object_store::Error {
        object_store::Error::PermissionDenied {
            path: "k".into(),
            source: "denied".into(),
        }
    }

    fn precondition() -> object_store::Error {
        object_store::Error::Precondition {
            path: "k".into(),
            source: "moved".into(),
        }
    }

    /// Pins that a 404 on a notification unit's read poisons the unit, as it
    /// does a lister's: it is no overwrite and no pipeline failure.
    #[test]
    fn a_404_on_a_notification_unit_poisons_it() {
        let e = not_found();
        assert_eq!(classify(&e), ErrorClass::Fatal);
        assert!(!is_pipeline_fatal(&e));
        assert!(!is_overwrite(&e, &notification(None)));
        assert_eq!(poison_kind(&e), crate::split_ctx::PoisonKind::NotFound);
    }

    /// Pins that a 403 on a notification unit's read fails the pipeline, as
    /// it does a lister's.
    #[test]
    fn a_403_on_a_notification_unit_fails_the_pipeline() {
        let e = denied();
        assert_eq!(classify(&e), ErrorClass::Fatal);
        assert!(is_pipeline_fatal(&e));
        assert!(!is_overwrite(&e, &notification(None)));
    }

    /// Pins that only a failed precondition on a notification unit naming no
    /// version is an overwrite; a lister's unit and a version-named read keep
    /// the poison classification, and a 404 or 403 is never one.
    #[test]
    fn only_a_412_on_a_notification_without_a_version_is_an_overwrite() {
        let origins = [
            (Origin::Lister, false),
            (notification(None), true),
            (notification(Some("v1")), false),
        ];
        for (origin, overwrite) in origins {
            assert_eq!(
                is_overwrite(&precondition(), &origin),
                overwrite,
                "{origin:?}"
            );
            assert!(!is_overwrite(&not_found(), &origin), "{origin:?}");
            assert!(!is_overwrite(&denied(), &origin), "{origin:?}");
        }
    }

    /// Pins that a GET the store answers with 416, as RustFS does for a range
    /// past the end of a smaller new object, is an overwrite on a
    /// notification unit naming no version, and stays a retryable read
    /// failure for a lister's unit or a version-named read.
    #[tokio::test]
    async fn a_416_on_a_notification_read_is_an_overwrite() {
        use object_store::ObjectStore as _;
        let store = crate::test_servers::store_at(&crate::test_servers::status_server(
            "416 Range Not Satisfiable",
        ));
        let e = store
            .get_opts(&"k".into(), object_store::GetOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(e, object_store::Error::Generic { .. }), "{e:?}");
        assert_eq!(classify(&e), ErrorClass::Retryable, "{e}");
        assert!(is_overwrite(&e, &notification(None)), "{e}");
        assert!(!is_overwrite(&e, &notification(Some("v1"))), "{e}");
        assert!(!is_overwrite(&e, &Origin::Lister), "{e}");
        let other = crate::test_servers::store_at(&crate::test_servers::status_server(
            "500 Internal Server Error",
        ));
        let e = other
            .get_opts(&"k".into(), object_store::GetOptions::default())
            .await
            .unwrap_err();
        assert!(!is_overwrite(&e, &notification(None)), "{e}");
    }
}
