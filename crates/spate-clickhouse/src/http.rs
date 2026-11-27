//! The HTTP(S) client behind each replica endpoint, the roots it verifies
//! `https://` replicas against, and the certificate rejections it reports.

use crate::writer::class_of;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use rustls::crypto::aws_lc_rs;
use rustls::{ClientConfig, RootCertStore};
use spate_core::clock::SyncClock;
use spate_core::error::ErrorClass;
use spate_core::task::{BoundError, ThreadBound};
use spate_core::tls::{SystemRoots, TlsSection, TrustError, TrustRoots};
use std::sync::Arc;
use std::time::Duration;

// Both values match the `clickhouse` crate's default client.
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

/// The roots `https://` replicas are verified against: the usable
/// certificates `system` yields, plus every certificate in `tls.root_ca`.
///
/// Returns an empty store without calling `system` when `needs_tls` is false.
/// Fails when `tls.root_ca` cannot be used or no root remains; the caller
/// anchors the error at `tls.root_ca`.
pub(crate) fn root_store(
    tls: &TlsSection,
    needs_tls: bool,
    system: impl FnOnce() -> SystemRoots,
) -> Result<RootCertStore, TrustError> {
    let mut roots = RootCertStore::empty();
    if needs_tls {
        let trusted = TrustRoots::load(system(), tls.root_ca.as_deref())?.require_any()?;
        roots.add_parsable_certificates(trusted.into_certs());
    }
    Ok(roots)
}

/// A TLS client config over `roots`, with the `aws-lc-rs` provider.
pub(crate) fn client_config(roots: RootCertStore) -> ClientConfig {
    // rustls has no process-wide default provider when a build enables both
    // `ring` and `aws-lc-rs`.
    ClientConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("aws-lc-rs supports rustls's default protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth()
}

/// A client with its own connection pool, serving `http://` and `https://`.
pub(crate) fn client(tls: &ClientConfig) -> clickhouse::Client {
    let mut http = HttpConnector::new();
    http.set_keepalive(Some(TCP_KEEPALIVE));
    // The HTTPS wrapper hands `https://` URIs to this connector.
    http.enforce_http(false);
    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls.clone())
        .https_or_http()
        .enable_http1()
        .wrap_connector(http);
    clickhouse::Client::with_http_client(
        HyperClient::builder(TokioExecutor::new())
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .build(connector),
    )
}

/// The `startup_timeout` bound on each request a sink sends a replica at
/// startup.
///
/// A [`ThreadBound`] times it, so it fires on a runtime built without its
/// timer.
#[derive(Clone, Debug)]
pub(crate) struct StartupBound {
    /// `None` waits with no bound.
    pub(crate) limit: Option<Duration>,
    pub(crate) clock: SyncClock,
}

impl StartupBound {
    /// A bound of `limit` on the system clock.
    pub(crate) fn new(limit: Option<Duration>) -> Self {
        StartupBound {
            limit,
            clock: SyncClock::system(),
        }
    }

    /// `request`'s answer, or the reason it has none with its class.
    ///
    /// A client error is classed by [`class_of`] with `Fatal` for a variant
    /// it does not name, since a startup read has no dedup token to make a
    /// retry of an unknown failure safe to assume. A request that passes the
    /// bound, or whose timing thread does not start, is `Retryable`.
    pub(crate) async fn request<T>(
        &self,
        request: impl Future<Output = Result<T, clickhouse::error::Error>>,
    ) -> Result<T, Classed<String>> {
        let classed = |e: clickhouse::error::Error| Classed {
            class: class_of(&e, ErrorClass::Fatal),
            error: error_reason(&e),
        };
        let Some(limit) = self.limit else {
            return request.await.map_err(classed);
        };
        let bound = ThreadBound::new("spate-clickhouse-startup-bound", limit)
            .with_clock(self.clock.clone());
        match bound.run(request).await {
            Ok(answer) => answer.map_err(classed),
            Err(stopped) => Err(Classed {
                class: ErrorClass::Retryable,
                error: match stopped {
                    BoundError::Elapsed => {
                        format!("no answer within {limit:?}, the `startup_timeout` bound")
                    }
                    other => other.to_string(),
                },
            }),
        }
    }
}

/// A startup failure with the class that decides whether connecting again
/// may succeed.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Classed<E> {
    pub(crate) class: ErrorClass,
    pub(crate) error: E,
}

impl<E> Classed<E> {
    /// The same class over the error `f` builds from this one.
    pub(crate) fn map<U>(self, f: impl FnOnce(E) -> U) -> Classed<U> {
        Classed {
            class: self.class,
            error: f(self.error),
        }
    }
}

/// An error with no class of its own, such as a mismatch between the
/// configuration and the table, is `Fatal`: connecting again fails the same
/// way.
impl<E: std::error::Error> From<E> for Classed<E> {
    fn from(error: E) -> Self {
        Classed {
            class: ErrorClass::Fatal,
            error,
        }
    }
}

/// `err`'s message, with the TLS rejection behind it appended.
pub(crate) fn error_reason(err: &clickhouse::error::Error) -> String {
    // A rejection needs a TLS connection, so an `https://` URL is configured
    // and `tls.root_ca` is accepted.
    match spate_core::tls_rejection!(rustls, err) {
        Some(tls) => format!(
            "{err}: {}",
            spate_core::tls_reason!(rustls, tls, Some("tls.root_ca"))
        ),
        None => err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_tls::{TestCa, is_unknown_issuer, serve};
    use rustls::pki_types::CertificateDer;
    use std::path::Path;
    use std::sync::mpsc;

    /// A startup request with no answer fails at its bound, retryable, and
    /// names the key; one that answers passes its answer through, and a bound
    /// past the clock's range, or none, waits for the answer. A client error
    /// the classifier does not name is fatal with a bound or without one.
    #[test]
    fn a_startup_request_is_bounded() {
        type Answer = Result<u8, clickhouse::error::Error>;
        let io = crate::test_startup::runtime();
        let (clock, given) = crate::test_startup::frozen();
        let bound = |limit| StartupBound {
            limit,
            clock: given.clone(),
        };
        let limit = Duration::from_secs(30);
        let (sent, held) = mpsc::channel();
        let silent = bound(Some(limit));
        let reason = crate::test_startup::past_the_bound(&io, &clock, limit, &held, async move {
            silent
                .request(async move {
                    let _ = sent.send(());
                    std::future::pending::<Answer>().await
                })
                .await
        });
        assert_eq!(
            reason,
            Err(Classed {
                class: ErrorClass::Retryable,
                error: "no answer within 30s, the `startup_timeout` bound".to_owned(),
            })
        );
        for limit in [Some(limit), Some(Duration::MAX), None] {
            let answered = std::future::ready::<Answer>(Ok(7));
            assert_eq!(
                io.block_on(bound(limit).request(answered)),
                Ok(7),
                "{limit:?}"
            );
        }
        for limit in [Some(limit), None] {
            let refused = std::future::ready::<Answer>(Err(clickhouse::error::Error::Custom(
                "refused".to_owned(),
            )));
            let failed = io.block_on(bound(limit).request(refused)).unwrap_err();
            assert_eq!(failed.class, ErrorClass::Fatal, "{limit:?}");
            assert!(failed.error.ends_with("refused"), "{}", failed.error);
            let dropped = std::future::ready::<Answer>(Err(clickhouse::error::Error::TimedOut));
            let failed = io.block_on(bound(limit).request(dropped)).unwrap_err();
            assert_eq!(failed.class, ErrorClass::Retryable, "{limit:?}");
        }
    }

    fn with_root_ca(path: &Path) -> TlsSection {
        let mut tls = TlsSection::default();
        tls.root_ca = Some(path.to_path_buf());
        tls
    }

    /// `tls.root_ca` adds to the system roots.
    #[test]
    fn root_ca_is_merged_with_the_system_roots() {
        let dir = tempfile::tempdir().unwrap();
        let (system, extra) = (TestCa::new("system"), TestCa::new("extra"));
        let roots = root_store(&with_root_ca(&extra.write(dir.path())), true, || {
            SystemRoots::from_certs(vec![system.der()])
        })
        .unwrap();
        assert_eq!(roots.len(), 2);
    }

    /// A system store with no usable certificate fails with no `tls.root_ca`,
    /// and with one the store holds that CA alone. No bundle is added.
    #[test]
    fn an_empty_system_store_needs_root_ca() {
        let unparsable = CertificateDer::from(vec![0u8; 8]);
        for system in [Vec::new(), vec![unparsable]] {
            let err = root_store(&TlsSection::default(), true, || {
                SystemRoots::from_certs(system.clone())
            })
            .unwrap_err();
            assert!(matches!(err, TrustError::NoRoots { .. }), "{err:?}");
            let dir = tempfile::tempdir().unwrap();
            let ca = TestCa::new("extra");
            let roots = root_store(&with_root_ca(&ca.write(dir.path())), true, || {
                SystemRoots::from_certs(system)
            })
            .unwrap();
            assert_eq!(roots.len(), 1);
        }
    }

    /// With no `https://` URL the store stays empty and the system store is
    /// not read.
    #[test]
    fn no_tls_reads_no_store() {
        let roots = root_store(&TlsSection::default(), false, || {
            panic!("the trust store was read")
        })
        .unwrap();
        assert!(roots.is_empty());
    }

    /// A `tls.root_ca` that is missing or holds no certificate fails with its
    /// path in the message.
    #[test]
    fn an_unusable_root_ca_fails() {
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("empty.pem");
        std::fs::write(&empty, "not a certificate\n").unwrap();
        let system = TestCa::new("system");
        for path in [dir.path().join("missing.pem"), empty] {
            let err = root_store(&with_root_ca(&path), true, || {
                SystemRoots::from_certs(vec![system.der()])
            })
            .unwrap_err()
            .to_string();
            assert!(err.starts_with(&format!("`{}`: ", path.display())), "{err}");
        }
    }

    /// A replica whose certificate chains to `tls.root_ca` is trusted with an
    /// empty system store.
    #[tokio::test]
    async fn a_replica_signed_by_root_ca_is_trusted() {
        let dir = tempfile::tempdir().unwrap();
        let ca = TestCa::new("private");
        let url = serve(&ca, None).await;
        let roots = root_store(&with_root_ca(&ca.write(dir.path())), true, || {
            SystemRoots::from_certs(Vec::new())
        })
        .unwrap();
        client(&client_config(roots))
            .with_url(url)
            .query("SELECT 1")
            .execute()
            .await
            .expect("the private CA is trusted");
    }

    /// A replica whose CA is in neither the system store nor `tls.root_ca`
    /// fails certificate verification.
    #[tokio::test]
    async fn a_replica_signed_by_an_unknown_ca_is_rejected() {
        let (server_ca, other) = (TestCa::new("server"), TestCa::new("other"));
        let url = serve(&server_ca, None).await;
        let roots = root_store(&TlsSection::default(), true, || {
            SystemRoots::from_certs(vec![other.der()])
        })
        .unwrap();
        let err = client(&client_config(roots))
            .with_url(url)
            .query("SELECT 1")
            .execute()
            .await
            .unwrap_err();
        assert!(is_unknown_issuer(&err), "{err:?}");
    }
}
