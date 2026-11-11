//! The HTTP(S) client behind each replica endpoint, the roots it verifies
//! `https://` replicas against, and the certificate rejections it reports.

use crate::config::TlsSection;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use rustls::crypto::aws_lc_rs;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use rustls::{CertificateError, ClientConfig, RootCertStore};
use rustls_native_certs::CertificateResult;
use spate_core::clock::SyncClock;
use spate_core::task::{BoundError, ThreadBound};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

// Both values match the `clickhouse` crate's default client.
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

/// The roots `https://` replicas are verified against. These are the
/// certificates `system` yields, or the Mozilla bundle when none of them parse,
/// plus every certificate in `tls.root_ca`.
///
/// Returns an empty store without calling `system` when `needs_tls` is false.
/// An unusable `tls.root_ca` fails with its path and the reason, which the
/// caller anchors at that key.
pub(crate) fn root_store(
    tls: &TlsSection,
    needs_tls: bool,
    system: impl FnOnce() -> CertificateResult,
) -> Result<RootCertStore, String> {
    let mut roots = RootCertStore::empty();
    if !needs_tls {
        return Ok(roots);
    }
    let loaded = system();
    if !loaded.errors.is_empty() {
        tracing::warn!(errors = ?loaded.errors, "sink.clickhouse: errors reading the system trust store");
    }
    let (added, unparsable) = roots.add_parsable_certificates(loaded.certs);
    if added == 0 {
        let problem = if unparsable == 0 {
            "the system trust store has no certificates".to_owned()
        } else {
            format!("none of the {unparsable} certificates in the system trust store parsed")
        };
        tracing::warn!(
            "sink.clickhouse: {problem}; verifying https replicas against the Mozilla root bundle"
        );
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    if let Some(path) = &tls.root_ca {
        add_root_ca(&mut roots, path)?;
    }
    Ok(roots)
}

fn add_root_ca(roots: &mut RootCertStore, path: &Path) -> Result<(), String> {
    let fail = |why: String| format!("`{}`: {why}", path.display());
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(|e| fail(e.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| fail(e.to_string()))?;
    if certs.is_empty() {
        return Err(fail("no PEM certificates found".into()));
    }
    for cert in certs {
        roots.add(cert).map_err(|e| fail(e.to_string()))?;
    }
    Ok(())
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

/// The `timeouts.startup` bound on each request a sink sends a replica at
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

    /// `request`'s answer, or the reason it has none.
    pub(crate) async fn request<T>(
        &self,
        request: impl Future<Output = Result<T, clickhouse::error::Error>>,
    ) -> Result<T, String> {
        let Some(limit) = self.limit else {
            return request.await.map_err(|e| error_reason(&e));
        };
        let bound = ThreadBound::new("spate-clickhouse-startup-bound", limit)
            .with_clock(self.clock.clone());
        match bound.run(request).await {
            Ok(answer) => answer.map_err(|e| error_reason(&e)),
            Err(BoundError::Elapsed) => Err(format!(
                "no answer within {limit:?}, the `timeouts.startup` bound"
            )),
            Err(other) => Err(other.to_string()),
        }
    }
}

/// `err`'s message, with the TLS rejection behind it appended.
pub(crate) fn error_reason(err: &clickhouse::error::Error) -> String {
    match spate_core::tls_rejection!(rustls, err) {
        Some(rustls::Error::InvalidCertificate(cert)) => {
            let hint = match cert {
                CertificateError::UnknownIssuer => {
                    "; add the issuing CA to the system trust store or `tls.root_ca`"
                }
                _ => "",
            };
            format!("{err}: invalid peer certificate: {cert}{hint}")
        }
        Some(tls) => format!("{err}: {tls}"),
        None => err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_tls::{TestCa, is_unknown_issuer, serve};
    use spate_test_support::native_certs;
    use std::sync::mpsc;

    /// A startup request with no answer fails at its bound and names the
    /// key, one that answers passes its answer through, and a bound past the
    /// clock's range, or none, waits for the answer.
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
            Err("no answer within 30s, the `timeouts.startup` bound".to_owned())
        );
        for limit in [Some(limit), Some(Duration::MAX), None] {
            let answered = std::future::ready::<Answer>(Ok(7));
            assert_eq!(
                io.block_on(bound(limit).request(answered)),
                Ok(7),
                "{limit:?}"
            );
        }
        let refused = std::future::ready::<Answer>(Err(clickhouse::error::Error::Custom(
            "refused".to_owned(),
        )));
        let reason = io
            .block_on(bound(Some(limit)).request(refused))
            .unwrap_err();
        assert!(reason.ends_with("refused"), "{reason}");
    }

    fn with_root_ca(path: &Path) -> TlsSection {
        TlsSection {
            root_ca: Some(path.to_path_buf()),
        }
    }

    /// `tls.root_ca` adds to the system roots, and a non-empty system store
    /// keeps the Mozilla bundle out.
    #[test]
    fn root_ca_is_merged_with_the_system_roots() {
        let dir = tempfile::tempdir().unwrap();
        let (system, extra) = (TestCa::new("system"), TestCa::new("extra"));
        let roots = root_store(&with_root_ca(&extra.write(dir.path())), true, || {
            native_certs(vec![system.der()])
        })
        .unwrap();
        assert_eq!(roots.len(), 2);
    }

    /// A store that yielded only certificates rustls refuses says so, and an
    /// empty one keeps its own line, and both take the Mozilla bundle.
    /// Regression for #1444.
    #[test]
    fn an_unparsable_store_is_not_called_empty() {
        let unparsable = rustls::pki_types::CertificateDer::from(vec![0u8; 8]);
        let lines = spate_test::capture_logs(tracing::Level::WARN, || {
            let certs = vec![unparsable.clone(), unparsable];
            let roots = root_store(&TlsSection::default(), true, || native_certs(certs)).unwrap();
            assert_eq!(roots.len(), webpki_roots::TLS_SERVER_ROOTS.len());
        });
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].contains("none of the 2 certificates in the system trust store parsed"),
            "{lines:?}"
        );
        let lines = spate_test::capture_logs(tracing::Level::WARN, || {
            root_store(&TlsSection::default(), true, || native_certs(Vec::new())).unwrap();
        });
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("has no certificates"), "{lines:?}");
    }

    /// An empty system store falls back to the Mozilla bundle, and
    /// `tls.root_ca` still adds to it.
    #[test]
    fn an_empty_system_store_falls_back_to_the_mozilla_roots() {
        let dir = tempfile::tempdir().unwrap();
        let ca = TestCa::new("extra");
        let roots = root_store(&TlsSection::default(), true, CertificateResult::default).unwrap();
        assert_eq!(roots.len(), webpki_roots::TLS_SERVER_ROOTS.len());
        let roots = root_store(
            &with_root_ca(&ca.write(dir.path())),
            true,
            CertificateResult::default,
        )
        .unwrap();
        assert_eq!(roots.len(), webpki_roots::TLS_SERVER_ROOTS.len() + 1);
    }

    /// A `tls.root_ca` that is missing or holds no certificate fails with its
    /// path in the message.
    #[test]
    fn an_unusable_root_ca_fails() {
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("empty.pem");
        std::fs::write(&empty, "not a certificate\n").unwrap();
        for path in [dir.path().join("missing.pem"), empty] {
            let err =
                root_store(&with_root_ca(&path), true, CertificateResult::default).unwrap_err();
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
            native_certs(vec![])
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
            native_certs(vec![other.der()])
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
