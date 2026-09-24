//! The HTTP(S) client behind each replica endpoint, the roots it verifies
//! `https://` replicas against, and the certificate rejections it reports.

use crate::config::TlsSection;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use rustls::{CertificateError, ClientConfig, RootCertStore};
use rustls_native_certs::CertificateResult;
use spate_core::config::ConfigError;
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

// Both values match the `clickhouse` crate's default client.
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

/// The roots `https://` replicas are verified against. These are the
/// certificates `system` yields, or the Mozilla bundle when it yields none,
/// plus every certificate in `tls.root_ca`.
///
/// Returns an empty store without calling `system` when `needs_tls` is false.
pub(crate) fn root_store(
    tls: &TlsSection,
    needs_tls: bool,
    system: impl FnOnce() -> CertificateResult,
) -> Result<RootCertStore, ConfigError> {
    let mut roots = RootCertStore::empty();
    if !needs_tls {
        return Ok(roots);
    }
    let loaded = system();
    if !loaded.errors.is_empty() {
        tracing::warn!(errors = ?loaded.errors, "sink.clickhouse: errors reading the system trust store");
    }
    let (added, _unparsable) = roots.add_parsable_certificates(loaded.certs);
    if added == 0 {
        tracing::warn!(
            "sink.clickhouse: the system trust store has no certificates; \
             verifying https replicas against the Mozilla root bundle"
        );
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    if let Some(path) = &tls.root_ca {
        add_root_ca(&mut roots, path)?;
    }
    Ok(roots)
}

fn add_root_ca(roots: &mut RootCertStore, path: &Path) -> Result<(), ConfigError> {
    let fail = |why: String| {
        ConfigError::Validation(format!(
            "sink.clickhouse: tls.root_ca `{}`: {why}",
            path.display()
        ))
    };
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
    ClientConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
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

/// The certificate rejection in `err`'s source chain, if the TLS handshake
/// failed to verify the server.
pub(crate) fn certificate_error<'a>(
    err: &'a (dyn Error + 'static),
) -> Option<&'a CertificateError> {
    let mut pending = vec![err];
    while let Some(e) = pending.pop() {
        if let Some(rustls::Error::InvalidCertificate(cert)) = e.downcast_ref() {
            return Some(cert);
        }
        // `io::Error::source` skips the error it wraps, so reach it through
        // `get_ref`.
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

/// `err`'s message, with the certificate rejection behind it appended.
pub(crate) fn error_reason(err: &clickhouse::error::Error) -> String {
    let Some(cert) = certificate_error(err) else {
        return err.to_string();
    };
    let hint = match cert {
        CertificateError::UnknownIssuer => {
            "; add the issuing CA to the system trust store or `tls.root_ca`"
        }
        _ => "",
    };
    format!("{err}: invalid peer certificate: {cert}{hint}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_tls::{TestCa, is_unknown_issuer};

    fn loaded(certs: Vec<CertificateDer<'static>>) -> CertificateResult {
        let mut result = CertificateResult::default();
        result.certs = certs;
        result
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
            loaded(vec![system.der()])
        })
        .unwrap();
        assert_eq!(roots.len(), 2);
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
            let err = root_store(&with_root_ca(&path), true, CertificateResult::default)
                .unwrap_err()
                .to_string();
            assert!(err.contains("tls.root_ca"), "{err}");
            assert!(err.contains(&path.display().to_string()), "{err}");
        }
    }

    /// A replica whose certificate chains to `tls.root_ca` is trusted with an
    /// empty system store.
    #[tokio::test]
    async fn a_replica_signed_by_root_ca_is_trusted() {
        let dir = tempfile::tempdir().unwrap();
        let ca = TestCa::new("private");
        let url = ca.serve().await;
        let roots = root_store(&with_root_ca(&ca.write(dir.path())), true, || {
            loaded(vec![])
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
        let url = server_ca.serve().await;
        let roots = root_store(&TlsSection::default(), true, || loaded(vec![other.der()])).unwrap();
        let err = client(&client_config(roots))
            .with_url(url)
            .query("SELECT 1")
            .execute()
            .await
            .unwrap_err();
        assert!(is_unknown_issuer(&err), "{err:?}");
    }
}
