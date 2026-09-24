//! The rustls client config the NATS store hands to async-nats for every TLS
//! connection, and the roots it verifies servers against.

use super::NatsTls;
use crate::store::StoreError;
use async_nats::rustls::pki_types::pem::PemObject;
use async_nats::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use async_nats::rustls::{ClientConfig, RootCertStore};
use rustls_native_certs::CertificateResult;
use std::path::Path;
use std::sync::Arc;

/// A client config over [`root_store`], with the client identity from `tls`
/// when one is set. The flag is true when the Mozilla bundle stands in for an
/// empty system store.
pub(super) fn client_config(
    tls: Option<&NatsTls>,
    tls_certain: bool,
    system: impl FnOnce() -> CertificateResult,
) -> Result<(ClientConfig, bool), StoreError> {
    let (roots, fallback) = root_store(tls, tls_certain, system)?;
    // rustls has no process-wide default provider when a build enables both
    // `ring` and `aws-lc-rs`.
    let builder = ClientConfig::builder_with_provider(Arc::new(
        async_nats::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("ring supports rustls's default protocol versions")
    .with_root_certificates(roots);
    let identity = tls.and_then(|t| t.client_cert.as_deref().zip(t.client_key.as_deref()));
    let config = match identity {
        Some((cert_file, key_file)) => {
            let chain = read_certs("client_cert", cert_file)?;
            let key = PrivateKeyDer::from_pem_file(key_file)
                .map_err(|e| fatal("client_key", key_file, &e.to_string()))?;
            builder.with_client_auth_cert(chain, key).map_err(|e| {
                StoreError::Fatal(format!(
                    "nats.tls: client_cert `{}` and client_key `{}`: {e}",
                    cert_file.display(),
                    key_file.display()
                ))
            })?
        }
        None => builder.with_no_client_auth(),
    };
    Ok((config, fallback))
}

/// The roots servers are verified against: the certificates `system` yields,
/// or the Mozilla bundle when it yields none, plus every certificate in
/// `tls.root_ca`.
///
/// Errors from `system` are fatal when `tls_certain`, since async-nats reloads
/// the system store on every TLS connect and fails on the same errors.
pub(super) fn root_store(
    tls: Option<&NatsTls>,
    tls_certain: bool,
    system: impl FnOnce() -> CertificateResult,
) -> Result<(RootCertStore, bool), StoreError> {
    let loaded = system();
    if !loaded.errors.is_empty() {
        let errors = loaded
            .errors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ");
        if tls_certain {
            return Err(StoreError::Fatal(format!(
                "nats.tls: reading the system trust store (check SSL_CERT_FILE and \
                 SSL_CERT_DIR): {errors}"
            )));
        }
        tracing::debug!(%errors, "errors reading the system trust store");
    }
    let mut roots = RootCertStore::empty();
    let (added, _unparsable) = roots.add_parsable_certificates(loaded.certs);
    let fallback = added == 0;
    if fallback {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    if let Some(path) = tls.and_then(|t| t.root_ca.as_deref()) {
        for cert in read_certs("root_ca", path)? {
            roots
                .add(cert)
                .map_err(|e| fatal("root_ca", path, &e.to_string()))?;
        }
    }
    Ok((roots, fallback))
}

fn read_certs(field: &str, path: &Path) -> Result<Vec<CertificateDer<'static>>, StoreError> {
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(|e| fatal(field, path, &e.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| fatal(field, path, &e.to_string()))?;
    if certs.is_empty() {
        return Err(fatal(field, path, "no PEM certificates found"));
    }
    Ok(certs)
}

fn fatal(field: &str, path: &Path, why: &str) -> StoreError {
    StoreError::Fatal(format!("nats.tls.{field} `{}`: {why}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::nats::test_tls::TestCa;
    use crate::store::nats::{NatsConfig, NatsStore};
    use crate::store::{CoordinationStore, Keyspace};
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::time::Duration;

    const URL: &str = "SPATE_TEST_NATS_TLS_URL";
    const ROOT_CA: &str = "SPATE_TEST_NATS_TLS_ROOT_CA";
    const CLIENT_CERT: &str = "SPATE_TEST_NATS_TLS_CLIENT_CERT";
    const CLIENT_KEY: &str = "SPATE_TEST_NATS_TLS_CLIENT_KEY";
    const WITH_TLS: &str = "SPATE_TEST_NATS_TLS_SECTION";
    const EXPECT_REJECT: &str = "SPATE_TEST_NATS_TLS_EXPECT_REJECT";

    fn loaded(certs: Vec<CertificateDer<'static>>) -> CertificateResult {
        let mut result = CertificateResult::default();
        result.certs = certs;
        result
    }

    fn with_root_ca(path: &Path) -> NatsTls {
        NatsTls {
            root_ca: Some(path.to_path_buf()),
            ..NatsTls::default()
        }
    }

    fn fatal_message(err: StoreError) -> String {
        match err {
            StoreError::Fatal(message) => message,
            StoreError::Retryable(message) => panic!("expected Fatal, got Retryable: {message}"),
        }
    }

    /// `root_ca` adds to the system roots, and a non-empty system store keeps
    /// the Mozilla bundle out.
    #[test]
    fn root_ca_is_merged_with_the_system_roots() {
        let dir = tempfile::tempdir().unwrap();
        let (system, extra) = (TestCa::new("system"), TestCa::new("extra"));
        let tls = with_root_ca(&extra.write(dir.path()));
        let (roots, fallback) =
            root_store(Some(&tls), true, || loaded(vec![system.der()])).unwrap();
        assert_eq!(roots.len(), 2);
        assert!(!fallback);
    }

    /// An empty system store falls back to the Mozilla bundle, and `root_ca`
    /// still adds to it.
    #[test]
    fn an_empty_system_store_falls_back_to_the_mozilla_roots() {
        let dir = tempfile::tempdir().unwrap();
        let tls = with_root_ca(&TestCa::new("extra").write(dir.path()));
        let (roots, fallback) = root_store(None, true, CertificateResult::default).unwrap();
        assert_eq!(roots.len(), webpki_roots::TLS_SERVER_ROOTS.len());
        assert!(fallback);
        let (roots, _) = root_store(Some(&tls), true, CertificateResult::default).unwrap();
        assert_eq!(roots.len(), webpki_roots::TLS_SERVER_ROOTS.len() + 1);
    }

    /// Errors reading the system store are fatal when TLS is certain, and
    /// ignored otherwise.
    #[test]
    fn system_store_errors_are_fatal_only_when_tls_is_certain() {
        let system = TestCa::new("system");
        let broken = || {
            let mut result = loaded(vec![system.der()]);
            result.errors.push(rustls_native_certs::Error {
                context: "failed to read PEM from file",
                kind: rustls_native_certs::ErrorKind::Io {
                    inner: std::io::Error::other("permission denied"),
                    path: "/etc/ssl/cert.pem".into(),
                },
            });
            result
        };
        let message = fatal_message(root_store(None, true, broken).unwrap_err());
        assert!(message.contains("SSL_CERT_FILE"), "{message}");
        assert!(message.contains("permission denied"), "{message}");
        let (roots, _) = root_store(None, false, broken).unwrap();
        assert_eq!(roots.len(), 1);
    }

    /// A TLS file that is missing or holds no PEM item fails with its key and
    /// path in the message.
    #[test]
    fn an_unusable_tls_file_fails() {
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("empty.pem");
        std::fs::write(&empty, "not a certificate\n").unwrap();
        let (cert, key) = TestCa::new("client").write_identity(dir.path());
        for path in [dir.path().join("missing.pem"), empty] {
            let root_ca = with_root_ca(&path);
            let bad_cert = NatsTls {
                client_cert: Some(path.clone()),
                client_key: Some(key.clone()),
                ..NatsTls::default()
            };
            let bad_key = NatsTls {
                client_cert: Some(cert.clone()),
                client_key: Some(path.clone()),
                ..NatsTls::default()
            };
            for (tls, field) in [
                (root_ca, "root_ca"),
                (bad_cert, "client_cert"),
                (bad_key, "client_key"),
            ] {
                let err = client_config(Some(&tls), true, CertificateResult::default).unwrap_err();
                let message = fatal_message(err);
                assert!(message.contains(&format!("nats.tls.{field}")), "{message}");
                assert!(message.contains(&path.display().to_string()), "{message}");
            }
        }
    }

    /// A client certificate paired with another certificate's key fails.
    #[test]
    fn a_mismatched_client_identity_fails() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, _) = TestCa::new("one").write_identity(dir.path());
        let (_, key) = TestCa::new("two").write_identity(dir.path());
        let tls = NatsTls {
            client_cert: Some(cert),
            client_key: Some(key),
            ..NatsTls::default()
        };
        let err = client_config(Some(&tls), true, CertificateResult::default).unwrap_err();
        let message = fatal_message(err);
        assert!(message.contains("client_cert"), "{message}");
        assert!(message.contains("client_key"), "{message}");
    }

    /// Connects with the config the environment describes and asserts the
    /// TLS handshake completed, or that the server certificate was rejected.
    /// Returns false outside a child process.
    async fn child_connects() -> bool {
        let Ok(url) = std::env::var(URL) else {
            return false;
        };
        let mut config = NatsConfig::new(vec![url], "tls_test");
        if std::env::var_os(WITH_TLS).is_some() {
            config.tls = Some(NatsTls {
                root_ca: std::env::var_os(ROOT_CA).map(Into::into),
                client_cert: std::env::var_os(CLIENT_CERT).map(Into::into),
                client_key: std::env::var_os(CLIENT_KEY).map(Into::into),
            });
        }
        let store = NatsStore::new(config, Duration::from_secs(30)).unwrap();
        let err = store.get(Keyspace::Durable, "k").await.unwrap_err();
        if std::env::var_os(EXPECT_REJECT).is_some() {
            assert!(err.to_string().contains("UnknownIssuer"), "{err}");
        } else {
            // The stub reports 2.10.0, so this error follows a completed
            // handshake.
            let message = fatal_message(err);
            assert!(message.contains("too old"), "{message}");
        }
        true
    }

    /// Runs test `name` in a child process whose system trust store is the
    /// PEM file `system`, with `env` set.
    async fn run_in_child(name: &str, system: PathBuf, env: Vec<(&'static str, OsString)>) {
        let (_, module) = module_path!().split_once("::").unwrap();
        let name = format!("{module}::{name}");
        let out = tokio::task::spawn_blocking(move || {
            std::process::Command::new(std::env::current_exe().expect("test binary"))
                .args(["--exact", &name])
                .env_remove("SSL_CERT_DIR")
                .env("SSL_CERT_FILE", system)
                .envs(env)
                .output()
                .expect("spawn the test binary")
        })
        .await
        .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        // A filter that matches nothing also exits 0.
        assert!(
            out.status.success() && stdout.contains("1 passed"),
            "{stdout}{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    fn tls_url(port: u16) -> OsString {
        format!("tls://127.0.0.1:{port}").into()
    }

    /// A server whose CA is in the system store is trusted when `root_ca`
    /// names another CA. Regression for #623.
    #[tokio::test(flavor = "multi_thread")]
    async fn root_ca_adds_to_the_system_trust_store() {
        if child_connects().await {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let (system, private) = (TestCa::new("system"), TestCa::new("private"));
        let port = system.serve_nats(None).await;
        let env = vec![
            (URL, tls_url(port)),
            (WITH_TLS, "1".into()),
            (ROOT_CA, private.write(dir.path()).into()),
        ];
        run_in_child(
            "root_ca_adds_to_the_system_trust_store",
            system.write(dir.path()),
            env,
        )
        .await;
    }

    /// A server whose CA is only in `root_ca` is trusted.
    #[tokio::test(flavor = "multi_thread")]
    async fn root_ca_trusts_a_private_ca() {
        if child_connects().await {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let (system, private) = (TestCa::new("system"), TestCa::new("private"));
        let port = private.serve_nats(None).await;
        let env = vec![
            (URL, tls_url(port)),
            (WITH_TLS, "1".into()),
            (ROOT_CA, private.write(dir.path()).into()),
        ];
        run_in_child("root_ca_trusts_a_private_ca", system.write(dir.path()), env).await;
    }

    /// A server whose CA is in neither the system store nor `root_ca` fails
    /// certificate verification.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unknown_ca_is_rejected() {
        if child_connects().await {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let (system, private, unknown) = (
            TestCa::new("system"),
            TestCa::new("private"),
            TestCa::new("unknown"),
        );
        let port = unknown.serve_nats(None).await;
        let env = vec![
            (URL, tls_url(port)),
            (WITH_TLS, "1".into()),
            (ROOT_CA, private.write(dir.path()).into()),
            (EXPECT_REJECT, "1".into()),
        ];
        run_in_child("an_unknown_ca_is_rejected", system.write(dir.path()), env).await;
    }

    /// With both rustls providers compiled in and no `tls` section, a
    /// `tls://` server and a `nats://` server that requires TLS both complete
    /// the handshake. Regression for #625.
    #[tokio::test(flavor = "multi_thread")]
    async fn tls_connects_with_both_rustls_providers() {
        if child_connects().await {
            return;
        }
        assert!(
            std::panic::catch_unwind(ClientConfig::builder).is_err(),
            "this build must enable both rustls providers"
        );
        let dir = tempfile::tempdir().unwrap();
        let system = TestCa::new("system");
        let port = system.serve_nats(None).await;
        for url in [tls_url(port), format!("nats://127.0.0.1:{port}").into()] {
            run_in_child(
                "tls_connects_with_both_rustls_providers",
                system.write(dir.path()),
                vec![(URL, url)],
            )
            .await;
        }
    }

    /// A server that requires a client certificate accepts the configured one.
    #[tokio::test(flavor = "multi_thread")]
    async fn mutual_tls_presents_the_client_identity() {
        if child_connects().await {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let (system, clients) = (TestCa::new("system"), TestCa::new("clients"));
        let port = system.serve_nats(Some(&clients)).await;
        let (cert, key) = clients.write_identity(dir.path());
        let env = vec![
            (URL, tls_url(port)),
            (WITH_TLS, "1".into()),
            (CLIENT_CERT, cert.into()),
            (CLIENT_KEY, key.into()),
        ];
        run_in_child(
            "mutual_tls_presents_the_client_identity",
            system.write(dir.path()),
            env,
        )
        .await;
    }
}
