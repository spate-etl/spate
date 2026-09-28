//! A private certificate authority for TLS tests, and a local TLS server
//! presenting a certificate it signed.

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use rustls_native_certs::CertificateResult;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

/// A self-signed CA that signs leaf certificates on demand.
#[derive(Debug)]
pub struct TestCa {
    name: String,
    der: CertificateDer<'static>,
    issuer: Issuer<'static, KeyPair>,
}

impl TestCa {
    /// A new CA whose common name, and the stem of the files it writes, is
    /// `name`.
    ///
    /// # Panics
    ///
    /// Panics when key generation or signing fails.
    #[must_use]
    pub fn new(name: &str) -> TestCa {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params.distinguished_name.push(DnType::CommonName, name);
        let key = KeyPair::generate().unwrap();
        let der = params.self_signed(&key).unwrap().der().clone();
        TestCa {
            name: name.to_owned(),
            der,
            issuer: Issuer::new(params, key),
        }
    }

    /// The CA certificate.
    #[must_use]
    pub fn der(&self) -> CertificateDer<'static> {
        self.der.clone()
    }

    /// Writes the CA certificate as PEM to `<name>.pem` in `dir` and returns
    /// its path.
    ///
    /// # Panics
    ///
    /// Panics when the file cannot be written.
    pub fn write(&self, dir: &Path) -> PathBuf {
        let path = dir.join(format!("{}.pem", self.name));
        std::fs::write(&path, pem("CERTIFICATE", &self.der)).unwrap();
        path
    }

    /// A certificate this CA signed for the DNS names or IP addresses in
    /// `names`, valid for both server and client authentication, and its key.
    ///
    /// # Panics
    ///
    /// Panics when a name is invalid, or key generation or signing fails.
    #[must_use]
    pub fn leaf(&self, names: &[&str]) -> (CertificateDer<'static>, PrivatePkcs8KeyDer<'static>) {
        let key = KeyPair::generate().unwrap();
        // rcgen's default validity starts before 2019-07-01, which exempts the
        // leaf from Apple's 825-day limit.
        let mut params =
            CertificateParams::new(names.iter().map(|&n| n.to_owned()).collect::<Vec<_>>())
                .unwrap();
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        (
            cert.der().clone(),
            PrivatePkcs8KeyDer::from(key.serialize_der()),
        )
    }

    /// A server configuration presenting a [`leaf`](Self::leaf) for
    /// `127.0.0.1`, which requires a client certificate signed by `clients`
    /// when that is set.
    ///
    /// # Panics
    ///
    /// Panics when the configuration cannot be built.
    #[must_use]
    pub fn server_config(&self, clients: Option<&TestCa>) -> Arc<ServerConfig> {
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let builder = ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap();
        let builder = match clients {
            Some(ca) => {
                let mut roots = RootCertStore::empty();
                roots.add(ca.der()).unwrap();
                let verifier =
                    WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                        .build()
                        .unwrap();
                builder.with_client_cert_verifier(verifier)
            }
            None => builder.with_no_client_auth(),
        };
        let (cert, key) = self.leaf(&["127.0.0.1"]);
        Arc::new(
            builder
                .with_single_cert(vec![cert], PrivateKeyDer::Pkcs8(key))
                .unwrap(),
        )
    }
}

/// `der` as a PEM block with the label `label`, such as `CERTIFICATE`.
fn pem(label: &str, der: &[u8]) -> String {
    use base64::Engine as _;
    let body = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for line in body.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).unwrap());
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

/// A system trust store lookup that found `certs` and no errors.
#[must_use]
pub fn native_certs(certs: Vec<CertificateDer<'static>>) -> CertificateResult {
    // `CertificateResult` is non-exhaustive.
    let mut result = CertificateResult::default();
    result.certs = certs;
    result
}

/// A TLS server on `127.0.0.1` under `config`, which hands each completed
/// handshake to `serve`, and returns its address.
///
/// A connection whose handshake fails is read to its end before it closes, so
/// the client receives the server's alert before any reset. The server runs
/// until the runtime shuts down.
///
/// # Panics
///
/// Panics when it cannot bind a local port.
pub async fn serve_tls<F, Fut>(config: Arc<ServerConfig>, serve: F) -> SocketAddr
where
    F: Fn(TlsStream<TcpStream>) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let acceptor = TlsAcceptor::from(config);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let (acceptor, serve) = (acceptor.clone(), serve.clone());
            tokio::spawn(async move {
                match acceptor.accept(tcp).into_fallible().await {
                    Ok(tls) => serve(tls).await,
                    Err((_, mut tcp)) => {
                        let _ = tokio::io::copy(&mut tcp, &mut tokio::io::sink()).await;
                    }
                }
            });
        }
    });
    addr
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::ClientConfig;
    use rustls::pki_types::ServerName;
    use rustls::pki_types::pem::PemObject as _;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio_rustls::TlsConnector;

    /// Serves `ok` to each connection that completes a handshake under `server`.
    async fn serve_ok(server: Arc<ServerConfig>) -> SocketAddr {
        serve_tls(server, |mut tls| async move {
            let _ = tls.write_all(b"ok").await;
            let _ = tls.shutdown().await;
        })
        .await
    }

    /// What a client under `config` reads from `addr`.
    async fn read_from(addr: SocketAddr, config: ClientConfig) -> std::io::Result<Vec<u8>> {
        let tcp = TcpStream::connect(addr).await?;
        let name = ServerName::try_from("127.0.0.1").unwrap();
        let mut tls = TlsConnector::from(Arc::new(config))
            .connect(name, tcp)
            .await?;
        let mut out = Vec::new();
        tls.read_to_end(&mut out).await?;
        Ok(out)
    }

    fn trusting(
        ca: &TestCa,
    ) -> rustls::ConfigBuilder<ClientConfig, rustls::client::WantsClientCert> {
        let mut roots = RootCertStore::empty();
        roots.add(ca.der()).unwrap();
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
    }

    /// A client trusting the CA completes a handshake with the server.
    #[tokio::test]
    async fn a_client_trusting_the_ca_is_served() {
        let ca = TestCa::new("server");
        let addr = serve_ok(ca.server_config(None)).await;
        let read = read_from(addr, trusting(&ca).with_no_client_auth()).await;
        assert_eq!(read.unwrap(), b"ok");
    }

    /// A leaf is accepted as a client identity by a server requiring one from
    /// its CA, and a client presenting none is refused.
    #[tokio::test]
    async fn a_leaf_is_a_client_identity() {
        let (server, clients) = (TestCa::new("server"), TestCa::new("clients"));
        let addr = serve_ok(server.server_config(Some(&clients))).await;
        let (cert, key) = clients.leaf(&["client"]);
        let config = trusting(&server)
            .with_client_auth_cert(vec![cert], PrivateKeyDer::Pkcs8(key))
            .unwrap();
        assert_eq!(read_from(addr, config).await.unwrap(), b"ok");
        let refused = read_from(addr, trusting(&server).with_no_client_auth()).await;
        assert!(
            refused
                .unwrap_err()
                .to_string()
                .contains("CertificateRequired")
        );
    }

    /// The PEM file `write` produces parses back to the CA certificate.
    #[test]
    fn write_round_trips_through_pem() {
        let dir = tempfile::tempdir().unwrap();
        let ca = TestCa::new("round-trip");
        let parsed = CertificateDer::from_pem_file(ca.write(dir.path())).unwrap();
        assert_eq!(parsed, ca.der());
    }
}
