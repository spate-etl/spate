//! A private CA for TLS tests, and a local NATS server stub that upgrades to
//! TLS with a certificate the CA signed.

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig};

pub(crate) struct TestCa {
    name: String,
    der: CertificateDer<'static>,
    issuer: Issuer<'static, KeyPair>,
}

impl TestCa {
    pub(crate) fn new(name: &str) -> TestCa {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name.push(DnType::CommonName, name);
        let key = KeyPair::generate().unwrap();
        let der = params.self_signed(&key).unwrap().der().clone();
        TestCa {
            name: name.to_owned(),
            der,
            issuer: Issuer::new(params, key),
        }
    }

    pub(crate) fn der(&self) -> CertificateDer<'static> {
        self.der.clone()
    }

    /// Writes the CA certificate as PEM into `dir` and returns its path.
    pub(crate) fn write(&self, dir: &Path) -> PathBuf {
        let path = dir.join(format!("{}.pem", self.name));
        std::fs::write(&path, pem("CERTIFICATE", &self.der)).unwrap();
        path
    }

    /// Writes a certificate and PKCS#8 key this CA signed for `127.0.0.1`
    /// into `dir`, and returns their paths.
    pub(crate) fn write_identity(&self, dir: &Path) -> (PathBuf, PathBuf) {
        let (cert, key) = self.leaf();
        let cert_path = dir.join(format!("{}-leaf.pem", self.name));
        let key_path = dir.join(format!("{}-leaf.key", self.name));
        std::fs::write(&cert_path, pem("CERTIFICATE", &cert)).unwrap();
        std::fs::write(&key_path, pem("PRIVATE KEY", key.secret_pkcs8_der())).unwrap();
        (cert_path, key_path)
    }

    fn leaf(&self) -> (CertificateDer<'static>, PrivatePkcs8KeyDer<'static>) {
        let key = KeyPair::generate().unwrap();
        let cert = CertificateParams::new(vec!["127.0.0.1".to_owned()])
            .unwrap()
            .signed_by(&key, &self.issuer)
            .unwrap();
        (
            cert.der().clone(),
            PrivatePkcs8KeyDer::from(key.serialize_der()),
        )
    }

    /// Serves the NATS handshake on `127.0.0.1` and returns the port. Each
    /// connection gets an `INFO` with `tls_required`, is upgraded to TLS over
    /// a certificate this CA signed, and has every `PING` answered. With
    /// `client_ca`, the server requires a client certificate that CA signed.
    /// The server runs until the runtime shuts down.
    pub(crate) async fn serve_nats(&self, client_ca: Option<&TestCa>) -> u16 {
        let provider = Arc::new(tokio_rustls::rustls::crypto::aws_lc_rs::default_provider());
        let builder = ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap();
        let builder = match client_ca {
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
        let (cert, key) = self.leaf();
        let config = builder
            .with_single_cert(vec![cert], PrivateKeyDer::Pkcs8(key))
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let info = format!(
            "INFO {{\"server_id\":\"test\",\"version\":\"2.10.0\",\"proto\":1,\
             \"host\":\"127.0.0.1\",\"port\":{port},\"max_payload\":1048576,\
             \"tls_required\":true}}\r\n"
        );
        tokio::spawn(async move {
            while let Ok((mut tcp, _)) = listener.accept().await {
                let (acceptor, info) = (acceptor.clone(), info.clone());
                tokio::spawn(async move {
                    if tcp.write_all(info.as_bytes()).await.is_err() {
                        return;
                    }
                    let Ok(tls) = acceptor.accept(tcp).await else {
                        return;
                    };
                    let mut tls = BufReader::new(tls);
                    let mut line = String::new();
                    while tls.read_line(&mut line).await.is_ok_and(|n| n > 0) {
                        if line.starts_with("PING")
                            && tls.get_mut().write_all(b"PONG\r\n").await.is_err()
                        {
                            return;
                        }
                        line.clear();
                    }
                });
            }
        });
        port
    }
}

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
