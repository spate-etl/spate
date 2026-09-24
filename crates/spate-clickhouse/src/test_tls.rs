//! A private CA for TLS tests, and a local HTTPS server presenting a
//! certificate it signed.

use crate::writer::ClickHouseEndpoint;
use base64::Engine as _;
use hyper::Response;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{CertificateError, RootCertStore, ServerConfig};
use std::convert::Infallible;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

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
        let body = base64::engine::general_purpose::STANDARD.encode(&self.der);
        let mut pem = String::from("-----BEGIN CERTIFICATE-----\n");
        for line in body.as_bytes().chunks(64) {
            pem.push_str(std::str::from_utf8(line).unwrap());
            pem.push('\n');
        }
        pem.push_str("-----END CERTIFICATE-----\n");
        let path = dir.join(format!("{}.pem", self.name));
        std::fs::write(&path, pem).unwrap();
        path
    }

    /// Serves an empty `200 OK` to every request on `127.0.0.1`, over a
    /// certificate for that address signed by this CA, and returns the
    /// server's `https://` URL. The server runs until the runtime shuts down.
    pub(crate) async fn serve(&self) -> String {
        let key = KeyPair::generate().unwrap();
        let leaf = CertificateParams::new(vec!["127.0.0.1".to_owned()])
            .unwrap()
            .signed_by(&key, &self.issuer)
            .unwrap();
        let config = ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![leaf.der().clone()],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
        )
        .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let Ok(tls) = acceptor.accept(tcp).await else {
                        return;
                    };
                    let ok =
                        service_fn(|_| async { Ok::<_, Infallible>(Response::new(String::new())) });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(tls), ok)
                        .await;
                });
            }
        });
        format!("https://127.0.0.1:{port}")
    }
}

/// An endpoint for `url` whose client trusts `ca` and nothing else.
pub(crate) fn endpoint_trusting(ca: &TestCa, url: &str) -> ClickHouseEndpoint {
    let mut roots = RootCertStore::empty();
    roots.add(ca.der()).unwrap();
    let client = crate::http::client(&crate::http::client_config(roots)).with_url(url);
    ClickHouseEndpoint::new(client, url.to_owned())
}

/// Whether `err`'s source chain holds rustls's unknown-issuer rejection.
pub(crate) fn is_unknown_issuer(err: &(dyn Error + 'static)) -> bool {
    matches!(
        crate::http::certificate_error(err),
        Some(CertificateError::UnknownIssuer)
    )
}
