//! Local HTTP and TLS servers for the crate's unit tests, and S3 stores that
//! point at them.

use object_store::RetryConfig;
use object_store::aws::AmazonS3Builder;
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::ClientCertVerifier;
use rustls::{RootCertStore, ServerConfig};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;

/// An HTTP server on `127.0.0.1` that answers every request with
/// `status_line`, such as `403 Forbidden`, and an S3 `AccessDenied` body.
///
/// Returns the server's `http://` URL. The server runs on a detached thread
/// for the life of the process.
pub(crate) fn status_server(status_line: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a local port");
    let url = format!("http://{}", listener.local_addr().expect("local address"));
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            std::thread::spawn(move || {
                let _ = answer(stream, status_line);
            });
        }
    });
    url
}

fn answer(mut stream: TcpStream, status_line: &str) -> std::io::Result<()> {
    let mut request = [0u8; 8192];
    let _ = stream.read(&mut request)?;
    let body = "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>";
    write!(
        stream,
        "HTTP/1.1 {status_line}\r\ncontent-type: application/xml\r\n\
         content-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )?;
    stream.flush()
}

/// A builder for bucket `b` at `endpoint`, with plain HTTP allowed and
/// object_store's own retries off.
pub(crate) fn builder_at(endpoint: &str) -> AmazonS3Builder {
    AmazonS3Builder::new()
        .with_bucket_name("b")
        .with_region("us-east-1")
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_retry(RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        })
}

/// [`builder_at`] with static credentials.
pub(crate) fn store_at(endpoint: &str) -> object_store::aws::AmazonS3 {
    builder_at(endpoint)
        .with_access_key_id("AKIDTEST")
        .with_secret_access_key("secret")
        .build()
        .expect("build the S3 store")
}

/// [`builder_at`] with static credentials, trusting `ca` and no system root.
pub(crate) fn tls_store_at(endpoint: &str, ca: &TestCa) -> object_store::aws::AmazonS3 {
    builder_at(endpoint)
        .with_client_options(tls_trusting(ca))
        .with_access_key_id("AKIDTEST")
        .with_secret_access_key("secret")
        .build()
        .expect("build the S3 store")
}

/// Client options that trust `ca` and no system root.
pub(crate) fn tls_trusting(ca: &TestCa) -> object_store::ClientOptions {
    object_store::ClientOptions::new()
        .with_no_system_certificates(true)
        .with_root_certificate(
            object_store::Certificate::from_der(&ca.der()).expect("parse the CA certificate"),
        )
}

/// A private CA, and TLS servers presenting a certificate it signed.
pub(crate) struct TestCa {
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
            der,
            issuer: Issuer::new(params, key),
        }
    }

    pub(crate) fn der(&self) -> CertificateDer<'static> {
        self.der.clone()
    }

    /// [`serve_with`](Self::serve_with), requiring a client certificate signed
    /// by `clients`.
    pub(crate) async fn serve_requiring_client_cert(&self, clients: &TestCa) -> String {
        let mut roots = RootCertStore::empty();
        roots.add(clients.der()).unwrap();
        let verifier = WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
        )
        .build()
        .unwrap();
        self.serve_with(Some(verifier)).await
    }

    /// A TLS server on `127.0.0.1` whose certificate for that address this CA
    /// signed, and which closes each connection after the handshake. Returns
    /// its `https://` URL; the server runs until the runtime shuts down.
    pub(crate) async fn serve_with(
        &self,
        client_verifier: Option<Arc<dyn ClientCertVerifier>>,
    ) -> String {
        let key = KeyPair::generate().unwrap();
        let leaf = CertificateParams::new(vec!["127.0.0.1".to_owned()])
            .unwrap()
            .signed_by(&key, &self.issuer)
            .unwrap();
        let builder = ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap();
        let builder = match client_verifier {
            Some(verifier) => builder.with_client_cert_verifier(verifier),
            None => builder.with_no_client_auth(),
        };
        let config = builder
            .with_single_cert(
                vec![leaf.der().clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
            )
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let _ = acceptor.accept(tcp).await;
                });
            }
        });
        format!("https://127.0.0.1:{port}")
    }
}
