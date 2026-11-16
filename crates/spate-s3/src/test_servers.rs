//! Local HTTP and TLS servers for the crate's unit tests, and S3 stores that
//! point at them. The plain-HTTP stubs come from test-support.

pub(crate) use spate_test_support::{
    ACCESS_DENIED, listing_server, oversized_failure_server, reply_server,
};

use object_store::RetryConfig;
use object_store::aws::AmazonS3Builder;

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

pub(crate) use spate_test_support::TestCa;

/// A TLS server on `127.0.0.1` over a certificate `ca` signed, which closes
/// each connection after the handshake, and returns its `https://` URL. With
/// `clients` set, the server requires a client certificate that CA signed.
pub(crate) async fn serve(ca: &TestCa, clients: Option<&TestCa>) -> String {
    let addr = spate_test_support::serve_tls(ca.server_config(clients), |_| async {}).await;
    format!("https://{addr}")
}

/// An error whose text is 1 MiB, with a TLS rejection behind it.
#[derive(Debug)]
pub(crate) struct LongTls(pub(crate) rustls::Error);

impl std::fmt::Display for LongTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&"x".repeat(1 << 20))
    }
}

impl std::error::Error for LongTls {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
