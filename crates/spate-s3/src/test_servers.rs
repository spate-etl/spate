//! Local HTTP and TLS servers for the crate's unit tests, and S3 stores that
//! point at them.

use object_store::RetryConfig;
use object_store::aws::AmazonS3Builder;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

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

pub(crate) use spate_test_support::TestCa;

/// A TLS server on `127.0.0.1` over a certificate `ca` signed, which closes
/// each connection after the handshake, and returns its `https://` URL. With
/// `clients` set, the server requires a client certificate that CA signed.
pub(crate) async fn serve(ca: &TestCa, clients: Option<&TestCa>) -> String {
    let addr = spate_test_support::serve_tls(ca.server_config(clients), |_| async {}).await;
    format!("https://{addr}")
}
