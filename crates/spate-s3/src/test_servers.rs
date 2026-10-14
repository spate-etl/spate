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

/// An HTTP server on `127.0.0.1` that answers `ListObjectsV2` with `pages`
/// in turn: the first request gets `pages[0]`, and a request carrying
/// `continuation-token=N` gets `pages[N]`. Any other request is a 404.
///
/// Returns the server's `http://` URL. The server runs on a detached thread
/// for the life of the process.
pub(crate) fn listing_server(pages: Vec<String>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a local port");
    let url = format!("http://{}", listener.local_addr().expect("local address"));
    let pages = std::sync::Arc::new(pages);
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let pages = std::sync::Arc::clone(&pages);
            std::thread::spawn(move || {
                let _ = answer_listing(stream, &pages);
            });
        }
    });
    url
}

fn answer_listing(mut stream: TcpStream, pages: &[String]) -> std::io::Result<()> {
    let request = read_head(&mut stream)?;
    let line = String::from_utf8_lossy(&request);
    let target = line.split_whitespace().nth(1).unwrap_or_default();
    let query = target.split_once('?').map_or("", |(_, q)| q);
    let page = query
        .split('&')
        .any(|p| p == "list-type=2")
        .then(|| {
            query
                .split('&')
                .find_map(|p| p.strip_prefix("continuation-token="))
                .map_or(Some(0), |t| t.parse::<usize>().ok())
        })
        .flatten()
        .and_then(|i| pages.get(i));
    let (status, body) = match page {
        Some(body) => ("200 OK", body.as_str()),
        None => ("404 Not Found", "<Error><Code>NoSuchKey</Code></Error>"),
    };
    write!(
        stream,
        "HTTP/1.1 {status}\r\ncontent-type: application/xml\r\n\
         content-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )?;
    stream.flush()
}

/// An HTTP server on `127.0.0.1` that answers its `i`th request with
/// `responses[i]`, a status line and an XML body, and every request after the
/// last with the last response.
///
/// Returns the server's `http://` URL. The server runs on a detached thread
/// for the life of the process.
pub(crate) fn sequence_server(responses: Vec<(&'static str, String)>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a local port");
    let url = format!("http://{}", listener.local_addr().expect("local address"));
    std::thread::spawn(move || {
        for (i, mut stream) in listener.incoming().flatten().enumerate() {
            let (status, body) = &responses[i.min(responses.len() - 1)];
            let _ = read_head(&mut stream).and_then(|_| {
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\ncontent-type: application/xml\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
            });
        }
    });
    url
}

/// Read a request up to the end of its headers, and return them.
fn read_head(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut request = Vec::new();
    let mut buf = [0u8; 4096];
    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            break;
        }
        request.extend_from_slice(&buf[..n]);
    }
    Ok(request)
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
