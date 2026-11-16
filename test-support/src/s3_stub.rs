//! Plain-HTTP S3 stubs on `127.0.0.1`, each returning its `http://` URL and
//! serving until the process exits.

use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use crate::serve_each;

/// The body of an S3 `AccessDenied` error.
pub const ACCESS_DENIED: &str = "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>";

const NO_SUCH_KEY: &str = "<Error><Code>NoSuchKey</Code></Error>";

/// Answers every request with `status_line`, such as `403 Forbidden`, and
/// `body` as XML, then closes the connection.
pub fn reply_server(status_line: &'static str, body: impl Into<String>) -> String {
    let body: String = body.into();
    url(serve_each(move |mut stream| {
        let _ = read_head(&mut stream).and_then(|_| {
            respond(
                &mut stream,
                status_line,
                "connection: close\r\n",
                body.as_bytes(),
            )
        });
    }))
}

/// Answers every request `403 Forbidden`, declares a body of `declared`
/// bytes, sends `sent` bytes of it in two writes and then holds the
/// connection open.
pub fn oversized_failure_server(declared: u64, sent: usize) -> String {
    url(serve_each(move |mut stream| {
        let _ = read_head(&mut stream);
        let _ = write!(stream, "{}", head("403 Forbidden", "", declared));
        let half = vec![b'~'; sent / 2];
        for _ in 0..2 {
            if stream
                .write_all(&half)
                .and_then(|()| stream.flush())
                .is_err()
            {
                return;
            }
        }
        // Held open, so a client reading past what was sent waits.
        std::thread::park();
    }))
}

/// Answers `ListObjectsV2` with `pages` in turn: a request with no
/// continuation token gets `pages[0]`, and one carrying
/// `continuation-token=N` gets `pages[N]`. Any other request is a 404.
pub fn listing_server(pages: Vec<String>) -> String {
    url(serve_each(move |mut stream| {
        let _ = read_head(&mut stream).and_then(|request| {
            let (_, query) = target(&request);
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
                None => ("404 Not Found", NO_SUCH_KEY),
            };
            respond(
                &mut stream,
                status,
                "connection: close\r\n",
                body.as_bytes(),
            )
        });
    }))
}

/// Serves bucket `b` path-style, keeping each connection open for further
/// requests: a `ListObjectsV2` request gets one page listing `objects`, a
/// GET of a listed key gets its body, and anything else a 404. Listed
/// without ETags, so every object is read with one plain GET.
pub fn bucket_server(objects: &[(&str, &[u8])]) -> String {
    let contents: String = objects
        .iter()
        .map(|(key, body)| {
            format!(
                "<Contents><Key>{key}</Key><LastModified>2026-10-11T13:38:17.000Z\
                 </LastModified><Size>{}</Size></Contents>",
                body.len()
            )
        })
        .collect();
    let page =
        format!("<ListBucketResult><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>");
    let bodies: Arc<BTreeMap<String, Vec<u8>>> = Arc::new(
        objects
            .iter()
            .map(|(key, body)| (format!("/b/{key}"), body.to_vec()))
            .collect(),
    );
    url(serve_each(move |mut stream| {
        let _ = serve_bucket(&mut stream, &page, &bodies);
    }))
}

fn serve_bucket(
    stream: &mut TcpStream,
    page: &str,
    bodies: &BTreeMap<String, Vec<u8>>,
) -> io::Result<()> {
    loop {
        let request = read_head(stream)?;
        if request.is_empty() {
            return Ok(());
        }
        let (path, query) = target(&request);
        let (status, body): (&str, &[u8]) = if query.split('&').any(|p| p == "list-type=2") {
            ("200 OK", page.as_bytes())
        } else if let Some(body) = bodies.get(path.as_str()) {
            ("200 OK", body)
        } else {
            ("404 Not Found", NO_SUCH_KEY.as_bytes())
        };
        respond(
            stream,
            status,
            "last-modified: Sun, 11 Oct 2026 13:38:17 GMT\r\n",
            body,
        )?;
    }
}

fn url(addr: std::net::SocketAddr) -> String {
    format!("http://{addr}")
}

/// Reads a request up to the end of its headers and returns them, or what
/// arrived before the client closed.
fn read_head(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
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

/// The request target's path and query.
fn target(request: &[u8]) -> (String, String) {
    let line = String::from_utf8_lossy(request);
    let target = line.split_whitespace().nth(1).unwrap_or_default();
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    (path.to_owned(), query.to_owned())
}

/// An XML response head declaring `length` body bytes, with `headers` (each
/// ending `\r\n`) after the content type.
fn head(status: &str, headers: &str, length: u64) -> String {
    format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/xml\r\n{headers}\
         content-length: {length}\r\n\r\n"
    )
}

fn respond(stream: &mut TcpStream, status: &str, headers: &str, body: &[u8]) -> io::Result<()> {
    let length = u64::try_from(body.len()).expect("a body fits in u64");
    stream.write_all(head(status, headers, length).as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// A read that waits this long has hung.
    const HANG: Duration = Duration::from_secs(60);

    /// A response: its head, lower-cased, and its body.
    struct Response {
        head: String,
        body: Vec<u8>,
    }

    impl Response {
        fn status(&self) -> &str {
            self.head.lines().next().unwrap_or_default()
        }

        fn header(&self, name: &str) -> Option<&str> {
            self.head
                .lines()
                .find_map(|l| l.strip_prefix(name)?.strip_prefix(": "))
        }
    }

    fn connect(url: &str) -> TcpStream {
        let stream = TcpStream::connect(url.strip_prefix("http://").unwrap()).unwrap();
        stream.set_read_timeout(Some(HANG)).unwrap();
        stream
    }

    /// Sends a GET for `target` and reads the head and `body_len` body bytes,
    /// or the declared `content-length` when `body_len` is `None`.
    fn get_on(stream: &mut TcpStream, target: &str, body_len: Option<usize>) -> Response {
        write!(stream, "GET {target} HTTP/1.1\r\nhost: stub\r\n\r\n").unwrap();
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            head.push(byte[0]);
        }
        let mut response = Response {
            head: String::from_utf8(head).unwrap().to_ascii_lowercase(),
            body: Vec::new(),
        };
        let len =
            body_len.unwrap_or_else(|| response.header("content-length").unwrap().parse().unwrap());
        response.body = vec![0u8; len];
        stream.read_exact(&mut response.body).unwrap();
        response
    }

    fn get(url: &str, target: &str) -> Response {
        get_on(&mut connect(url), target, None)
    }

    #[test]
    fn reply_server_answers_every_connection_and_closes_it() {
        let url = reply_server("503 Slow Down", "<Error/>");
        for _ in 0..2 {
            let mut stream = connect(&url);
            let response = get_on(&mut stream, "/b/k", None);
            assert_eq!(response.status(), "http/1.1 503 slow down");
            assert_eq!(response.header("connection"), Some("close"));
            assert_eq!(response.body, b"<Error/>");
            assert_eq!(stream.read(&mut [0u8; 1]).unwrap(), 0, "closed");
        }
    }

    #[test]
    fn listing_server_serves_pages_by_continuation_token() {
        let url = listing_server(vec!["zero".to_owned(), "one".to_owned()]);
        let page = |target: &str| get(&url, target);
        assert_eq!(page("/b?list-type=2").body, b"zero");
        assert_eq!(page("/b?list-type=2&continuation-token=1").body, b"one");
        assert_eq!(page("/b?list-type=2").header("connection"), Some("close"));
        for target in [
            "/b?continuation-token=1",
            "/b?list-type=2&continuation-token=x",
            "/b?list-type=2&continuation-token=2",
        ] {
            let response = page(target);
            assert_eq!(response.status(), "http/1.1 404 not found", "{target}");
            assert_eq!(response.body, NO_SUCH_KEY.as_bytes(), "{target}");
        }
    }

    #[test]
    fn bucket_server_lists_and_serves_its_objects_on_one_connection() {
        let url = bucket_server(&[("a.ndjson", b"{}\n")]);
        let mut stream = connect(&url);
        let listing = get_on(&mut stream, "/b?list-type=2&prefix=", None);
        assert_eq!(listing.status(), "http/1.1 200 ok");
        let listing = String::from_utf8(listing.body).unwrap();
        assert!(
            listing.contains("<Key>a.ndjson</Key>") && listing.contains("<Size>3</Size>"),
            "{listing}"
        );
        let object = get_on(&mut stream, "/b/a.ndjson", None);
        assert_eq!(object.status(), "http/1.1 200 ok");
        assert_eq!(
            object.header("last-modified"),
            Some("sun, 11 oct 2026 13:38:17 gmt")
        );
        assert_eq!(object.body, b"{}\n");
        let missing = get_on(&mut stream, "/b/other", None);
        assert_eq!(missing.status(), "http/1.1 404 not found");
    }

    #[test]
    fn oversized_failure_server_declares_more_than_it_sends_on_each_connection() {
        let url = oversized_failure_server(1 << 20, 64);
        // The first connection is held open, so the second is served on a
        // thread of its own.
        let mut held = Vec::new();
        for _ in 0..2 {
            let mut stream = connect(&url);
            let response = get_on(&mut stream, "/b/k", Some(64));
            assert_eq!(response.status(), "http/1.1 403 forbidden");
            assert_eq!(response.header("content-length"), Some("1048576"));
            assert_eq!(response.body, vec![b'~'; 64]);
            held.push(stream);
        }
    }
}
