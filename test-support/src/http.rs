//! A blocking HTTP/1.1 exchange over a plain `TcpStream`.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(5);

/// Sends `method` for `path` to `addr` with an empty body, and returns the
/// response's status code and body.
///
/// Connecting, writing and reading each time out after five seconds. The body
/// is returned as sent, without decoding a chunked transfer encoding.
///
/// # Errors
///
/// Returns the I/O error when the exchange fails, and `InvalidData` carrying
/// the response when it has no status code.
pub fn http(addr: SocketAddr, method: &str, path: &str) -> io::Result<(u16, String)> {
    http_with_body(addr, method, path, "")
}

/// [`http`] with `body` as the request body, sent with its `Content-Length`.
///
/// # Errors
///
/// As [`http`].
pub fn http_with_body(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: &str,
) -> io::Result<(u16, String)> {
    let mut stream = TcpStream::connect_timeout(&addr, TIMEOUT)?;
    stream.set_read_timeout(Some(TIMEOUT))?;
    stream.set_write_timeout(Some(TIMEOUT))?;
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("no status code in: {response}"),
            )
        })?;
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    Ok((status, body))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::io::BufRead as _;
    use std::net::TcpListener;

    /// One request as the server read it.
    #[derive(Debug)]
    pub(crate) struct Request {
        /// The request line, without its line ending.
        pub(crate) line: String,
        /// Each header line, without its line ending.
        pub(crate) headers: Vec<String>,
        /// The body, read to its `Content-Length`.
        pub(crate) body: String,
    }

    /// Serves one connection: returns its request and answers `response`.
    pub(crate) fn serve_once(
        response: &'static str,
    ) -> (SocketAddr, std::thread::JoinHandle<Request>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = io::BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let mut headers = Vec::new();
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                if header.trim_end().is_empty() {
                    break;
                }
                headers.push(header.trim_end().to_owned());
            }
            let length: usize = headers
                .iter()
                .find_map(|h| h.strip_prefix("Content-Length: "))
                .map_or(0, |n| n.parse().unwrap());
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            reader.get_mut().write_all(response.as_bytes()).unwrap();
            Request {
                line: line.trim_end().to_owned(),
                headers,
                body: String::from_utf8(body).unwrap(),
            }
        });
        (addr, server)
    }

    /// The method and path reach the server, and the status and body come back.
    #[test]
    fn returns_the_status_and_body() {
        let (addr, server) = serve_once("HTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\nok");
        let (status, body) = http(addr, "PUT", "/bucket").unwrap();
        assert_eq!((status, body.as_str()), (201, "ok"));
        let request = server.join().unwrap();
        assert_eq!(request.line, "PUT /bucket HTTP/1.1");
        assert!(request.headers.contains(&"Content-Length: 0".to_owned()));
        assert_eq!(request.body, "");
    }

    /// The body reaches the server whole, under its byte length.
    #[test]
    fn sends_the_body_with_its_length() {
        let (addr, server) = serve_once("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
        let body = r#"{"name":"é"}"#;
        let (status, _) = http_with_body(addr, "POST", "/proxies", body).unwrap();
        assert_eq!(status, 200);
        let request = server.join().unwrap();
        assert_eq!(request.line, "POST /proxies HTTP/1.1");
        assert!(
            request.headers.contains(&"Content-Length: 13".to_owned()),
            "{request:?}"
        );
        assert_eq!(request.body, body);
    }

    /// A response with no status code is `InvalidData` and names what arrived.
    #[test]
    fn a_response_without_a_status_is_invalid_data() {
        let (addr, server) = serve_once("garbage");
        let err = http(addr, "GET", "/").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("garbage"), "{err}");
        server.join().unwrap();
    }
}
