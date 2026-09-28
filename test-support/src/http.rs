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
    let mut stream = TcpStream::connect_timeout(&addr, TIMEOUT)?;
    stream.set_read_timeout(Some(TIMEOUT))?;
    stream.set_write_timeout(Some(TIMEOUT))?;
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
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
mod tests {
    use super::*;
    use std::io::BufRead as _;
    use std::net::TcpListener;

    /// Serves one connection: returns its request line and answers `response`.
    fn serve_once(response: &'static str) -> (SocketAddr, std::thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = io::BufReader::new(stream);
            let mut request_line = String::new();
            reader.read_line(&mut request_line).unwrap();
            let mut line = String::new();
            while reader.read_line(&mut line).unwrap() > 2 {
                line.clear();
            }
            reader.get_mut().write_all(response.as_bytes()).unwrap();
            request_line
        });
        (addr, server)
    }

    /// The method and path reach the server, and the status and body come back.
    #[test]
    fn returns_the_status_and_body() {
        let (addr, server) = serve_once("HTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\nok");
        let (status, body) = http(addr, "PUT", "/bucket").unwrap();
        assert_eq!((status, body.as_str()), (201, "ok"));
        assert_eq!(server.join().unwrap(), "PUT /bucket HTTP/1.1\r\n");
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
