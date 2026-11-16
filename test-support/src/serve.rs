//! A local TCP server that hands each connection to a handler on a thread of
//! its own.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;

/// Binds a port on `127.0.0.1` and serves it with [`serve_each_on`].
///
/// # Panics
///
/// Panics when no local port can be bound.
pub fn serve_each(handle: impl Fn(TcpStream) + Send + Sync + 'static) -> SocketAddr {
    serve_each_on(
        TcpListener::bind("127.0.0.1:0").expect("bind a local port"),
        handle,
    )
}

/// Accepts on `listener` from a detached thread and runs `handle` on a new
/// thread for each connection, skipping a failed accept. Returns the
/// listener's address. The threads serve until the process exits.
///
/// # Panics
///
/// Panics when the listener has no local address.
#[expect(
    clippy::disallowed_methods,
    reason = "the server's threads serve until the test process exits"
)]
pub fn serve_each_on(
    listener: TcpListener,
    handle: impl Fn(TcpStream) + Send + Sync + 'static,
) -> SocketAddr {
    let addr = listener.local_addr().expect("local address");
    let handle = Arc::new(handle);
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let handle = Arc::clone(&handle);
            std::thread::spawn(move || handle(stream));
        }
    });
    addr
}

/// A server that reads every request and never answers, for running a client
/// into its timeout. Each connection is read until its client closes it.
pub fn silent_server() -> SocketAddr {
    serve_each(|mut stream| {
        let _ = io::copy(&mut stream, &mut io::sink());
    })
}

/// The `http://` URL of a server on `addr`.
pub(crate) fn url(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

/// Reads a request up to the end of its headers and returns them, or what
/// arrived before the client closed.
pub(crate) fn read_head(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
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
pub(crate) fn target(request: &[u8]) -> (String, String) {
    let line = String::from_utf8_lossy(request);
    let target = line.split_whitespace().nth(1).unwrap_or_default();
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    (path.to_owned(), query.to_owned())
}

/// A response head of `content_type` declaring `length` body bytes, with
/// `headers` (each ending `\r\n`) after the content type.
pub(crate) fn head(status: &str, content_type: &str, headers: &str, length: u64) -> String {
    format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\n{headers}\
         content-length: {length}\r\n\r\n"
    )
}

/// Writes a response of `content_type` with `headers` and `body`.
pub(crate) fn respond(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    headers: &str,
    body: &[u8],
) -> io::Result<()> {
    let length = u64::try_from(body.len()).expect("a body fits in u64");
    stream.write_all(head(status, content_type, headers, length).as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Shutdown;
    use std::time::Duration;

    /// A socket wait that takes this long has hung.
    const HANG: Duration = Duration::from_secs(60);

    /// More than a socket's buffers hold, so the write completes only while
    /// the server reads.
    const REQUEST: usize = 4 << 20;

    #[test]
    fn silent_server_reads_every_request_and_never_answers() {
        let addr = silent_server();
        for _ in 0..2 {
            let mut stream = TcpStream::connect(addr).unwrap();
            stream.set_read_timeout(Some(HANG)).unwrap();
            stream.set_write_timeout(Some(HANG)).unwrap();
            stream
                .write_all(&vec![b'x'; REQUEST])
                .expect("the server reads the whole request");
            stream.shutdown(Shutdown::Write).unwrap();
            let mut answer = Vec::new();
            stream
                .read_to_end(&mut answer)
                .expect("the server closes once the client has");
            assert!(answer.is_empty(), "{} bytes answered", answer.len());
        }
    }
}
