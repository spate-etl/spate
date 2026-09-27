//! A server that refuses every TLS handshake with one fatal alert.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

/// A TCP server on `127.0.0.1` that answers every TLS client with the fatal
/// alert whose description is `alert` (RFC 8446 numbering, e.g. 40 for
/// `handshake_failure`).
///
/// Per connection it writes `preamble`, reads the client's first TLS record,
/// sends the alert, and reads until the client closes, so the client receives
/// the alert before any reset. The server runs on a detached thread for the
/// life of the process.
///
/// # Panics
///
/// Panics when it cannot bind a local port.
#[must_use]
pub fn tls_alert_server(preamble: &'static [u8], alert: u8) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a local port");
    let addr = listener.local_addr().expect("local address");
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            std::thread::spawn(move || {
                let _ = refuse(stream, preamble, alert);
            });
        }
    });
    addr
}

fn refuse(mut stream: TcpStream, preamble: &[u8], alert: u8) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.write_all(preamble)?;
    let mut header = [0u8; 5];
    stream.read_exact(&mut header)?;
    let mut body = vec![0u8; usize::from(u16::from_be_bytes([header[3], header[4]]))];
    stream.read_exact(&mut body)?;
    // An alert record: type 21, version TLS 1.2, length 2, level fatal.
    stream.write_all(&[21, 3, 3, 0, 2, 2, alert])?;
    stream.flush()?;
    // Closing with unread input sends a reset, which can reach the client
    // ahead of the alert.
    std::io::copy(&mut stream, &mut std::io::sink())?;
    Ok(())
}
