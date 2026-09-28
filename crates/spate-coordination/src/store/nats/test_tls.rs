//! A local NATS server stub that upgrades to TLS with a certificate a test CA
//! signed.

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

pub(crate) use spate_test_support::TestCa;

/// A NATS `INFO` from a 2.10.0 server that requires TLS. async-nats reads its
/// `port` only for logging.
pub(crate) const INFO_REQUIRING_TLS: &[u8] = b"INFO {\"server_id\":\"test\",\
    \"version\":\"2.10.0\",\"proto\":1,\"host\":\"127.0.0.1\",\"port\":4222,\
    \"max_payload\":1048576,\"tls_required\":true}\r\n";

/// Serves the NATS handshake on `127.0.0.1` and returns the port. Each
/// connection gets an `INFO` with `tls_required`, is upgraded to TLS over a
/// certificate `ca` signed, and has every `PING` answered. With `client_ca`,
/// the server requires a client certificate that CA signed. The server runs
/// until the runtime shuts down.
pub(crate) async fn serve_nats(ca: &TestCa, client_ca: Option<&TestCa>) -> u16 {
    let acceptor = TlsAcceptor::from(ca.server_config(client_ca));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut tcp, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                if tcp.write_all(INFO_REQUIRING_TLS).await.is_err() {
                    return;
                }
                let tls = match acceptor.accept(tcp).into_fallible().await {
                    Ok(tls) => tls,
                    Err((_, mut tcp)) => {
                        // Dropping a socket with unread bytes sends a reset,
                        // which can reach the client before the alert does.
                        let _ = tokio::io::copy(&mut tcp, &mut tokio::io::sink()).await;
                        return;
                    }
                };
                let mut tls = BufReader::new(tls);
                let mut line = String::new();
                while tls.read_line(&mut line).await.is_ok_and(|n| n > 0) {
                    if line.starts_with("PING")
                        && tls.get_mut().write_all(b"PONG\r\n").await.is_err()
                    {
                        return;
                    }
                    line.clear();
                }
            });
        }
    });
    port
}
