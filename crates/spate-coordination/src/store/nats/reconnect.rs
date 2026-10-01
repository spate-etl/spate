//! Detection of a credential, certificate or TLS handshake that every server
//! rejects while the client reconnects after startup.

use crate::store::StoreError;
use async_nats::{ConnectErrorKind, Server, Statistics};
use spate_core::error::TLS_REJECTION_ALERTS;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, OnceLock};

/// The rejection the reconnecting client last met on every server, stamped
/// with the client's connect count at the time.
#[derive(Default)]
pub(super) struct Rejection {
    record: Mutex<Option<(String, u64)>>,
    stats: OnceLock<Arc<Statistics>>,
}

impl Rejection {
    /// Starts recording; pool snapshots observed before this are ignored.
    pub(super) fn attach(&self, client: &async_nats::Client) {
        let _ = self.stats.set(client.statistics());
    }

    /// Records the pool's rejection when every server's latest attempt was
    /// one, and clears the record otherwise.
    ///
    /// A discovered server that has never been reached and holds no error is
    /// left out, since an address that does not resolve never records one.
    pub(super) fn observe(&self, pool: &[Server]) {
        let Some(stats) = self.stats.get() else {
            return;
        };
        let mut reasons = Vec::new();
        let mut rejected = true;
        for server in pool {
            match server.last_error.as_deref() {
                Some(error) if rejection(error) => {
                    if !reasons.contains(&error) {
                        reasons.push(error);
                    }
                }
                None if server.is_discovered && !server.did_connect => {}
                _ => rejected = false,
            }
        }
        let record = (rejected && !reasons.is_empty())
            .then(|| (reasons.join("; "), stats.connects.load(Ordering::Relaxed)));
        *self.record.lock().expect("rejection record poisoned") = record;
    }

    /// Fatal while `client` is disconnected and no connection has succeeded
    /// since the rejection was recorded.
    pub(super) fn check(&self, client: &async_nats::Client) -> Result<(), StoreError> {
        // The state is read before the count: a reconnect bumps the count
        // before it reports `Connected`, so a record from before a later
        // successful reconnect never matches.
        if client.connection_state() == async_nats::connection::State::Connected {
            return Ok(());
        }
        let connects = client.statistics().connects.load(Ordering::Relaxed);
        match &*self.record.lock().expect("rejection record poisoned") {
            Some((reason, at)) if *at == connects => Err(StoreError::Fatal(format!(
                "reconnecting to NATS: every server rejected the connection: {reason}"
            ))),
            _ => Ok(()),
        }
    }
}

/// Whether a server's `last_error` reports a rejected credential, a
/// certificate either side rejects, a listed TLS alert, or no handshake
/// parameter in common.
///
/// `TLS error` is left out: on a reconnect it reports a failure to read the
/// system trust store, which retries.
fn rejection(last_error: &str) -> bool {
    let auth = ConnectErrorKind::AuthorizationViolation.to_string();
    if last_error
        .strip_prefix(auth.as_str())
        .is_some_and(|rest| rest.is_empty() || rest.starts_with(": "))
    {
        return true;
    }
    let Some(tls) = last_error.strip_prefix(&format!("{}: ", ConnectErrorKind::Io)) else {
        return false;
    };
    tls.starts_with("invalid peer certificate: ")
        || tls.starts_with("peer is incompatible: ")
        || TLS_REJECTION_ALERTS
            .iter()
            .any(|&alert| tls == async_nats::rustls::Error::AlertReceived(alert.into()).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::nats::NatsConfig;
    use crate::store::nats::test_tls::{INFO_REQUIRING_TLS, TestCa, serve_nats};
    use crate::store::nats::tests::serve_authorization_violation;
    use async_nats::rustls::{self, AlertDescription, CertificateError, PeerIncompatible};
    use async_nats::{ConnectError, ServerAddr};
    use std::time::Duration;

    fn io_error(tls: rustls::Error) -> String {
        ConnectError::with_source(
            ConnectErrorKind::Io,
            std::io::Error::new(std::io::ErrorKind::InvalidData, tls),
        )
        .to_string()
    }

    fn auth_error() -> String {
        ConnectError::with_source(
            ConnectErrorKind::AuthorizationViolation,
            async_nats::ServerError::AuthorizationViolation,
        )
        .to_string()
    }

    /// The text async-nats records for a rejection classifies as one, and
    /// the text for an outage, an unlisted alert or an unreadable trust
    /// store does not.
    #[test]
    fn rejection_matches_the_text_async_nats_records() {
        for error in [
            auth_error(),
            ConnectErrorKind::AuthorizationViolation.to_string(),
            io_error(rustls::Error::InvalidCertificate(
                CertificateError::UnknownIssuer,
            )),
            io_error(rustls::Error::PeerIncompatible(
                PeerIncompatible::NoCipherSuitesInCommon,
            )),
            io_error(rustls::Error::AlertReceived(
                AlertDescription::HandshakeFailure,
            )),
            io_error(rustls::Error::AlertReceived(
                AlertDescription::CertificateRequired,
            )),
        ] {
            assert!(rejection(&error), "{error}");
        }
        for error in [
            io_error(rustls::Error::AlertReceived(AlertDescription::DecodeError)),
            io_error(rustls::Error::AlertReceived(
                AlertDescription::InternalError,
            )),
            ConnectError::with_source(
                ConnectErrorKind::Io,
                std::io::Error::from(std::io::ErrorKind::ConnectionRefused),
            )
            .to_string(),
            ConnectError::new(ConnectErrorKind::TimedOut).to_string(),
            ConnectError::with_source(
                ConnectErrorKind::Tls,
                std::io::Error::other("could not load platform certs: bad file"),
            )
            .to_string(),
        ] {
            assert!(!rejection(&error), "{error}");
        }
    }

    /// Every alert in `TLS_REJECTION_ALERTS` classifies as a rejection.
    #[test]
    fn every_listed_alert_is_a_rejection() {
        for &alert in TLS_REJECTION_ALERTS {
            let error = io_error(rustls::Error::AlertReceived(alert.into()));
            assert!(rejection(&error), "{error}");
        }
    }

    fn server(port: u16, last_error: Option<String>) -> Server {
        Server {
            addr: format!("nats://127.0.0.1:{port}")
                .parse::<ServerAddr>()
                .unwrap(),
            failed_attempts: usize::from(last_error.is_some()),
            did_connect: false,
            is_discovered: false,
            last_error,
        }
    }

    fn attached(connects: u64) -> Rejection {
        let rejection = Rejection::default();
        let stats = Arc::new(Statistics::default());
        stats.connects.store(connects, Ordering::Relaxed);
        rejection.stats.set(stats).unwrap();
        rejection
    }

    fn recorded(rejection: &Rejection) -> Option<(String, u64)> {
        rejection.record.lock().unwrap().clone()
    }

    /// A pool whose every server last met a rejection is recorded with the
    /// connect count; one server without a rejection clears it.
    #[test]
    fn observe_records_only_a_pool_that_rejects_on_every_server() {
        let rejection = attached(3);
        let alert = io_error(rustls::Error::AlertReceived(
            AlertDescription::HandshakeFailure,
        ));
        rejection.observe(&[
            server(1, Some(alert.clone())),
            server(2, Some(auth_error())),
        ]);
        let (reason, at) = recorded(&rejection).expect("every server rejected");
        assert!(reason.contains("HandshakeFailure"), "{reason}");
        assert!(reason.contains("authorization violation"), "{reason}");
        assert_eq!(at, 3);

        let timed_out = ConnectError::new(ConnectErrorKind::TimedOut).to_string();
        for other in [None, Some(timed_out)] {
            rejection.observe(&[server(1, Some(alert.clone()))]);
            assert!(recorded(&rejection).is_some());
            rejection.observe(&[server(1, Some(alert.clone())), server(2, other)]);
            assert!(recorded(&rejection).is_none());
        }
    }

    /// A discovered server never reached and holding no error does not keep
    /// the pool from being rejected; an empty pool is never rejected.
    #[test]
    fn observe_skips_an_unreached_discovered_server() {
        let rejection = attached(1);
        let discovered = Server {
            is_discovered: true,
            ..server(2, None)
        };
        rejection.observe(&[server(1, Some(auth_error())), discovered.clone()]);
        assert!(recorded(&rejection).is_some());
        let reached = Server {
            did_connect: true,
            ..discovered
        };
        rejection.observe(&[server(1, Some(auth_error())), reached]);
        assert!(recorded(&rejection).is_none());
        rejection.observe(&[]);
        assert!(recorded(&rejection).is_none());
    }

    /// Before `attach`, a rejected pool records nothing.
    #[test]
    fn observe_ignores_pools_before_attach() {
        let rejection = Rejection::default();
        rejection.observe(&[server(1, Some(auth_error()))]);
        assert!(recorded(&rejection).is_none());
    }

    const TARGET: &str = "SPATE_TEST_NATS_RECONNECT_TARGET";
    const EXPECT: &str = "SPATE_TEST_NATS_RECONNECT_EXPECT";

    /// Serves a plain NATS 2.11.0 server on `127.0.0.1` that answers every
    /// `PING`, and returns its port.
    async fn serve_plain() -> u16 {
        use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut tcp = BufReader::new(tcp);
                    let info = format!(
                        "INFO {{\"server_id\":\"plain\",\"version\":\"2.11.0\",\"proto\":1,\
                         \"host\":\"127.0.0.1\",\"port\":{port},\"max_payload\":1048576}}\r\n"
                    );
                    if tcp.get_mut().write_all(info.as_bytes()).await.is_err() {
                        return;
                    }
                    let mut line = String::new();
                    while tcp.read_line(&mut line).await.is_ok_and(|n| n > 0) {
                        if line.starts_with("PING")
                            && tcp.get_mut().write_all(b"PONG\r\n").await.is_err()
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

    /// Connects through the store's client to a plain server, moves the pool
    /// to `target` and reconnects, and returns the fatal message the check
    /// then reports.
    async fn fatal_after_reconnecting_to(target: &str) -> String {
        let plain = serve_plain().await;
        let config = NatsConfig::new(vec![format!("nats://127.0.0.1:{plain}")], "reconnect");
        let servers = config.validate().unwrap();
        let rejection = Arc::new(Rejection::default());
        let client = crate::store::nats::client(&config, &servers, &rejection)
            .await
            .unwrap();
        rejection.check(&client).unwrap();
        client.set_server_pool(target).await.unwrap();
        client.force_reconnect().await.unwrap();
        let mut message = None;
        tokio::task::block_in_place(|| {
            spate_test::wait_until(Duration::from_secs(10), "a fatal check", || {
                match rejection.check(&client) {
                    Err(StoreError::Fatal(m)) => message = Some(m),
                    Err(StoreError::Retryable(m)) => panic!("expected Fatal, got Retryable: {m}"),
                    Ok(()) => {}
                }
                message.is_some()
            });
        });
        message.unwrap()
    }

    /// A server that answers the reconnect with an authorization violation
    /// makes the check fatal.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_credential_rejected_at_reconnect_is_fatal() {
        let port = serve_authorization_violation().await;
        let message = fatal_after_reconnecting_to(&format!("nats://127.0.0.1:{port}")).await;
        assert!(message.contains("authorization violation"), "{message}");
    }

    /// A server whose certificate no trusted CA signed, or that answers the
    /// handshake with `HandshakeFailure`, makes the check fatal after a
    /// reconnect. The child pins the trust store, which async-nats re-reads
    /// on every TLS upgrade.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_tls_rejection_at_reconnect_is_fatal() {
        if let (Ok(target), Ok(expect)) = (std::env::var(TARGET), std::env::var(EXPECT)) {
            let message = fatal_after_reconnecting_to(&target).await;
            assert!(message.contains(&expect), "{message}");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let system = TestCa::new("system");
        let unknown = serve_nats(&TestCa::new("unknown"), None).await;
        let alert = AlertDescription::HandshakeFailure;
        let refusing = spate_test::tls_alert_server(INFO_REQUIRING_TLS, u8::from(alert));
        let (_, module) = module_path!().split_once("::").unwrap();
        let name = format!("{module}::a_tls_rejection_at_reconnect_is_fatal");
        for (target, expect) in [
            (
                format!("tls://127.0.0.1:{unknown}"),
                "UnknownIssuer".to_string(),
            ),
            (
                format!("tls://127.0.0.1:{}", refusing.port()),
                format!("{alert:?}"),
            ),
        ] {
            let system = system.write(dir.path());
            tokio::task::block_in_place(|| {
                spate_test_support::run_in_child(&name, |child| {
                    child
                        .env_remove("SSL_CERT_DIR")
                        .env("SSL_CERT_FILE", system)
                        .env(TARGET, target)
                        .env(EXPECT, expect)
                });
            });
        }
    }
}
