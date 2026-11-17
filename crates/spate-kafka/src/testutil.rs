//! Shared test fixtures: librdkafka statistics values, a refused broker
//! address, a broker that refuses a SASL login, a raised fatal error and a
//! stop on a librdkafka log line.

use crate::context::SourceContext;
use rdkafka::bindings::rd_kafka_resp_err_t as RespErr;
use rdkafka::client::{Client, ClientContext};
use rdkafka::consumer::{BaseConsumer, Consumer as _};
use rdkafka::statistics::{Broker, Window};
use spate_test::HANG;
use std::ffi::CStr;
use std::time::Duration;

#[path = "../tests/support/mock.rs"]
pub(crate) mod mock;

#[path = "../tests/support/topics.rs"]
#[allow(dead_code, reason = "the integration suites use the rest")]
pub(crate) mod topics;

#[path = "../tests/support/wire.rs"]
#[allow(dead_code, reason = "the integration suites use the rest")]
pub(crate) mod wire;

pub(crate) use spate_test_support::{assert_returns, returns_within};

/// A consumer enters the wait for an assign call. Its `delegating assign` line
/// and the rebalance event follow.
pub(crate) const ASSIGN_STOP: &str = "-> wait-assign-call";

/// Serve `consumer` up to librdkafka's `line`, and return with every event
/// logged before it on the queue.
pub(crate) fn stop_at(consumer: &BaseConsumer<SourceContext>, line: &str) {
    spate_test::wait_until(HANG, line, || {
        spate_test::capture_logs(tracing::Level::DEBUG, || {
            let _ = consumer.poll(Duration::ZERO);
        })
        .iter()
        .any(|l| l.contains(line))
    });
    // A round trip on librdkafka's thread, which has queued what it logged by
    // the time it answers.
    consumer.assignment().expect("assignment");
}

/// An `UP` broker entry keyed `name`, with its node name taken from `name`
/// without the `/<nodeid>` suffix.
pub(crate) fn broker(name: &str, source: &str, nodeid: i32) -> Broker {
    Broker {
        name: name.to_owned(),
        nodename: name
            .trim_end_matches(|c: char| c == '/' || c.is_ascii_digit())
            .to_owned(),
        source: source.to_owned(),
        nodeid,
        state: "UP".to_owned(),
        ..Default::default()
    }
}

pub(crate) fn window(avg: i64, p99: i64, cnt: i64) -> Window {
    Window {
        avg,
        p99,
        cnt,
        ..Default::default()
    }
}

/// Raise `code` as `client`'s fatal error, with `reason` as its text.
pub(crate) fn raise_fatal<C: ClientContext>(client: &Client<C>, code: RespErr, reason: &CStr) {
    // SAFETY: `client` is live for the call, and `reason` is NUL-terminated.
    let raised = unsafe {
        rdkafka::bindings::rd_kafka_test_fatal_error(client.native_ptr(), code, reason.as_ptr())
    };
    assert_eq!(raised, RespErr::RD_KAFKA_RESP_ERR_NO_ERROR);
}

/// Asserts that `lines` hold exactly one cleartext-credential warning, for
/// the `sasl.password` key of the section at `path`, and no password.
#[cfg(feature = "tls")]
pub(crate) fn assert_one_cleartext_warning(lines: &[String], path: &str) {
    let cleartext: Vec<_> = lines
        .iter()
        .filter(|l| l.contains("the credential is sent without TLS"))
        .collect();
    assert_eq!(cleartext.len(), 1, "{lines:?}");
    let want = format!(
        "{path}.rdkafka.\"sasl.password\": rdkafka.\"security.protocol\" is sasl_plaintext; \
         the credential is sent without TLS"
    );
    assert!(
        cleartext[0].contains("WARN") && cleartext[0].contains(&want),
        "{cleartext:?}"
    );
    assert!(!lines.iter().any(|l| l.contains("hunter2")), "{lines:?}");
}

/// A loopback `host:port` that refuses connections at once.
///
/// The port is bound and released, so the refusal is real. A port nothing has
/// bound can hang the connect instead of refusing it on some hosts, such as
/// WSL2 with mirrored networking.
pub(crate) fn refused_broker() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    addr.to_string()
}

/// A broker that answers ApiVersions, accepts the SASL `PLAIN`
/// mechanism, and refuses the login with `SASL_AUTHENTICATION_FAILED`,
/// as a broker does for a wrong password. Returns its address.
#[cfg(feature = "tls")]
pub(crate) fn refuses_sasl_login() -> std::net::SocketAddr {
    fn string(body: &mut Vec<u8>, s: &str) {
        body.extend_from_slice(&(s.len() as i16).to_be_bytes());
        body.extend_from_slice(s.as_bytes());
    }
    frame_server(|key, ver, body| {
        match key {
            18 => api_versions(ver, &[(18, 0, 0), (17, 0, 1), (36, 0, 0), (3, 0, 0)], body),
            // SaslHandshake: no error, the one mechanism.
            17 => {
                body.extend_from_slice(&0i16.to_be_bytes());
                body.extend_from_slice(&1i32.to_be_bytes());
                string(body, "PLAIN");
            }
            // SaslAuthenticate v0: SASL_AUTHENTICATION_FAILED, its message,
            // and no auth bytes.
            36 => {
                body.extend_from_slice(&58i16.to_be_bytes());
                string(body, "Authentication failed: Invalid username or password");
                body.extend_from_slice(&0i32.to_be_bytes());
            }
            _ => return false,
        }
        true
    })
}

/// A broker that answers ApiVersions and drops the connection once the
/// SASL mechanism handshake request arrives, as a broker or a proxy in
/// front of it does when it restarts mid-handshake. Returns its address.
#[cfg(feature = "tls")]
pub(crate) fn drops_during_sasl_handshake() -> std::net::SocketAddr {
    frame_server(|key, ver, body| {
        // SaslHandshake, or anything else: drop the connection.
        if key != 18 {
            return false;
        }
        api_versions(ver, &[(18, 0, 0), (17, 0, 0), (3, 0, 0)], body);
        true
    })
}

/// A broker on `127.0.0.1` that reads each connection's length-prefixed
/// requests and frames the response body `answer` writes after the
/// correlation id, given the request's API key and version. A `false` from
/// `answer`, or a failed read or write, drops the connection.
#[cfg(feature = "tls")]
fn frame_server(answer: fn(i16, i16, &mut Vec<u8>) -> bool) -> std::net::SocketAddr {
    use std::io::{Read, Write};
    spate_test_support::serve_each(move |mut s| {
        loop {
            let mut len = [0u8; 4];
            if s.read_exact(&mut len).is_err() {
                return;
            }
            let mut req = vec![0u8; u32::from_be_bytes(len) as usize];
            if s.read_exact(&mut req).is_err() {
                return;
            }
            let key = i16::from_be_bytes([req[0], req[1]]);
            let ver = i16::from_be_bytes([req[2], req[3]]);
            let mut body = req[4..8].to_vec();
            if !answer(key, ver, &mut body) {
                return;
            }
            let mut frame = (body.len() as u32).to_be_bytes().to_vec();
            frame.extend_from_slice(&body);
            if s.write_all(&frame).is_err() {
                return;
            }
        }
    })
}

/// An ApiVersions response listing `apis` as `(key, min, max)` at version 0.
/// A later version is refused with `UNSUPPORTED_VERSION`, so the client
/// retries at 0.
#[cfg(feature = "tls")]
fn api_versions(ver: i16, apis: &[(i16, i16, i16)], body: &mut Vec<u8>) {
    if ver > 0 {
        body.extend_from_slice(&35i16.to_be_bytes());
        body.extend_from_slice(&0i32.to_be_bytes());
        return;
    }
    body.extend_from_slice(&0i16.to_be_bytes());
    body.extend_from_slice(&(apis.len() as i32).to_be_bytes());
    for (k, lo, hi) in apis {
        body.extend_from_slice(&k.to_be_bytes());
        body.extend_from_slice(&lo.to_be_bytes());
        body.extend_from_slice(&hi.to_be_bytes());
    }
}
