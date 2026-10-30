//! Shared test fixtures: librdkafka statistics values, a refused broker
//! address and a broker that refuses a SASL login.

use rdkafka::statistics::{Broker, Window};

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
#[expect(
    clippy::disallowed_methods,
    reason = "the stub server's threads serve until the test process exits"
)]
pub(crate) fn refuses_sasl_login() -> std::net::SocketAddr {
    use std::io::{Read, Write};
    fn string(body: &mut Vec<u8>, s: &str) {
        body.extend_from_slice(&(s.len() as i16).to_be_bytes());
        body.extend_from_slice(s.as_bytes());
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { return };
            std::thread::spawn(move || {
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
                    match (key, ver) {
                        (18, v) if v > 0 => {
                            body.extend_from_slice(&35i16.to_be_bytes());
                            body.extend_from_slice(&0i32.to_be_bytes());
                        }
                        (18, _) => {
                            body.extend_from_slice(&0i16.to_be_bytes());
                            let apis: [(i16, i16, i16); 4] =
                                [(18, 0, 0), (17, 0, 1), (36, 0, 0), (3, 0, 0)];
                            body.extend_from_slice(&(apis.len() as i32).to_be_bytes());
                            for (k, lo, hi) in apis {
                                body.extend_from_slice(&k.to_be_bytes());
                                body.extend_from_slice(&lo.to_be_bytes());
                                body.extend_from_slice(&hi.to_be_bytes());
                            }
                        }
                        // SaslHandshake: no error, the one mechanism.
                        (17, _) => {
                            body.extend_from_slice(&0i16.to_be_bytes());
                            body.extend_from_slice(&1i32.to_be_bytes());
                            string(&mut body, "PLAIN");
                        }
                        // SaslAuthenticate v0: SASL_AUTHENTICATION_FAILED,
                        // its message, and no auth bytes.
                        (36, _) => {
                            body.extend_from_slice(&58i16.to_be_bytes());
                            string(
                                &mut body,
                                "Authentication failed: Invalid username or password",
                            );
                            body.extend_from_slice(&0i32.to_be_bytes());
                        }
                        _ => return,
                    }
                    let mut frame = (body.len() as u32).to_be_bytes().to_vec();
                    frame.extend_from_slice(&body);
                    if s.write_all(&frame).is_err() {
                        return;
                    }
                }
            });
        }
    });
    addr
}
