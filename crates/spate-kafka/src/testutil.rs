//! Shared test fixtures: librdkafka statistics values and a refused broker address.

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
