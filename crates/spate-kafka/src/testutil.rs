//! librdkafka statistics fixtures shared by the source and sink metrics tests.

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
