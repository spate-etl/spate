//! Lane-draining helpers shared by the Kafka integration suites.

use spate_core::error::{ErrorClass, SourceError};
use spate_core::source::{PayloadBatch, Source, SourceEvent, SourceLane};
use spate_kafka::KafkaSource;
use std::time::{Duration, Instant};

/// Serve the source's control plane once. Panics on any event but `Idle` and
/// on a non-retryable error; retryable errors are appended to `errors`.
pub(crate) fn serve_events(source: &mut KafkaSource, errors: &mut Vec<String>) {
    match source.poll_events(Duration::from_millis(50)) {
        Ok(SourceEvent::Idle) => {}
        Ok(other) => panic!("unexpected source event: {other:?}"),
        Err(
            e @ SourceError::Client {
                class: ErrorClass::Retryable,
                ..
            },
        ) => errors.push(e.to_string()),
        Err(e) => panic!("poll_events: {e}"),
    }
}

/// Poll a lane until `want` payloads arrive, serving the source between
/// polls; returns (payload, key, offset). Panics if they do not arrive within
/// `timeout`.
pub(crate) fn drain_lane(
    source: &mut KafkaSource,
    lane: &mut <KafkaSource as Source>::Lane,
    want: usize,
    timeout: Duration,
) -> Vec<(Vec<u8>, Vec<u8>, i64)> {
    let mut got = Vec::new();
    let mut errors = Vec::new();
    let deadline = Instant::now() + timeout;
    while got.len() < want {
        assert!(
            Instant::now() < deadline,
            "lane delivered {}/{want} before deadline; retryable errors: {errors:?}",
            got.len()
        );
        serve_events(source, &mut errors);
        let Some(mut batch) = lane
            .poll(64, Duration::from_millis(500))
            .expect("lane poll")
        else {
            continue;
        };
        while let Some(raw) = batch.next_payload() {
            got.push((
                raw.bytes.to_vec(),
                raw.key.unwrap_or(&[]).to_vec(),
                raw.offset,
            ));
        }
    }
    got
}
