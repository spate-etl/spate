//! Constructors for the framework values a stage or connector test feeds in:
//! records, raw payloads, Confluent-framed payloads, acknowledgment handles,
//! component configs, and per-test names.

use spate_core::checkpoint::AckRef;
use spate_core::config::ComponentConfig;
use spate_core::record::{PartitionId, RawPayload, Record, RecordMeta};
use std::sync::atomic::{AtomicU64, Ordering};

/// An acknowledgment handle whose resolution is discarded.
///
/// To observe a resolution, take the handle from a batch polled against a
/// real `Checkpointer`, as the crate-level example does.
#[must_use]
pub fn test_ack() -> AckRef {
    AckRef::test_pair().0
}

/// A record carrying `payload`, with zeroed metadata and a [`test_ack`] handle.
#[must_use]
pub fn record<T>(payload: T) -> Record<T> {
    Record {
        payload,
        meta: RecordMeta {
            partition: PartitionId(0),
            offset: 0,
            event_time_ms: 0,
            key_hash: None,
        },
        ack: test_ack(),
    }
}

/// A keyless payload over `bytes` at partition 0, offset 0, timestamp 0.
#[must_use]
pub fn raw_payload(bytes: &[u8]) -> RawPayload<'_> {
    RawPayload {
        bytes,
        key: None,
        partition: PartitionId(0),
        offset: 0,
        timestamp_ms: 0,
    }
}

/// A component config tagged `type_tag`, with `yaml` as its body.
///
/// `yaml` is the section's body without the tag line, and may be indented.
///
/// # Panics
///
/// Panics if `yaml` does not parse.
#[must_use]
pub fn component_config(type_tag: &str, yaml: &str) -> ComponentConfig {
    let raw = serde_yaml::from_str(yaml)
        .unwrap_or_else(|e| panic!("component config for {type_tag:?} does not parse: {e}"));
    ComponentConfig::new(type_tag, raw)
}

/// `body` in the Confluent wire format: the magic byte `0x00`, `schema_id`
/// big-endian, then `body`.
///
/// Schema-registry serializers write this header in front of an Avro datum
/// or a Protobuf message, so `body` is whatever follows the schema id.
#[must_use]
pub fn confluent_frame(schema_id: u32, body: &[u8]) -> Vec<u8> {
    [&[0x00][..], &schema_id.to_be_bytes(), body].concat()
}

/// `prefix` with a suffix no other call in this process returns.
///
/// Gauge ownership is process-wide (INV-10), so tests sharing a binary name
/// each pipeline or component with this.
#[must_use]
pub fn unique_name(prefix: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!("{prefix}-{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confluent_frame_writes_magic_then_big_endian_id_then_body() {
        assert_eq!(
            confluent_frame(0x0102_0304, &[0xAA, 0xBB]),
            [0x00, 0x01, 0x02, 0x03, 0x04, 0xAA, 0xBB]
        );
    }

    #[test]
    fn confluent_frame_of_an_empty_body_is_the_header() {
        assert_eq!(confluent_frame(7, &[]), [0x00, 0x00, 0x00, 0x00, 0x07]);
    }

    #[test]
    fn unique_name_never_repeats() {
        let a = unique_name("p");
        let b = unique_name("p");
        assert_ne!(a, b);
        assert!(a.starts_with("p-") && b.starts_with("p-"));
    }

    /// An indented body parses as the mapping it would be under its tag, and
    /// an empty mapping parses too.
    #[test]
    fn component_config_takes_an_indented_body() {
        assert_eq!(
            component_config("t", "  brokers: a\n  topic: b\n"),
            component_config("t", "brokers: a\ntopic: b\n"),
        );
        assert_eq!(component_config("t", "  {}\n").type_tag(), "t");
    }
}
