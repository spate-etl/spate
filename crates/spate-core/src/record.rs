//! Core record types: payloads, metadata, and the flow-control signal.
//!
//! A [`Record`] is what moves through an operator chain: a payload (which
//! may borrow from the source's buffers), a [`Copy`] metadata struct, and a
//! clone of the source batch's acknowledgment handle
//! ([`AckRef`](crate::checkpoint::AckRef)). Records are born inside a chain's
//! `push_batch` call (deserialization) and die inside the same call
//! (serialized into shard frames, filtered out, or failed) — they are never
//! stored across the chain boundary, which is what makes borrowed payloads
//! sound (ADR-0013).

use crate::checkpoint::AckRef;

/// Flow-control result of pushing one record downstream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flow {
    /// Keep pushing.
    Continue,
    /// Downstream is full: stop pushing and resume from the current cursor
    /// once capacity frees. Never block — see the backpressure invariant.
    Blocked,
}

/// Identifier of a source partition (dense, source-assigned).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionId(pub u32);

/// Per-record metadata. `Copy`, no drop glue — moves through operators for
/// free and never touches the heap.
#[derive(Clone, Copy, Debug)]
pub struct RecordMeta {
    /// Source partition this record was read from.
    pub partition: PartitionId,
    /// Source offset (e.g. Kafka offset) of the originating payload.
    pub offset: i64,
    /// Event time in milliseconds since the epoch, as reported by the
    /// source (e.g. the Kafka message timestamp).
    pub event_time_ms: i64,
    /// Stable hash of the message key, when the source provides one; used
    /// by key-based shard routing.
    pub key_hash: Option<u64>,
}

/// A record flowing through the chain.
#[derive(Debug)]
pub struct Record<T> {
    /// The payload; may borrow from the source's buffers.
    pub payload: T,
    /// Copyable metadata, preserved automatically by operators.
    pub meta: RecordMeta,
    /// Acknowledgment handle shared by every record derived from the same
    /// source poll batch. Dropping it (filter, error-skip, or successful
    /// write) resolves this record's share of the batch.
    pub ack: AckRef,
}

impl<T> Record<T> {
    /// Transform the payload, preserving metadata and the ack handle.
    #[inline(always)]
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Record<U> {
        Record {
            payload: f(self.payload),
            meta: self.meta,
            ack: self.ack,
        }
    }
}

/// A raw payload borrowed from the source's buffers, valid for `'buf`.
#[derive(Clone, Copy, Debug)]
pub struct RawPayload<'buf> {
    /// The message value.
    pub bytes: &'buf [u8],
    /// The message key, when the source provides one.
    pub key: Option<&'buf [u8]>,
    /// Source partition.
    pub partition: PartitionId,
    /// Source offset.
    pub offset: i64,
    /// Source-reported event time (ms since epoch).
    pub timestamp_ms: i64,
}

impl RawPayload<'_> {
    /// Metadata for records derived from this payload. The key hash uses a
    /// stable 64-bit hash so shard routing survives restarts and versions.
    #[inline]
    pub fn meta(&self) -> RecordMeta {
        RecordMeta {
            partition: self.partition,
            offset: self.offset,
            event_time_ms: self.timestamp_ms,
            key_hash: self.key.map(stable_key_hash),
        }
    }
}

/// Stable, seedless 64-bit hash for shard routing (FNV-1a). Not for
/// adversarial input — routing only. Stability across processes, versions,
/// and platforms is a contract: changing this function reshuffles shards.
#[inline]
#[must_use]
pub fn stable_key_hash(key: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET;
    for &b in key {
        h ^= u64::from(b);
        h = h.wrapping_mul(PRIME);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    // Constructs an AckRef, whose internals are loom types under the loom
    // cfg and may only be touched inside a loom model.
    #[cfg(not(loom))]
    #[test]
    fn record_map_preserves_meta_and_ack() {
        let (ack, _rx) = crate::checkpoint::AckRef::test_pair();
        let rec = Record {
            payload: 21u32,
            meta: RecordMeta {
                partition: PartitionId(3),
                offset: 42,
                event_time_ms: 1_000,
                key_hash: Some(7),
            },
            ack,
        };
        let mapped = rec.map(|v| u64::from(v) * 2);
        assert_eq!(mapped.payload, 42);
        assert_eq!(mapped.meta.partition, PartitionId(3));
        assert_eq!(mapped.meta.offset, 42);
        assert_eq!(mapped.meta.key_hash, Some(7));
    }

    #[test]
    fn key_hash_is_stable() {
        // Pinned expectations: changing the hash reshuffles shard routing,
        // so a change here must be a conscious, breaking decision.
        assert_eq!(stable_key_hash(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(stable_key_hash(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_ne!(stable_key_hash(b"user-1"), stable_key_hash(b"user-2"));
    }

    #[test]
    fn raw_payload_meta_hashes_key() {
        let payload = RawPayload {
            bytes: b"v",
            key: Some(b"k"),
            partition: PartitionId(0),
            offset: 9,
            timestamp_ms: 5,
        };
        assert_eq!(payload.meta().key_hash, Some(stable_key_hash(b"k")));
        let keyless = RawPayload {
            key: None,
            ..payload
        };
        assert_eq!(keyless.meta().key_hash, None);
    }

    #[test]
    fn meta_is_copy_and_small() {
        // The hot path passes RecordMeta in registers; keep it lean.
        assert!(size_of::<RecordMeta>() <= 40);
        fn assert_copy<T: Copy>() {}
        assert_copy::<RecordMeta>();
        assert_copy::<Flow>();
        assert_copy::<PartitionId>();
    }
}
