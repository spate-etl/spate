//! The CPU half of the Kafka sink: turning records into framed messages on
//! pipeline threads.
//!
//! Two seams, one adapter: a [`MessageEncoder`] produces one Kafka message
//! (key, headers, payload) per record into a reusable [`KafkaMessage`]
//! accumulator, and [`KafkaEncoder`] adapts any `MessageEncoder` to the
//! framework's [`RowEncoder`] contract — enforcing the `max_message_bytes`
//! guard and writing the connector's internal frame format. Ship-with
//! implementations: [`KafkaBytesEncoder`] (payload passthrough) and
//! [`KafkaJsonEncoder`] (serde_json).

use crate::sink::frame;
use bytes::BytesMut;
use serde::Serialize;
use spate_core::deser::{Owned, RecFamily};
use spate_core::error::{ErrorClass, SinkError};
use spate_core::record::Record;
use spate_core::sink::RowEncoder;
use std::marker::PhantomData;

/// librdkafka's `message.max.bytes` default (1,000,000 bytes — not 1 MiB),
/// the default basis for the encode-time size guard.
pub(crate) const DEFAULT_MAX_MESSAGE_BYTES: usize = 1_000_000;

/// One Kafka message under assembly: the accumulator a [`MessageEncoder`]
/// fills per record. Reused across records — internal buffers are cleared,
/// not freed, so steady-state encoding does not allocate.
///
/// A message starts keyless with an empty payload. Setting no payload
/// leaves an empty (present) payload; only [`set_tombstone`](Self::set_tombstone)
/// produces a null payload.
#[derive(Debug, Default)]
pub struct KafkaMessage {
    key: Vec<u8>,
    has_key: bool,
    headers: Vec<(String, Vec<u8>)>,
    live_headers: usize,
    payload: Vec<u8>,
    tombstone: bool,
}

impl KafkaMessage {
    fn clear(&mut self) {
        self.key.clear();
        self.has_key = false;
        self.live_headers = 0;
        self.payload.clear();
        self.tombstone = false;
    }

    /// Set the message key (drives Kafka's default partitioner). Copies
    /// `key` into the reusable buffer. An empty key is a real key, distinct
    /// from no key.
    pub fn set_key(&mut self, key: &[u8]) {
        self.key.clear();
        self.key.extend_from_slice(key);
        self.has_key = true;
    }

    /// Append one message header. Headers are produced in insertion order.
    pub fn add_header(&mut self, name: &str, value: &[u8]) {
        if let Some(slot) = self.headers.get_mut(self.live_headers) {
            slot.0.clear();
            slot.0.push_str(name);
            slot.1.clear();
            slot.1.extend_from_slice(value);
        } else {
            self.headers.push((name.to_owned(), value.to_vec()));
        }
        self.live_headers += 1;
    }

    /// Set the payload by copying `payload` in.
    pub fn set_payload(&mut self, payload: &[u8]) {
        self.tombstone = false;
        self.payload.clear();
        self.payload.extend_from_slice(payload);
    }

    /// Write the payload directly (e.g. `serde_json::to_writer`). Clears
    /// any tombstone mark; whatever is in the buffer when the encoder
    /// returns is the payload.
    pub fn payload_mut(&mut self) -> &mut Vec<u8> {
        self.tombstone = false;
        &mut self.payload
    }

    /// Mark the message as a Kafka tombstone (null payload, used by
    /// compacted topics as a delete marker). Discards any payload bytes.
    pub fn set_tombstone(&mut self) {
        self.tombstone = true;
        self.payload.clear();
    }

    /// The size the guard checks: key + payload + header bytes — a
    /// conservative upper bound on the record's contribution to the broker's
    /// `message.max.bytes` limit, which counts headers (unlike librdkafka's
    /// client-side key+payload check). Counting headers here keeps an
    /// oversized-with-headers record under the record-level Skip/Fail policy
    /// instead of letting it slip to the writer and fail the whole batch
    /// fatally at the broker.
    fn guarded_size(&self) -> usize {
        let key = if self.has_key { self.key.len() } else { 0 };
        let payload = if self.tombstone {
            0
        } else {
            self.payload.len()
        };
        let headers: usize = self.headers[..self.live_headers]
            .iter()
            .map(|(name, value)| name.len() + value.len())
            .sum();
        key + payload + headers
    }

    fn key(&self) -> Option<&[u8]> {
        self.has_key.then_some(self.key.as_slice())
    }

    fn payload(&self) -> Option<&[u8]> {
        (!self.tombstone).then_some(self.payload.as_slice())
    }

    fn headers_iter(&self) -> impl ExactSizeIterator<Item = (&str, &[u8])> {
        self.headers[..self.live_headers]
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_slice()))
    }
}

/// The record→message seam of the Kafka sink: produce exactly one message
/// into `msg` per record. Runs on pinned pipeline threads inside
/// [`KafkaEncoder`] — no I/O, no blocking.
///
/// Errors are record-level unless the encoder itself is broken: return
/// [`ErrorClass::RecordLevel`] for a record that cannot be represented
/// (subject to the sink stage's Skip/Fail policy) and
/// [`ErrorClass::Fatal`] only when every subsequent record would fail
/// identically.
///
/// Implementations must be `Clone` (the terminal stage clones one encoder
/// per shard) and cheap to clone.
pub trait MessageEncoder<F: RecFamily>: Send + Clone + 'static {
    /// Fill `msg` (already cleared) from `rec`.
    fn encode<'buf>(
        &mut self,
        rec: &Record<F::Rec<'buf>>,
        msg: &mut KafkaMessage,
    ) -> Result<(), SinkError>;
}

/// Adapts a [`MessageEncoder`] to the framework's [`RowEncoder`] seam:
/// runs the message encoder, enforces the `max_message_bytes` guard, and
/// appends the message to the chunk frame in the connector's internal
/// format (parsed back by the writer).
///
/// Oversized records fail with [`ErrorClass::RecordLevel`], honoring the
/// sink stage's error policy (default Skip: drop, count, continue). Keep
/// the limit aligned with the topic/broker `message.max.bytes` — a record
/// passing this guard but exceeding the broker's limit fails the whole
/// batch fatally at the writer instead.
///
/// Construct via [`KafkaSink`](crate::KafkaSink)'s `encoder_*` helpers
/// (which bake the configured limit in) or directly with [`new`](Self::new)
/// / [`with_max_message_bytes`](Self::with_max_message_bytes).
#[derive(Debug)]
pub struct KafkaEncoder<F, M> {
    inner: M,
    max_message_bytes: usize,
    scratch: KafkaMessage,
    _family: PhantomData<fn(F)>,
}

impl<F: RecFamily, M: MessageEncoder<F>> KafkaEncoder<F, M> {
    /// An encoder enforcing librdkafka's default message limit
    /// (1,000,000 bytes).
    #[must_use]
    pub fn new(inner: M) -> Self {
        Self::with_max_message_bytes(inner, DEFAULT_MAX_MESSAGE_BYTES)
    }

    /// An encoder enforcing a specific `max_message_bytes` (key + payload +
    /// headers).
    #[must_use]
    pub fn with_max_message_bytes(inner: M, max_message_bytes: usize) -> Self {
        KafkaEncoder {
            inner,
            max_message_bytes,
            scratch: KafkaMessage::default(),
            _family: PhantomData,
        }
    }
}

impl<F, M: Clone> Clone for KafkaEncoder<F, M> {
    fn clone(&self) -> Self {
        // Fresh scratch per clone: each shard's encoder accumulates
        // independently.
        KafkaEncoder {
            inner: self.inner.clone(),
            max_message_bytes: self.max_message_bytes,
            scratch: KafkaMessage::default(),
            _family: PhantomData,
        }
    }
}

impl<F: RecFamily, M: MessageEncoder<F>> RowEncoder<F> for KafkaEncoder<F, M> {
    fn encode<'buf>(
        &mut self,
        rec: &Record<F::Rec<'buf>>,
        buf: &mut BytesMut,
    ) -> Result<(), SinkError> {
        self.scratch.clear();
        self.inner.encode(rec, &mut self.scratch)?;
        let size = self.scratch.guarded_size();
        if size > self.max_message_bytes {
            return Err(SinkError::Client {
                class: ErrorClass::RecordLevel,
                reason: format!(
                    "message of {size} bytes (key + payload + headers) exceeds \
                     max_message_bytes ({}); raise the sink's limit together \
                     with the topic/broker `message.max.bytes`, or shrink the \
                     record",
                    self.max_message_bytes
                ),
            });
        }
        frame::write_message(
            buf,
            self.scratch.key(),
            self.scratch.headers_iter(),
            self.scratch.payload(),
        )
        .map_err(|e| SinkError::Client {
            class: ErrorClass::RecordLevel,
            reason: format!("message cannot be framed: {e}"),
        })
    }
}

/// Key extractor for [`KafkaBytesEncoder`]: derives an optional message
/// key from the payload bytes (e.g. a fixed-width prefix or an embedded
/// field). A plain `fn` item, so it is `Copy` and needs no allocation.
pub type BytesKeyFn = for<'r> fn(&'r [u8]) -> Option<&'r [u8]>;

/// Payload passthrough for owned byte records (`Owned<Vec<u8>>`): the
/// record's bytes become the message payload verbatim — the natural fit
/// for Kafka→Kafka pipelines over [`BytesPassthrough`] deserialization.
/// Keyless by default; [`with_key_fn`](Self::with_key_fn) derives a key
/// from the payload.
///
/// Note that source message keys do **not** survive deserialization (a
/// record carries only the key *hash* in its metadata), so a Kafka→Kafka
/// pipeline that must preserve keys has to re-derive them from the payload
/// here, or carry them in a custom record type with a custom
/// [`MessageEncoder`].
///
/// [`BytesPassthrough`]: spate_core::deser::BytesPassthrough
#[derive(Clone, Copy, Debug, Default)]
pub struct KafkaBytesEncoder {
    key_fn: Option<BytesKeyFn>,
}

impl KafkaBytesEncoder {
    /// Keyless passthrough.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Passthrough with a payload-derived key.
    #[must_use]
    pub fn with_key_fn(key_fn: BytesKeyFn) -> Self {
        KafkaBytesEncoder {
            key_fn: Some(key_fn),
        }
    }
}

impl MessageEncoder<Owned<Vec<u8>>> for KafkaBytesEncoder {
    fn encode<'buf>(
        &mut self,
        rec: &Record<Vec<u8>>,
        msg: &mut KafkaMessage,
    ) -> Result<(), SinkError> {
        if let Some(key_fn) = self.key_fn
            && let Some(key) = key_fn(&rec.payload)
        {
            msg.set_key(key);
        }
        msg.set_payload(&rec.payload);
        Ok(())
    }
}

/// Key extractor for [`KafkaJsonEncoder`]: derives an optional message key
/// from the record payload. A plain `fn` item — borrowing families hit the
/// same closure-inference limit as `map_rec` (ADR-0004), and `fn` items are
/// naturally higher-ranked.
pub type KeyFn<F> = for<'r, 'buf> fn(&'r <F as RecFamily>::Rec<'buf>) -> Option<&'r [u8]>;

/// JSON encoder: serializes each record's payload with `serde_json` as the
/// message payload. Works for any family whose records implement
/// [`Serialize`] at every lifetime (owned structs via
/// [`Owned`], or borrowed zero-copy families). Keyless by default;
/// [`with_key_fn`](Self::with_key_fn) derives a key from the record.
#[derive(Debug)]
pub struct KafkaJsonEncoder<F: RecFamily> {
    key_fn: Option<KeyFn<F>>,
    _family: PhantomData<fn(F)>,
}

impl<F: RecFamily> KafkaJsonEncoder<F> {
    /// Keyless JSON encoding.
    #[must_use]
    pub fn new() -> Self {
        KafkaJsonEncoder {
            key_fn: None,
            _family: PhantomData,
        }
    }

    /// JSON encoding with a record-derived key.
    #[must_use]
    pub fn with_key_fn(key_fn: KeyFn<F>) -> Self {
        KafkaJsonEncoder {
            key_fn: Some(key_fn),
            _family: PhantomData,
        }
    }
}

impl<F: RecFamily> Default for KafkaJsonEncoder<F> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F: RecFamily> Clone for KafkaJsonEncoder<F> {
    fn clone(&self) -> Self {
        KafkaJsonEncoder {
            key_fn: self.key_fn,
            _family: PhantomData,
        }
    }
}

impl<F> MessageEncoder<F> for KafkaJsonEncoder<F>
where
    F: RecFamily,
    for<'b> F::Rec<'b>: Serialize,
{
    fn encode<'buf>(
        &mut self,
        rec: &Record<F::Rec<'buf>>,
        msg: &mut KafkaMessage,
    ) -> Result<(), SinkError> {
        if let Some(key_fn) = self.key_fn
            && let Some(key) = key_fn(&rec.payload)
        {
            msg.set_key(key);
        }
        serde_json::to_writer(msg.payload_mut(), &rec.payload).map_err(|e| SinkError::Client {
            class: ErrorClass::RecordLevel,
            reason: format!("JSON encoding failed: {e}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::frame::FrameParser;
    use spate_core::checkpoint::AckRef;
    use spate_core::record::{PartitionId, RecordMeta};

    fn record<T>(payload: T) -> Record<T> {
        let (ack, _rx) = AckRef::test_pair();
        Record {
            payload,
            meta: RecordMeta {
                partition: PartitionId(0),
                offset: 0,
                event_time_ms: 0,
                key_hash: None,
            },
            ack,
        }
    }

    fn single_message(buf: &[u8]) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        let messages: Vec<_> = FrameParser::new(buf).map(|m| m.unwrap()).collect();
        assert_eq!(messages.len(), 1);
        (
            messages[0].key.map(<[u8]>::to_vec),
            messages[0].payload.map(<[u8]>::to_vec),
        )
    }

    #[test]
    fn bytes_encoder_passthrough() {
        let mut enc = KafkaEncoder::new(KafkaBytesEncoder::new());
        let mut buf = BytesMut::new();
        enc.encode(&record(b"raw bytes".to_vec()), &mut buf)
            .unwrap();
        let (key, payload) = single_message(&buf);
        assert_eq!(key, None);
        assert_eq!(payload, Some(b"raw bytes".to_vec()));
    }

    #[test]
    fn bytes_encoder_derives_keys_from_payload() {
        fn first_four(payload: &[u8]) -> Option<&[u8]> {
            payload.get(..4)
        }
        let mut enc = KafkaEncoder::new(KafkaBytesEncoder::with_key_fn(first_four));
        let mut buf = BytesMut::new();
        enc.encode(&record(b"user-1234:event".to_vec()), &mut buf)
            .unwrap();
        let (key, payload) = single_message(&buf);
        assert_eq!(key, Some(b"user".to_vec()));
        assert_eq!(payload, Some(b"user-1234:event".to_vec()));
    }

    #[test]
    fn json_encoder_serializes_and_extracts_key() {
        #[derive(serde::Serialize)]
        struct Event {
            user: String,
            n: u32,
        }
        fn key_of(event: &Event) -> Option<&[u8]> {
            Some(event.user.as_bytes())
        }
        let mut enc = KafkaEncoder::new(KafkaJsonEncoder::<Owned<Event>>::with_key_fn(key_of));
        let mut buf = BytesMut::new();
        enc.encode(
            &record(Event {
                user: "ada".into(),
                n: 7,
            }),
            &mut buf,
        )
        .unwrap();
        let (key, payload) = single_message(&buf);
        assert_eq!(key, Some(b"ada".to_vec()));
        assert_eq!(payload, Some(br#"{"user":"ada","n":7}"#.to_vec()));
    }

    #[test]
    fn oversized_record_is_record_level() {
        let mut enc = KafkaEncoder::with_max_message_bytes(KafkaBytesEncoder::new(), 8);
        let err = enc
            .encode(&record(vec![0u8; 9]), &mut BytesMut::new())
            .unwrap_err();
        match err {
            SinkError::Client { class, reason } => {
                assert_eq!(class, ErrorClass::RecordLevel);
                assert!(reason.contains("max_message_bytes"), "actionable: {reason}");
                assert!(reason.contains("message.max.bytes"), "actionable: {reason}");
            }
            other => panic!("unexpected error shape: {other:?}"),
        }
    }

    #[test]
    fn size_guard_counts_key_plus_payload() {
        fn whole(payload: &[u8]) -> Option<&[u8]> {
            Some(payload)
        }
        // 5-byte payload alone passes an 8-byte limit; key + payload (10)
        // does not — the guard sums key + payload (+ headers, none here).
        let mut keyless = KafkaEncoder::with_max_message_bytes(KafkaBytesEncoder::new(), 8);
        keyless
            .encode(&record(vec![0u8; 5]), &mut BytesMut::new())
            .unwrap();
        let mut keyed =
            KafkaEncoder::with_max_message_bytes(KafkaBytesEncoder::with_key_fn(whole), 8);
        let err = keyed
            .encode(&record(vec![0u8; 5]), &mut BytesMut::new())
            .unwrap_err();
        match err {
            SinkError::Client { class, .. } => assert_eq!(class, ErrorClass::RecordLevel),
            other => panic!("unexpected error shape: {other:?}"),
        }
    }

    #[test]
    fn size_guard_counts_header_bytes() {
        // A record whose key + payload fits but whose headers push it over the
        // limit must fail record-level here — otherwise the oversized message
        // escapes to the writer and fails the whole batch fatally at the
        // broker (which counts headers).
        #[derive(Clone)]
        struct BigHeader;
        impl MessageEncoder<Owned<Vec<u8>>> for BigHeader {
            fn encode<'buf>(
                &mut self,
                rec: &Record<Vec<u8>>,
                msg: &mut KafkaMessage,
            ) -> Result<(), SinkError> {
                msg.set_payload(&rec.payload);
                msg.add_header("big", &[0u8; 100]);
                Ok(())
            }
        }
        // 5-byte payload alone clears an 80-byte limit; payload + "big" (3) +
        // 100-byte value (108) does not.
        let mut enc = KafkaEncoder::with_max_message_bytes(BigHeader, 80);
        let err = enc
            .encode(&record(vec![0u8; 5]), &mut BytesMut::new())
            .unwrap_err();
        match err {
            SinkError::Client { class, reason } => {
                assert_eq!(class, ErrorClass::RecordLevel);
                assert!(reason.contains("headers"), "actionable: {reason}");
            }
            other => panic!("unexpected error shape: {other:?}"),
        }
    }

    #[test]
    fn scratch_reuse_does_not_leak_state_across_records() {
        // A keyed record followed by a keyless one: the second must not
        // inherit the first's key (regression against accumulator reuse).
        fn key_if_marked(payload: &[u8]) -> Option<&[u8]> {
            payload.starts_with(b"K:").then(|| &payload[..1])
        }
        let mut enc = KafkaEncoder::new(KafkaBytesEncoder::with_key_fn(key_if_marked));
        let mut buf = BytesMut::new();
        enc.encode(&record(b"K:first".to_vec()), &mut buf).unwrap();
        enc.encode(&record(b"second".to_vec()), &mut buf).unwrap();
        let messages: Vec<_> = FrameParser::new(&buf).map(|m| m.unwrap()).collect();
        assert_eq!(messages[0].key, Some(&b"K"[..]));
        assert_eq!(messages[1].key, None, "stale key must not leak");
    }

    #[test]
    fn encoder_is_clone_and_boxable_for_split() {
        // The split terminal boxes branch encoders as
        // `dyn RowEncoder<F> + Clone + 'static`; assert the concrete
        // adapter satisfies both halves.
        let enc = KafkaEncoder::new(KafkaJsonEncoder::<Owned<Vec<u8>>>::new());
        let cloned = enc.clone();
        let _boxed: Box<dyn RowEncoder<Owned<Vec<u8>>>> = Box::new(cloned);
    }
}
