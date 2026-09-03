//! Operator-chain tests: combinators, resume semantics, terminal handoff.

use super::*;
use crate::backpressure::InflightBudget;
use crate::checkpoint::{AckMsg, AckRef, AckStatus};
use crate::deser::{BytesPassthrough, Deserializer, EmitRecord, Owned, RecFamily};
use crate::error::{DeserError, ErrorPolicy, SinkError};
use crate::record::{PartitionId, RawPayload, Record};
use crate::sink::{
    EncodedChunk, KeyHashRouter, RecordRouter, RowEncoder, ShardRouter, shard_queues,
};
use crate::source::PayloadBatch;
use bytes::BytesMut;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// ---- fixtures --------------------------------------------------------------

/// Borrowed parse type: `key:seg|seg|...` payloads.
#[derive(Debug, PartialEq)]
struct LogEvent<'buf> {
    key: &'buf str,
    body: &'buf [u8],
}

struct LogF;
impl RecFamily for LogF {
    type Rec<'buf> = LogEvent<'buf>;
}

/// Borrowed fan-out type.
#[derive(Debug)]
struct SubEvent<'buf> {
    chunk: &'buf [u8],
}

struct SubF;
impl RecFamily for SubF {
    type Rec<'buf> = SubEvent<'buf>;
}

/// Deserializer for `key:body`; errors on payloads starting `ERR`;
/// emits nothing for payloads starting `SKIP`.
#[derive(Clone, Default)]
struct LogDeser;

impl Deserializer<LogF> for LogDeser {
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, LogEvent<'buf>>,
    ) -> Result<(), DeserError> {
        if raw.bytes.starts_with(b"ERR") {
            return Err(DeserError::Malformed {
                reason: "poison".into(),
            });
        }
        if raw.bytes.starts_with(b"SKIP") {
            return Ok(());
        }
        let pos =
            raw.bytes
                .iter()
                .position(|&b| b == b':')
                .ok_or_else(|| DeserError::Malformed {
                    reason: "missing key separator".into(),
                })?;
        let key = std::str::from_utf8(&raw.bytes[..pos]).map_err(|_| DeserError::Malformed {
            reason: "non-utf8 key".into(),
        })?;
        let _ = out.emit(Record {
            payload: LogEvent {
                key,
                body: &raw.bytes[pos + 1..],
            },
            meta: raw.meta(),
            ack: ack.clone(),
        });
        Ok(())
    }
}

/// Length-prefixed encoder for `SubEvent`; fails record-level on `BADROW`
/// chunks and fatally on `FATALROW` chunks.
#[derive(Clone, Default)]
struct SubEncoder;

impl RowEncoder<SubF> for SubEncoder {
    fn encode<'buf>(
        &mut self,
        rec: &Record<SubEvent<'buf>>,
        buf: &mut BytesMut,
    ) -> Result<(), SinkError> {
        // Write the prefix first so partial-write rollback is exercised.
        buf.extend_from_slice(&(u32::try_from(rec.payload.chunk.len()).unwrap()).to_le_bytes());
        if rec.payload.chunk == b"BADROW" {
            return Err(SinkError::Client {
                class: crate::error::ErrorClass::RecordLevel,
                reason: "unencodable row".into(),
            });
        }
        if rec.payload.chunk == b"FATALROW" {
            return Err(SinkError::Client {
                class: crate::error::ErrorClass::Fatal,
                reason: "encoder broken".into(),
            });
        }
        buf.extend_from_slice(rec.payload.chunk);
        Ok(())
    }
}

/// A second borrowed destination family for the split-terminal tests: stores
/// the event key. Distinct row type from [`SubEvent`], so the two branches of
/// a split are heterogeneous.
#[derive(Debug)]
struct KeyEvent<'buf> {
    key: &'buf [u8],
}

struct KeyF;
impl RecFamily for KeyF {
    type Rec<'buf> = KeyEvent<'buf>;
}

/// Length-prefixed encoder for [`KeyEvent`].
#[derive(Clone, Default)]
struct KeyEncoder;

impl RowEncoder<KeyF> for KeyEncoder {
    fn encode<'buf>(
        &mut self,
        rec: &Record<KeyEvent<'buf>>,
        buf: &mut BytesMut,
    ) -> Result<(), SinkError> {
        buf.extend_from_slice(&(u32::try_from(rec.payload.key.len()).unwrap()).to_le_bytes());
        buf.extend_from_slice(rec.payload.key);
        Ok(())
    }
}

/// Length-prefixed encoder for owned byte records.
#[derive(Clone, Default)]
struct VecEncoder;

impl RowEncoder<Owned<Vec<u8>>> for VecEncoder {
    fn encode<'buf>(&mut self, rec: &Record<Vec<u8>>, buf: &mut BytesMut) -> Result<(), SinkError> {
        buf.extend_from_slice(&(u32::try_from(rec.payload.len()).unwrap()).to_le_bytes());
        buf.extend_from_slice(&rec.payload);
        Ok(())
    }
}

/// A mock **columnar** encoder: buffers rows inside `self` during `encode`
/// (writing nothing to the per-chunk buffer), then emits one self-describing
/// block (`[u32 row_count]` followed by length-prefixed rows) in
/// `finish_chunk`. Stands in for the ClickHouse Native encoder to prove the
/// terminal stage drives the columnar contract: per-shard buffering, the
/// `buffered_bytes` seal threshold, and `finish_chunk` at every seal.
#[derive(Clone, Default)]
struct ColumnarEncoder {
    buffered: Vec<Vec<u8>>,
}

impl RowEncoder<Owned<Vec<u8>>> for ColumnarEncoder {
    fn encode<'buf>(&mut self, rec: &Record<Vec<u8>>, buf: &mut BytesMut) -> Result<(), SinkError> {
        // A columnar encoder writes NOTHING here; it transposes on finalize.
        assert!(
            buf.is_empty(),
            "columnar encode must not touch the chunk buffer"
        );
        self.buffered.push(rec.payload.clone());
        Ok(())
    }
    fn buffered_bytes(&self) -> usize {
        4 + self.buffered.iter().map(|r| 4 + r.len()).sum::<usize>()
    }
    fn finish_chunk(&mut self, buf: &mut BytesMut) -> Result<(), SinkError> {
        buf.extend_from_slice(&(u32::try_from(self.buffered.len()).unwrap()).to_le_bytes());
        for r in &self.buffered {
            buf.extend_from_slice(&(u32::try_from(r.len()).unwrap()).to_le_bytes());
            buf.extend_from_slice(r);
        }
        self.buffered.clear();
        Ok(())
    }
}

/// A columnar encoder that accepts every row and fails to finalize the block.
#[derive(Clone, Default)]
struct BrokenFinishEncoder;

impl RowEncoder<Owned<Vec<u8>>> for BrokenFinishEncoder {
    fn encode<'buf>(
        &mut self,
        _rec: &Record<Vec<u8>>,
        _buf: &mut BytesMut,
    ) -> Result<(), SinkError> {
        Ok(())
    }
    fn buffered_bytes(&self) -> usize {
        1
    }
    fn finish_chunk(&mut self, _buf: &mut BytesMut) -> Result<(), SinkError> {
        Err(SinkError::Client {
            class: crate::error::ErrorClass::Fatal,
            reason: "cannot finalize the block".into(),
        })
    }
}

/// Length-prefixed encoder whose `finish_chunk` fails after it has encoded a
/// row ending `FATALFINISH`. Each shard owns its own clone, so row content
/// picks the failing shard, and the reason names the row, so a reason
/// identifies which shard raised it.
#[derive(Clone, Default)]
struct SelectiveFinishEncoder {
    doomed: Option<String>,
}

impl RowEncoder<Owned<Vec<u8>>> for SelectiveFinishEncoder {
    fn encode<'buf>(&mut self, rec: &Record<Vec<u8>>, buf: &mut BytesMut) -> Result<(), SinkError> {
        if rec.payload.ends_with(b"FATALFINISH") {
            self.doomed
                .get_or_insert_with(|| String::from_utf8_lossy(&rec.payload).into_owned());
        }
        buf.extend_from_slice(&(u32::try_from(rec.payload.len()).unwrap()).to_le_bytes());
        buf.extend_from_slice(&rec.payload);
        Ok(())
    }
    fn finish_chunk(&mut self, _buf: &mut BytesMut) -> Result<(), SinkError> {
        if let Some(row) = &self.doomed {
            return Err(SinkError::Client {
                class: crate::error::ErrorClass::Fatal,
                reason: format!("cannot finalize the block after {row}"),
            });
        }
        Ok(())
    }
}

/// Decode a [`ColumnarEncoder`] block: leading `u32` row count, then rows.
fn decode_block(frame: &[u8]) -> Vec<Vec<u8>> {
    let count = u32::from_le_bytes(frame[0..4].try_into().unwrap()) as usize;
    let mut rows = Vec::with_capacity(count);
    let mut at = 4;
    for _ in 0..count {
        let len = u32::from_le_bytes(frame[at..at + 4].try_into().unwrap()) as usize;
        at += 4;
        rows.push(frame[at..at + len].to_vec());
        at += len;
    }
    rows
}

/// Cloneable owned passthrough for factory tests (the framework's
/// `BytesPassthrough` does not derive `Clone`).
#[derive(Clone, Default)]
struct OwnedPassthrough;

impl Deserializer<Owned<Vec<u8>>> for OwnedPassthrough {
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, Vec<u8>>,
    ) -> Result<(), DeserError> {
        let _ = out.emit(Record {
            payload: raw.bytes.to_vec(),
            meta: raw.meta(),
            ack: ack.clone(),
        });
        Ok(())
    }
}

/// Route a row whose payload starts `s1:` to shard 1, every other row to
/// shard 0.
#[derive(Clone, Copy)]
struct ByPrefix;
impl RecordRouter<Owned<Vec<u8>>> for ByPrefix {
    fn route_record<'buf>(&self, rec: &Record<Vec<u8>>, n: usize) -> usize {
        usize::from(rec.payload.starts_with(b"s1:")) % n
    }
}

/// Route everything to shard 0 (single-shard tests).
#[derive(Clone, Copy)]
struct ToZero;
impl ShardRouter for ToZero {
    fn route(&self, _meta: &crate::record::RecordMeta, _n: usize) -> usize {
        0
    }
}

/// A test batch over pre-filled payload buffers.
struct TestBatch<'a> {
    payloads: &'a [Vec<u8>],
    idx: usize,
    ack: AckRef,
}

impl<'a> TestBatch<'a> {
    fn new(payloads: &'a [Vec<u8>]) -> (Self, crossbeam_channel::Receiver<AckMsg>) {
        let (ack, rx) = AckRef::test_pair();
        (
            TestBatch {
                payloads,
                idx: 0,
                ack,
            },
            rx,
        )
    }
}

impl<'a> PayloadBatch<'a> for TestBatch<'a> {
    fn next_payload(&mut self) -> Option<RawPayload<'a>> {
        let buf = self.payloads.get(self.idx)?;
        let offset = self.idx as i64;
        self.idx += 1;
        Some(RawPayload {
            bytes: buf,
            key: None,
            partition: PartitionId(0),
            offset,
            timestamp_ms: offset * 10,
        })
    }

    fn ack(&self) -> &AckRef {
        &self.ack
    }
}

fn payloads(specs: &[&str]) -> Vec<Vec<u8>> {
    specs.iter().map(|s| s.as_bytes().to_vec()).collect()
}

fn decode_rows(frame: &[u8]) -> Vec<Vec<u8>> {
    let mut rows = Vec::new();
    let mut at = 0;
    while at < frame.len() {
        let len = u32::from_le_bytes(frame[at..at + 4].try_into().unwrap()) as usize;
        at += 4;
        rows.push(frame[at..at + len].to_vec());
        at += len;
    }
    rows
}

fn drain_rows(rx: &mut tokio::sync::mpsc::Receiver<EncodedChunk>) -> Vec<Vec<u8>> {
    let mut rows = Vec::new();
    while let Ok(chunk) = rx.try_recv() {
        rows.extend(decode_rows(&chunk.frame));
        // These tests play the sink: consuming a chunk here stands in for
        // a durable write, so resolve its acknowledgments as delivered
        // (an AckSet fails them on plain drop, for teardown safety).
        chunk.acks.deliver();
    }
    rows
}

/// Split each body at `|` into sub-events. A fn item: naturally
/// higher-ranked, as the borrowed-family builder tier requires.
fn split_body<'buf>(e: LogEvent<'buf>, em: &mut Emitter<'_, SubF>) {
    for chunk in e.body.split(|&b| b == b'|') {
        em.emit(SubEvent { chunk });
    }
}

fn non_empty(e: &LogEvent<'_>) -> bool {
    !e.body.is_empty()
}

// ---- borrowed end-to-end ---------------------------------------------------

#[test]
fn borrowed_chain_end_to_end() {
    let (queues, mut rxs) = shard_queues(1, 64);
    let budget = Arc::new(InflightBudget::new());
    let mut c = chain(LogDeser)
        .filter(non_empty)
        .flat_map::<SubF, _>(split_body)
        .sink(
            SubEncoder,
            ToZero,
            ChunkConfig::default(),
            queues,
            Arc::clone(&budget),
        )
        .build();

    let bufs = payloads(&["a:one|two", "b:", "c:three"]);
    let (mut batch, ack_rx) = TestBatch::new(&bufs);
    assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    assert!(matches!(c.flush(), PushOutcome::Done));

    let rows = drain_rows(&mut rxs[0]);
    assert_eq!(
        rows,
        vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]
    );
    assert!(budget.usage() > 0, "sealed bytes are budgeted");

    drop(batch);
    let msg = ack_rx.try_recv().expect("batch resolved");
    assert_eq!(msg.status, AckStatus::Delivered);
}

#[test]
fn owned_chain_with_closures_end_to_end() {
    let (queues, mut rxs) = shard_queues(1, 64);
    let budget = Arc::new(InflightBudget::new());
    let mut c = chain_owned(BytesPassthrough)
        .map(|mut b: Vec<u8>| {
            b.make_ascii_uppercase();
            b
        })
        .filter(|b: &Vec<u8>| b != b"DROPME")
        .try_map(
            |b: Vec<u8>| -> Result<Vec<u8>, std::string::FromUtf8Error> {
                String::from_utf8(b).map(String::into_bytes)
            },
            ErrorPolicy::Skip,
        )
        .sink(
            VecEncoder,
            KeyHashRouter,
            ChunkConfig::default(),
            queues,
            budget,
        )
        .build();

    let bufs = payloads(&["hello", "dropme", "world"]);
    let (mut batch, _ack_rx) = TestBatch::new(&bufs);
    assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    assert!(matches!(c.flush(), PushOutcome::Done));
    assert_eq!(
        drain_rows(&mut rxs[0]),
        vec![b"HELLO".to_vec(), b"WORLD".to_vec()]
    );
}

// ---- error policies --------------------------------------------------------

#[test]
fn deser_skip_policy_drops_and_continues() {
    let (queues, mut rxs) = shard_queues(1, 64);
    let mut c = chain(LogDeser)
        .flat_map::<SubF, _>(split_body)
        .sink(
            SubEncoder,
            ToZero,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    let bufs = payloads(&["a:x", "ERR:poison", "b:y"]);
    let (mut batch, ack_rx) = TestBatch::new(&bufs);
    assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    let _ = c.flush();
    assert_eq!(drain_rows(&mut rxs[0]), vec![b"x".to_vec(), b"y".to_vec()]);
    drop(batch);
    assert_eq!(
        ack_rx.try_recv().unwrap().status,
        AckStatus::Delivered,
        "skips count as handled"
    );
}

#[test]
fn deser_fail_policy_is_fatal_and_fails_the_batch() {
    let (queues, _rxs) = shard_queues(1, 64);
    let mut c = chain(LogDeser)
        .deser_error_policy(ErrorPolicy::Fail)
        .flat_map::<SubF, _>(split_body)
        .sink(
            SubEncoder,
            ToZero,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    let bufs = payloads(&["a:x", "ERR:poison", "b:y"]);
    let (mut batch, ack_rx) = TestBatch::new(&bufs);
    let PushOutcome::Fatal(f) = c.push_batch(&mut batch, 0) else {
        panic!("expected fatal");
    };
    assert!(f.reason.contains("poison"));
    drop(batch);
    // Records buffered in the terminal stage hold ack clones; a failed
    // pipeline releases them when the chain is torn down.
    drop(c);
    assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Failed);
}

#[test]
fn try_map_fail_policy_is_fatal() {
    let (queues, _rxs) = shard_queues(1, 64);
    let mut c = chain_owned(BytesPassthrough)
        .try_map(
            |b: Vec<u8>| -> Result<Vec<u8>, &'static str> {
                if b == b"boom" { Err("kaboom") } else { Ok(b) }
            },
            ErrorPolicy::Fail,
        )
        .sink(
            VecEncoder,
            KeyHashRouter,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    let bufs = payloads(&["fine", "boom", "after"]);
    let (mut batch, ack_rx) = TestBatch::new(&bufs);
    let PushOutcome::Fatal(f) = c.push_batch(&mut batch, 0) else {
        panic!("expected fatal");
    };
    assert!(f.reason.contains("kaboom"));
    assert!(f.component.starts_with("try_map"));
    drop(batch);
    drop(c);
    assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Failed);
}

#[test]
fn encoder_skip_policy_rolls_back_partial_rows() {
    let (queues, mut rxs) = shard_queues(1, 64);
    let mut c = chain(LogDeser)
        .flat_map::<SubF, _>(split_body)
        .sink(
            SubEncoder,
            ToZero,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    // BADROW's length prefix is written before the encoder errors; the
    // rollback must leave the frame decodable.
    let bufs = payloads(&["a:ok|BADROW|also"]);
    let (mut batch, _rx) = TestBatch::new(&bufs);
    assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    let _ = c.flush();
    assert_eq!(
        drain_rows(&mut rxs[0]),
        vec![b"ok".to_vec(), b"also".to_vec()]
    );
}

#[test]
fn encoder_fail_policy_is_fatal() {
    let (queues, _rxs) = shard_queues(1, 64);
    let cfg = ChunkConfig {
        encode_policy: ErrorPolicy::Fail,
        ..ChunkConfig::default()
    };
    let mut c = chain(LogDeser)
        .flat_map::<SubF, _>(split_body)
        .sink(
            SubEncoder,
            ToZero,
            cfg,
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    let bufs = payloads(&["a:BADROW"]);
    let (mut batch, ack_rx) = TestBatch::new(&bufs);
    assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Fatal(_)));
    drop(batch);
    assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Failed);
}

#[test]
fn fatal_class_encoder_errors_override_the_skip_policy() {
    // A Fatal-class error says the encoder itself is broken ("processing
    // must stop"); under the default Skip policy it must still stop the
    // pipeline instead of silently dropping every record.
    let (queues, _rxs) = shard_queues(1, 64);
    let mut c = chain(LogDeser)
        .flat_map::<SubF, _>(split_body)
        .sink(
            SubEncoder,
            ToZero,
            ChunkConfig::default(), // encode_policy: Skip
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    let bufs = payloads(&["a:FATALROW"]);
    let (mut batch, ack_rx) = TestBatch::new(&bufs);
    assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Fatal(_)));
    drop(batch);
    assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Failed);

    // Record-level errors still skip under the same policy.
    let (queues, mut rxs) = shard_queues(1, 64);
    let mut c = chain(LogDeser)
        .flat_map::<SubF, _>(split_body)
        .sink(
            SubEncoder,
            ToZero,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();
    let bufs = payloads(&["a:ok|BADROW"]);
    let (mut batch, _rx) = TestBatch::new(&bufs);
    assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    let _ = c.flush();
    assert_eq!(drain_rows(&mut rxs[0]), vec![b"ok".to_vec()]);
}

/// When two records of one payload both fail fatally, the sink handoff reports
/// the first, and the record between them never reaches a shard queue.
/// Reversing the order reverses the reason, so this pins push order rather than
/// a precedence between the two error classes.
#[test]
fn a_sink_handoff_reports_the_first_fatal_of_a_payload() {
    /// The reason one payload reports, and the rows that reached shard 0.
    fn run(payload: &str) -> (String, Vec<Vec<u8>>) {
        let (queues, mut rxs) = shard_queues(1, 64);
        // `Fail` is what lets a record-level `BADROW` error write the slot;
        // under `Skip` it is only counted. A `target_bytes` of 1 seals any
        // row that does encode, so a row pushed after the fatal shows up in
        // the returned rows.
        let cfg = ChunkConfig {
            target_bytes: 1,
            encode_policy: ErrorPolicy::Fail,
        };
        let mut c = chain(LogDeser)
            .flat_map::<SubF, _>(split_body)
            .sink(
                SubEncoder,
                ToZero,
                cfg,
                queues,
                Arc::new(InflightBudget::new()),
            )
            .build();

        let bufs = payloads(&[payload]);
        let (mut batch, _rx) = TestBatch::new(&bufs);
        let PushOutcome::Fatal(f) = c.push_batch(&mut batch, 0) else {
            panic!("expected fatal");
        };
        (f.reason, drain_rows(&mut rxs[0]))
    }

    let (first, rows) = run("a:FATALROW|ok|BADROW");
    assert!(first.contains("encoder broken"), "{first}");
    assert!(
        rows.is_empty(),
        "a row after the fatal was sealed: {rows:?}"
    );

    let (reversed, rows) = run("a:BADROW|ok|FATALROW");
    assert!(reversed.contains("unencodable row"), "{reversed}");
    assert!(
        rows.is_empty(),
        "a row after the fatal was sealed: {rows:?}"
    );
}

/// When two records of one payload both fail a `try_map` under `Fail`, the
/// stage reports the first, and the record between them never reaches the
/// sink. Reversing the order reverses the reason.
#[test]
fn a_try_map_reports_the_first_fatal_of_a_payload() {
    /// Split each body at `|` into owned rows, which puts the chain on the
    /// owned builder tier where `try_map` takes a plain closure.
    fn split_body_owned(e: LogEvent<'_>, em: &mut Emitter<'_, Owned<Vec<u8>>>) {
        for chunk in e.body.split(|&b| b == b'|') {
            em.emit(chunk.to_vec());
        }
    }

    /// The reason one payload reports, and the rows that reached shard 0.
    fn run(payload: &str) -> (String, Vec<Vec<u8>>) {
        let (queues, mut rxs) = shard_queues(1, 64);
        let mut c = chain(LogDeser)
            .flat_map::<Owned<Vec<u8>>, _>(split_body_owned)
            .try_map(
                |b: Vec<u8>| -> Result<Vec<u8>, &'static str> {
                    match b.as_slice() {
                        b"boom1" => Err("kaboom one"),
                        b"boom2" => Err("kaboom two"),
                        _ => Ok(b),
                    }
                },
                ErrorPolicy::Fail,
            )
            .sink(
                VecEncoder,
                KeyHashRouter,
                // A `target_bytes` of 1 seals any row that gets past the
                // `try_map`, so a record pushed after the fatal shows up in
                // the returned rows.
                ChunkConfig {
                    target_bytes: 1,
                    ..ChunkConfig::default()
                },
                queues,
                Arc::new(InflightBudget::new()),
            )
            .build();

        let bufs = payloads(&[payload]);
        let (mut batch, _rx) = TestBatch::new(&bufs);
        let PushOutcome::Fatal(f) = c.push_batch(&mut batch, 0) else {
            panic!("expected fatal");
        };
        (f.reason, drain_rows(&mut rxs[0]))
    }

    let (first, rows) = run("a:boom1|ok|boom2");
    assert!(first.contains("kaboom one"), "{first}");
    assert!(
        rows.is_empty(),
        "a row after the fatal was sealed: {rows:?}"
    );

    let (reversed, rows) = run("a:boom2|ok|boom1");
    assert!(reversed.contains("kaboom two"), "{reversed}");
    assert!(
        rows.is_empty(),
        "a row after the fatal was sealed: {rows:?}"
    );
}

/// When two shards fail to finalize in one flush, the reported reason names the
/// shard the seal loop reached first, whichever shard's row arrived first. A
/// shard that finalizes cleanly after another has latched still ships its chunk.
/// Regression for #361.
#[test]
fn a_flush_reports_the_first_shard_that_failed_to_finalize() {
    /// The reason one flush reports, and the rows that reached shard 1.
    fn run(rows: &[&str]) -> (String, Vec<Vec<u8>>) {
        // The default 64KiB target holds every row until the flush, so both
        // shards seal in the same `flush_terminal` pass.
        let (queues, mut rxs) = shard_queues(2, 64);
        let mut c = chain_owned(BytesPassthrough)
            .sink(
                SelectiveFinishEncoder::default(),
                ByPrefix,
                ChunkConfig::default(),
                queues,
                Arc::new(InflightBudget::new()),
            )
            .build();

        let bufs = payloads(rows);
        let (mut batch, _rx) = TestBatch::new(&bufs);
        assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
        let PushOutcome::Fatal(f) = c.flush() else {
            panic!("expected fatal");
        };
        (f.reason, drain_rows(&mut rxs[1]))
    }

    let (first, _) = run(&["s0:FATALFINISH", "s1:FATALFINISH"]);
    assert!(first.contains("s0:"), "{first}");

    let (reversed, _) = run(&["s1:FATALFINISH", "s0:FATALFINISH"]);
    assert!(reversed.contains("s0:"), "{reversed}");

    let (mixed, shipped) = run(&["s0:FATALFINISH", "s1:ok"]);
    assert!(mixed.contains("s0:"), "{mixed}");
    assert_eq!(
        shipped,
        vec![b"s1:ok".to_vec()],
        "a shard that finalized cleanly must still ship its chunk"
    );
}

// ---- backpressure / resume semantics ----------------------------------------

#[test]
fn blocked_resumes_without_rerunning_operators() {
    // Capacity 1 chunk, 1-byte target: every record seals a chunk, so the
    // second in-flight chunk parks and the chain reports Blocked between
    // payloads.
    let (queues, mut rxs) = shard_queues(1, 1);
    let cfg = ChunkConfig {
        target_bytes: 1,
        ..ChunkConfig::default()
    };
    let seen = Arc::new(AtomicUsize::new(0));
    let seen2 = Arc::clone(&seen);
    let mut c = chain_owned(BytesPassthrough)
        .map(move |b: Vec<u8>| {
            seen2.fetch_add(1, Ordering::Relaxed);
            b
        })
        .sink(
            VecEncoder,
            KeyHashRouter,
            cfg,
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    let bufs = payloads(&["r0", "r1", "r2", "r3"]);
    let (mut batch, _ack) = TestBatch::new(&bufs);

    let mut rows = Vec::new();
    let mut from = 0;
    let mut pushes = 0;
    loop {
        pushes += 1;
        assert!(pushes < 32, "must converge");
        match c.push_batch(&mut batch, from) {
            PushOutcome::Done => break,
            PushOutcome::Blocked { resume_at, reason } => {
                assert_eq!(reason, BlockReason::Capacity, "full queues are capacity");
                assert!(resume_at >= from);
                from = resume_at;
                rows.extend(drain_rows(&mut rxs[0]));
            }
            PushOutcome::Fatal(f) => panic!("unexpected fatal: {f}"),
        }
    }
    while !matches!(c.flush(), PushOutcome::Done) {
        rows.extend(drain_rows(&mut rxs[0]));
    }
    rows.extend(drain_rows(&mut rxs[0]));

    assert_eq!(
        rows,
        vec![
            b"r0".to_vec(),
            b"r1".to_vec(),
            b"r2".to_vec(),
            b"r3".to_vec()
        ],
        "all rows delivered exactly once, in order"
    );
    assert_eq!(
        seen.load(Ordering::Relaxed),
        4,
        "operators ran exactly once per record"
    );
    assert!(pushes > 1, "the scenario must actually exercise Blocked");
}

#[test]
fn flush_blocked_then_done_after_draining() {
    let (queues, mut rxs) = shard_queues(1, 1);
    let mut c = chain_owned(BytesPassthrough)
        .sink(
            VecEncoder,
            KeyHashRouter,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    // Two payloads accumulate (64KiB target, never sealed inline)...
    let bufs = payloads(&["a", "b"]);
    let (mut batch, _rx) = TestBatch::new(&bufs);
    assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    // ...then flush seals one chunk into the size-1 queue: Done. Push more
    // and flush again: the queue is full, so the chunk parks → Blocked.
    assert!(matches!(c.flush(), PushOutcome::Done));
    let bufs2 = payloads(&["c"]);
    let (mut batch2, _rx2) = TestBatch::new(&bufs2);
    assert!(matches!(c.push_batch(&mut batch2, 0), PushOutcome::Done));
    assert!(matches!(c.flush(), PushOutcome::Blocked { .. }));
    let first = drain_rows(&mut rxs[0]);
    assert_eq!(first, vec![b"a".to_vec(), b"b".to_vec()]);
    assert!(matches!(c.flush(), PushOutcome::Done));
    assert_eq!(drain_rows(&mut rxs[0]), vec![b"c".to_vec()]);
}

/// A flush that parks a chunk and latches a `finish_chunk` fatal in the same
/// pass returns the fatal.
#[test]
fn flush_reports_a_fatal_latched_while_the_terminal_is_blocked() {
    // Two shards, queue capacity 1, the default 64KiB target so nothing seals
    // inline. The second flush seals shard 0 into a full queue, which parks
    // the chunk, and then fails to finalize shard 1.
    fn run(second_row: &str) -> PushOutcome {
        let (queues, _rxs) = shard_queues(2, 1);
        let mut c = chain_owned(BytesPassthrough)
            .sink(
                SelectiveFinishEncoder::default(),
                ByPrefix,
                ChunkConfig::default(),
                queues,
                Arc::new(InflightBudget::new()),
            )
            .build();

        let bufs = payloads(&["s0:a"]);
        let (mut batch, _rx) = TestBatch::new(&bufs);
        assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
        // Fills shard 0's queue. Asserted rather than discarded, so that
        // dropping `_rxs` fails here instead of parking this chunk and
        // blocking the second flush for a different reason.
        assert!(matches!(c.flush(), PushOutcome::Done));

        let bufs2 = payloads(&["s0:b", second_row]);
        let (mut batch2, _rx2) = TestBatch::new(&bufs2);
        assert!(matches!(c.push_batch(&mut batch2, 0), PushOutcome::Done));
        c.flush()
    }

    assert!(
        matches!(
            run("s1:ok"),
            PushOutcome::Blocked {
                reason: BlockReason::Capacity,
                ..
            }
        ),
        "with shard 1 finalizing cleanly the same flush reports backpressure"
    );
    assert!(matches!(run("s1:FATALFINISH"), PushOutcome::Fatal(_)));
}

// ---- handoff details ---------------------------------------------------------

#[test]
fn chunk_acks_are_deduped_per_source_batch() {
    let (queues, mut rxs) = shard_queues(1, 64);
    let mut c = chain(LogDeser)
        .flat_map::<SubF, _>(split_body)
        .sink(
            SubEncoder,
            ToZero,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    let bufs = payloads(&["a:1|2|3|4|5"]);
    let (mut batch, _rx) = TestBatch::new(&bufs);
    assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    let _ = c.flush();
    let chunk = rxs[0].try_recv().unwrap();
    assert_eq!(chunk.rows, 5);
    assert_eq!(
        chunk.acks.len(),
        1,
        "five records of one source batch share one ack handle"
    );
}

#[test]
fn budget_grows_by_sealed_frame_bytes() {
    let (queues, mut rxs) = shard_queues(1, 64);
    let budget = Arc::new(InflightBudget::new());
    let mut c = chain_owned(BytesPassthrough)
        .sink(
            VecEncoder,
            KeyHashRouter,
            ChunkConfig::default(),
            queues,
            Arc::clone(&budget),
        )
        .build();

    let bufs = payloads(&["0123456789"]);
    let (mut batch, _rx) = TestBatch::new(&bufs);
    let _ = c.push_batch(&mut batch, 0);
    let _ = c.flush();
    let chunk = rxs[0].try_recv().unwrap();
    assert_eq!(budget.usage(), chunk.frame.len());
}

#[test]
fn multi_shard_routing_by_key_hash() {
    #[derive(Clone, Copy)]
    struct ByLen;
    impl ShardRouter for ByLen {
        fn route(&self, meta: &crate::record::RecordMeta, n: usize) -> usize {
            usize::try_from(meta.offset).unwrap() % n
        }
    }
    let (queues, mut rxs) = shard_queues(2, 64);
    let mut c = chain_owned(BytesPassthrough)
        .sink(
            VecEncoder,
            ByLen,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();
    let bufs = payloads(&["even", "odd", "even2", "odd2"]);
    let (mut batch, _rx) = TestBatch::new(&bufs);
    let _ = c.push_batch(&mut batch, 0);
    let _ = c.flush();
    assert_eq!(
        drain_rows(&mut rxs[0]),
        vec![b"even".to_vec(), b"even2".to_vec()]
    );
    assert_eq!(
        drain_rows(&mut rxs[1]),
        vec![b"odd".to_vec(), b"odd2".to_vec()]
    );
}

#[test]
fn record_router_routes_flat_map_children_independently_by_payload() {
    /// Record-aware: routes on the child's own payload, which no meta-only
    /// router can see (flat_map children share the parent's meta).
    #[derive(Clone, Copy)]
    struct ByChunkLen;
    impl RecordRouter<SubF> for ByChunkLen {
        fn route_record<'buf>(&self, rec: &Record<SubEvent<'buf>>, n: usize) -> usize {
            rec.payload.chunk.len() % n
        }
    }
    let (queues, mut rxs) = shard_queues(2, 64);
    let mut c = chain(LogDeser)
        .flat_map::<SubF, _>(split_body)
        .sink(
            SubEncoder,
            ByChunkLen,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    // ONE parent record: all four children carry identical RecordMeta, yet
    // route by their own chunk length, with even to shard 0 and odd to
    // shard 1.
    let bufs = payloads(&["k:aa|b|cc|d"]);
    let (mut batch, _rx) = TestBatch::new(&bufs);
    assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    assert!(matches!(c.flush(), PushOutcome::Done));

    assert_eq!(
        drain_rows(&mut rxs[0]),
        vec![b"aa".to_vec(), b"cc".to_vec()],
        "even-length children route to shard 0 by payload"
    );
    assert_eq!(
        drain_rows(&mut rxs[1]),
        vec![b"b".to_vec(), b"d".to_vec()],
        "odd-length children of the SAME parent route to shard 1"
    );
}

#[test]
fn meta_only_router_colocates_flat_map_children() {
    // The tier boundary, and the bridge exercised through the full terminal
    // stage: children inherit the parent's meta, so a meta-only router
    // (keyless here → stable partition hash) sends every child to one shard.
    let (queues, mut rxs) = shard_queues(2, 64);
    let mut c = chain(LogDeser)
        .flat_map::<SubF, _>(split_body)
        .sink(
            SubEncoder,
            KeyHashRouter,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    let bufs = payloads(&["k:aa|b|cc|d"]);
    let (mut batch, _rx) = TestBatch::new(&bufs);
    assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    assert!(matches!(c.flush(), PushOutcome::Done));

    let (zero, one) = (drain_rows(&mut rxs[0]), drain_rows(&mut rxs[1]));
    assert_eq!(
        zero.len() + one.len(),
        4,
        "every child reaches exactly one shard"
    );
    assert!(
        zero.is_empty() || one.is_empty(),
        "a meta-only router colocates all children of one parent"
    );
}

// ---- columnar (block-format) handoff -----------------------------------------

#[test]
fn columnar_encoder_seals_one_block_per_chunk_at_flush() {
    let (queues, mut rxs) = shard_queues(1, 64);
    let budget = Arc::new(InflightBudget::new());
    let mut c = chain_owned(BytesPassthrough)
        .sink(
            ColumnarEncoder::default(),
            ToZero,
            ChunkConfig::default(),
            queues,
            Arc::clone(&budget),
        )
        .build();

    let bufs = payloads(&["aa", "bbb", "c"]);
    let (mut batch, ack_rx) = TestBatch::new(&bufs);
    assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    // Nothing sealed yet: a columnar encoder buffers until the block closes.
    assert!(rxs[0].try_recv().is_err(), "no block before flush");
    assert!(matches!(c.flush(), PushOutcome::Done));

    let chunk = rxs[0].try_recv().expect("one columnar block at flush");
    assert_eq!(chunk.rows, 3, "block carries every buffered row");
    assert_eq!(
        decode_block(&chunk.frame),
        vec![b"aa".to_vec(), b"bbb".to_vec(), b"c".to_vec()]
    );
    assert!(rxs[0].try_recv().is_err(), "exactly one block");

    // Delivering the block resolves the buffered rows' acks, never before.
    chunk.acks.deliver();
    drop(batch);
    assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Delivered);
}

#[test]
fn columnar_buffered_bytes_seals_before_flush() {
    // A small target: the encoder's `buffered_bytes` crosses it mid-batch and
    // seals a block without waiting for flush (proves the threshold reads the
    // columnar encoder's internal size, not the empty chunk buffer).
    let cfg = ChunkConfig {
        target_bytes: 16,
        ..ChunkConfig::default()
    };
    let (queues, mut rxs) = shard_queues(1, 64);
    let mut c = chain_owned(BytesPassthrough)
        .sink(
            ColumnarEncoder::default(),
            ToZero,
            cfg,
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    // buffered_bytes = 4 header + per row (4 len + 4 bytes). After 2 rows =
    // 4 + 8 + 8 = 20 >= 16 -> seal; the 3rd row seals at flush.
    let bufs = payloads(&["aaaa", "bbbb", "cccc"]);
    let (mut batch, _rx) = TestBatch::new(&bufs);
    let _ = c.push_batch(&mut batch, 0);
    let _ = c.flush();

    let b1 = rxs[0].try_recv().expect("mid-batch block");
    let b2 = rxs[0].try_recv().expect("flush block");
    assert_eq!(b1.rows, 2, "sealed once the target was reached");
    assert_eq!(b2.rows, 1, "remainder sealed at flush");
    let all: Vec<_> = decode_block(&b1.frame)
        .into_iter()
        .chain(decode_block(&b2.frame))
        .collect();
    assert_eq!(
        all,
        vec![b"aaaa".to_vec(), b"bbbb".to_vec(), b"cccc".to_vec()]
    );
}

#[test]
fn columnar_blocks_are_per_shard_pure_under_interleaving() {
    #[derive(Clone, Copy)]
    struct ByOffset;
    impl ShardRouter for ByOffset {
        fn route(&self, meta: &crate::record::RecordMeta, n: usize) -> usize {
            usize::try_from(meta.offset).unwrap() % n
        }
    }
    let (queues, mut rxs) = shard_queues(2, 64);
    let mut c = chain_owned(BytesPassthrough)
        .sink(
            ColumnarEncoder::default(),
            ByOffset,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    // Offsets 0..6 route 0,1,0,1,0,1, interleaved across two per-shard
    // encoders. A single shared encoder would mix these into one block.
    let bufs = payloads(&["s0a", "s1a", "s0b", "s1b", "s0c", "s1c"]);
    let (mut batch, _rx) = TestBatch::new(&bufs);
    let _ = c.push_batch(&mut batch, 0);
    let _ = c.flush();

    let block0 = rxs[0].try_recv().expect("shard 0 block");
    let block1 = rxs[1].try_recv().expect("shard 1 block");
    assert_eq!(
        decode_block(&block0.frame),
        vec![b"s0a".to_vec(), b"s0b".to_vec(), b"s0c".to_vec()],
        "shard 0's block holds only shard 0's rows, in order"
    );
    assert_eq!(
        decode_block(&block1.frame),
        vec![b"s1a".to_vec(), b"s1b".to_vec(), b"s1c".to_vec()],
    );
    assert_eq!((block0.rows, block1.rows), (3, 3));
}

#[test]
fn columnar_buffered_rows_at_teardown_fail_for_replay() {
    let (queues, _rxs) = shard_queues(1, 64);
    let mut c = chain_owned(BytesPassthrough)
        .sink(
            ColumnarEncoder::default(),
            ToZero,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    let bufs = payloads(&["buffered-but-never-sealed"]);
    let (mut batch, ack_rx) = TestBatch::new(&bufs);
    let _ = c.push_batch(&mut batch, 0);
    // No flush: the row is buffered in the encoder and never becomes a block.
    drop(c); // teardown drops the shard (encoder + its captured acks)
    drop(batch);
    assert_eq!(
        ack_rx.try_recv().unwrap().status,
        AckStatus::Failed,
        "buffered-but-unwritten rows must fail so they replay after restart"
    );
}

// ---- factory ----------------------------------------------------------------

#[test]
fn factory_stamps_independent_chains() {
    let (queues, mut rxs) = shard_queues(1, 64);
    let factory = chain_owned(OwnedPassthrough)
        .map(|b: Vec<u8>| b)
        .sink(
            VecEncoder,
            KeyHashRouter,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build_factory();

    let mut c1 = factory.make();
    let mut c2 = factory.make();

    let b1 = payloads(&["one"]);
    let b2 = payloads(&["two"]);
    let (mut batch1, _r1) = TestBatch::new(&b1);
    let (mut batch2, _r2) = TestBatch::new(&b2);
    assert!(matches!(c1.push_batch(&mut batch1, 0), PushOutcome::Done));
    assert!(matches!(c2.push_batch(&mut batch2, 0), PushOutcome::Done));
    let _ = c1.flush();
    let _ = c2.flush();
    let rows = drain_rows(&mut rxs[0]);
    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&b"one".to_vec()));
    assert!(rows.contains(&b"two".to_vec()));
}

// ---- split terminal ----------------------------------------------------------

#[test]
fn split_routes_records_to_their_typed_branch() {
    // One borrowed source stream fans into two heterogeneously-typed branches:
    // `a*` keys route their body to the SubEvent branch, `k*` keys route their
    // key to the KeyEvent branch. `z*` matches neither (unmatched Skip).
    let (sub_q, mut sub_rx) = shard_queues(1, 64);
    let (key_q, mut key_rx) = shard_queues(1, 64);
    let budget = Arc::new(InflightBudget::new());

    let mut split = chain(LogDeser).split(ErrorPolicy::Skip);
    let sub = split.add::<SubF, _, _>(
        SubEncoder,
        ToZero,
        SinkCtx::new("sub".into(), sub_q, Arc::clone(&budget)),
    );
    let key = split.add::<KeyF, _, _>(
        KeyEncoder,
        ToZero,
        SinkCtx::new("key".into(), key_q, Arc::clone(&budget)),
    );
    let mut c = split
        .route(move |e: LogEvent<'_>, out| {
            if e.key.starts_with('a') {
                out.emit(sub, SubEvent { chunk: e.body });
            } else if e.key.starts_with('k') {
                out.emit(
                    key,
                    KeyEvent {
                        key: e.key.as_bytes(),
                    },
                );
            }
            // else: matches no branch -> `unmatched` (Skip here)
        })
        .build();

    let bufs = payloads(&["a1:body-a", "k1:body-k", "z9:dropme"]);
    let (mut batch, ack_rx) = TestBatch::new(&bufs);
    assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    assert!(matches!(c.flush(), PushOutcome::Done));

    assert_eq!(
        drain_rows(&mut sub_rx[0]),
        vec![b"body-a".to_vec()],
        "the a-keyed record's body reached the SubEvent branch"
    );
    assert_eq!(
        drain_rows(&mut key_rx[0]),
        vec![b"k1".to_vec()],
        "the k-keyed record's key reached the KeyEvent branch"
    );

    // The unmatched `z9` record was dropped by the Skip policy; its ack share
    // releases as success, so the batch resolves Delivered.
    drop(batch);
    assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Delivered);
}

#[test]
fn split_unmatched_fail_stops_the_pipeline() {
    let (sub_q, _rx) = shard_queues(1, 64);
    let budget = Arc::new(InflightBudget::new());

    let mut split = chain(LogDeser).split(ErrorPolicy::Fail);
    let sub = split.add::<SubF, _, _>(
        SubEncoder,
        ToZero,
        SinkCtx::new("sub".into(), sub_q, budget),
    );
    let mut c = split
        .route(move |e: LogEvent<'_>, out| {
            if e.key.starts_with('a') {
                out.emit(sub, SubEvent { chunk: e.body });
            }
        })
        .build();

    let bufs = payloads(&["z9:nomatch"]);
    let (mut batch, ack_rx) = TestBatch::new(&bufs);
    let PushOutcome::Fatal(f) = c.push_batch(&mut batch, 0) else {
        panic!("unmatched record under Fail policy must stop the pipeline");
    };
    assert!(f.reason.contains("no split branch"));
    drop(batch);
    drop(c);
    assert_eq!(ack_rx.try_recv().unwrap().status, AckStatus::Failed);
}

#[test]
fn split_holds_watermark_until_every_branch_is_written() {
    // A single source batch fans across two branches; the batch resolves
    // Delivered only once BOTH branches' chunks are durably written, the
    // multi-sink at-least-once contract, straight out of the shared AckRef.
    let (sub_q, mut sub_rx) = shard_queues(1, 64);
    let (key_q, mut key_rx) = shard_queues(1, 64);
    let budget = Arc::new(InflightBudget::new());

    let mut split = chain(LogDeser).split(ErrorPolicy::Fail);
    let sub = split.add::<SubF, _, _>(
        SubEncoder,
        ToZero,
        SinkCtx::new("sub".into(), sub_q, Arc::clone(&budget)),
    );
    let key = split.add::<KeyF, _, _>(
        KeyEncoder,
        ToZero,
        SinkCtx::new("key".into(), key_q, Arc::clone(&budget)),
    );
    let mut c = split
        .route(move |e: LogEvent<'_>, out| {
            if e.key.starts_with('a') {
                out.emit(sub, SubEvent { chunk: e.body });
            } else {
                out.emit(
                    key,
                    KeyEvent {
                        key: e.key.as_bytes(),
                    },
                );
            }
        })
        .build();

    let bufs = payloads(&["a:xa", "k:xk"]);
    let (mut batch, ack_rx) = TestBatch::new(&bufs);
    assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    assert!(matches!(c.flush(), PushOutcome::Done));
    drop(batch);

    // Deliver only the SubEvent branch: the KeyEvent branch still holds an ack
    // clone of the same source batch, so the watermark must NOT advance.
    while let Ok(chunk) = sub_rx[0].try_recv() {
        chunk.acks.deliver();
    }
    assert!(
        ack_rx.try_recv().is_err(),
        "batch stays unresolved while a branch has not written"
    );

    // Deliver the KeyEvent branch too: the last clone drops and it resolves.
    while let Ok(chunk) = key_rx[0].try_recv() {
        chunk.acks.deliver();
    }
    assert_eq!(
        ack_rx
            .try_recv()
            .expect("batch resolves once all branches wrote")
            .status,
        AckStatus::Delivered
    );
}

#[test]
fn split_one_branch_failure_fails_the_whole_batch() {
    // Worst-status merge: if any branch's write is abandoned (its chunk's
    // AckSet drops undelivered), the source batch resolves Failed so its
    // offsets never commit, even though the other branch wrote successfully.
    let (sub_q, mut sub_rx) = shard_queues(1, 64);
    let (key_q, mut key_rx) = shard_queues(1, 64);
    let budget = Arc::new(InflightBudget::new());

    let mut split = chain(LogDeser).split(ErrorPolicy::Fail);
    let sub = split.add::<SubF, _, _>(
        SubEncoder,
        ToZero,
        SinkCtx::new("sub".into(), sub_q, Arc::clone(&budget)),
    );
    let key = split.add::<KeyF, _, _>(
        KeyEncoder,
        ToZero,
        SinkCtx::new("key".into(), key_q, Arc::clone(&budget)),
    );
    let mut c = split
        .route(move |e: LogEvent<'_>, out| {
            if e.key.starts_with('a') {
                out.emit(sub, SubEvent { chunk: e.body });
            } else {
                out.emit(
                    key,
                    KeyEvent {
                        key: e.key.as_bytes(),
                    },
                );
            }
        })
        .build();

    let bufs = payloads(&["a:xa", "k:xk"]);
    let (mut batch, ack_rx) = TestBatch::new(&bufs);
    assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    assert!(matches!(c.flush(), PushOutcome::Done));
    drop(batch);

    // The SubEvent branch writes durably...
    while let Ok(chunk) = sub_rx[0].try_recv() {
        chunk.acks.deliver();
    }
    // ...but the KeyEvent branch's write is abandoned (chunk dropped undelivered).
    while let Ok(chunk) = key_rx[0].try_recv() {
        drop(chunk);
    }
    assert_eq!(
        ack_rx.try_recv().expect("batch resolves").status,
        AckStatus::Failed,
        "a single branch's abandoned write must fail the whole batch"
    );
}

// ---- metrics ------------------------------------------------------------------

#[test]
fn stages_flush_batch_metrics() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        let (queues, _rxs) = shard_queues(1, 64);
        let mut c = chain(LogDeser)
            .with_metrics("testpipe", "main")
            .filter(non_empty)
            .flat_map::<SubF, _>(split_body)
            .sink(
                SubEncoder,
                ToZero,
                ChunkConfig::default(),
                queues,
                Arc::new(InflightBudget::new()),
            )
            .build();

        let bufs = payloads(&["a:1|2", "b:", "ERR:x"]);
        let (mut batch, _rx) = TestBatch::new(&bufs);
        assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    });
    let rendered = handle.render();
    assert!(
        rendered.contains("spate_operator_records_in_total"),
        "operator counters exported: {rendered}"
    );
    assert!(
        rendered.contains(r#"component="main.0_filter""#),
        "filter stage labeled: {rendered}"
    );
    assert!(
        rendered.contains(r#"reason="filtered""#),
        "filtered drop counted: {rendered}"
    );
    assert!(
        rendered.contains(r#"component="main.deserializer""#),
        "deser metrics present: {rendered}"
    );
    assert!(
        rendered.contains(r#"component="main.1_flat_map""#),
        "flat_map stage present: {rendered}"
    );
}

/// Every `spate_operator_errors_total` series in `rendered`, sorted.
fn error_series(rendered: &str) -> Vec<String> {
    let mut out: Vec<String> = rendered
        .lines()
        .filter(|l| {
            l.starts_with(&format!(
                "{}{{",
                crate::metrics::names::OPERATOR_ERRORS_TOTAL
            ))
        })
        .map(str::to_owned)
        .collect();
    out.sort();
    out
}

/// The labels a stage of this index and kind publishes under.
fn stage_labels(idx: usize, kind: &str) -> String {
    format!(r#"pipeline="errtest",component="main.{idx}_{kind}",component_type="{kind}""#)
}

/// The labels a split branch's terminal publishes under.
fn branch_labels(sink: &str) -> String {
    format!(r#"pipeline="errtest",component="main.sink.{sink}",component_type="sink_handoff""#)
}

/// The `spate_operator_errors_total` series one stage publishes, in
/// [`error_series`] order. Handles register eagerly, so a stage that has not
/// erred still renders both values at `0`.
fn stage_errors(labels: &str, record_level: u64, fatal: u64) -> Vec<String> {
    let name = crate::metrics::names::OPERATOR_ERRORS_TOTAL;
    vec![
        format!(r#"{name}{{{labels},error_type="fatal"}} {fatal}"#),
        format!(r#"{name}{{{labels},error_type="record_level"}} {record_level}"#),
    ]
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

/// Render `f` against a recorder of its own.
fn render(f: impl FnOnce()) -> String {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, f);
    handle.render()
}

/// A `try_map` stopping the pipeline is counted on the stage that stopped it,
/// and no stage carries a `retryable` series. Regression for #335.
#[test]
fn a_try_map_fail_trip_counts_a_fatal_on_its_own_stage() {
    let rendered = render(|| {
        let (queues, _rxs) = shard_queues(1, 64);
        let mut c = chain_owned(OwnedPassthrough)
            .with_metrics("errtest", "main")
            .try_map(
                |b: Vec<u8>| -> Result<Vec<u8>, &'static str> {
                    if b == b"boom" { Err("kaboom") } else { Ok(b) }
                },
                ErrorPolicy::Fail,
            )
            .sink(
                VecEncoder,
                KeyHashRouter,
                ChunkConfig::default(),
                queues,
                Arc::new(InflightBudget::new()),
            )
            .build();

        let bufs = payloads(&["fine", "boom", "after"]);
        let (mut batch, _rx) = TestBatch::new(&bufs);
        assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Fatal(_)));
    });

    let mut expected = stage_errors(&stage_labels(0, "try_map"), 0, 1);
    expected.extend(stage_errors(&stage_labels(1, "sink_handoff"), 0, 0));
    assert_eq!(
        error_series(&rendered),
        sorted(expected),
        "the try_map stage is the only one that erred, and no stage carries a \
         retryable series:\n{rendered}"
    );
}

/// An encoder error counts under the `error_type` naming its disposition. A
/// `Fatal` class overrides the Skip policy and counts as fatal.
#[test]
fn encoder_error_classes_land_on_their_own_error_type() {
    let record_level = render(|| {
        let (queues, mut rxs) = shard_queues(1, 64);
        let mut c = chain(LogDeser)
            .with_metrics("errtest", "main")
            .flat_map::<SubF, _>(split_body)
            .sink(
                SubEncoder,
                ToZero,
                ChunkConfig::default(),
                queues,
                Arc::new(InflightBudget::new()),
            )
            .build();
        let bufs = payloads(&["a:ok|BADROW"]);
        let (mut batch, _rx) = TestBatch::new(&bufs);
        assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
        let _ = c.flush();
        drain_rows(&mut rxs[0]);
    });
    let mut expected = stage_errors(&stage_labels(0, "flat_map"), 0, 0);
    expected.extend(stage_errors(&stage_labels(1, "sink_handoff"), 1, 0));
    assert_eq!(
        error_series(&record_level),
        sorted(expected),
        "a skipped row is record-level:\n{record_level}"
    );

    let fatal = render(|| {
        let (queues, _rxs) = shard_queues(1, 64);
        let mut c = chain(LogDeser)
            .with_metrics("errtest", "main")
            .flat_map::<SubF, _>(split_body)
            .sink(
                SubEncoder,
                ToZero,
                ChunkConfig::default(), // encode_policy: Skip, which Fatal overrides
                queues,
                Arc::new(InflightBudget::new()),
            )
            .build();
        let bufs = payloads(&["a:FATALROW|FATALROW|ok"]);
        let (mut batch, _rx) = TestBatch::new(&bufs);
        assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Fatal(_)));
    });
    let mut expected = stage_errors(&stage_labels(0, "flat_map"), 0, 0);
    expected.extend(stage_errors(&stage_labels(1, "sink_handoff"), 0, 1));
    assert_eq!(
        error_series(&fatal),
        sorted(expected),
        "a Fatal-class encoder error overrides Skip and counts a fatal:\n{fatal}"
    );
}

/// A record matching no split branch counts a fatal under `Fail`, and under
/// `Skip` counts a drop and no error.
#[test]
fn split_unmatched_counts_a_fatal_only_under_the_fail_policy() {
    let fail = render(|| {
        let (sub_q, _rx) = shard_queues(1, 64);
        let mut split = chain(LogDeser)
            .with_metrics("errtest", "main")
            .split(ErrorPolicy::Fail);
        let sub = split.add::<SubF, _, _>(
            SubEncoder,
            ToZero,
            SinkCtx::new("sub".into(), sub_q, Arc::new(InflightBudget::new())),
        );
        let mut c = split
            .route(move |e: LogEvent<'_>, out| {
                if e.key.starts_with('a') {
                    out.emit(sub, SubEvent { chunk: e.body });
                }
            })
            .build();
        let bufs = payloads(&["z9:nomatch"]);
        let (mut batch, _rx) = TestBatch::new(&bufs);
        assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Fatal(_)));
    });
    let mut expected = stage_errors(&stage_labels(0, "split"), 0, 1);
    expected.extend(stage_errors(&branch_labels("sub"), 0, 0));
    assert_eq!(
        error_series(&fail),
        sorted(expected),
        "the split stage stopped the pipeline:\n{fail}"
    );

    let skip = render(|| {
        let (sub_q, _rx) = shard_queues(1, 64);
        let mut split = chain(LogDeser)
            .with_metrics("errtest", "main")
            .split(ErrorPolicy::Skip);
        let sub = split.add::<SubF, _, _>(
            SubEncoder,
            ToZero,
            SinkCtx::new("sub".into(), sub_q, Arc::new(InflightBudget::new())),
        );
        let mut c = split
            .route(move |e: LogEvent<'_>, out| {
                if e.key.starts_with('a') {
                    out.emit(sub, SubEvent { chunk: e.body });
                }
            })
            .build();
        let bufs = payloads(&["z9:nomatch"]);
        let (mut batch, _rx) = TestBatch::new(&bufs);
        assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    });
    let mut expected = stage_errors(&stage_labels(0, "split"), 0, 0);
    expected.extend(stage_errors(&branch_labels("sub"), 0, 0));
    assert_eq!(
        error_series(&skip),
        sorted(expected),
        "an unrouted record under Skip is a drop, not an error:\n{skip}"
    );
    assert!(
        skip.contains(r#"reason="unrouted""#),
        "it is counted as a drop:\n{skip}"
    );
}

/// A `finish_chunk` failure is returned by the `flush` that latched it and
/// counts one fatal, however many times the stage is flushed afterwards.
/// Regression for #351.
#[test]
fn a_finish_chunk_failure_counts_a_fatal_on_the_stage_that_raised_it() {
    let rendered = render(|| {
        let (queues, mut rxs) = shard_queues(1, 64);
        let mut c = chain_owned(OwnedPassthrough)
            .with_metrics("errtest", "main")
            .sink(
                BrokenFinishEncoder,
                KeyHashRouter,
                ChunkConfig::default(),
                queues,
                Arc::new(InflightBudget::new()),
            )
            .build();

        let bufs = payloads(&["row"]);
        let (mut batch, _rx) = TestBatch::new(&bufs);
        assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
        assert!(matches!(c.flush(), PushOutcome::Fatal(_)));
        // The drain flushes again over the same rows.
        let _ = c.flush();
        let _ = c.flush();
        assert!(
            rxs[0].try_recv().is_err(),
            "a chunk whose finalize failed must not be shipped"
        );
    });

    assert_eq!(
        error_series(&rendered),
        stage_errors(&stage_labels(0, "sink_handoff"), 0, 1),
        "a broken finalize is one fatal for the stage that raised it:\n{rendered}"
    );
}

/// A record the Skip policy drops counts a record-level error and no fatal.
#[test]
fn a_skipped_record_counts_no_fatal() {
    let rendered = render(|| {
        let (queues, _rxs) = shard_queues(1, 64);
        let mut c = chain_owned(OwnedPassthrough)
            .with_metrics("errtest", "main")
            .try_map(
                |b: Vec<u8>| -> Result<Vec<u8>, &'static str> {
                    if b == b"boom" { Err("kaboom") } else { Ok(b) }
                },
                ErrorPolicy::Skip,
            )
            .sink(
                VecEncoder,
                KeyHashRouter,
                ChunkConfig::default(),
                queues,
                Arc::new(InflightBudget::new()),
            )
            .build();

        let bufs = payloads(&["fine", "boom"]);
        let (mut batch, _rx) = TestBatch::new(&bufs);
        assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    });

    let mut expected = stage_errors(&stage_labels(0, "try_map"), 1, 0);
    expected.extend(stage_errors(&stage_labels(1, "sink_handoff"), 0, 0));
    assert_eq!(
        error_series(&rendered),
        sorted(expected),
        "a Skip trip is record-level and stops nothing:\n{rendered}"
    );
}

/// `ChainFactory` promises `Send + Sync` when its parts are, so no stage may
/// hold a non-`Sync` field.
#[test]
fn a_chain_factory_of_sync_parts_is_sync() {
    fn assert_send_sync<T: Send + Sync>(_: &T) {}
    let (queues, _rxs) = shard_queues(1, 64);
    let factory = chain_owned(OwnedPassthrough)
        .try_map(
            |b: Vec<u8>| -> Result<Vec<u8>, &'static str> { Ok(b) },
            ErrorPolicy::Fail,
        )
        .sink(
            VecEncoder,
            KeyHashRouter,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        );
    assert_send_sync(&factory);
}

// ---- teardown safety --------------------------------------------------------

#[test]
fn dropping_a_blocked_chain_fails_unsent_acks() {
    // Queue capacity 1 and per-row chunk sealing: the first chunk fills the
    // queue, the second parks. Tearing the chain down without draining must
    // resolve the batch Failed, never Delivered, or offsets would commit
    // for rows that no sink ever wrote.
    let (queues, rxs) = shard_queues(1, 1);
    let budget = Arc::new(InflightBudget::new());
    let cfg = ChunkConfig {
        target_bytes: 1,
        ..ChunkConfig::default()
    };
    let c = chain(LogDeser)
        .filter(non_empty)
        .flat_map::<SubF, _>(split_body)
        .sink(SubEncoder, ToZero, cfg, queues, Arc::clone(&budget));
    let mut c = c.build();

    let bufs = payloads(&["a:one", "b:two"]);
    let (mut batch, ack_rx) = TestBatch::new(&bufs);
    let _ = c.push_batch(&mut batch, 0);
    let _ = c.flush();
    drop(batch);
    drop(c);
    // The successfully-queued first chunk still holds an ack clone; the
    // batch resolves once the sink side lets go of it too.
    drop(rxs);
    let msg = ack_rx.try_recv().expect("batch resolves at teardown");
    assert_eq!(
        msg.status,
        AckStatus::Failed,
        "unsent output must fail the batch so its offsets never commit"
    );
}

#[test]
fn dropping_a_chain_with_partial_buffers_fails_their_acks() {
    // Default chunk config: rows accumulate in the shard buffer and are
    // never sealed. Dropping the chain without flush() must fail the batch.
    let (queues, _rxs) = shard_queues(1, 64);
    let budget = Arc::new(InflightBudget::new());
    let mut c = chain(LogDeser)
        .filter(non_empty)
        .flat_map::<SubF, _>(split_body)
        .sink(
            SubEncoder,
            ToZero,
            ChunkConfig::default(),
            queues,
            Arc::clone(&budget),
        )
        .build();

    let bufs = payloads(&["a:one|two"]);
    let (mut batch, ack_rx) = TestBatch::new(&bufs);
    assert!(matches!(c.push_batch(&mut batch, 0), PushOutcome::Done));
    drop(batch);
    drop(c); // no flush: rows still buffered
    let msg = ack_rx.try_recv().expect("batch resolves at teardown");
    assert_eq!(msg.status, AckStatus::Failed);
}

// ---- not-ready replay -------------------------------------------------------

/// Wraps LogDeser; payloads whose body starts with `wait:` return NotReady
/// a fixed number of times before decoding normally.
struct FlakyDeser {
    inner: LogDeser,
    not_ready_remaining: u32,
    attempts_on_flaky: u32,
}

impl Deserializer<LogF> for FlakyDeser {
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, LogEvent<'buf>>,
    ) -> Result<(), DeserError> {
        if raw.bytes.ends_with(b"|wait") {
            self.attempts_on_flaky += 1;
            if self.not_ready_remaining > 0 {
                self.not_ready_remaining -= 1;
                return Err(DeserError::NotReady {
                    reason: "schema fetch in flight".into(),
                });
            }
        }
        self.inner.deserialize(raw, ack, out)
    }
}

#[test]
fn not_ready_blocks_then_replays_without_loss_or_duplication() {
    let (queues, mut rxs) = shard_queues(1, 64);
    let budget = Arc::new(InflightBudget::new());
    let mut c = chain(FlakyDeser {
        inner: LogDeser,
        not_ready_remaining: 2,
        attempts_on_flaky: 0,
    })
    .filter(non_empty)
    .flat_map::<SubF, _>(split_body)
    .sink(
        SubEncoder,
        ToZero,
        ChunkConfig::default(),
        queues,
        Arc::clone(&budget),
    )
    .build();

    // Payload 1 is the flaky one; payloads 0 and 2 decode immediately.
    let bufs = payloads(&["a:one", "b:two|wait", "c:three"]);
    let (mut batch, ack_rx) = TestBatch::new(&bufs);

    // First push: payload 0 flows, payload 1 reports NotReady → Blocked at
    // index 1, and the payload is stashed for replay.
    let PushOutcome::Blocked { resume_at, reason } = c.push_batch(&mut batch, 0) else {
        panic!("expected Blocked while the schema is not ready");
    };
    assert_eq!(resume_at, 1);
    assert_eq!(
        reason,
        BlockReason::NotReady,
        "a dependency wait must not read as sink backpressure"
    );

    // Still not ready: Blocked again at the same index.
    let PushOutcome::Blocked { resume_at, .. } = c.push_batch(&mut batch, 1) else {
        panic!("expected Blocked on second attempt");
    };
    assert_eq!(resume_at, 1);

    // Ready now: the replayed payload decodes and the rest of the batch
    // completes.
    assert!(matches!(c.push_batch(&mut batch, 1), PushOutcome::Done));
    assert!(matches!(c.flush(), PushOutcome::Done));

    let rows = drain_rows(&mut rxs[0]);
    assert_eq!(
        rows,
        vec![
            b"one".to_vec(),
            b"two".to_vec(),
            b"wait".to_vec(),
            b"three".to_vec()
        ],
        "every record exactly once, in order"
    );

    // NotReady is not an error: the batch resolves Delivered.
    drop(batch);
    drop(c);
    let msg = ack_rx.try_recv().expect("batch resolved");
    assert_eq!(msg.status, AckStatus::Delivered);
}

/// After the driver abandons a batch blocked mid-push at shutdown,
/// `abandon_batch` must clear the chain's mid-batch cursor and not-ready
/// stash so a fresh batch pushed from index 0 neither trips the resume
/// asserts (debug) nor replays the stale payload under the new ack
/// (release). Terminal chunks already parked from the abandoned batch keep
/// their own (now failed) acks.
#[test]
fn abandon_batch_resets_mid_batch_state_for_the_next_batch() {
    let (queues, mut rxs) = shard_queues(1, 64);
    let budget = Arc::new(InflightBudget::new());
    let mut c = chain(FlakyDeser {
        inner: LogDeser,
        // Never becomes ready during this test: the flaky payload always
        // reports NotReady and stays stashed until we abandon it.
        not_ready_remaining: 9,
        attempts_on_flaky: 0,
    })
    .flat_map::<SubF, _>(split_body)
    .sink(
        SubEncoder,
        ToZero,
        ChunkConfig::default(),
        queues,
        Arc::clone(&budget),
    )
    .build();

    // Batch 1: payload 0 ("a:one") flows into the terminal, payload 1
    // ("b:two|wait") reports NotReady → blocked mid-batch at cursor 1 with
    // the payload stashed.
    let b1 = payloads(&["a:one", "b:two|wait"]);
    let (mut batch1, _ack1_rx) = TestBatch::new(&b1);
    let PushOutcome::Blocked { resume_at, reason } = c.push_batch(&mut batch1, 0) else {
        panic!("expected Blocked NotReady");
    };
    assert_eq!(resume_at, 1);
    assert_eq!(reason, BlockReason::NotReady);

    // The driver's shutdown-abandon path: fail the ack, then discard the
    // chain's per-batch state.
    batch1.ack().fail();
    c.abandon_batch();
    drop(batch1);

    // A fresh batch from index 0. Without abandon_batch this would panic on
    // `debug_assert_eq!(from, self.cursor)` (0 != 1) or replay "b:two|wait".
    let b2 = payloads(&["c:three", "d:four"]);
    let (mut batch2, ack2_rx) = TestBatch::new(&b2);
    assert!(matches!(c.push_batch(&mut batch2, 0), PushOutcome::Done));
    assert!(matches!(c.flush(), PushOutcome::Done));

    let rows = drain_rows(&mut rxs[0]);
    // "one" is batch 1's already-parked terminal row (kept, with its failed
    // ack); the stashed "two"/"wait" is gone; batch 2's rows follow. No
    // duplication, no misattributed replay.
    assert_eq!(
        rows,
        vec![b"one".to_vec(), b"three".to_vec(), b"four".to_vec()]
    );
    drop(batch2);
    assert_eq!(ack2_rx.try_recv().unwrap().status, AckStatus::Delivered);
}
