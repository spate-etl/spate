//! Operator-chain tests: combinators, resume semantics, terminal handoff.

use super::*;
use crate::backpressure::InflightBudget;
use crate::checkpoint::{AckMsg, AckRef, AckStatus};
use crate::deser::{BytesPassthrough, Deserializer, EmitRecord, Owned, RecFamily};
use crate::error::{DeserError, ErrorPolicy, SinkError};
use crate::record::{PartitionId, RawPayload, Record};
use crate::sink::{EncodedChunk, KeyHashRouter, RowEncoder, ShardRouter, shard_queues};
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

/// Length-prefixed encoder for `SubEvent`; fails on `BADROW` chunks.
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
        buf.extend_from_slice(rec.payload.chunk);
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
            PushOutcome::Blocked { resume_at } => {
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
        rendered.contains("etl_operator_records_in_total"),
        "operator counters exported: {rendered}"
    );
    assert!(
        rendered.contains(r#"component="main.0_filter""#),
        "filter stage labelled: {rendered}"
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

// ---- teardown safety --------------------------------------------------------

#[test]
fn dropping_a_blocked_chain_fails_unsent_acks() {
    // Queue capacity 1 and per-row chunk sealing: the first chunk fills the
    // queue, the second parks. Tearing the chain down without draining must
    // resolve the batch Failed — never Delivered — or offsets would commit
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
