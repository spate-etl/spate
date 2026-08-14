//! The operator-chain bench rigs, shared by `benches/chain_wall.rs` (wall
//! time), `benches/chain_gungraun.rs` (instruction counts) and
//! `tests/bench_fixtures.rs` (the corpus pins).
//!
//! Included with `#[path]` rather than imported, because a bench target is its
//! own crate and several can only agree on a workload by compiling the same
//! source. If they measured different rigs, a wall-time result and an
//! instruction count would not describe the same code, and the test would be
//! pinning bytes neither of them ran.

#![allow(dead_code, reason = "each target uses a different subset")]

use spate_core::backpressure::InflightBudget;
use spate_core::checkpoint::AckRef;
use spate_core::deser::{Deserializer, EmitRecord, Owned, RecFamily};
use spate_core::error::DeserError;
use spate_core::ops::{ChunkConfig, Emitter, PushOutcome, RunnableChain, chain, chain_owned};
use spate_core::record::{PartitionId, RawPayload, Record, RecordMeta};
use spate_core::sink::{EncodedChunk, KeyHashRouter, RowEncoder, ShardRouter, shard_queues};
use spate_core::source::PayloadBatch;
use std::sync::Arc;

struct LogEvent<'buf> {
    body: &'buf [u8],
}
struct LogF;
impl RecFamily for LogF {
    type Rec<'buf> = LogEvent<'buf>;
}
struct SubEvent<'buf> {
    chunk: &'buf [u8],
}
struct SubF;
impl RecFamily for SubF {
    type Rec<'buf> = SubEvent<'buf>;
}

#[derive(Clone)]
struct BorrowDeser;
impl Deserializer<LogF> for BorrowDeser {
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, LogEvent<'buf>>,
    ) -> Result<(), DeserError> {
        let _ = out.emit(Record {
            payload: LogEvent { body: raw.bytes },
            meta: raw.meta(),
            ack: ack.clone(),
        });
        Ok(())
    }
}

/// Owned-payload equivalent: copies every payload into a fresh Vec, the
/// cost the zero-copy design avoids.
#[derive(Clone)]
struct OwnedDeser;
impl Deserializer<Owned<Vec<u8>>> for OwnedDeser {
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

#[derive(Clone)]
struct SubEncoder;
impl RowEncoder<SubF> for SubEncoder {
    fn encode<'buf>(
        &mut self,
        rec: &Record<SubEvent<'buf>>,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), spate_core::error::SinkError> {
        buf.extend_from_slice(&(rec.payload.chunk.len() as u32).to_le_bytes());
        buf.extend_from_slice(rec.payload.chunk);
        Ok(())
    }
}

#[derive(Clone)]
struct VecEncoder;
impl RowEncoder<Owned<Vec<u8>>> for VecEncoder {
    fn encode<'buf>(
        &mut self,
        rec: &Record<Vec<u8>>,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), spate_core::error::SinkError> {
        buf.extend_from_slice(&(rec.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&rec.payload);
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct ToZero;
impl ShardRouter for ToZero {
    fn route(&self, _m: &RecordMeta, _n: usize) -> usize {
        0
    }
}

/// Which router a rig builds, and whether its corpus carries message keys.
///
/// These are one choice rather than two axes. [`KeyHashRouter`] falls back to
/// a hash of the source partition for a keyless record, and every payload
/// these rigs produce comes from partition 0, so the real router over a
/// keyless corpus places *every* record on shard 0 exactly as [`ToZero`]
/// does. A multi-shard case built that way would leave every shard buffer but
/// the first untouched and measure an idle `Vec`, not routing.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Routing {
    /// Constant router, keyless payloads: no key hashed, everything on shard
    /// 0. What the single-shard baselines hold fixed.
    Fixed,
    /// The production [`KeyHashRouter`] over keyed payloads. Two costs land
    /// together, as they do on the production path. `RawPayload::meta` hashes
    /// each key (FNV-1a over the key bytes) during deserialization, and the
    /// router takes that hash modulo the shard count per record.
    KeyHash,
}

/// A batch's payloads and, for [`Routing::KeyHash`], the parallel keys.
/// `keys` is empty for a keyless corpus.
pub(crate) struct Corpus {
    payloads: Vec<Vec<u8>>,
    keys: Vec<Vec<u8>>,
}

impl Corpus {
    /// The batch's payloads, in the order [`TestBatch`] yields them.
    pub(crate) fn payloads(&self) -> &[Vec<u8>] {
        &self.payloads
    }

    /// The parallel message keys, empty for a keyless corpus.
    pub(crate) fn keys(&self) -> &[Vec<u8>] {
        &self.keys
    }
}

struct TestBatch<'a> {
    corpus: &'a Corpus,
    idx: usize,
    ack: AckRef,
}

impl<'a> PayloadBatch<'a> for TestBatch<'a> {
    fn next_payload(&mut self) -> Option<RawPayload<'a>> {
        let buf = self.corpus.payloads.get(self.idx)?;
        let key = self.corpus.keys.get(self.idx).map(Vec::as_slice);
        let offset = self.idx as i64;
        self.idx += 1;
        Some(RawPayload {
            bytes: buf,
            key,
            partition: PartitionId(0),
            offset,
            timestamp_ms: 0,
        })
    }
    fn ack(&self) -> &AckRef {
        &self.ack
    }
}

/// Payloads per driven batch.
pub(crate) const BATCH: usize = 512;

/// Encoded bytes one borrowed batch produces: `split3` cuts each 39-byte
/// payload at its two separators into three sub-records, and `SubEncoder`
/// writes a 4-byte length prefix plus the slice for each, [`PAYLOAD_BYTES`]
/// per payload.
///
/// A target strictly above this never trips the seal check, so the batch's
/// single chunk seals at `flush`. A target that divides it seals that many
/// chunks entirely inside `push` (see [`assert_seals_on_a_record_boundary`]).
///
/// A chunk case divides this, so a payload shape that drifted from the
/// arithmetic would silently change how many chunks a case seals.
/// [`assert_encodes_to`] pins it.
pub(crate) const BORROWED_BATCH_BYTES: usize = BATCH * PAYLOAD_BYTES;

/// Encoded bytes one payload contributes: `39 - 2 + 3 * 4`.
const PAYLOAD_BYTES: usize = 49;

/// Pure functions of the index (no `rand`, no `DefaultHasher`), so every run
/// of every bench target encodes the same bytes and routes the same way.
///
/// Reachable on its own as well as through a rig. The wall tier's routing and
/// hashing cases measure the corpus without a chain around it, and
/// `tests/bench_fixtures.rs` pins its bytes.
pub(crate) fn corpus(routing: Routing) -> Corpus {
    Corpus {
        payloads: (0..BATCH)
            .map(|i| format!("payload-{i:04}-abcdefgh|ijklmnop|qrstuvwx").into_bytes())
            .collect(),
        keys: match routing {
            Routing::Fixed => Vec::new(),
            Routing::KeyHash => (0..BATCH)
                .map(|i| format!("key-{i:04}").into_bytes())
                .collect(),
        },
    }
}

fn split3<'buf>(e: LogEvent<'buf>, em: &mut Emitter<'_, SubF>) {
    for chunk in e.body.split(|&b| b == b'|') {
        em.emit(SubEvent { chunk });
    }
}

fn keep(e: &LogEvent<'_>) -> bool {
    !e.body.is_empty()
}

pub(crate) struct Rig {
    chain: Box<dyn RunnableChain>,
    rxs: Vec<tokio::sync::mpsc::Receiver<EncodedChunk>>,
    corpus: Corpus,
}

impl Rig {
    /// The bytes this rig drives, for a caller that has to prove two builds
    /// measured the same ones. The wall tier folds these into its corpus
    /// digest, and a pair of legs whose corpora drifted is demoted.
    ///
    /// Bytes only. The rig's other parameters (the shard count, the chunk
    /// target, the queue depth) are not in the digest, so a change to one of
    /// those passes the check and is charged to the diff as a performance
    /// difference. `tests/bench_fixtures.rs` pins them instead.
    pub(crate) fn corpus(&self) -> &Corpus {
        &self.corpus
    }

    /// One full batch through the chain, drained to encoded chunks. Returns
    /// the row count so a caller can keep the work observable.
    ///
    /// The drain sweeps every shard receiver, which is rig scaffolding; in
    /// production a shard worker owns the other end. Each receiver costs its
    /// chunks plus one failing `try_recv` to end its loop, so the sweep grows
    /// with the shard count whether or not a shard is idle. That term is why a
    /// shard-count case is read against its own single-shard sibling rather
    /// than in absolute terms.
    ///
    /// Nothing here returns bytes to the [`InflightBudget`]. Only a sink
    /// worker or a parked chunk's drop does that, and this rig has neither, so
    /// the budget climbs across drives. That is cost-neutral while the seal
    /// path's `add` is one value-independent atomic and nothing reads
    /// `usage()`; a rig that grew a component which *does* read it would need
    /// a real consumer instead.
    pub(crate) fn drive(&mut self) -> usize {
        let (ack, _rx) = AckRef::test_pair();
        let mut batch = TestBatch {
            corpus: &self.corpus,
            idx: 0,
            ack,
        };
        assert!(matches!(
            self.chain.push_batch(&mut batch, 0),
            PushOutcome::Done
        ));
        assert!(matches!(self.chain.flush(), PushOutcome::Done));
        let mut rows = 0;
        for rx in &mut self.rxs {
            while let Ok(chunk) = rx.try_recv() {
                rows += chunk.rows as usize;
            }
        }
        rows
    }
}

/// A chunk target must land on a sub-record boundary, or a chunk case seals a
/// different number of times than its name claims.
///
/// The seal check is `buf.len() >= target` *after* each sub-record, so a
/// target that divides [`PAYLOAD_BYTES`]' running total is reached at exactly
/// equality and seals there. A target that does not (say a third of the batch)
/// is first *overshot* mid-payload, which makes the chunks unequal and leaves
/// a remainder for `flush`. Both are legitimate workloads, but only the first
/// is the one the `chunk_*` case names describe.
fn assert_seals_on_a_record_boundary(target_bytes: usize) {
    if target_bytes >= BORROWED_BATCH_BYTES {
        return; // Never trips the check: the batch's one chunk seals at flush.
    }
    assert_eq!(
        BORROWED_BATCH_BYTES % target_bytes,
        0,
        "a chunk target that does not divide the batch overshoots mid-payload"
    );
    assert_eq!(
        target_bytes % PAYLOAD_BYTES,
        0,
        "a chunk target that does not divide a payload's encoding overshoots \
         mid-payload"
    );
}

/// [`BORROWED_BATCH_BYTES`] must be what the borrowed chain encodes, or a
/// chunk case seals a different number of times than its name claims.
fn assert_encodes_to(corpus: &Corpus) {
    let encoded: usize = corpus
        .payloads
        .iter()
        .map(|p| {
            // `split3` drops each separator and emits one more sub-record than
            // there are separators; each sub-record carries a 4-byte prefix.
            let separators = p.iter().filter(|&&b| b == b'|').count();
            p.len() - separators + (separators + 1) * 4
        })
        .sum();
    assert_eq!(
        encoded, BORROWED_BATCH_BYTES,
        "the payload shape no longer encodes to BORROWED_BATCH_BYTES"
    );
}

/// Every shard must receive at least one record, or a shard-count case
/// measures an idle `Vec` of buffers rather than routing.
///
/// Checked here, in the rig builder, which every caller runs outside its
/// measured region; gungraun evaluates a `#[bench]` argument expression before
/// it starts collecting, and the wall harness builds the rig in a case's
/// `setup`. A corpus that stopped spreading (a different key shape, a wider
/// shard count) therefore fails loudly instead of quietly reporting a number
/// for work that is not happening.
fn assert_spreads(corpus: &Corpus, shards: usize) {
    let mut hit = vec![false; shards];
    for key in &corpus.keys {
        let meta = RawPayload {
            bytes: b"",
            key: Some(key),
            partition: PartitionId(0),
            offset: 0,
            timestamp_ms: 0,
        }
        .meta();
        hit[KeyHashRouter.route(&meta, shards)] = true;
    }
    assert!(
        hit.iter().all(|&h| h),
        "the keyed corpus leaves some of the {shards} shards empty"
    );
}

/// Borrowed payloads: deserialize → filter → flat_map → encode → handoff,
/// with the default constant router, one shard, and default chunking.
pub(crate) fn borrowed_rig() -> Rig {
    borrowed_rig_with(Routing::Fixed, 1, ChunkConfig::default().target_bytes)
}

/// The borrowed rig with the three parameters the production terminal stage
/// varies: which router runs per record, how many shard buffers it routes
/// between, and the frame size a chunk seals at.
pub(crate) fn borrowed_rig_with(routing: Routing, shards: usize, target_bytes: usize) -> Rig {
    let corpus = corpus(routing);
    assert_encodes_to(&corpus);
    assert_seals_on_a_record_boundary(target_bytes);
    if routing == Routing::KeyHash {
        assert_spreads(&corpus, shards);
    }
    let (queues, rxs) = shard_queues(shards, 4096);
    let cfg = ChunkConfig {
        target_bytes,
        ..ChunkConfig::default()
    };
    let budget = Arc::new(InflightBudget::new());
    // The stages ahead of the sink are shared; only the `sink` call differs,
    // because the router is one of its generic parameters. The arms are
    // mutually exclusive, so each may move `stages`, `queues` and `budget`.
    // Both erase to `Box<dyn RunnableChain>`.
    let stages = chain(BorrowDeser).filter(keep).flat_map::<SubF, _>(split3);
    let chain = match routing {
        Routing::Fixed => stages.sink(SubEncoder, ToZero, cfg, queues, budget).build(),
        Routing::KeyHash => stages
            .sink(SubEncoder, KeyHashRouter, cfg, queues, budget)
            .build(),
    };
    Rig { chain, rxs, corpus }
}

/// Owned payloads: the same shape with a copy per record.
pub(crate) fn owned_rig() -> Rig {
    let (queues, rxs) = shard_queues(1, 4096);
    let chain = chain_owned(OwnedDeser)
        .filter(|b: &Vec<u8>| !b.is_empty())
        .map(|b: Vec<u8>| b)
        .sink(
            VecEncoder,
            ToZero,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();
    Rig {
        chain,
        rxs,
        corpus: corpus(Routing::Fixed),
    }
}
