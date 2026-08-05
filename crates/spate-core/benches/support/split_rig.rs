//! The split-terminal bench rig: one poll batch through a chain whose
//! terminal fans records out across several typed sink branches.
//!
//! Included with `#[path]` by `benches/split_wall.rs` (wall time),
//! `benches/split_gungraun.rs` (instruction counts) and
//! `tests/bench_fixtures.rs` (the corpus pins). A bench target is its own
//! crate, so compiling one source is the only way several can agree on a
//! workload — a wall-time result and an instruction count that measured
//! different rigs would not be talking about the same code. It is a separate
//! file rather than part of a target for the reason the connector crates'
//! `benches/support/` modules are: a rig read as a fixture is easier to check
//! for the properties a measurement depends on than one interleaved with the
//! benchmark macros.

#![allow(dead_code, reason = "each target uses a different subset")]

use spate_core::backpressure::InflightBudget;
use spate_core::checkpoint::AckRef;
use spate_core::deser::{Deserializer, EmitRecord, RecFamily};
use spate_core::error::{DeserError, ErrorPolicy, SinkError};
use spate_core::ops::{ChunkConfig, PushOutcome, RunnableChain, SinkCtx, chain};
use spate_core::record::{PartitionId, RawPayload, Record, RecordMeta};
use spate_core::sink::{EncodedChunk, RowEncoder, ShardRouter, shard_queues};
use spate_core::source::PayloadBatch;
use std::sync::Arc;

/// Payloads per driven batch.
///
/// Larger than the operator-chain rig's batch because the split terminal is
/// the *only* per-record stage here — there is no `filter` or `flat_map` in
/// front of it — so the batch has to carry the record count on its own for
/// the terminal's per-record cost to dominate the fixed cost of a
/// `push_batch`/`flush` pair. Every payload is distinct and built from its
/// index; this is a corpus, not one payload replayed.
pub(crate) const PAYLOADS: usize = 8192;

/// The source stream's record: the whole payload, tag byte included, exactly
/// as a deserializer that has not yet classified anything sees it.
struct LogEvent<'buf> {
    body: &'buf [u8],
}

/// Source family.
struct LogF;

impl RecFamily for LogF {
    type Rec<'buf> = LogEvent<'buf>;
}

/// One destination branch's row. `TAG` makes each branch's record type — and
/// so, through [`RecFamily`], the branch's own concrete `SinkHandoff` type —
/// distinct, which is what the split's per-emit `Any` downcast has to
/// discriminate between. A single row type shared by every branch would
/// leave the downcast with nothing to tell apart and would not be a split.
struct Row<'buf, const TAG: u8> {
    body: &'buf [u8],
}

/// Destination family for branch `TAG`.
struct RowF<const TAG: u8>;

impl<const TAG: u8> RecFamily for RowF<TAG> {
    type Rec<'buf> = Row<'buf, TAG>;
}

/// Borrowing deserializer: one record per payload, no copy.
#[derive(Clone)]
struct BodyDeser;

impl Deserializer<LogF> for BodyDeser {
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

/// Length-prefixed row writer, one concrete encoder type per branch — which
/// is what a split over several tables has, and what the branch's boxed
/// encoder erases.
#[derive(Clone)]
struct TagEncoder<const TAG: u8>;

impl<const TAG: u8> RowEncoder<RowF<TAG>> for TagEncoder<TAG> {
    fn encode<'buf>(
        &mut self,
        rec: &Record<Row<'buf, TAG>>,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), SinkError> {
        buf.extend_from_slice(&(rec.payload.body.len() as u32).to_le_bytes());
        buf.extend_from_slice(rec.payload.body);
        Ok(())
    }
}

/// Constant router. Every branch here owns one shard, so routing is held
/// fixed: the axes this rig varies are the branch count and the match-hit
/// ratio, and a shard sweep is what the operator-chain rig already carries.
#[derive(Clone, Copy)]
struct ToShardZero;

impl ShardRouter for ToShardZero {
    fn route(&self, _m: &RecordMeta, _n: usize) -> usize {
        0
    }
}

/// Shard-queue depth. Well above what one batch seals, so no case blocks:
/// a blocked push parks records inside the terminal and returns a resume
/// cursor, which would measure the park path instead of the dispatch path.
const QUEUE_DEPTH: usize = 4096;

/// How a corpus distributes its tag bytes across the branches a case
/// declares.
#[derive(Clone, Copy)]
pub(crate) enum Tags {
    /// Round-robin over two branches; every record matches.
    TwoBranches,
    /// Round-robin over four branches; every record matches.
    FourBranches,
    /// Four branches, one payload in four carrying a tag no arm of the route
    /// closure names, so it reaches the `unmatched` policy — and the other
    /// three still spread evenly over all four branches.
    FourBranchesQuarterUnrouted,
}

/// The tag byte a matched payload carries for branch `b`.
const fn tag_byte(b: u8) -> u8 {
    b'0' + b
}

/// The tag byte that matches no branch.
const UNROUTED: u8 = b'-';

/// The partially-routed corpus's repeating cycle: four payloads in sixteen
/// reach no branch, and the twelve that do are spread three each over the
/// four branches.
///
/// A cycle rather than `i % 4 == 3`, which is the obvious construction and
/// the wrong one: it would aim every unrouted payload at what would have been
/// branch 3, leaving that branch empty for the whole batch, and the case
/// would be three branches plus a drop rather than four branches at
/// three-quarter hit rate. [`assert_hits_every_branch`] rejects that
/// construction, and did.
const QUARTER_UNROUTED_CYCLE: [u8; 16] = [
    tag_byte(0),
    tag_byte(1),
    tag_byte(2),
    UNROUTED,
    tag_byte(3),
    tag_byte(0),
    tag_byte(1),
    UNROUTED,
    tag_byte(2),
    tag_byte(3),
    tag_byte(0),
    UNROUTED,
    tag_byte(1),
    tag_byte(2),
    tag_byte(3),
    UNROUTED,
];

impl Tags {
    /// Tag byte for payload `i`.
    fn tag(self, i: usize) -> u8 {
        match self {
            Tags::TwoBranches => tag_byte((i % 2) as u8),
            Tags::FourBranches => tag_byte((i % 4) as u8),
            Tags::FourBranchesQuarterUnrouted => {
                QUARTER_UNROUTED_CYCLE[i % QUARTER_UNROUTED_CYCLE.len()]
            }
        }
    }

    /// Records the batch routes to a branch — everything the corpus holds,
    /// less whatever it aims at no branch.
    fn routed(self) -> usize {
        match self {
            Tags::TwoBranches | Tags::FourBranches => PAYLOADS,
            Tags::FourBranchesQuarterUnrouted => PAYLOADS - PAYLOADS / 4,
        }
    }
}

/// Payload bodies, pure functions of the index — no `rand`, no
/// `DefaultHasher` — so every run encodes the same bytes and takes the same
/// route arm. The tag is the leading byte and the rest is fixed-width, so
/// every case's corpus holds the same number of bytes and only the
/// distribution of arms differs.
pub(crate) fn corpus(tags: Tags) -> Vec<Vec<u8>> {
    assert!(
        PAYLOADS.is_multiple_of(QUARTER_UNROUTED_CYCLE.len()),
        "the corpus must hold whole tag cycles, or the unrouted share is not \
         the quarter the case name claims"
    );
    (0..PAYLOADS)
        .map(|i| {
            let mut p = Vec::with_capacity(BODY_BYTES + 1);
            p.push(tags.tag(i));
            p.extend_from_slice(format!("row-{i:06}-abcdefghijklmnop").as_bytes());
            p
        })
        .collect()
}

/// Bytes of a payload after its tag byte, and so the bytes one matched
/// record encodes on top of its 4-byte length prefix: `"row-" + 6 index
/// digits + "-" + 16 filler`.
const BODY_BYTES: usize = 27;

/// The corpus must carry the body width [`BODY_BYTES`] claims, or the
/// per-case seal count silently changes with a payload-shape edit.
fn assert_body_width(corpus: &[Vec<u8>]) {
    for p in corpus {
        assert_eq!(
            p.len(),
            BODY_BYTES + 1,
            "a payload is not one tag byte plus BODY_BYTES of body"
        );
    }
}

/// Every declared branch must receive records, or a branch-count case
/// measures an idle `Vec` of branches rather than dispatch.
///
/// Checked in the rig builder, which gungraun evaluates before it starts
/// collecting, so a corpus that stopped spreading fails loudly instead of
/// quietly reporting a number for work that is not happening.
fn assert_hits_every_branch(corpus: &[Vec<u8>], branches: u8) {
    for b in 0..branches {
        assert!(
            corpus.iter().any(|p| p[0] == tag_byte(b)),
            "no payload routes to branch {b}"
        );
    }
}

/// One poll batch over a corpus, carrying the batch's acknowledgement handle.
struct TestBatch<'a> {
    corpus: &'a [Vec<u8>],
    idx: usize,
    ack: AckRef,
}

impl<'a> PayloadBatch<'a> for TestBatch<'a> {
    fn next_payload(&mut self) -> Option<RawPayload<'a>> {
        let buf = self.corpus.get(self.idx)?;
        let offset = self.idx as i64;
        self.idx += 1;
        Some(RawPayload {
            bytes: buf,
            key: None,
            partition: PartitionId(0),
            offset,
            timestamp_ms: 0,
        })
    }

    fn ack(&self) -> &AckRef {
        &self.ack
    }
}

/// A built split chain, its branches' receiving ends, and the corpus it
/// runs.
pub(crate) struct Rig {
    chain: Box<dyn RunnableChain>,
    rxs: Vec<tokio::sync::mpsc::Receiver<EncodedChunk>>,
    corpus: Vec<Vec<u8>>,
    /// Rows the batch must produce. Asserted rather than returned
    /// unchecked, so a corpus that silently stopped reaching a branch could
    /// not pass as a fast one — the match-hit ratio *is* one of the two axes
    /// these cases separate.
    pub(crate) expect_rows: usize,
}

impl Rig {
    /// The bytes this rig drives, for a caller that has to prove two builds
    /// measured the same ones — the wall tier folds these into its corpus
    /// digest, which is what demotes a pair of legs whose corpora drifted.
    ///
    /// Bytes only. The branch count, the chunk target and [`QUEUE_DEPTH`] are
    /// not in the digest, so a change to one of those passes the check and is
    /// charged to the diff as a performance difference.
    /// `tests/bench_fixtures.rs` is what pins them instead.
    pub(crate) fn corpus(&self) -> &[Vec<u8>] {
        &self.corpus
    }

    /// One full batch through the chain, drained to encoded chunks across
    /// every branch. Returns the row count so a caller can keep the work
    /// observable.
    ///
    /// The drain sweeps every branch's receiver, which is rig scaffolding —
    /// in production a shard worker owns the other end. Each receiver costs
    /// its chunks plus one failing `try_recv` to end its loop, so the sweep
    /// grows with the branch count whether or not a branch is idle. That
    /// term is why the four-branch cases are read against each other and
    /// against the two-branch case, not in absolute terms.
    ///
    /// Nothing here returns bytes to the [`InflightBudget`]: only a sink
    /// worker or a parked chunk's drop does that, and this rig has neither,
    /// so the budget climbs across drives. Cost-neutral — the seal path's
    /// `add` is one value-independent atomic and nothing reads `usage()`.
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

/// A split over two branches, every record routed.
pub(crate) fn two_branch_rig() -> Rig {
    let corpus = corpus(Tags::TwoBranches);
    assert_body_width(&corpus);
    assert_hits_every_branch(&corpus, 2);

    let (q0, rx0) = shard_queues(1, QUEUE_DEPTH);
    let (q1, rx1) = shard_queues(1, QUEUE_DEPTH);
    let budget = Arc::new(InflightBudget::new());
    let cfg = ChunkConfig::default();

    let mut split = chain(BodyDeser).split(ErrorPolicy::Skip);
    let b0 = split.add::<RowF<0>, _, _>(
        TagEncoder::<0>,
        ToShardZero,
        SinkCtx::new("b0".into(), q0, Arc::clone(&budget)).with_chunk(cfg),
    );
    let b1 = split.add::<RowF<1>, _, _>(
        TagEncoder::<1>,
        ToShardZero,
        SinkCtx::new("b1".into(), q1, Arc::clone(&budget)).with_chunk(cfg),
    );
    let chain = split
        .route(move |e: LogEvent<'_>, out| match e.body[0] {
            b'0' => out.emit(b0, Row { body: &e.body[1..] }),
            b'1' => out.emit(b1, Row { body: &e.body[1..] }),
            _ => {}
        })
        .build();

    Rig {
        chain,
        rxs: [rx0, rx1].into_iter().flatten().collect(),
        expect_rows: Tags::TwoBranches.routed(),
        corpus,
    }
}

/// A split over four branches, at the given match-hit ratio.
pub(crate) fn four_branch_rig(tags: Tags) -> Rig {
    let corpus = corpus(tags);
    assert_body_width(&corpus);
    assert_hits_every_branch(&corpus, 4);

    let (q0, rx0) = shard_queues(1, QUEUE_DEPTH);
    let (q1, rx1) = shard_queues(1, QUEUE_DEPTH);
    let (q2, rx2) = shard_queues(1, QUEUE_DEPTH);
    let (q3, rx3) = shard_queues(1, QUEUE_DEPTH);
    let budget = Arc::new(InflightBudget::new());
    let cfg = ChunkConfig::default();

    let mut split = chain(BodyDeser).split(ErrorPolicy::Skip);
    let b0 = split.add::<RowF<0>, _, _>(
        TagEncoder::<0>,
        ToShardZero,
        SinkCtx::new("b0".into(), q0, Arc::clone(&budget)).with_chunk(cfg),
    );
    let b1 = split.add::<RowF<1>, _, _>(
        TagEncoder::<1>,
        ToShardZero,
        SinkCtx::new("b1".into(), q1, Arc::clone(&budget)).with_chunk(cfg),
    );
    let b2 = split.add::<RowF<2>, _, _>(
        TagEncoder::<2>,
        ToShardZero,
        SinkCtx::new("b2".into(), q2, Arc::clone(&budget)).with_chunk(cfg),
    );
    let b3 = split.add::<RowF<3>, _, _>(
        TagEncoder::<3>,
        ToShardZero,
        SinkCtx::new("b3".into(), q3, Arc::clone(&budget)).with_chunk(cfg),
    );
    let chain = split
        .route(move |e: LogEvent<'_>, out| match e.body[0] {
            b'0' => out.emit(b0, Row { body: &e.body[1..] }),
            b'1' => out.emit(b1, Row { body: &e.body[1..] }),
            b'2' => out.emit(b2, Row { body: &e.body[1..] }),
            b'3' => out.emit(b3, Row { body: &e.body[1..] }),
            _ => {}
        })
        .build();

    Rig {
        chain,
        rxs: [rx0, rx1, rx2, rx3].into_iter().flatten().collect(),
        expect_rows: tags.routed(),
        corpus,
    }
}
