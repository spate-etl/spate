//! Hard allocation assertions for the operator-chain hot path.
//!
//! Pushing a batch of borrowed payloads through deserialize → filter →
//! flat_map → encode → handoff must not allocate per record. Per-iteration
//! overhead (one ack channel pair, chunk seals at flush) is bounded by a
//! constant; quadrupling the record count must not increase the allocation
//! count materially.

use spate_core::backpressure::InflightBudget;
use spate_core::checkpoint::{AckMsg, AckRef};
use spate_core::deser::{Deserializer, EmitRecord, RecFamily};
use spate_core::error::DeserError;
use spate_core::ops::{ChunkConfig, Emitter, PushOutcome, chain};
use spate_core::record::{PartitionId, RawPayload, Record, RecordMeta};
use spate_core::sink::{RowEncoder, ShardRouter, shard_queues};
use spate_core::source::PayloadBatch;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

struct CountingAlloc;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static COUNTING: AtomicBool = AtomicBool::new(false);

// SAFETY: delegates all allocation to `System`, adding only a relaxed
// counter increment; layout handling is untouched.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: same contract as the caller's; System upholds it.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: same contract as the caller's; System upholds it.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: same contract as the caller's; System upholds it.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

// ---- fixtures (mirror the unit-test fixtures; borrowed family) -------------

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
struct LogDeser;
impl Deserializer<LogF> for LogDeser {
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

#[derive(Clone, Copy)]
struct ToZero;
impl ShardRouter for ToZero {
    fn route(&self, _m: &RecordMeta, _n: usize) -> usize {
        0
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

struct TestBatch<'a> {
    payloads: &'a [Vec<u8>],
    idx: usize,
    ack: AckRef,
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
            timestamp_ms: 0,
        })
    }
    fn ack(&self) -> &AckRef {
        &self.ack
    }
}

fn run_iters(
    c: &mut Box<dyn spate_core::ops::RunnableChain>,
    rx: &mut tokio::sync::mpsc::Receiver<spate_core::sink::EncodedChunk>,
    payloads: &[Vec<u8>],
    iters: usize,
) -> u64 {
    let start = ALLOCS.load(Ordering::Relaxed);
    for _ in 0..iters {
        let (ack, _ack_rx): (AckRef, crossbeam_channel::Receiver<AckMsg>) = AckRef::test_pair();
        let mut batch = TestBatch {
            payloads,
            idx: 0,
            ack,
        };
        match c.push_batch(&mut batch, 0) {
            PushOutcome::Done => {}
            other => panic!("expected Done in steady state, got {other:?}"),
        }
        match c.flush() {
            PushOutcome::Done => {}
            other => panic!("flush must succeed with a drained queue: {other:?}"),
        }
        while rx.try_recv().is_ok() {}
        drop(batch);
        while _ack_rx.try_recv().is_ok() {}
    }
    ALLOCS.load(Ordering::Relaxed) - start
}

#[test]
fn steady_state_allocations_do_not_scale_with_records() {
    // ~3 records per payload after flat_map.
    let small: Vec<Vec<u8>> = (0..512)
        .map(|i| format!("aa{i}|bb|cc").into_bytes())
        .collect();
    let big: Vec<Vec<u8>> = (0..2048)
        .map(|i| format!("aa{i}|bb|cc").into_bytes())
        .collect();

    let (queues, mut rxs) = shard_queues(1, 4096);
    let mut c = chain(LogDeser)
        .filter(keep)
        .flat_map::<SubF, _>(split3)
        .sink(
            SubEncoder,
            ToZero,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    // Warm up buffers and lazy statics outside the counting window.
    for _ in 0..10 {
        let _ = run_iters(&mut c, &mut rxs[0], &small, 1);
    }

    const ITERS: u64 = 32;
    COUNTING.store(true, Ordering::Relaxed);
    let small_allocs = run_iters(&mut c, &mut rxs[0], &small, ITERS as usize);
    let big_allocs = run_iters(&mut c, &mut rxs[0], &big, ITERS as usize);
    COUNTING.store(false, Ordering::Relaxed);

    let small_per_iter = small_allocs / ITERS;
    let big_per_iter = big_allocs / ITERS;
    // 512 payloads → ~1536 records flow per small iteration; constant
    // per-iteration overhead is the ack channel pair plus chunk seals.
    assert!(
        small_per_iter <= 64,
        "per-iteration allocations must be a small constant, got {small_per_iter}"
    );
    // Quadrupling the records (≈ +4600/iter) must not add per-record
    // allocations, only a few extra chunk seals and buffer regrowths.
    assert!(
        big_per_iter <= small_per_iter + 48,
        "allocations scale with records: small {small_per_iter}/iter, big {big_per_iter}/iter"
    );
}
