//! Operator-chain throughput and allocation benches (divan).
//!
//! Measures the production path: `Box<dyn RunnableChain>` fed borrowed
//! payload batches through deserialize → filter → flat_map → encode →
//! handoff, against the owned-payload equivalent. The static-vs-dyn
//! boundary delta was measured by the seam prototype (~+9% on a trivial
//! chain, amortizing to 1–2% with realistic work; see
//! `docs/benchmarks/zero-copy-seam.mdx`) — the boundary is one virtual call per batch by
//! construction.
//!
//! Run: `cargo bench -p spate-core --bench chain`

use divan::AllocProfiler;
use spate_core::backpressure::InflightBudget;
use spate_core::checkpoint::AckRef;
use spate_core::deser::{Deserializer, EmitRecord, Owned, RecFamily};
use spate_core::error::DeserError;
use spate_core::ops::{ChunkConfig, Emitter, PushOutcome, RunnableChain, chain, chain_owned};
use spate_core::record::{PartitionId, RawPayload, Record, RecordMeta};
use spate_core::sink::{EncodedChunk, RowEncoder, ShardRouter, shard_queues};
use spate_core::source::PayloadBatch;
use std::sync::Arc;

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn main() {
    divan::main();
}

// ---- fixtures ---------------------------------------------------------------

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

const BATCH: usize = 512;

fn make_payloads() -> Vec<Vec<u8>> {
    (0..BATCH)
        .map(|i| format!("payload-{i:04}-abcdefgh|ijklmnop|qrstuvwx").into_bytes())
        .collect()
}

fn split3<'buf>(e: LogEvent<'buf>, em: &mut Emitter<'_, SubF>) {
    for chunk in e.body.split(|&b| b == b'|') {
        em.emit(SubEvent { chunk });
    }
}

fn keep(e: &LogEvent<'_>) -> bool {
    !e.body.is_empty()
}

struct Rig {
    chain: Box<dyn RunnableChain>,
    rx: tokio::sync::mpsc::Receiver<EncodedChunk>,
    payloads: Vec<Vec<u8>>,
}

impl Rig {
    fn drive(&mut self) -> usize {
        let (ack, _rx) = AckRef::test_pair();
        let mut batch = TestBatch {
            payloads: &self.payloads,
            idx: 0,
            ack,
        };
        assert!(matches!(
            self.chain.push_batch(&mut batch, 0),
            PushOutcome::Done
        ));
        assert!(matches!(self.chain.flush(), PushOutcome::Done));
        let mut rows = 0;
        while let Ok(chunk) = self.rx.try_recv() {
            rows += chunk.rows as usize;
        }
        rows
    }
}

fn borrowed_rig() -> Rig {
    let (queues, mut rxs) = shard_queues(1, 4096);
    let chain = chain(BorrowDeser)
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
    Rig {
        chain,
        rx: rxs.remove(0),
        payloads: make_payloads(),
    }
}

fn owned_rig() -> Rig {
    let (queues, mut rxs) = shard_queues(1, 4096);
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
        rx: rxs.remove(0),
        payloads: make_payloads(),
    }
}

#[divan::bench]
fn borrowed_chain_batch(bencher: divan::Bencher<'_, '_>) {
    let mut rig = borrowed_rig();
    rig.drive();
    bencher
        .counter(divan::counter::ItemsCount::new(BATCH * 3))
        .bench_local(|| rig.drive());
}

#[divan::bench]
fn owned_chain_batch(bencher: divan::Bencher<'_, '_>) {
    let mut rig = owned_rig();
    rig.drive();
    bencher
        .counter(divan::counter::ItemsCount::new(BATCH))
        .bench_local(|| rig.drive());
}
