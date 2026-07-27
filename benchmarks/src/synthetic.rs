//! In-process generator source, zero-copy record family, and null writer —
//! the pieces `pipeline_synthetic` runs through the *real* chain, sink
//! pool, and runtime to measure the framework's own ceiling.

use bytes::BytesMut;
use spate_core::checkpoint::{AckIssuer, AckRef};
use spate_core::deser::{Deserializer, EmitRecord, RecFamily};
use spate_core::error::{DeserError, SinkError, SourceError};
use spate_core::record::{PartitionId, RawPayload, Record};
use spate_core::sink::{RowEncoder, SealedBatch, ShardWriter};
use spate_core::source::{LaneId, PayloadBatch, Source, SourceCtx, SourceEvent, SourceLane};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

/// Zero-copy record: a view of the payload bytes.
#[derive(Debug)]
pub struct RawView<'buf> {
    /// The payload slice, borrowed from the lane's arena.
    pub bytes: &'buf [u8],
}

/// Family for [`RawView`] — the borrowed fast path.
#[derive(Debug)]
pub struct RawFam;

impl RecFamily for RawFam {
    type Rec<'buf> = RawView<'buf>;
}

/// One borrowed record per payload; no copies, no allocation.
#[derive(Clone, Debug, Default)]
pub struct RawDeser;

impl Deserializer<RawFam> for RawDeser {
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, RawView<'buf>>,
    ) -> Result<(), DeserError> {
        let _ = out.emit(Record {
            payload: RawView { bytes: raw.bytes },
            meta: raw.meta(),
            ack: ack.clone(),
        });
        Ok(())
    }
}

/// Length-prefix encoder for [`RawView`] rows.
#[derive(Clone, Debug, Default)]
pub struct RawEncoder;

impl RowEncoder<RawFam> for RawEncoder {
    fn encode<'buf>(
        &mut self,
        rec: &Record<RawView<'buf>>,
        buf: &mut BytesMut,
    ) -> Result<(), SinkError> {
        use bytes::BufMut;
        buf.put_u32_le(u32::try_from(rec.payload.bytes.len()).unwrap_or(u32::MAX));
        buf.put_slice(rec.payload.bytes);
        Ok(())
    }
}

/// A sink writer that acknowledges instantly — the framework's egress cost
/// without any I/O.
#[derive(Debug, Default)]
pub struct NullWriter {
    batches: AtomicU64,
    rows: AtomicU64,
}

impl NullWriter {
    /// Rows durably "written".
    pub fn rows(&self) -> u64 {
        self.rows.load(Ordering::Relaxed)
    }

    /// Batches "written".
    pub fn batches(&self) -> u64 {
        self.batches.load(Ordering::Relaxed)
    }
}

impl ShardWriter for NullWriter {
    type Endpoint = ();

    fn write_batch(
        &self,
        _endpoint: &Self::Endpoint,
        batch: &SealedBatch,
    ) -> impl Future<Output = Result<(), SinkError>> + Send {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.rows.fetch_add(batch.rows, Ordering::Relaxed);
        async { Ok(()) }
    }
}

/// A [`NullWriter`] that paces acknowledgement to a fixed row rate.
///
/// The rig needs a backfill to last tens of seconds so coordination spans
/// dozens of heartbeat rounds, but the S3 source has no throttle hook. The
/// proven lever is sink pacing: a slow acknowledgement fills the in-flight
/// budget, which backpressures the chain and pauses the source. This writer is
/// that lever with no I/O.
///
/// Pacing is on an **absolute schedule**, not a per-batch sleep: the first
/// batch anchors a start instant, and every batch completes no earlier than
/// `start + cumulative_rows / rows_per_s`. A per-batch `sleep(rows/rate)` would
/// undercount whenever batches overlap (the sink pool holds several in flight
/// per shard) and drift with linger; the absolute schedule holds the aggregate
/// rate at `rows_per_s` regardless of concurrency.
#[derive(Debug)]
pub struct ThrottledNullWriter {
    batches: AtomicU64,
    rows: AtomicU64,
    rows_per_s: f64,
    start: OnceLock<Instant>,
}

impl ThrottledNullWriter {
    /// A writer pacing acknowledgement to `rows_per_s` rows per second.
    pub fn new(rows_per_s: f64) -> ThrottledNullWriter {
        ThrottledNullWriter {
            batches: AtomicU64::new(0),
            rows: AtomicU64::new(0),
            rows_per_s,
            start: OnceLock::new(),
        }
    }

    /// Rows durably "written".
    pub fn rows(&self) -> u64 {
        self.rows.load(Ordering::Relaxed)
    }

    /// Batches "written".
    pub fn batches(&self) -> u64 {
        self.batches.load(Ordering::Relaxed)
    }
}

impl ShardWriter for ThrottledNullWriter {
    type Endpoint = ();

    fn write_batch(
        &self,
        _endpoint: &Self::Endpoint,
        batch: &SealedBatch,
    ) -> impl Future<Output = Result<(), SinkError>> + Send {
        let start = *self.start.get_or_init(Instant::now);
        self.batches.fetch_add(1, Ordering::Relaxed);
        let cumulative = self.rows.fetch_add(batch.rows, Ordering::Relaxed) + batch.rows;
        // The instant by which this many rows should have been acked.
        let target = start + Duration::from_secs_f64(cumulative as f64 / self.rows_per_s);
        async move {
            if let Some(wait) = target.checked_duration_since(Instant::now()) {
                tokio::time::sleep(wait).await;
            }
            Ok(())
        }
    }
}

/// Generator source: one lane per pipeline thread, each yielding
/// pre-filled payload batches at full tilt until stopped.
#[derive(Debug)]
pub struct SyntheticSource {
    lanes: usize,
    payload_size: usize,
    /// When set, every payload in every lane's arena is a copy of this exact
    /// buffer (e.g. a pre-encoded Avro datum) rather than synthetic filler.
    template: Option<Arc<Vec<u8>>>,
    issuer: Option<AckIssuer>,
    assigned: bool,
    produced: Arc<AtomicU64>,
    commits: Arc<AtomicU64>,
}

impl SyntheticSource {
    /// A source with `lanes` lanes producing `payload_size`-byte payloads.
    /// `produced` counts records yielded across all lanes.
    pub fn new(lanes: usize, payload_size: usize, produced: Arc<AtomicU64>) -> Self {
        SyntheticSource {
            lanes,
            payload_size,
            template: None,
            issuer: None,
            assigned: false,
            produced,
            commits: Arc::new(AtomicU64::new(0)),
        }
    }

    /// A source that replays one fixed payload — every yielded record is a
    /// byte-for-byte copy of `payload` (e.g. a pre-encoded Avro datum). Used
    /// by the `avro_pipeline` rig so a real deserializer decodes it downstream.
    pub fn replaying(lanes: usize, payload: Vec<u8>, produced: Arc<AtomicU64>) -> Self {
        SyntheticSource {
            lanes,
            payload_size: payload.len(),
            template: Some(Arc::new(payload)),
            issuer: None,
            assigned: false,
            produced,
            commits: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Commit calls observed (checkpointing liveness cross-check).
    pub fn commits(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.commits)
    }
}

impl Source for SyntheticSource {
    type Lane = SyntheticLane;

    fn open(&mut self, ctx: SourceCtx) -> Result<(), SourceError> {
        self.issuer = Some(ctx.issuer);
        Ok(())
    }

    fn poll_events(
        &mut self,
        timeout: Duration,
    ) -> Result<SourceEvent<SyntheticLane>, SourceError> {
        if !self.assigned {
            self.assigned = true;
            let issuer = self.issuer.as_ref().expect("open() before events");
            // A shared arena per lane: 4096 distinct payloads reused
            // round-robin, so batches slice contiguous windows without any
            // per-batch allocation.
            let lanes = (0..self.lanes)
                .map(|i| {
                    let arena: Arc<Vec<Vec<u8>>> = Arc::new(
                        (0..4096)
                            .map(|j| match &self.template {
                                // Replay mode: every payload is the exact datum,
                                // so a real deserializer decodes identical input.
                                Some(t) => t.as_ref().clone(),
                                None => {
                                    let mut p = vec![0xa5u8; self.payload_size];
                                    let tag = (j as u64).to_le_bytes();
                                    let n = tag.len().min(p.len());
                                    p[..n].copy_from_slice(&tag[..n]);
                                    p
                                }
                            })
                            .collect(),
                    );
                    SyntheticLane {
                        id: LaneId(u32::try_from(i).expect("lane id")),
                        partition: PartitionId(u32::try_from(i).expect("partition")),
                        arena,
                        cursor: 0,
                        offset: 0,
                        issuer: issuer.clone(),
                        produced: Arc::clone(&self.produced),
                    }
                })
                .collect();
            return Ok(SourceEvent::LanesAssigned(lanes));
        }
        std::thread::sleep(timeout);
        Ok(SourceEvent::Idle)
    }

    fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
        self.commits
            .fetch_add(watermarks.len() as u64, Ordering::Relaxed);
        Ok(())
    }
}

/// One generator lane; see [`SyntheticSource`].
#[derive(Debug)]
pub struct SyntheticLane {
    id: LaneId,
    partition: PartitionId,
    arena: Arc<Vec<Vec<u8>>>,
    cursor: usize,
    offset: i64,
    issuer: AckIssuer,
    produced: Arc<AtomicU64>,
}

/// A contiguous window over the lane's arena.
#[derive(Debug)]
pub struct SyntheticBatch<'a> {
    payloads: &'a [Vec<u8>],
    idx: usize,
    partition: PartitionId,
    base_offset: i64,
    ack: AckRef,
}

impl<'a> PayloadBatch<'a> for SyntheticBatch<'a> {
    fn next_payload(&mut self) -> Option<RawPayload<'a>> {
        let buf = self.payloads.get(self.idx)?;
        let offset = self.base_offset + self.idx as i64;
        self.idx += 1;
        Some(RawPayload {
            bytes: buf,
            key: None,
            partition: self.partition,
            offset,
            timestamp_ms: 0,
        })
    }

    fn ack(&self) -> &AckRef {
        &self.ack
    }
}

impl SourceLane for SyntheticLane {
    type Batch<'a> = SyntheticBatch<'a>;

    fn id(&self) -> LaneId {
        self.id
    }

    fn partition(&self) -> PartitionId {
        self.partition
    }

    fn poll(
        &mut self,
        max_records: usize,
        _timeout: Duration,
    ) -> Result<Option<SyntheticBatch<'_>>, SourceError> {
        // Slice a contiguous window, wrapping at the arena boundary.
        let len = self.arena.len();
        if self.cursor >= len {
            self.cursor = 0;
        }
        let take = max_records.min(len - self.cursor);
        let start = self.cursor;
        self.cursor += take;
        let base_offset = self.offset;
        self.offset += take as i64;
        self.produced.fetch_add(take as u64, Ordering::Relaxed);
        let ack = self
            .issuer
            .issue(self.partition, base_offset + take as i64 - 1);
        Ok(Some(SyntheticBatch {
            payloads: &self.arena[start..start + take],
            idx: 0,
            partition: self.partition,
            base_offset,
            ack,
        }))
    }
}
