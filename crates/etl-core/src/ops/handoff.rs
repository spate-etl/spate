//! The terminal stage: route, encode, chunk, and hand off to the sink
//! queues — all on the pipeline thread.
//!
//! Pressure discipline (matches [`StageLifecycle`]'s contract): `push`
//! never rejects a record. When a sealed chunk cannot be sent it is parked
//! and pressure is reported through `relieve()`, which the chain checks
//! between payloads. Parked chunks always drain before newer ones, so
//! per-shard order is preserved.

use super::Collector;
use super::chain::{FatalSlot, OpMeterSlot, StageLifecycle};
use crate::backpressure::InflightBudget;
use crate::checkpoint::{AckRef, BatchId};
use crate::deser::RecFamily;
use crate::error::{ErrorPolicy, FatalError};
use crate::record::{Flow, Record};
use crate::sink::{ChunkSendError, EncodedChunk, RowEncoder, ShardQueues, ShardRouter};
use crate::telemetry::RateLimit;
use bytes::BytesMut;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

/// Tuning for the terminal stage's per-shard chunking.
#[derive(Clone, Copy, Debug)]
pub struct ChunkConfig {
    /// Seal and send a chunk once its frame reaches this size. Small
    /// enough to flow steadily, large enough to amortize queue traffic;
    /// sink workers merge chunks into full-size batches, so this does
    /// **not** bound insert sizes.
    pub target_bytes: usize,
    /// Policy for record-level encoder failures. `Skip` drops the record
    /// (metrics-counted); `Fail` stops the pipeline.
    pub encode_policy: ErrorPolicy,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        ChunkConfig {
            target_bytes: 64 * 1024,
            encode_policy: ErrorPolicy::Skip,
        }
    }
}

/// Per-shard accumulation state.
#[derive(Debug, Default)]
struct ShardBuf {
    buf: BytesMut,
    rows: u32,
    acks: Vec<AckRef>,
    last_batch: Option<BatchId>,
}

static ENCODE_SKIP_WARN: RateLimit = RateLimit::new(5, Duration::from_secs(10));

/// The chain's terminal stage. Owns one accumulation buffer per shard,
/// seals [`EncodedChunk`]s at [`ChunkConfig::target_bytes`], and hands
/// them to the sink workers through the bounded [`ShardQueues`] — a
/// `try_send` that never blocks the pipeline thread.
#[derive(Debug)]
pub struct SinkHandoff<F: RecFamily, E, R> {
    encoder: E,
    router: R,
    queues: ShardQueues,
    budget: Arc<InflightBudget>,
    cfg: ChunkConfig,
    shards: Vec<ShardBuf>,
    /// Sealed chunks that could not be sent, in seal order.
    parked: VecDeque<(usize, EncodedChunk)>,
    pub(crate) meter: OpMeterSlot,
    pub(crate) fatal: FatalSlot,
    component: Arc<str>,
    _family: std::marker::PhantomData<fn() -> F>,
}

impl<F: RecFamily, E, R> SinkHandoff<F, E, R> {
    pub(crate) fn new(
        encoder: E,
        router: R,
        queues: ShardQueues,
        budget: Arc<InflightBudget>,
        cfg: ChunkConfig,
        meter: OpMeterSlot,
        component: Arc<str>,
    ) -> Self {
        assert!(cfg.target_bytes > 0, "chunk target must be non-zero");
        let shards = (0..queues.num_shards())
            .map(|_| ShardBuf::default())
            .collect();
        SinkHandoff {
            encoder,
            router,
            queues,
            budget,
            cfg,
            shards,
            parked: VecDeque::new(),
            meter,
            fatal: FatalSlot(None),
            component,
            _family: std::marker::PhantomData,
        }
    }

    /// Seal shard `idx`'s buffer into a chunk and try to send it. The
    /// in-flight budget grows at seal time — a parked chunk is in-flight
    /// memory too; the sink worker releases the bytes after the batch is
    /// written or abandoned.
    fn seal_and_send(&mut self, idx: usize) {
        let shard = &mut self.shards[idx];
        if shard.rows == 0 {
            return;
        }
        let frame = shard.buf.split().freeze();
        self.budget.add(frame.len());
        let chunk = EncodedChunk {
            frame,
            rows: shard.rows,
            acks: std::mem::take(&mut shard.acks),
        };
        shard.rows = 0;
        shard.last_batch = None;
        match self.queues.try_send(idx, chunk) {
            Ok(()) => {}
            Err(ChunkSendError(chunk)) => self.parked.push_back((idx, chunk)),
        }
    }

    /// Drain parked chunks in seal order. Returns whether all cleared.
    fn drain_parked(&mut self) -> bool {
        while let Some((idx, chunk)) = self.parked.pop_front() {
            match self.queues.try_send(idx, chunk) {
                Ok(()) => {}
                Err(ChunkSendError(chunk)) => {
                    self.parked.push_front((idx, chunk));
                    return false;
                }
            }
        }
        true
    }
}

impl<'buf, F, E, R> Collector<<F as RecFamily>::Rec<'buf>> for SinkHandoff<F, E, R>
where
    F: RecFamily,
    E: RowEncoder<F>,
    R: ShardRouter,
{
    fn push(&mut self, rec: Record<F::Rec<'buf>>) -> Flow {
        self.meter.0.seen();
        if self.fatal.0.is_some() {
            return Flow::Continue;
        }
        let idx = self.router.route(&rec.meta, self.shards.len());
        let shard = &mut self.shards[idx];
        let before = shard.buf.len();
        match self.encoder.encode(&rec, &mut shard.buf) {
            Ok(()) => {
                shard.rows += 1;
                self.meter.0.out();
                let bid = rec.ack.batch_id();
                if shard.last_batch != Some(bid) {
                    shard.acks.push(rec.ack.clone());
                    shard.last_batch = Some(bid);
                }
                if shard.buf.len() >= self.cfg.target_bytes {
                    self.seal_and_send(idx);
                }
                Flow::Continue
            }
            Err(e) => {
                // The encoder may have written a partial row; roll it back
                // so the frame stays well-formed.
                shard.buf.truncate(before);
                match self.cfg.encode_policy {
                    ErrorPolicy::Skip => {
                        self.meter.0.skipped();
                        self.meter.0.record_error();
                        crate::rate_limited_warn!(
                            ENCODE_SKIP_WARN,
                            component = &*self.component,
                            error = %e,
                            "record skipped by sink encoder error policy"
                        );
                    }
                    _ => {
                        self.fatal.0 = Some(FatalError {
                            component: self.component.to_string(),
                            reason: e.to_string(),
                        });
                    }
                }
                Flow::Continue
            }
        }
    }
}

impl<F: RecFamily, E, R> StageLifecycle for SinkHandoff<F, E, R> {
    fn on_batch_end(&mut self, elapsed: Duration) {
        self.meter.0.flush(elapsed);
    }

    fn take_fatal(&mut self) -> Option<FatalError> {
        self.fatal.0.take()
    }

    fn relieve(&mut self) -> Flow {
        if self.parked.is_empty() || self.drain_parked() {
            Flow::Continue
        } else {
            Flow::Blocked
        }
    }

    fn flush_terminal(&mut self) -> Flow {
        for idx in 0..self.shards.len() {
            self.seal_and_send(idx);
        }
        if self.drain_parked() {
            Flow::Continue
        } else {
            Flow::Blocked
        }
    }
}
