//! The terminal stage: route, encode, chunk, and hand off to the sink
//! queues, all on the pipeline thread.
//!
//! Pressure discipline (matches [`StageLifecycle`]'s contract): `push`
//! never rejects a record. When a sealed chunk cannot be sent it is parked
//! and pressure is reported through `relieve()`, which the chain checks
//! between payloads. Parked chunks always drain before newer ones, so
//! per-shard order is preserved.

use super::Collector;
use super::chain::{FatalSlot, OpMeterSlot, StageLifecycle};
use crate::backpressure::InflightBudget;
use crate::checkpoint::{AckSet, BatchId};
use crate::deser::RecFamily;
use crate::error::{ErrorClass, ErrorPolicy, FatalError, SinkError};
use crate::record::{Flow, Record};
use crate::sink::{ChunkSendError, EncodedChunk, RecordRouter, RowEncoder, ShardQueues};
use crate::telemetry::RateLimit;
use bytes::BytesMut;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Tuning for the terminal stage's per-shard chunking.
#[derive(Clone, Copy, Debug)]
pub struct ChunkConfig {
    /// Seal and send a chunk once its frame reaches this size. Small
    /// enough to flow steadily, large enough to amortize queue traffic;
    /// sink workers merge chunks into full-size batches, so this does
    /// **not** bound insert sizes. A buffer below this size is not held
    /// indefinitely: the controller flushes partial chunks on every commit
    /// tick (`checkpoint.interval`), and the driver flushes them on an idle
    /// lull, so while the pipeline is unblocked a partial buffer holds its
    /// acknowledgments for at most ~one checkpoint interval. (A driver
    /// wedged retrying a blocked batch defers the flush until the sink
    /// drains, though nothing commits during that time anyway.)
    pub target_bytes: usize,
    /// Policy for record-level encoder failures. `Skip` drops the record
    /// (metrics-counted); `Fail` stops the pipeline. An encoder error of
    /// [`ErrorClass::Fatal`] stops the pipeline regardless of this policy;
    /// fatal means the component is broken, not the record.
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

/// Per-shard accumulation state, including this shard's own encoder
/// instance. A columnar encoder (ClickHouse Native) buffers its rows
/// internally until the chunk is finalized, so each shard must own its
/// encoder. A single shared encoder would interleave rows from different
/// shards into one block. Row formats clone a trivial unit and are
/// unaffected.
#[derive(Debug)]
struct ShardBuf<E> {
    encoder: E,
    buf: BytesMut,
    rows: u32,
    acks: AckSet,
    last_batch: Option<BatchId>,
    /// When the first record of the open chunk arrived.
    first_ingest: Option<Instant>,
    /// Smallest event time seen in the open chunk (ms since epoch).
    oldest_event_ms: i64,
}

static ENCODE_SKIP_WARN: RateLimit = RateLimit::new(5, Duration::from_secs(10));

/// The chain's terminal stage. Owns one accumulation buffer per shard,
/// seals [`EncodedChunk`]s at [`ChunkConfig::target_bytes`], and hands
/// them to the sink workers through the bounded [`ShardQueues`], with a
/// `try_send` that never blocks the pipeline thread.
#[derive(Debug)]
pub struct SinkHandoff<F: RecFamily, E, R> {
    router: R,
    queues: ShardQueues,
    budget: Arc<InflightBudget>,
    cfg: ChunkConfig,
    shards: Vec<ShardBuf<E>>,
    /// Sealed chunks that could not be sent, in seal order.
    parked: VecDeque<(usize, EncodedChunk)>,
    pub(crate) meter: OpMeterSlot,
    pub(crate) fatal: FatalSlot,
    component: Arc<str>,
    _family: std::marker::PhantomData<fn() -> F>,
}

impl<F: RecFamily, E, R> SinkHandoff<F, E, R>
where
    E: RowEncoder<F> + Clone,
{
    pub(crate) fn new(
        encoder: E,
        router: R,
        queues: ShardQueues,
        budget: Arc<InflightBudget>,
        cfg: ChunkConfig,
        meter: OpMeterSlot,
        component: Arc<str>,
    ) -> Self {
        // Defense-in-depth on the cold per-thread build path. The
        // field-named rejection lives at load time (`chunk.target_bytes` in
        // the YAML/`SinkOptions` paths, via
        // `config::chunk::ChunkSection::resolve` and
        // `Pipeline::add_sink_with`); this catches a direct construction bug,
        // where a zero would break `BytesMut::with_capacity` and the seal
        // check below.
        assert!(cfg.target_bytes > 0, "chunk target must be non-zero");
        let shards = (0..queues.num_shards())
            .map(|_| ShardBuf {
                encoder: encoder.clone(),
                // Pre-size so the first chunk fills a target-sized buffer
                // instead of regrowing (realloc + memcpy) from zero.
                buf: BytesMut::with_capacity(cfg.target_bytes),
                rows: 0,
                acks: AckSet::new(),
                last_batch: None,
                first_ingest: None,
                oldest_event_ms: i64::MAX,
            })
            .collect();
        SinkHandoff {
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
    /// in-flight budget grows at seal time, because a parked chunk is
    /// in-flight memory too; the sink worker releases the bytes after the
    /// batch is written or abandoned.
    fn seal_and_send(&mut self, idx: usize) {
        let shard = &mut self.shards[idx];
        if shard.rows == 0 {
            return;
        }
        // Columnar encoders buffer their rows internally; flush the pending
        // block into `buf` as one complete frame before sealing (a no-op for
        // row formats, which already wrote every row in `encode`). A finalize
        // failure means a broken encoder, not a bad record: record it fatal
        // and ship nothing. The shard's captured acks fail on teardown, so
        // the rows replay.
        if let Err(e) = shard.encoder.finish_chunk(&mut shard.buf) {
            self.meter.0.fatal_error();
            self.fatal.0 = Some(FatalError {
                component: self.component.to_string(),
                reason: e.to_string(),
            });
            return;
        }
        let frame = shard.buf.split().freeze();
        // `split()` left the emptied buffer sharing the frozen frame's
        // allocation, so growing it in place is impossible while that frame is
        // in flight. Reserve a fresh target-sized allocation now (one alloc
        // per seal) so the next chunk accumulates without repeatedly
        // reallocating and copying the partial frame.
        shard.buf.reserve(self.cfg.target_bytes);
        self.budget.add(frame.len());
        let chunk = EncodedChunk {
            frame,
            rows: shard.rows,
            acks: std::mem::take(&mut shard.acks),
            oldest_ingest: shard.first_ingest.take().unwrap_or_else(Instant::now),
            oldest_event_ms: shard.oldest_event_ms,
        };
        shard.rows = 0;
        shard.last_batch = None;
        shard.oldest_event_ms = i64::MAX;
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

/// Teardown safety: un-sent output (parked chunks after a drain deadline,
/// partial shard buffers) holds its acknowledgments in fail-on-drop
/// [`AckSet`]s, so tearing the handoff down stalls those watermarks and the
/// records replay after restart. This `Drop` reconciles only the in-flight
/// byte budget for parked chunks (their bytes were added at seal time).
impl<F: RecFamily, E, R> Drop for SinkHandoff<F, E, R> {
    fn drop(&mut self) {
        for (_, chunk) in self.parked.drain(..) {
            self.budget.sub(chunk.frame.len());
        }
    }
}

impl<'buf, F, E, R> Collector<<F as RecFamily>::Rec<'buf>> for SinkHandoff<F, E, R>
where
    F: RecFamily,
    E: RowEncoder<F> + Clone,
    R: RecordRouter<F>,
{
    fn push(&mut self, rec: Record<F::Rec<'buf>>) -> Flow {
        self.meter.0.seen();
        if self.fatal.0.is_some() {
            return Flow::Continue;
        }
        let idx = self.router.route_record(&rec, self.shards.len());
        let shard = &mut self.shards[idx];
        let before = shard.buf.len();
        match shard.encoder.encode(&rec, &mut shard.buf) {
            Ok(()) => {
                shard.rows += 1;
                self.meter.0.out();
                shard.first_ingest.get_or_insert_with(Instant::now);
                shard.oldest_event_ms = shard.oldest_event_ms.min(rec.meta.event_time_ms);
                let bid = rec.ack.batch_id();
                if shard.last_batch != Some(bid) {
                    shard.acks.push(rec.ack.clone());
                    shard.last_batch = Some(bid);
                }
                // Columnar encoders hold the block in `encoder`, not `buf`;
                // count what they've buffered so a block still seals at the
                // target size. `buffered_bytes()` is 0 for row formats, so
                // this reduces to the plain `buf.len()` check for them.
                if shard.buf.len() + shard.encoder.buffered_bytes() >= self.cfg.target_bytes {
                    self.seal_and_send(idx);
                }
                Flow::Continue
            }
            Err(e) => {
                // The encoder may have written a partial row; roll it back
                // so the frame stays well-formed.
                shard.buf.truncate(before);
                // A Fatal-class error means the component is broken, not
                // the record ("processing must stop"), so it overrides the
                // record-level policy. Skipping it once per record would
                // silently drop everything.
                let fatal_class = matches!(
                    e,
                    SinkError::Client {
                        class: ErrorClass::Fatal,
                        ..
                    }
                );
                match self.cfg.encode_policy {
                    ErrorPolicy::Skip if !fatal_class => {
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
                        self.meter.0.fatal_error();
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

impl<F: RecFamily, E, R> StageLifecycle for SinkHandoff<F, E, R>
where
    E: RowEncoder<F> + Clone,
{
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
