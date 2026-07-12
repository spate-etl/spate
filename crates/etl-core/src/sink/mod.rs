//! Sink abstraction: pipeline threads encode, shard workers batch and
//! write.
//!
//! The division of labour (see `docs/DESIGN.md` § Sink):
//!
//! - **Pipeline threads** route each record to a shard — two tiers share
//!   one seam: meta-only [`ShardRouter`] (the default [`KeyHashRouter`]:
//!   key hash, else a stable partition hash) or record-aware
//!   [`RecordRouter`] for payload-derived shard affinity — then run the
//!   sink's [`RowEncoder`] inside the chain's terminal stage, accumulating
//!   encoded rows into small [`EncodedChunk`] frames per shard and
//!   `try_send`ing them into bounded per-shard queues (never blocking — a
//!   full queue surfaces as backpressure).
//! - **Shard workers** (tokio tasks) merge chunks from all pipeline
//!   threads into full-size batches, seal on `max_rows` / `max_bytes` /
//!   `linger`, and dispatch up to `max_inflight` concurrent
//!   [`ShardWriter::write_batch`] calls rotating across healthy replicas.
//!   Merging at the worker keeps batches large regardless of the pipeline
//!   thread count.
//!
//! A connector implements [`RowEncoder`] (CPU half) and [`ShardWriter`]
//! (I/O half), and may ship a [`RecordRouter`] when the target's sharding
//! is payload-derived; the framework owns everything between them.

mod breaker;
mod bundle;
mod config;
mod pool;
#[cfg(test)]
mod pool_tests;
mod queue;
mod retry;
mod worker;

pub use bundle::{SinkBundle, SinkParts};
pub use config::{BatchConfig, BreakerConfig, InflightConfig, RetryConfig, SinkPoolConfig};
pub use pool::{DrainReport, SinkPool};
pub use queue::{ChunkSendError, ShardQueues, shard_queues};

/// Boxed sink drain hook: budget in, report out. Produced by sink
/// assemblies (wrapping [`SinkPool::drain`]), consumed once at shutdown by
/// the pipeline runtime.
pub type SinkDrainFn = Box<
    dyn FnOnce(std::time::Duration) -> std::pin::Pin<Box<dyn Future<Output = DrainReport> + Send>>
        + Send,
>;

/// Boxed, repeatable sink connectivity probe (readiness). The runtime
/// probes at startup and then periodically, driving the sinks-connected
/// half of `/readyz`.
pub type SinkProbeFn = Box<
    dyn Fn() -> std::pin::Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send>> + Send + Sync,
>;

/// Build a [`SinkProbeFn`] that probes every replica of every shard in
/// `shard_endpoints` (indexed `[shard][replica]`) via
/// [`ShardWriter::probe`] — the readiness loop
/// [`SinkParts::with_probe`](crate::sink::SinkParts::with_probe) expects.
/// Back `writer` with an independent probe client set, never the insert
/// clients (see [`SinkParts::probe`](crate::sink::SinkParts)).
pub fn endpoint_probe<W>(
    writer: W,
    shard_endpoints: std::sync::Arc<Vec<Vec<W::Endpoint>>>,
) -> SinkProbeFn
where
    W: ShardWriter + Clone,
{
    Box::new(move || {
        let writer = writer.clone();
        let shard_endpoints = std::sync::Arc::clone(&shard_endpoints);
        Box::pin(async move {
            for shard in shard_endpoints.iter() {
                for endpoint in shard {
                    writer.probe(endpoint).await?;
                }
            }
            Ok(())
        })
    })
}

use crate::checkpoint::AckSet;
use crate::deser::RecFamily;
use crate::error::SinkError;
use crate::metrics::Meter;
use crate::record::{Record, RecordMeta};
use bytes::{Bytes, BytesMut};
use std::time::Instant;

/// A small frame of encoded rows produced on a pipeline thread, the unit
/// shipped over the per-shard queues. Wire frames are concatenable — either
/// the format is headerless (RowBinary rows appended back-to-back) or each
/// frame is one complete, self-describing block (ClickHouse Native), and a
/// concatenation of complete blocks is itself a legal insert stream — so
/// workers accumulate chunks without re-encoding.
///
/// Teardown safety: `acks` is an [`AckSet`] — dropping a chunk anywhere
/// (a closed queue, an aborted worker, a parked chunk at teardown) fails
/// its batches so their offsets never commit; only a completed durable
/// write delivers them.
#[derive(Debug)]
pub struct EncodedChunk {
    /// Encoded rows in the sink's wire format.
    pub frame: Bytes,
    /// Number of rows in `frame`.
    pub rows: u32,
    /// Acknowledgement handles of the source batches represented in
    /// `frame`. Consecutive records usually share a batch, so this stays
    /// short (the encoder dedupes consecutive identical handles).
    pub acks: AckSet,
    /// When the oldest record in `frame` entered the terminal stage
    /// (ingest-basis end-to-end latency).
    pub oldest_ingest: Instant,
    /// Smallest record event time in `frame`, milliseconds since the epoch
    /// (event-basis end-to-end latency).
    pub oldest_event_ms: i64,
}

/// The CPU half of a sink connector: encodes one record into the sink's
/// wire format. Runs on pinned pipeline threads inside the chain's
/// terminal stage; must not perform I/O. Family-generic and dyn-compatible,
/// like [`Deserializer`](crate::deser::Deserializer).
pub trait RowEncoder<F: RecFamily>: Send {
    /// Append `rec`'s encoding to `buf`. Errors are record-level and
    /// subject to the sink stage's `ErrorPolicy` — except errors of
    /// [`ErrorClass::Fatal`](crate::error::ErrorClass::Fatal), which stop
    /// the pipeline regardless of policy (fatal means the encoder itself
    /// is broken, e.g. the row type cannot match the target schema; every
    /// subsequent record would fail identically).
    fn encode<'buf>(
        &mut self,
        rec: &Record<F::Rec<'buf>>,
        buf: &mut BytesMut,
    ) -> Result<(), SinkError>;

    /// Bytes the encoder is holding internally that have **not** yet been
    /// flushed to a frame. Row formats append directly in
    /// [`encode`](Self::encode) and buffer nothing, so the default is `0`.
    /// Columnar formats (which must transpose a whole block before any bytes
    /// exist) return the approximate size of the block under assembly; the
    /// terminal stage adds this to the shard buffer length when deciding
    /// whether to seal a chunk, so a columnar block still respects
    /// [`ChunkConfig::target_bytes`](crate::ops::ChunkConfig).
    fn buffered_bytes(&self) -> usize {
        0
    }

    /// Finalize the pending chunk: flush any internally-buffered rows into
    /// `buf` as exactly **one** complete, self-describing wire frame, leaving
    /// the encoder empty and ready for the next chunk. Row formats already
    /// wrote every row in [`encode`](Self::encode), so the default is a
    /// no-op. The terminal stage calls this immediately before it seals each
    /// [`EncodedChunk`] — in steady state, on data lulls, and at drain — so a
    /// columnar encoder's buffered rows are never silently dropped.
    ///
    /// An `Err` is fatal (a broken encoder, not a bad record): the stage
    /// ships no partial frame and the buffered rows' acknowledgements fail on
    /// teardown, so the data replays. Because a Native block concatenates
    /// with the blocks around it, each `finish_chunk` frame is independently
    /// valid — workers still accumulate frames without re-encoding.
    fn finish_chunk(&mut self, buf: &mut BytesMut) -> Result<(), SinkError> {
        let _ = buf;
        Ok(())
    }
}

/// A batch sealed by a shard worker, ready to write. Frames concatenate to
/// the full wire payload (a stream of one or more self-describing blocks for
/// block formats like ClickHouse Native).
#[derive(Debug)]
pub struct SealedBatch {
    /// Encoded frames, in order.
    pub frames: Vec<Bytes>,
    /// Total rows across `frames`.
    pub rows: u64,
    /// Total bytes across `frames`.
    pub bytes: u64,
    /// Deterministic-within-a-session batch identity. Retries of the same
    /// sealed batch — including on other replicas — reuse the same token,
    /// so sinks with server-side deduplication windows treat them as
    /// idempotent. Crash replay produces different tokens (documented
    /// at-least-once semantics).
    pub dedup_token: String,
}

/// The I/O half of a sink connector: writes one sealed batch to one
/// replica endpoint. Returning `Ok` is the durable-ack point — only then
/// may the framework resolve the batch's acknowledgements.
pub trait ShardWriter: Send + Sync + 'static {
    /// A connected replica endpoint (e.g. one HTTP client per replica).
    type Endpoint: Send + Sync + 'static;

    /// Receive a [`Meter`] scoped `etl_<component_type>_sink_*` for the sink's
    /// own metric families, pre-labelled with the standard
    /// `pipeline`/`component`/`component_type`. Called once by the builder
    /// before the writer is shared across shard workers; resolve handles here
    /// and store them (they are `Arc`-backed, so `write_batch`'s `&self` can
    /// touch them). `None` when the sink's `component_type` cannot scope a
    /// family: the default `"custom"` (reserved for pipeline-author metrics) or
    /// a reserved root opts out silently, and a malformed value is logged and
    /// also yields `None` — so declare a distinct `component_type` via
    /// [`SinkParts::with_component_type`](crate::sink::SinkParts::with_component_type)
    /// to receive a scope. Defaults to ignoring it.
    fn attach_metrics(&mut self, meter: Option<Meter>) {
        let _ = meter;
    }

    /// Write `batch` to `endpoint` durably.
    fn write_batch(
        &self,
        endpoint: &Self::Endpoint,
        batch: &SealedBatch,
    ) -> impl Future<Output = Result<(), SinkError>> + Send;

    /// Connectivity probe for readiness. Defaults to healthy.
    fn probe(
        &self,
        endpoint: &Self::Endpoint,
    ) -> impl Future<Output = Result<(), SinkError>> + Send {
        let _ = endpoint;
        async { Ok(()) }
    }
}

/// Routes records to shards on metadata alone — the **meta-only tier** of
/// sink routing. Pure and cheap — called per record on pipeline threads.
///
/// Every `ShardRouter` is also a [`RecordRouter`] for every record family
/// through a blanket bridge, so meta-only routers plug into the same
/// builder seam unchanged. Implement [`RecordRouter`] directly instead
/// when routing needs the payload.
pub trait ShardRouter: Send + Sync {
    /// The shard index in `0..num_shards` for a record.
    fn route(&self, meta: &RecordMeta, num_shards: usize) -> usize;
}

/// Default router: key hash modulo shards, falling back to the source
/// partition for keyless records (keeps a partition's keyless records
/// together and the distribution stable).
#[derive(Clone, Copy, Debug, Default)]
pub struct KeyHashRouter;

impl ShardRouter for KeyHashRouter {
    #[inline]
    fn route(&self, meta: &RecordMeta, num_shards: usize) -> usize {
        debug_assert!(num_shards > 0);
        let h = meta
            .key_hash
            .unwrap_or_else(|| u64::from(meta.partition.0).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        (h % num_shards as u64) as usize
    }
}

/// Routes records to shards with access to the full record — the
/// **record-aware tier** of sink routing. Pure and cheap: called once per
/// record on pinned pipeline threads, strictly before encoding; it must
/// not perform I/O, block, or allocate per call. Family-generic and
/// dyn-compatible, like [`RowEncoder`].
///
/// Two tiers, one seam:
///
/// - **Meta-only** ([`ShardRouter`]): routes on [`RecordMeta`] alone (key
///   hash, source partition). The default [`KeyHashRouter`] lives here.
///   Every `ShardRouter` is automatically a `RecordRouter` for every
///   family through a blanket bridge, so meta-only routers plug into the
///   same builder seam unchanged.
/// - **Record-aware** (this trait): routes on the payload itself —
///   required when shard affinity derives from a field of the terminal
///   record type (e.g. matching a sink cluster's own sharding expression),
///   and the only way to route `flat_map` children independently: children
///   inherit their parent's [`RecordMeta`], so a meta-only router
///   necessarily colocates them.
///
/// The router sees the record exactly as the [`RowEncoder`] will — after
/// every transform — so a routing key must survive to the terminal record
/// type. A router may hold state (a weights table, an atomic counter);
/// `&self` plus interior mutability covers stateful strategies.
///
/// A router must also be **total**: return a shard index for every record
/// and never panic. Routing deliberately has no per-record error policy —
/// a record either has a well-defined shard or the router picks a
/// deterministic fallback. Unlike an encoder error, which honors the sink
/// stage's Skip/Fail policy, a router panic fails the in-flight batch and
/// stops the pipeline; restart then replays the same record, so a
/// payload-dependent panic is a deterministic crash loop until a code fix
/// ships.
///
/// # Examples
///
/// A record-aware router over an owned family:
///
/// ```
/// use etl_core::deser::Owned;
/// use etl_core::record::Record;
/// use etl_core::sink::RecordRouter;
///
/// struct ByLen;
/// impl RecordRouter<Owned<Vec<u8>>> for ByLen {
///     fn route_record<'buf>(&self, rec: &Record<Vec<u8>>, num_shards: usize) -> usize {
///         rec.payload.len() % num_shards
///     }
/// }
/// ```
///
/// Implement **either** this trait **or** [`ShardRouter`], never both —
/// the bridge makes implementing both a coherence overlap:
///
/// ```compile_fail,E0119
/// use etl_core::deser::Owned;
/// use etl_core::record::{Record, RecordMeta};
/// use etl_core::sink::{RecordRouter, ShardRouter};
///
/// struct Both;
/// impl ShardRouter for Both {
///     fn route(&self, _: &RecordMeta, _: usize) -> usize { 0 }
/// }
/// impl RecordRouter<Owned<Vec<u8>>> for Both {
///     fn route_record<'buf>(&self, _: &Record<Vec<u8>>, _: usize) -> usize { 0 }
/// }
/// ```
pub trait RecordRouter<F: RecFamily>: Send + Sync {
    /// The shard index in `0..num_shards` for `rec`. Must be
    /// `< num_shards` — the terminal stage indexes its shard buffers
    /// directly with the result.
    fn route_record<'buf>(&self, rec: &Record<F::Rec<'buf>>, num_shards: usize) -> usize;
}

/// Bridge: every meta-only [`ShardRouter`] routes any record family by
/// ignoring the payload and delegating to [`ShardRouter::route`] on the
/// record's metadata.
impl<F: RecFamily, R: ShardRouter> RecordRouter<F> for R {
    #[inline]
    fn route_record<'buf>(&self, rec: &Record<F::Rec<'buf>>, num_shards: usize) -> usize {
        self.route(&rec.meta, num_shards)
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::checkpoint::AckRef;
    use crate::deser::Owned;
    use crate::record::PartitionId;

    fn meta(key_hash: Option<u64>, partition: u32) -> RecordMeta {
        RecordMeta {
            partition: PartitionId(partition),
            offset: 0,
            event_time_ms: 0,
            key_hash,
        }
    }

    #[test]
    fn key_hash_router_uses_key_then_partition() {
        let r = KeyHashRouter;
        assert_eq!(r.route(&meta(Some(10), 0), 4), (10 % 4) as usize);
        // Keyless: stable per partition, and different partitions spread.
        let a = r.route(&meta(None, 0), 4);
        let b = r.route(&meta(None, 0), 4);
        assert_eq!(a, b);
        let spread: std::collections::HashSet<_> =
            (0..16).map(|p| r.route(&meta(None, p), 4)).collect();
        assert!(spread.len() > 1, "keyless records must not all colocate");
    }

    #[test]
    fn shard_router_bridges_to_record_router_ignoring_payload() {
        let (ack, _rx) = AckRef::test_pair();
        for key_hash in [Some(10), Some(u64::MAX), None] {
            let rec = Record {
                payload: vec![1u8, 2, 3],
                meta: meta(key_hash, 3),
                ack: ack.clone(),
            };
            for n in [1usize, 2, 4, 7] {
                assert_eq!(
                    RecordRouter::<Owned<Vec<u8>>>::route_record(&KeyHashRouter, &rec, n),
                    ShardRouter::route(&KeyHashRouter, &rec.meta, n),
                    "the bridge must delegate to the meta-only route"
                );
            }
        }
    }

    #[test]
    fn record_router_is_dyn_compatible_for_a_concrete_family() {
        let _router: &dyn RecordRouter<Owned<Vec<u8>>> = &KeyHashRouter;
    }
}
