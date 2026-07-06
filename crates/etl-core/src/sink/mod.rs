//! Sink abstraction: pipeline threads encode, shard workers batch and
//! write.
//!
//! The division of labour (see `docs/DESIGN.md` § Sink):
//!
//! - **Pipeline threads** run the sink's [`RowEncoder`] inside the chain's
//!   terminal stage, accumulating encoded rows into small [`EncodedChunk`]
//!   frames per shard and `try_send`ing them into bounded per-shard queues
//!   (never blocking — a full queue surfaces as backpressure).
//! - **Shard workers** (tokio tasks) merge chunks from all pipeline
//!   threads into full-size batches, seal on `max_rows` / `max_bytes` /
//!   `linger`, and dispatch up to `max_inflight` concurrent
//!   [`ShardWriter::write_batch`] calls rotating across healthy replicas.
//!   Merging at the worker keeps batches large regardless of the pipeline
//!   thread count.
//!
//! A connector implements [`RowEncoder`] (CPU half) and [`ShardWriter`]
//! (I/O half); the framework owns everything between them.

mod breaker;
mod config;
mod pool;
#[cfg(test)]
mod pool_tests;
mod queue;
mod retry;
mod worker;

pub use config::{BatchConfig, BreakerConfig, InflightConfig, RetryConfig, SinkPoolConfig};
pub use pool::{DrainReport, SinkPool};
pub use queue::{ChunkSendError, ShardQueues, shard_queues};

use crate::checkpoint::AckSet;
use crate::deser::RecFamily;
use crate::error::SinkError;
use crate::record::{Record, RecordMeta};
use bytes::{Bytes, BytesMut};
use std::time::Instant;

/// A small frame of encoded rows produced on a pipeline thread, the unit
/// shipped over the per-shard queues. Wire frames are concatenable (row
/// formats like RowBinary carry no per-frame header), so workers accumulate
/// chunks without re-encoding.
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
}

/// A batch sealed by a shard worker, ready to write. Frames concatenate to
/// the full wire payload.
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

/// Routes records to shards. Pure and cheap — called per record on
/// pipeline threads.
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

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
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
}
