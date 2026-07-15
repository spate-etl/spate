//! Bounded object-storage (S3) backfill source for `etl-rs`.
//!
//! Point the source at a bucket/prefix and it streams every object's
//! records through the pipeline: a **bounded** job that checkpoints
//! durably, resumes after a restart, and self-terminates (the pipeline
//! exits [`Completed`](etl_core::pipeline::ExitState)) once the prefix is
//! exhausted. Delivery is at-least-once: a restart replays from the last
//! committed position, so duplicates are possible and loss is not.
//!
//! # Shape
//!
//! - **Lanes, not objects, are partitions.** The source runs a fixed number
//!   of lanes (`lanes: K`); the object listing is dealt round-robin across
//!   them, and each lane streams its slice sequentially. One lane is one
//!   framework partition with one monotonic offset stream.
//! - **Offsets are composite.** A record's `i64` offset packs (object
//!   ordinal within the lane, record index within the object) — an
//!   internal 23/40-bit layout, versioned by [`MANIFEST_SCHEMA`] — which
//!   satisfies the checkpoint tracker's contiguous watermark contract
//!   across object boundaries.
//! - **Commits are connector-durable.** Object storage has no broker-side
//!   commit, so watermarks are persisted to a small manifest object (see
//!   `checkpoint.url`) on every commit tick. The manifest also pins the
//!   listing identity (per-lane key/etag/rolling hash) so a listing that
//!   changed between runs fails fast instead of replaying or skipping the
//!   wrong data.
//! - **Records are framed, not decoded, here.** The source is
//!   format-agnostic: it streams object bytes (after gzip/zstd decompression)
//!   through a [`RecordFramer`](etl_core::framing::RecordFramer) *you supply*
//!   for the objects' format via
//!   [`S3Source::with_framer`](crate::S3Source::with_framer) — e.g. `etl-json`'s
//!   `NdjsonFramer` for NDJSON — emitting one raw payload per framed record.
//!   Deserialization then stays in the operator chain (`etl-json` etc.),
//!   exactly as with the Kafka source.
//!
//! # The frozen-key-set contract
//!
//! The bucket prefix must not change for the lifetime of the backfill,
//! including across restarts: resume positions are ordinals into the
//! lexicographic listing. Keys added, removed, or overwritten below a
//! committed position are detected at resume (manifest key/etag/hash
//! checks) and fail the pipeline. Write new data to a different prefix and
//! run a new backfill over it instead.
//!
//! # Scaling
//!
//! One pipeline process owns one prefix. The source does **not** scale
//! horizontally: running two processes over the same prefix duplicates the
//! entire backfill (each lists everything) and they race on the manifest.
//! Scale vertically with `lanes` and pipeline threads.
//!
//! No `object_store` types appear in this crate's public API (the same
//! dependency policy that keeps rdkafka out of `etl-kafka`'s).

mod config;
mod error;
mod fetch;
mod framer;
mod lane;
mod metrics;
mod offset;
mod source;
mod store;
#[cfg(test)]
mod testutil;

pub use config::{CheckpointStoreConfig, Compression, S3SourceConfig};
pub use lane::{S3Batch, S3Lane};
pub use source::S3Source;
pub use store::{
    LaneState, MANIFEST_SCHEMA, Manifest, OffsetStore, OffsetStoreError, SourceIdentity,
};
