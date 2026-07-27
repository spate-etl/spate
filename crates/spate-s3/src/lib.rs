//! Coordinated object-storage (S3) backfill source for Spate.
//!
//! Point the source at a bucket/prefix and it streams every object's
//! records through the pipeline: a **bounded** job that self-terminates
//! (the pipeline exits [`Completed`](spate_core::pipeline::ExitState)) once
//! the prefix is exhausted. Delivery is at-least-once: a resume replays
//! from the last committed position, so duplicates are possible and loss
//! is not.
//!
//! # Shape
//!
//! - **Work is planned as splits.** The fleet's elected leader lists the
//!   prefix once and packs the sorted listing into **splits** — small
//!   batches of whole objects, ~64 MiB by default — each with a
//!   deterministic identity ([`split_id_for`]) and a self-contained
//!   [`SplitDescriptor`] carrying its member keys, sizes, and ETags.
//!   Workers lease splits through the coordination store and read them
//!   straight from the descriptors: **workers never list**.
//! - **One lane per in-flight split.** A gained split materializes one
//!   data lane (one framework partition, one monotonic offset stream); a
//!   record's `i64` offset packs (member ordinal within the split, record
//!   index within the object). `coordination.max_in_flight` bounds the
//!   working set and therefore read parallelism.
//! - **Progress lives in the coordination store — nowhere else.** Commits
//!   are fenced per-split writes; a lost or stolen split resumes on its
//!   next owner from the acked watermark, drift-checked against the
//!   descriptor's ETag pins. The coordinator is **assembly wiring**: hand
//!   one in via [`S3Source::with_coordinator`] (e.g. `spate-coordination`'s
//!   `StoreCoordinator` over its NATS JetStream store) and every replica
//!   of the pipeline shares the backfill, takes a leader-assigned share, and
//!   takes over from the dead. Without one the source runs solo over an
//!   in-process store: correct, but a restart replays the prefix (a
//!   startup WARN says so). This crate names no concrete backend — which
//!   store a deployment uses is the deployer's choice, not a connector
//!   compile-time feature.
//! - **Bad objects poison their split, not the pipeline.** An object
//!   deleted after planning, overwritten under its ETag pin, corrupt, or
//!   unreadable past the retry budget hands its split back (surfaced in
//!   `spate_s3_source_objects_failed_total{reason}`); at the attempt cap
//!   the split is quarantined. A bounded job with quarantined splits ends
//!   **failed**, never silently incomplete. Credentials/configuration
//!   errors are still immediately fatal.
//! - **Records are framed, not decoded, here.** The source is
//!   format-agnostic: it streams object bytes (after gzip/zstd decompression)
//!   through a [`RecordFramer`](spate_core::framing::RecordFramer) *you supply*
//!   for the objects' format via
//!   [`S3Source::with_framer`](crate::S3Source::with_framer) — e.g. `spate-json`'s
//!   `NdjsonFramer` for NDJSON — emitting one raw payload per framed record.
//!   Deserialization then stays in the operator chain (`spate-json` etc.),
//!   exactly as with the Kafka source.
//!
//! # Split identity and drift
//!
//! A split's id digests its sorted member keys **and ETags** (plus the
//! packing-algorithm version), and every GET is pinned `If-Match` to the
//! descriptor's ETag. The consequences:
//!
//! - Replanning an unchanged prefix reproduces identical ids — replans
//!   are create-if-absent no-ops, and completed splits stay completed.
//! - An **overwritten** object shows up as a new split (new id) on the
//!   next plan; under an in-flight split it trips the `If-Match` pin and
//!   poisons the split. Either way it can never silently splice into
//!   committed progress.
//! - A **deleted** object poisons only the splits that still needed it.
//!
//! Prefer an append-only prefix for the lifetime of a backfill; by
//! default the listing is taken once (`refresh_listing: false`), so
//! late-arriving keys are simply not part of the job.
//!
//! No `object_store` types appear in this crate's public API (the same
//! dependency policy that keeps rdkafka out of `spate-kafka`'s). The one
//! seam that would break it, `S3Source::with_store`, is gated behind the
//! off-by-default `testing` feature so tests can inject wrapped or
//! fault-injecting stores without that type reaching a real consumer.

mod config;
mod error;
mod fetch;
mod framer;
mod lane;
mod metrics;
mod offset;
mod planner;
mod source;
mod split;
mod split_ctx;
#[cfg(test)]
mod testutil;

pub use config::{Compression, S3SourceConfig};
pub use lane::{S3Batch, S3Lane};
pub use source::S3Source;
pub use split::{DESCRIPTOR_VERSION, DescriptorObject, SplitDescriptor, split_id_for};
