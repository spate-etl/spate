//! Object-storage (S3) source for Spate, reading through a work feed.
//!
//! Point the source at a bucket/prefix and a pipeline `feed:` section, and it
//! streams every object's records through the pipeline: a **bounded** job
//! self-terminates (the pipeline exits
//! [`Completed`](spate_core::pipeline::ExitState)) once the prefix is
//! exhausted. Delivery is at-least-once: a unit that its worker did not finish
//! is read again from its start, so duplicates are possible and loss is not.
//! A pipeline with no `feed:` section fails to start.
//!
//! The source reads work units from the feed the section names, each unit
//! naming one object or one byte range of one object, and storage
//! notifications, one part per non-empty created object. On a durable feed it
//! lists nothing itself: a bounded job's units come from a separate lister run,
//! [`S3Source::list_once`], once per job, and a continuous job's from storage
//! notifications or code. On the in-process feed the run lists its own prefix
//! into the feed. A whole object that qualifies for byte ranges is
//! read in ranges. The section's `units_held` bounds its read parallelism,
//! one lane per object or range in progress. When the section names a
//! `location`, the job is bounded: the source waits for the job fingerprint
//! its lister writes there, refuses to start when it differs from its own,
//! and ends once the lister's end marker holds over a settling window in
//! which the feed reads empty. A solo run on the in-process feed the runtime
//! built ends once its listing is read. Any other job on a feed ends at a
//! shutdown.
//!
//! # Shape
//!
//! - **Work is a stream of units.** Each unit has a deterministic identity
//!   ([`split_id_for`], [`split_id_for_range`]) and a self-contained
//!   [`SplitDescriptor`] carrying its member key, size and ETag, and its range.
//!   Workers read units straight from their descriptors: **workers never
//!   list**.
//! - **One lane per unit in progress.** A received unit materializes one
//!   data lane (one framework partition, one monotonic offset stream); a
//!   record's `i64` offset packs (member ordinal within the unit, record
//!   index within the object, or within the range for a unit over a byte
//!   range). `feed.units_held` bounds the lanes and therefore read
//!   parallelism.
//! - **Progress lives in the feed.** The source acknowledges each finished
//!   unit to the feed once the sink has acknowledged every record in it, and a
//!   redelivered unit is read again from its start. This crate names no
//!   concrete backend and has no feed feature.
//! - **Bad objects fail their unit, not the pipeline.** An object read as
//!   `404` after it was listed or announced, a listed key that is not a valid
//!   object path, and an object overwritten under its ETag pin, corrupt, or
//!   unreadable past the retry budget each fail their unit back to the feed
//!   (surfaced in `spate_s3_source_objects_failed_total{reason}`), which
//!   dead-letters it at its cap. A bounded job with a dead-lettered unit ends
//!   **failed**, never silently incomplete. A read the store rejects with
//!   `401` or `403` and a client-configuration error are immediately fatal.
//!   AWS S3 answers a read of a deleted object with `403` to a role without
//!   `s3:ListBucket`. A notification unit's object overwritten under its pin
//!   is re-enqueued at its current ETag instead.
//! - **Records are framed, not decoded, here.** The source is
//!   format-agnostic: it streams object bytes (after gzip/zstd decompression)
//!   through a [`RecordFramer`](spate_core::framing::RecordFramer) *you supply*
//!   for the objects' format via
//!   [`S3Source::with_framer`](crate::S3Source::with_framer) (e.g.
//!   `spate-json`'s `NdjsonFramer` for NDJSON), emitting one raw payload per
//!   framed record. Deserialization then stays in the operator chain
//!   (`spate-json` etc.), as with the Kafka source.
//!
//! # Unit identity and drift
//!
//! A unit's id digests its sorted member keys **and ETags** (plus a version
//! constant, and a byte-range unit's range), and every GET is pinned
//! `If-Match` to the descriptor's ETag. The consequences:
//!
//! - Listing an unchanged prefix again reproduces identical ids.
//! - An **overwritten** object shows up as a new unit (new id) on the next
//!   listing; under an in-flight unit it trips the `If-Match` pin and fails
//!   the unit. Either way it can never silently splice into earlier progress.
//! - A **deleted** object fails only the units that still need it.
//!
//! Prefer an append-only prefix for the lifetime of a backfill: the lister
//! takes the listing once, so late-arriving keys are not part of the job.
//!
//! No `object_store` types appear in this crate's public API (the same
//! dependency policy that keeps rdkafka out of `spate-kafka`'s). The one
//! seam that would break it, `S3Source::with_store`, is gated behind the
//! off-by-default `testing` feature so tests can inject wrapped or
//! fault-injecting stores without that type reaching a real consumer.

#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod bench_seams;
mod config;
mod error;
mod fetch;
mod framer;
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod fuzz_seams;
mod job_end;
mod lane;
mod lane_ctx;
mod lister;
mod listing;
mod location;
mod metrics;
mod offset;
mod source;
mod split;
#[cfg(test)]
mod test_servers;
#[cfg(test)]
mod testutil;
mod trim;
mod unit;
mod unit_reader;

pub use config::{Compression, S3SourceConfig};
pub use lane::{S3Batch, S3Lane};
pub use source::S3Source;
pub use split::{
    DESCRIPTOR_VERSION, DescriptorObject, SplitDescriptor, SplitError, SplitRange, split_id_for,
    split_id_for_range,
};
