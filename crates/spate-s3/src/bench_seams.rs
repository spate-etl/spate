//! Measurable entry points into work this crate does not otherwise expose.
//!
//! Benches in this workspace drive their crate's public API — `spate-core`,
//! `spate-avro`, `spate-json` and `spate-clickhouse` all do, and a new crate
//! is expected to. This module is the one exception, and the reason is not
//! that the internals are more interesting than the surface.
//!
//! This crate's public entry point is [`S3Source`](crate::S3Source), which is
//! asynchronous and driven by object-store I/O. Those are exactly the two
//! properties that stop an instruction count being deterministic: under a
//! runtime the number becomes a function of how the scheduler interleaved
//! polls, which is not a property of the code under review. Packing, split
//! identity and record framing are pure, synchronous and on the per-object
//! path — countable — but reaching them means reaching past the async
//! surface. So the seam exists because the public API is unmeasurable *by
//! that instrument*, not because finer detail is wanted.
//!
//! Consequences of that reasoning, which a future seam has to honour:
//!
//! - It is behind the off-by-default `testing` feature and `#[doc(hidden)]`,
//!   so it is not part of this crate's semver surface and no consumer of the
//!   `spate` facade can see it.
//! - It exports **functions, not types**. `ObjectFramer` and `ObjectEntry`
//!   stay private and free to change; only the shape of the work is fixed.
//! - Each function is one whole unit of per-object work, not one internal
//!   call. Attribution below that level comes from the callgrind profile the
//!   bench already writes, not from a finer seam.

use crate::config::Compression;
use crate::fetch::ObjectEntry;
use crate::framer::{Codec, ObjectFramer};
use crate::split::{pack, split_id_for};
use spate_core::coordination::SplitId;
use spate_core::framing::RecordFramer;
use std::io;
use std::sync::Arc;

/// Builds a fresh record framer, as the source does per object.
pub type MakeFramer = Arc<dyn Fn() -> Box<dyn RecordFramer> + Send + Sync>;

/// Pack a listing into splits and mint each split's id — what the leader's
/// planner does with a listing once, per plan.
///
/// `objects` is `(key, size_bytes, etag)` in listing order, taken by value so
/// the keys move into the entries rather than being cloned inside the
/// measured region. The two halves are deliberately one function: minting is
/// per-split work the planner always does immediately after packing, and
/// folding them together is what makes a preimage built with `format!` per
/// member visible without a second case.
///
/// # Panics
///
/// If a packed bin is empty, which [`pack`] cannot produce.
#[must_use]
pub fn plan_listing(objects: Vec<(String, u64, String)>, target_bytes: u64) -> Vec<SplitId> {
    let entries: Vec<ObjectEntry> = objects
        .into_iter()
        .map(|(key, size, etag)| ObjectEntry {
            key,
            size,
            etag: Some(etag),
            last_modified_ms: 0,
        })
        .collect();

    pack(entries, target_bytes)
        .into_iter()
        .map(|bin| {
            split_id_for(bin.iter().map(|e| (e.key.as_str(), e.etag.as_deref())))
                .expect("pack never returns an empty bin")
        })
        .collect()
}

/// Frame a run of objects into records through one framer, returning how
/// many records they produced in total.
///
/// Mirrors the lane, which builds a single `ObjectFramer` and cycles
/// `begin_object`/`finish_object` over the members of its split — so a run of
/// several objects here measures the per-object codec resolution and state
/// reset that a multi-member split really pays, not the cost of constructing
/// a framer per object. One object is a one-element slice.
///
/// Each object is `(key, chunks)`: the key because the codec is resolved from
/// it per object, and the chunks already split the way a fetcher would
/// deliver them — and already compressed, if `compression` says so, since
/// compressing here would count the compressor rather than the decompressor.
///
/// Entering an object part-way through needs no parameter: the framer's
/// contract is that the record sequence is a pure function of the bytes it is
/// given, so a mid-object entry is a chunk list that starts at an offset.
///
/// # Errors
///
/// Whatever the decompressor or the framer reports — a truncated stream, a
/// corrupt frame, or a record over the framer's cap.
pub fn frame_objects(
    compression: Compression,
    make_framer: MakeFramer,
    objects: &[(String, Vec<Vec<u8>>)],
) -> io::Result<usize> {
    let mut framer = ObjectFramer::new(make_framer);
    let mut records = 0;
    for (key, chunks) in objects {
        framer.begin_object(Codec::resolve(compression, key))?;
        for chunk in chunks {
            framer.push_chunk(chunk)?;
        }
        framer.finish_object()?;
        // Drained per object, as the lane drains into its batch before
        // starting the next one: a record never spans an object boundary.
        while framer.pop_record().is_some() {
            records += 1;
        }
    }
    Ok(records)
}
