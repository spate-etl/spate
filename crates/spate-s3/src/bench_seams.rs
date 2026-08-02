//! Measurable entry points into work this crate does not otherwise expose.
//!
//! Benches in this workspace drive their crate's public API, and a new crate
//! is expected to. Two do not: this module, and `spate-coordination`'s. The
//! reason is the same for both, and it is not that the internals are more
//! interesting than the surface.
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
//! - It exports **functions and the aliases they need, never this crate's own
//!   types**. `ObjectFramer`, `ObjectEntry` and the rest stay private and free
//!   to change; only the shape of the work is fixed. `MakeFramer` is an alias
//!   over `std` and `spate-core` types, which a caller must be able to name to
//!   supply a framer at all.
//! - Each function is one whole unit of the work a stage does — a plan, an
//!   object run — not one internal call. Attribution below that level comes
//!   from the callgrind profile the bench already writes, not from a finer
//!   seam.

use crate::config::Compression;
use crate::fetch::ObjectEntry;
use crate::framer::{Codec, ObjectFramer};
use crate::split::{SplitDescriptor, pack, split_id_for};
use spate_core::coordination::SplitId;
use spate_core::framing::RecordFramer;
use std::hint::black_box;
use std::io;
use std::sync::Arc;

/// Builds a fresh record framer, as the source does per object.
pub type MakeFramer = Arc<dyn Fn() -> Box<dyn RecordFramer> + Send + Sync>;

/// Pack a listing into splits, mint each split's id, and encode each
/// descriptor — the whole of what the leader's planner does with a listing,
/// once per plan.
///
/// `objects` is `(key, size_bytes, etag)` **sorted by key**, which is the
/// order `list_all` hands the planner; packing preserves listing order, so
/// feeding it anything else measures a arrangement production never produces.
/// Taken by value so the keys move into the entries rather than being cloned
/// inside the measured region.
///
/// The three stages are one function because the planner does them as one
/// pass per split and a change to any of them moves the others: the id
/// digests the member keys and ETags, and the descriptor serialises the same
/// members immediately afterwards. Splitting them would measure the parts
/// while missing the per-split loop that carries all three — and the
/// serialisation is the term most likely to dominate, so a seam that skipped
/// it would answer a different question than the planner asks.
///
/// # Panics
///
/// If a packed bin is empty, which `pack` cannot produce, or if a descriptor
/// fails to encode. Neither is reachable from a well-formed listing; both are
/// asserted rather than propagated because a bench has no policy to apply.
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
            let id = split_id_for(bin.iter().map(|e| (e.key.as_str(), e.etag.as_deref())))
                .expect("pack never returns an empty bin");
            // The planner encodes the descriptor here too, and its cost is
            // charged to the plan rather than hoisted out: the members are
            // consumed by it, so the measured region ends where the planner's
            // per-split work ends rather than part-way through it.
            let encoded = SplitDescriptor::from_entries(&bin)
                .encode()
                .expect("a packed bin encodes");
            black_box(encoded);
            id
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
            // Drained per chunk, because the lane drains at the top of every
            // poll-loop iteration rather than at end of object. The push and
            // pop counts are the same either way, so instruction counts do
            // not care — but the queue is unbounded, so draining only at the
            // end would hold a whole object's records live at once and report
            // a DHAT peak the source never reaches. DHAT is attached to this
            // bench, so that shape is part of what it publishes.
            while framer.pop_record().is_some() {
                records += 1;
            }
        }
        framer.finish_object()?;
        // The tail: `finish_object` completes an unterminated final record.
        // A record never spans an object boundary, so the queue is empty
        // before the next `begin_object`.
        while framer.pop_record().is_some() {
            records += 1;
        }
    }
    Ok(records)
}
