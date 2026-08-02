//! Measurable entry points into work this crate does not otherwise expose.
//!
//! Benches in this workspace drive their crate's public API — `spate-core`
//! and `spate-avro`, the two that have them, both do, and a new crate is
//! expected to. This module is the one exception, and the reason is not that
//! the internals are more interesting than the surface.
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
//! - It exports **functions, not types**. `ObjectEntry` and the rest stay
//!   private and free to change; only the shape of the work is fixed.
//! - Each function is one whole unit of the work a stage does — a plan, an
//!   object run — not one internal call. Attribution below that level comes
//!   from the callgrind profile the bench already writes, not from a finer
//!   seam.

use crate::fetch::ObjectEntry;
use crate::split::{SplitDescriptor, pack, split_id_for};
use spate_core::coordination::SplitId;
use std::hint::black_box;

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
