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
//! - It exports **functions, not types**. `ObjectEntry` and the rest stay
//!   private and free to change; only the shape of the work is fixed.
//! - Each function is one whole unit of per-object work, not one internal
//!   call. Attribution below that level comes from the callgrind profile the
//!   bench already writes, not from a finer seam.

use crate::fetch::ObjectEntry;
use crate::split::{pack, split_id_for};
use spate_core::coordination::SplitId;

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
