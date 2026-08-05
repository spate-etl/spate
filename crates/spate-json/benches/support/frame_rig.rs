//! The framing rig both bench tiers drive: a stream already split into the
//! chunks a source would `push`, and the loop that feeds them through one
//! framer.
//!
//! Included with `#[path]` rather than imported: a bench target is its own
//! crate, so two targets can only agree on what they measure by compiling the
//! same source. `lines.rs` already works that way for the streams; this is the
//! loop that reads them. `framing_gungraun.rs` counts instructions inside
//! [`frame_stream`] and `framing_wall.rs` times the same function.
//!
//! What stays in each target is the case list — which stream, at which chunk
//! size. That is the part the two tiers are entitled to differ on: the counted
//! tier can afford a case the wall tier would spend a minute of a person's
//! afternoon on.
//!
//! Nothing here mentions a corpus. A [`Rig`] is chunks and two expectations,
//! so the streams stay in `lines.rs` and no corpus type crosses this boundary.

// Each includer compiles this module separately and may use a different subset
// of it, which is a module-wide `allow` rather than per-item `expect`: an
// `expect` would itself go unfulfilled in whichever target does use the item.
#![allow(dead_code, reason = "each bench target uses a different subset")]

use spate_core::framing::RecordFramer;
use spate_json::NdjsonFramer;
use std::hint::black_box;

/// The framer's per-record cap. Generous relative to every fixture here, so no
/// case is measuring the cap rejecting anything.
pub(crate) const MAX_RECORD_BYTES: usize = 1 << 20;

/// One stream, already split into the chunks a source would `push`.
pub(crate) struct Rig {
    pub(crate) chunks: Vec<Vec<u8>>,
    /// What the framer must emit, and how many bytes those records must carry.
    /// Asserted rather than returned: a framer that silently stopped splitting
    /// — or stopped stripping a `\r`, or started counting blank lines — would
    /// otherwise read as a changed number rather than as a failure.
    pub(crate) expect_records: usize,
    pub(crate) expect_bytes: usize,
}

/// The measured work: one stream through one framer, chunk by chunk, popping
/// records as they complete.
///
/// `#[inline(never)]` is load-bearing, not stylistic, and removing it does not
/// fail anything — it silently empties the measurement. Callgrind toggles
/// collection on the benchmark function's module, and a toggle flips
/// collection rather than forcing it on, so a loop the optimiser reshapes
/// across that boundary leaves the region holding whatever else was running —
/// usually the allocator freeing the corpus. A named frame the optimiser may
/// not erase is what keeps the loop inside the region.
///
/// The wall tier does not need the attribute and is not harmed by it: it times
/// a region it opens and closes itself. Keeping one function for both tiers is
/// worth more than saving a call.
///
/// The framer is built here rather than held on the rig, which is what makes
/// the function repeatable: it takes the rig by shared reference and leaves
/// nothing behind, so the wall harness can drive it thousands of times against
/// one rig and get the same answer every time. Opening a framer is a `VecDeque`
/// and three scalars, charged identically to every case.
///
/// Records are popped inside the loop rather than after `finish`, because that
/// is what a source does: it hands each framed record onward and keeps the
/// framer's queue short, where draining at the end would measure a queue eight
/// thousand entries deep that production never builds. The popped buffers are
/// dropped here too, which is the allocation a source really pays.
///
/// Summing the record lengths is what keeps the loop alive: the framed bytes
/// are otherwise unobserved and the optimiser is free to delete the calls this
/// exists to count. The caller asserts both totals.
#[inline(never)]
pub(crate) fn frame_stream(rig: &Rig) -> (usize, usize) {
    let mut framer = NdjsonFramer::new(MAX_RECORD_BYTES);
    let (mut records, mut bytes) = (0usize, 0usize);
    for chunk in &rig.chunks {
        framer
            .push(black_box(chunk.as_slice()))
            .expect("the fixture stays inside the record cap");
        while let Some(record) = framer.pop() {
            records += 1;
            bytes += record.len();
        }
    }
    framer.finish().expect("the fixture frames cleanly");
    while let Some(record) = framer.pop() {
        records += 1;
        bytes += record.len();
    }
    (records, bytes)
}
