//! What a lane shares with the unit reader: completion accounting and
//! object-level poison reports.
//!
//! The lane runs on the pipeline thread and the unit reader on the controller
//! thread. These two seams are the state they share besides the fetcher
//! channel.

use spate_core::record::PartitionId;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};

/// Fetcher→lane channel depth, in messages. Queued zero-copy chunks can retain
/// earlier GET buffers while the fetcher buffers the next window.
pub(crate) const LANE_HANDOFF_CHUNKS: usize = 4;

/// Names the work a lane reads in failure reports and logs.
pub(crate) type LaneLabel = Arc<str>;

/// Sentinel for "the lane has not observed end-of-input yet".
const UNKNOWN: i64 = i64::MIN;

/// The bit of `SplitTracker::objects_done` that marks the unit closed.
const CLOSED: u32 = 1 << 31;

/// Lane→control-plane completion accounting, lock-free.
///
/// `terminal` is one-shot (the end-of-input decision); `objects_done`
/// counts fully framed member objects so the control plane can settle the
/// `objects_remaining` gauge when a unit closes mid-way. After
/// [`close`](Self::close), the lane takes back the count of each object it
/// framed before the close and the closer takes back the rest.
#[derive(Debug)]
pub(crate) struct SplitTracker {
    terminal: AtomicI64,
    /// The framed count, with [`CLOSED`] set once the split is closed.
    objects_done: AtomicU32,
}

impl SplitTracker {
    pub(crate) fn new() -> SplitTracker {
        SplitTracker {
            terminal: AtomicI64::new(UNKNOWN),
            objects_done: AtomicU32::new(0),
        }
    }

    /// Record the terminal watermark. Called once, from the lane, at its
    /// end-of-input decision point.
    pub(crate) fn set_terminal(&self, watermark: i64) {
        debug_assert_ne!(watermark, UNKNOWN, "terminal watermark is a real offset");
        let prev = self.terminal.swap(watermark, Ordering::Release);
        debug_assert!(
            prev == UNKNOWN || prev == watermark,
            "a lane decides end-of-input once; conflicting terminals {prev} vs {watermark}"
        );
    }

    /// The terminal watermark, once the lane has decided end-of-input.
    pub(crate) fn terminal(&self) -> Option<i64> {
        match self.terminal.load(Ordering::Acquire) {
            UNKNOWN => None,
            w => Some(w),
        }
    }

    /// Count one member object as fully framed. Returns whether the lane
    /// takes the object's count back from `objects_remaining`, which is
    /// `false` once the split is closed.
    pub(crate) fn object_done(&self) -> bool {
        self.objects_done.fetch_add(1, Ordering::Relaxed) & CLOSED == 0
    }

    /// Close the split to counting and return the member objects framed
    /// before it closed. The caller takes back the count of every other
    /// member object. Called once per tracker.
    pub(crate) fn close(&self) -> u32 {
        self.objects_done.fetch_or(CLOSED, Ordering::Relaxed) & !CLOSED
    }
}

/// What kind of object-level poison a unit hit. This is the bounded
/// `reason` label of `spate_s3_source_objects_failed_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PoisonKind {
    /// A read of an object the unit names answered `404` (deleted after it
    /// was listed or announced), or a listed key is not a valid object path.
    NotFound,
    /// The object's content changed under its pin: a failed `If-Match`
    /// precondition, or content that no longer matches committed progress.
    EtagDrift,
    /// The object's content cannot be decoded (corrupt, truncated, over
    /// the per-object record limit, or unverifiable without an ETag).
    Undecodable,
    /// Reads kept failing past the attempt budget.
    RetriesExhausted,
}

impl PoisonKind {
    /// The metric label value; bounded and stable.
    pub(crate) fn reason_label(self) -> &'static str {
        match self {
            PoisonKind::NotFound => "not_found",
            PoisonKind::EtagDrift => "etag_drift",
            PoisonKind::Undecodable => "undecodable",
            PoisonKind::RetriesExhausted => "retries_exhausted",
        }
    }
}

/// A lane's report that its unit hit object-level poison. The reporting lane
/// goes quiescent; the unit reader drains these on the controller thread and
/// fails the unit back to the feed.
#[derive(Debug)]
pub(crate) struct PoisonReport {
    /// The lane that sent the report.
    pub(crate) partition: PartitionId,
    /// Failure classification (bounded; drives the failure metric).
    pub(crate) kind: PoisonKind,
    /// Human-readable cause, naming the object.
    pub(crate) reason: String,
    /// What the key holds after an overwrite cut a notification unit's read
    /// short. The lane emits what it framed, ends cleanly, and is not
    /// poisoned; `None` for every other report.
    pub(crate) overwrite: Option<Box<crate::fetch::Overwrite>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracker_is_unknown_until_set_and_stable_after() {
        let t = SplitTracker::new();
        assert_eq!(t.terminal(), None);
        t.set_terminal(42);
        assert_eq!(t.terminal(), Some(42));
        t.set_terminal(42);
        assert_eq!(t.terminal(), Some(42));
    }

    #[test]
    fn tracker_carries_zero_watermarks_and_counts_objects() {
        // 0 is a legitimate terminal (an empty split emitted nothing).
        let t = SplitTracker::new();
        t.set_terminal(0);
        assert_eq!(t.terminal(), Some(0));
        assert!(t.object_done());
        assert!(t.object_done());
        assert_eq!(t.close(), 2);
        assert!(!t.object_done(), "a closed split's lane takes nothing back");
    }
}
