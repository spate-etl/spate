//! The pending-ceiling gate: shared per-partition counters that let a
//! driver bound registered-but-unadvanced batches at the poll boundary.
//!
//! One [`AdvanceCounter`] per (epoch, partition), owned by the
//! [`Checkpointer`](super::Checkpointer) and handed to the owning driver
//! inside a [`PendingGate`] along with each lane. The controller adds the
//! number of batches a watermark advance retired; the driver compares that
//! against the batches it issued (read from the contiguous per-partition
//! acknowledgment sequence) and skips a lane's poll while the difference
//! is at the configured ceiling.
//!
//! All operations are `Relaxed`. The incrementing side of `issued` is the
//! gate's own reader (a partition's lanes live on exactly one thread), so
//! the direction that matters for the bound is program order; a stale read
//! of `advanced` only under-estimates progress, which holds the gate
//! closed one round longer and never opens it early. Read-modify-write
//! operations cannot lose updates under `Relaxed` (each observes the
//! latest value in the modification order).

use super::sync::{Arc, AtomicU64, Ordering};

/// Cumulative batches advanced past for one partition within one epoch.
#[derive(Clone, Debug)]
pub(crate) struct AdvanceCounter {
    advanced: Arc<AtomicU64>,
}

impl AdvanceCounter {
    pub(crate) fn new() -> Self {
        AdvanceCounter {
            advanced: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Record `n` batches retired by a watermark advance.
    pub(crate) fn add(&self, n: u64) {
        self.advanced.fetch_add(n, Ordering::Relaxed);
    }

    /// Batches advanced so far. Possibly stale under contention: an
    /// under-read only holds the gate closed one round longer.
    pub(crate) fn get(&self) -> u64 {
        self.advanced.load(Ordering::Relaxed)
    }
}

/// Handed to a driver with each lane: the lane's assignment epoch and the
/// advance counter for its partition. Batches acknowledged under a
/// different epoch are stale by construction and must not be counted
/// against the gate. Their registrations are discarded by the
/// checkpointer, so nothing would ever retire them.
#[derive(Clone, Debug)]
pub(crate) struct PendingGate {
    pub(crate) epoch: u32,
    pub(crate) advanced: AdvanceCounter,
}
