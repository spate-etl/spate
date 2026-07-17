//! The monotonic time source behind lease expiry and the starvation
//! self-fence.
//!
//! Production uses [`SystemClock`] — real wall time. Tests inject a frozen
//! (or manually advanced) clock so that time-based fencing is deterministic
//! regardless of CI scheduler jitter: the coordinator forces a multi-thread
//! runtime, so `tokio::time::pause()` is unavailable and the timing would
//! otherwise track real time.
//!
//! Only the two *expiry* surfaces read this clock — the task's owned-split
//! self-fence and [`MemoryStore`](crate::store::memory::MemoryStore)'s
//! ephemeral deadlines. Renewal *scheduling* and the renewal cadence gate
//! stay on real time, so an injected clock never stalls the protocol.

use tokio::time::Instant;

/// A monotonic clock. Returns [`tokio::time::Instant`] so the value slots
/// straight into the existing lease/deadline fields without a type change.
///
/// `Debug` is required because [`MemoryStore`](crate::store::memory::MemoryStore)
/// derives it and holds a clock.
pub trait Clock: Send + Sync + std::fmt::Debug {
    /// The current instant on this clock's timeline.
    fn now(&self) -> Instant;
}

/// Real wall time — the production clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}
