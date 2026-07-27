//! The monotonic time source behind the coordination control loop.
//!
//! Production uses [`SystemClock`] — real wall time. Tests inject
//! `TestClock`, which only moves when the test moves it, so every
//! time-driven transition is deterministic regardless of CI scheduler
//! jitter: the coordinator forces a multi-thread runtime, so
//! `tokio::time::pause()` is unavailable and the timing would otherwise
//! track real time.
//!
//! The clock owns both halves of "when": reading the current instant
//! ([`Clock::now`]) *and* waiting for one ([`Clock::sleep_until`]). Every
//! deadline that drives a *protocol state transition* is drawn from it —
//! lease expiry and the starvation self-fence, the heartbeat/reconcile/
//! replan cadence, the `rebalance_delay` grace window, the drain deadline,
//! and the renewal cadence gate — so a frozen clock cannot freeze half the
//! protocol and let the other half race.
//!
//! Deliberately *not* on the clock, because each bounds real I/O rather
//! than protocol time: `op_timeout` (`store::metered`), the command reply
//! budget (`coordinator`), the startup and re-watch retry backoffs
//! (`task`), and `MemoryStore`'s sweeper cadence — the sweeper's *cadence*
//! is real, but what it expires is judged against the clock. So a frozen
//! clock stops the protocol; it does not stop the crate from using real
//! time where real time is the thing being bounded. Commands are served on
//! a clock-independent select arm, which is what lets a test drive a
//! coordinator whose clock is not moving.
//!
//! # Advancing a frozen clock
//!
//! Time only moves on `TestClock::advance`, and what is safe to advance
//! by depends on whether the worker under test can still renew:
//!
//! - **Advance to expire.** Jumping past a whole lease is deterministic
//!   *only* when the target cannot renew anyway — a crashed runtime, a
//!   kill-switch store, an injected fault. Nothing is racing the jump.
//! - **Advance to settle.** For "hold steady and assert nothing changes"
//!   windows the worker is alive and must win its renewals, so step in
//!   increments no larger than `renew_interval` and let the task run
//!   between them. `TestClock::advance_stepped` does exactly that; a
//!   single large jump here would make a lease expiry and the renewal that
//!   prevents it come due at the same instant, which is a real race.

use std::future::Future;
use std::pin::Pin;
// Only `TestClock` needs `Duration`; the trait and `SystemClock` do not.
#[cfg(any(test, feature = "testing"))]
use std::time::Duration;
use tokio::time::Instant;

/// A future that resolves when a [`Clock`] reaches some instant.
///
/// Boxed and `'static`: implementations must not borrow the clock, so a
/// caller can hold the future across a `select!` arm whose body needs
/// `&mut self`.
///
/// One is drawn per timer arm per `select!` entry, so the rate is the
/// control loop's wakeup rate — not a fixed cadence. A commit wakes the
/// loop (`Command::Commit` is sent unconditionally), and the pipeline
/// controller tightens to `FAST_COMMIT_POLL` while chasing a fast-commit
/// partition, so the loop can iterate in the low milliseconds. Still off
/// the per-record path, and every such iteration already `Box::pin`s its
/// handler — the same trade, at the same rate.
pub type Sleep = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// A monotonic clock. Returns [`tokio::time::Instant`] so the value slots
/// straight into the existing lease/deadline fields without a type change.
///
/// `Debug` is required because [`MemoryStore`](crate::store::memory::MemoryStore)
/// derives it and holds a clock.
pub trait Clock: Send + Sync + std::fmt::Debug {
    /// The current instant on this clock's timeline.
    fn now(&self) -> Instant;

    /// Resolve once this clock reaches `deadline`.
    ///
    /// Returns immediately if `deadline` is already past. On a frozen
    /// clock this parks until the test advances past `deadline` — it must
    /// never fall back to real time, or the freeze would leak.
    fn sleep_until(&self, deadline: Instant) -> Sleep;
}

/// Real wall time — the production clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep_until(&self, deadline: Instant) -> Sleep {
        Box::pin(tokio::time::sleep_until(deadline))
    }
}

/// A clock that only moves when a test moves it. See the [module
/// docs](self) for the two advancing patterns.
///
/// Behind the `testing` feature and `#[doc(hidden)]`: it has to be `pub`
/// for this crate's `tests/` binaries, which are external crates and
/// cannot see `#[cfg(test)]` items, but it must not reach a consumer — a
/// `TestClock` wired into production would stop the control loop dead.
/// Off by default and never enabled by the `spate` facade, the same shape as
/// `spate-s3`'s `testing` feature.
#[cfg(any(test, feature = "testing"))]
#[doc(hidden)]
#[derive(Debug)]
pub struct TestClock {
    /// The instant this clock was created at. Fixed for its lifetime.
    base: Instant,
    /// Nanoseconds elapsed on this clock's timeline. The watch channel is
    /// the single source of truth for "now" *and* the wakeup for parked
    /// [`Clock::sleep_until`] futures, so the two can never disagree.
    offset_nanos: tokio::sync::watch::Sender<u64>,
}

#[cfg(any(test, feature = "testing"))]
impl TestClock {
    /// A clock frozen at construction. Build it after the runtime exists
    /// so `tokio::time::Instant::now()` reads the runtime's clock.
    #[must_use]
    pub fn frozen() -> std::sync::Arc<TestClock> {
        std::sync::Arc::new(TestClock {
            base: Instant::now(),
            offset_nanos: tokio::sync::watch::Sender::new(0),
        })
    }

    /// Move the clock forward, waking everything parked on a deadline this
    /// jump passes.
    ///
    /// Safe as one jump only when nothing is racing it — see the [module
    /// docs](self). Prefer [`advance_stepped`](TestClock::advance_stepped)
    /// for a live worker.
    pub fn advance(&self, by: Duration) {
        let nanos = u64::try_from(by.as_nanos()).expect("test clock advance fits in u64 nanos");
        self.offset_nanos.send_modify(|n| *n += nanos);
    }

    /// Advance by `total` in `step`-sized increments, running `between`
    /// after each one.
    ///
    /// This is the shape a live worker needs: it gets a chance to renew
    /// inside every step, so its lease never expires merely because the
    /// test moved time faster than the protocol could react. `between` is
    /// where the test drives its coordinators (or just yields).
    pub fn advance_stepped(&self, total: Duration, step: Duration, mut between: impl FnMut()) {
        assert!(!step.is_zero(), "advance_stepped needs a non-zero step");
        let mut moved = Duration::ZERO;
        while moved < total {
            let chunk = step.min(total - moved);
            self.advance(chunk);
            moved += chunk;
            between();
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl Clock for TestClock {
    fn now(&self) -> Instant {
        self.base + Duration::from_nanos(*self.offset_nanos.borrow())
    }

    fn sleep_until(&self, deadline: Instant) -> Sleep {
        let base = self.base;
        let mut rx = self.offset_nanos.subscribe();
        Box::pin(async move {
            loop {
                if base + Duration::from_nanos(*rx.borrow_and_update()) >= deadline {
                    return;
                }
                if rx.changed().await.is_err() {
                    // The clock is gone, so it can never reach `deadline`.
                    // Park rather than return: waking would fire a timer
                    // whose time never came.
                    std::future::pending::<()>().await;
                }
            }
        })
    }
}
