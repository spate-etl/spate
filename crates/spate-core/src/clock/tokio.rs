//! Injectable time on the tokio timer: [`Clock`] reads
//! [`tokio::time::Instant`] and sleeps until a deadline.
//!
//! [`SystemClock`] follows the runtime's clock, so paused tokio time applies
//! to it. `TestClock` moves only when a test advances it, for code on a
//! multi-thread runtime, where tokio time cannot be paused.

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
pub type Sleep = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// A monotonic clock on [`tokio::time::Instant`].
pub trait Clock: Send + Sync + std::fmt::Debug {
    /// The current instant on this clock's timeline.
    fn now(&self) -> Instant;

    /// Resolve once this clock reaches `deadline`.
    ///
    /// Returns immediately if `deadline` is already past. On a frozen
    /// clock this parks until the test advances past `deadline`. It must
    /// never fall back to real time, or the freeze leaks.
    fn sleep_until(&self, deadline: Instant) -> Sleep;
}

/// The production clock, on the runtime's tokio time.
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

/// A clock that only moves when a test moves it.
///
/// Wired into production code, it stops every timer drawn from it.
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
    /// One jump fires every deadline it passes at the same instant. Use
    /// [`advance_stepped`](TestClock::advance_stepped) when code under test
    /// must react between them.
    pub fn advance(&self, by: Duration) {
        let nanos = u64::try_from(by.as_nanos()).expect("test clock advance fits in u64 nanos");
        self.offset_nanos.send_modify(|n| *n += nanos);
    }

    /// Advance by `total` in `step`-sized increments, running `between`
    /// after each one.
    ///
    /// `between` is where the test lets the code under test react before
    /// the next step. Panics if `step` is zero.
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
