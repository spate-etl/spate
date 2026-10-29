//! Injectable time, so a test controls the clock that time-dependent code
//! reads.
//!
//! Code whose behaviour depends on time reads it from an injected clock:
//! [`Clock`] on OS threads, and [`tokio::Clock`] on the tokio runtime, whose
//! deadlines run on the tokio timer. A read that must stay on the real clock
//! names [`SystemClock`] at the site and says why. A duration that is only
//! measured goes through [`Stopwatch`], and a wall-clock timestamp through
//! [`wall_now`].

pub mod tokio;

#[cfg(any(test, feature = "testing"))]
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime};

/// A monotonic time source.
pub trait Clock {
    /// Current monotonic instant.
    fn now(&self) -> Instant;
}

/// The production clock, over [`Instant::now`].
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    #[inline]
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Measures how far a [`Clock`] moves between two reads.
///
/// It exposes no [`Instant`], so a measurement cannot feed a deadline. Pass
/// the same clock to [`start`](Self::start) and [`elapsed`](Self::elapsed).
#[derive(Clone, Copy, Debug)]
pub struct Stopwatch {
    started: Instant,
}

impl Stopwatch {
    /// Starts measuring at `clock`'s current instant.
    #[inline]
    #[must_use]
    pub fn start<C: Clock + ?Sized>(clock: &C) -> Self {
        Self {
            started: clock.now(),
        }
    }

    /// Time `clock` has moved since [`start`](Self::start), zero if it reads
    /// earlier.
    #[inline]
    #[must_use]
    pub fn elapsed<C: Clock + ?Sized>(&self, clock: &C) -> Duration {
        clock.now().saturating_duration_since(self.started)
    }
}

/// The system's wall-clock time.
///
/// For timestamps and identifiers only: wall-clock time can step backwards,
/// so it never feeds a deadline.
#[must_use]
pub fn wall_now() -> SystemTime {
    SystemTime::now()
}

/// A clock that only moves when a test moves it.
#[cfg(any(test, feature = "testing"))]
#[doc(hidden)]
#[derive(Debug)]
pub struct TestClock {
    base: Instant,
    offset_nanos: AtomicU64,
}

#[cfg(any(test, feature = "testing"))]
impl TestClock {
    /// A clock frozen at construction.
    #[must_use]
    pub fn frozen() -> Arc<TestClock> {
        Arc::new(TestClock {
            base: Instant::now(),
            offset_nanos: AtomicU64::new(0),
        })
    }

    /// Move the clock forward.
    pub fn advance(&self, by: Duration) {
        let nanos = u64::try_from(by.as_nanos()).expect("test clock advance fits in u64 nanos");
        self.offset_nanos.fetch_add(nanos, Ordering::Relaxed);
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
        self.base + Duration::from_nanos(self.offset_nanos.load(Ordering::Relaxed))
    }
}

#[cfg(any(test, feature = "testing"))]
impl Clock for &TestClock {
    fn now(&self) -> Instant {
        (**self).now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `advance_stepped` lands exactly on `total`, with a short last step,
    /// and runs `between` once per step.
    #[test]
    fn advance_stepped_reaches_total_in_steps() {
        let clock = TestClock::frozen();
        let start = clock.now();
        let mut seen = Vec::new();
        clock.advance_stepped(Duration::from_millis(25), Duration::from_millis(10), || {
            seen.push(clock.now() - start);
        });
        assert_eq!(
            seen,
            [10, 20, 25].map(Duration::from_millis),
            "one reading per step, the last one short"
        );
    }

    /// A stopwatch reports exactly how far its clock moved.
    #[test]
    fn a_stopwatch_reports_the_time_its_clock_moved() {
        let clock = TestClock::frozen();
        let watch = Stopwatch::start(&*clock);
        clock.advance(Duration::from_millis(250));
        assert_eq!(watch.elapsed(&*clock), Duration::from_millis(250));
    }

    #[test]
    #[should_panic(expected = "non-zero step")]
    fn advance_stepped_rejects_a_zero_step() {
        TestClock::frozen().advance_stepped(Duration::from_secs(1), Duration::ZERO, || {});
    }
}
