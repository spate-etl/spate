//! Injectable time, so a test controls the clock that time-dependent code
//! reads.
//!
//! [`Clock`] reads [`std::time::Instant`] for synchronous code. Code whose
//! deadlines run on the tokio timer uses [`tokio`].

pub mod tokio;

use std::time::Instant;
#[cfg(any(test, feature = "testing"))]
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

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

    #[test]
    #[should_panic(expected = "non-zero step")]
    fn advance_stepped_rejects_a_zero_step() {
        TestClock::frozen().advance_stepped(Duration::from_secs(1), Duration::ZERO, || {});
    }
}
