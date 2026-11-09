//! Injectable time on the tokio timer: [`Clock`] reads
//! [`tokio::time::Instant`] and sleeps until a deadline.
//!
//! [`SystemClock`] follows the runtime's clock, so paused tokio time applies
//! to it. `TestClock` moves only when a test advances it, for code on a
//! multi-thread runtime, where tokio time cannot be paused. A duration that
//! is only measured on this timeline goes through [`Stopwatch`].

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
#[cfg(any(test, feature = "testing"))]
use tokio::sync::watch;
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

/// Runs `fut` until it ends, or returns `None` once `clock` reaches `at`.
///
/// `fut` is polled first, so an output ready as `at` passes counts. The
/// deadline is read outside tokio's coop budget, so it fires over a `fut` that
/// spends the budget on every poll. The sleep is built on the first poll, so
/// the returned future may be created outside the runtime that awaits it.
///
/// # Examples
///
/// ```
/// use spate_core::clock::tokio::{Clock, SystemClock, before};
/// use std::time::Duration;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let later = SystemClock.now() + Duration::from_secs(30);
/// assert_eq!(before(&SystemClock, later, async { 7 }).await, Some(7));
///
/// let now = SystemClock.now();
/// let never = std::future::pending::<()>();
/// assert_eq!(before(&SystemClock, now, never).await, None);
/// # }
/// ```
pub async fn before<C: Clock + ?Sized, F: Future>(
    clock: &C,
    at: Instant,
    fut: F,
) -> Option<F::Output> {
    crate::task::first(fut, clock.sleep_until(at)).await.ok()
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
    offset_nanos: watch::Sender<u64>,
    /// Every deadline passed to [`Clock::sleep_until`], so a test can wait
    /// until code under test has armed a timer.
    slept_until: std::sync::Mutex<Vec<Instant>>,
}

#[cfg(any(test, feature = "testing"))]
impl TestClock {
    /// A clock frozen at construction. Build it after the runtime exists
    /// so `tokio::time::Instant::now()` reads the runtime's clock.
    #[must_use]
    pub fn frozen() -> std::sync::Arc<TestClock> {
        std::sync::Arc::new(TestClock {
            base: Instant::now(),
            offset_nanos: watch::Sender::new(0),
            slept_until: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// Whether anything has called [`Clock::sleep_until`] for `deadline`.
    #[must_use]
    pub fn has_slept_until(&self, deadline: Instant) -> bool {
        self.slept_until
            .lock()
            .expect("test clock lock")
            .contains(&deadline)
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
        self.slept_until
            .lock()
            .expect("test clock lock")
            .push(deadline);
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

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use tokio::task::coop;

    /// A stopwatch reports exactly how far its clock moved, and nothing before
    /// it moves.
    #[test]
    fn a_tokio_stopwatch_reports_the_time_its_clock_moved() {
        let clock = TestClock::frozen();
        let watch = Stopwatch::start(&*clock);
        assert_eq!(watch.elapsed(&*clock), Duration::ZERO);
        clock.advance(Duration::from_millis(250));
        assert_eq!(watch.elapsed(&*clock), Duration::from_millis(250));
    }

    /// Polls `fut` once with a waker that does nothing.
    fn poll_once<F: Future>(fut: Pin<&mut F>) -> std::task::Poll<F::Output> {
        fut.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    }

    /// Pins that `before` polls its future first: on a `TestClock`, a sleep to
    /// the current instant is ready on its first poll, as the future is.
    #[tokio::test]
    async fn before_returns_an_output_ready_as_the_deadline_passes() {
        let clock = TestClock::frozen();
        assert_eq!(before(&*clock, clock.now(), async { 7 }).await, Some(7));
    }

    /// Pins that `before` ends at its deadline on the given clock, and not a
    /// nanosecond earlier.
    #[tokio::test]
    async fn before_ends_when_its_clock_reaches_the_deadline() {
        let clock = TestClock::frozen();
        let at = clock.now() + Duration::from_secs(5);
        let mut race = std::pin::pin!(before(&*clock, at, std::future::pending::<()>()));
        assert!(poll_once(race.as_mut()).is_pending());
        clock.advance(Duration::from_secs(5) - Duration::from_nanos(1));
        assert!(poll_once(race.as_mut()).is_pending());
        clock.advance(Duration::from_nanos(1));
        assert_eq!(poll_once(race.as_mut()), std::task::Poll::Ready(None));
    }

    /// Pins that `before` ends at a deadline already past even when the future
    /// it races spends the task's coop budget on every poll.
    #[tokio::test]
    async fn before_ends_when_the_future_spends_the_budget() {
        // Spends the whole budget each task poll, and ends on its own only
        // after 1000 task polls.
        let busy = async {
            for _ in 0..1000 {
                for _ in 0..200 {
                    coop::consume_budget().await;
                }
            }
        };
        assert!(before(&SystemClock, Instant::now(), busy).await.is_none());
    }

    /// Pins that `before` builds its sleep on the first poll: the future is
    /// created where no runtime is entered, and a tokio sleep panics there.
    #[test]
    fn before_builds_its_sleep_on_the_first_poll() {
        let race = before(
            &SystemClock,
            Instant::now() + Duration::from_secs(60),
            async { 3 },
        );
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        assert_eq!(rt.block_on(race), Some(3));
    }
}
