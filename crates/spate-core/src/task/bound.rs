//! A bound on a future, timed on a thread of its own.
//!
//! [`ThreadBound`] holds for a connect or startup step that may run on a
//! runtime whose timer nothing drives.

use super::spawn_thread;
use crate::clock::SyncClock;
use std::future::Future;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;
use tokio::sync::oneshot;

/// How often the timing thread reads the clock and asks for a rejection.
const TICK: Duration = Duration::from_millis(50);

/// A check for a rejection that ends the work early.
type Rejection = Box<dyn Fn() -> Option<String> + Send>;

/// A limit on how long a future may run, ended early by an optional rejection
/// check.
///
/// The bound is timed on a thread of its own, so it fires on any runtime: one
/// whose timer nothing drives, as under
/// [`Handle::block_on`](tokio::runtime::Handle::block_on) on a current-thread
/// runtime, one with timers disabled, and one that has shut down. The thread
/// reads the clock every 50 ms of real time, so the bound fires up to that
/// long after the limit passes, and it ends when the work does.
///
/// # Examples
///
/// ```
/// use spate_core::task::ThreadBound;
/// use std::time::Duration;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let io = tokio::runtime::Builder::new_current_thread().build()?;
/// let bound = ThreadBound::new("connect-bound", Duration::from_secs(30));
/// assert_eq!(io.block_on(bound.run(async { 7 }))?, 7);
/// # Ok(()) }
/// ```
#[must_use = "a bound does nothing until `run`"]
pub struct ThreadBound {
    name: &'static str,
    limit: Duration,
    clock: SyncClock,
    rejection: Option<Rejection>,
}

impl ThreadBound {
    /// A bound of `limit` on the system clock, timed on a thread named `name`,
    /// with no rejection check. A `limit` past the clock's range never fires.
    pub fn new(name: &'static str, limit: Duration) -> Self {
        ThreadBound {
            name,
            limit,
            clock: SyncClock::system(),
            rejection: None,
        }
    }

    /// Time the bound on `clock`.
    pub fn with_clock(self, clock: SyncClock) -> Self {
        ThreadBound { clock, ..self }
    }

    /// End the work as soon as `check` returns a text, which
    /// [`BoundError::Rejected`] carries.
    ///
    /// `check` runs on the timing thread once a tick, and before the limit is
    /// read, so a rejection present at the limit wins. It must not block:
    /// ending [`run`](Self::run) waits for the call under way. A panic in it
    /// is logged and ends the work as [`BoundError::Elapsed`].
    pub fn with_rejection(self, check: impl Fn() -> Option<String> + Send + 'static) -> Self {
        ThreadBound {
            rejection: Some(Box::new(check)),
            ..self
        }
    }

    /// Run `work` under the bound, which counts from the first poll.
    ///
    /// `work` is polled before the bound is read, so work that finishes on the
    /// poll the bound woke returns its output. Dropping the returned future
    /// drops `work` and ends the thread. When the bound fires, `work` is
    /// dropped with it.
    ///
    /// # Errors
    ///
    /// [`BoundError::Elapsed`] once the limit passes,
    /// [`BoundError::Rejected`] once the check returns a text, and
    /// [`BoundError::Thread`] when the system cannot start the thread.
    pub async fn run<F: Future>(self, work: F) -> Result<F::Output, BoundError> {
        let ThreadBound {
            name,
            limit,
            clock,
            rejection,
        } = self;
        // `Some` carries a rejection's text, `None` the limit passing.
        let (fire, fired) = oneshot::channel::<Option<String>>();
        let (finished, done) = mpsc::channel::<()>();
        let deadline = clock.now().checked_add(limit);
        let _timing = spawn_thread(name, move || {
            loop {
                let left = deadline.map_or(TICK, |d| d.saturating_duration_since(clock.now()));
                if done.recv_timeout(left.min(TICK)) != Err(RecvTimeoutError::Timeout) {
                    return;
                }
                if let Some(text) = rejection.as_ref().and_then(|check| check()) {
                    let _ = fire.send(Some(text));
                    return;
                }
                if deadline.is_some_and(|d| clock.now() >= d) {
                    let _ = fire.send(None);
                    return;
                }
            }
        })
        .map_err(BoundError::Thread)?;
        // Declared after `_timing`, so it drops first and ends the thread the
        // handle's drop then joins.
        let _finished = finished;
        super::first(work, fired)
            .await
            .map_err(|fired| match fired.ok().flatten() {
                Some(text) => BoundError::Rejected(text),
                None => BoundError::Elapsed,
            })
    }
}

impl std::fmt::Debug for ThreadBound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadBound")
            .field("name", &self.name)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

/// Why [`ThreadBound::run`] ended before its work.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BoundError {
    /// The limit passed on the bound's clock.
    #[error("the bound passed")]
    Elapsed,
    /// The rejection check returned this text.
    #[error("{0}")]
    Rejected(String),
    /// The system could not start the timing thread.
    #[error("starting a thread: {0}")]
    Thread(std::io::Error),
}

#[cfg(test)]
#[expect(
    clippy::disallowed_methods,
    reason = "a test thread ends with the call it runs, and a fixed thread keeps a hang from reaching the test's own thread"
)]
mod tests {
    use super::*;
    use crate::clock::TestClock;
    use std::future::poll_fn;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Poll;
    use tokio::runtime::Handle;
    use tokio::sync::mpsc::unbounded_channel;

    /// How long a test waits for a run to end.
    const DEADLINE: Duration = Duration::from_secs(10);

    const LONG: Duration = Duration::from_secs(600);

    fn current_thread() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn on(clock: &Arc<TestClock>, limit: Duration) -> ThreadBound {
        ThreadBound::new("test-bound", limit).with_clock(SyncClock::new(Arc::clone(clock) as _))
    }

    /// `f` on a thread of its own, its result on the returned channel.
    fn on_thread<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> mpsc::Receiver<T> {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(f());
        });
        rx
    }

    /// A run of work that never finishes.
    struct Hanging {
        done: mpsc::Receiver<Result<(), BoundError>>,
    }

    /// `bound` over work that never finishes, through `Handle::block_on` on
    /// `io`. Returns once the work is first polled, by when the limit counts.
    fn hanging(io: Handle, bound: ThreadBound) -> Hanging {
        let (reached, started) = mpsc::channel();
        let done = on_thread(move || {
            io.block_on(bound.run(async move {
                let _ = reached.send(());
                std::future::pending::<()>().await;
            }))
        });
        started.recv_timeout(DEADLINE).unwrap();
        Hanging { done }
    }

    /// Pins that the bound fires through `Handle::block_on` on a current-thread
    /// runtime, which drives no timer there, on a runtime with timers disabled,
    /// and on one that has shut down.
    #[test]
    fn the_bound_fires_on_any_runtime() {
        let current = current_thread();
        let untimed = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let stopped = current_thread().handle().clone();
        for (name, io) in [
            ("current-thread", current.handle().clone()),
            ("timers disabled", untimed.handle().clone()),
            ("shut down", stopped),
        ] {
            let clock = TestClock::frozen();
            let run = hanging(io, on(&clock, LONG));
            clock.advance(LONG);
            match run.done.recv_timeout(DEADLINE) {
                Ok(Err(BoundError::Elapsed)) => {}
                other => panic!("{name}: expected the bound to pass, got {other:?}"),
            }
        }
    }

    /// A check that hands over at each call, so the test paces the thread
    /// through its reads of the limit, which follow each call.
    fn paced(bound: ThreadBound) -> (ThreadBound, mpsc::Receiver<()>) {
        let (ask, asked) = mpsc::sync_channel(0);
        let bound = bound.with_rejection(move || {
            let _ = ask.send(());
            None
        });
        (bound, asked)
    }

    /// Pins that the bound fires once `limit` has passed on its clock, and not
    /// before.
    #[test]
    fn the_bound_fires_at_its_limit_on_the_given_clock() {
        let io = current_thread();
        let clock = TestClock::frozen();
        let (bound, asked) = paced(on(&clock, LONG));
        let run = hanging(io.handle().clone(), bound);
        asked.recv_timeout(DEADLINE).unwrap();
        clock.advance(LONG - Duration::from_nanos(1));
        // The read after this call sees the advance, and a thread that fired
        // on it drops its check, so the next receive would fail.
        asked.recv_timeout(DEADLINE).unwrap();
        asked.recv_timeout(DEADLINE).unwrap();
        clock.advance(Duration::from_nanos(1));
        // A pass under way may have read the clock before this advance, so the
        // pass after it is the last.
        for _ in 0..2 {
            let _ = asked.recv_timeout(DEADLINE);
        }
        match run.done.recv_timeout(DEADLINE) {
            Ok(Err(BoundError::Elapsed)) => {}
            other => panic!("expected the bound to pass, got {other:?}"),
        }
    }

    #[test]
    fn a_rejection_ends_the_work_before_the_bound() {
        let io = current_thread();
        let clock = TestClock::frozen();
        let bound = on(&clock, LONG).with_rejection(|| Some("refused".to_owned()));
        let run = hanging(io.handle().clone(), bound);
        match run.done.recv_timeout(DEADLINE) {
            Ok(Err(BoundError::Rejected(text))) => assert_eq!(text, "refused"),
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    /// A limit of zero has passed by the first read, so only the order of the
    /// two checks decides this.
    #[test]
    fn a_rejection_recorded_by_the_bound_wins() {
        let io = current_thread();
        let clock = TestClock::frozen();
        let bound = on(&clock, Duration::ZERO).with_rejection(|| Some("refused".to_owned()));
        let run = hanging(io.handle().clone(), bound);
        match run.done.recv_timeout(DEADLINE) {
            Ok(Err(BoundError::Rejected(text))) => assert_eq!(text, "refused"),
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    /// Pins the output, and that the run ends on a clock that never moves, so
    /// the timing thread ended with the work.
    #[test]
    fn work_that_finishes_returns_its_output() {
        let clock = TestClock::frozen();
        let bound = on(&clock, LONG);
        let done = on_thread(move || current_thread().block_on(bound.run(async { 7 })));
        match done.recv_timeout(DEADLINE) {
            Ok(Ok(output)) => assert_eq!(output, 7),
            other => panic!("expected the output, got {other:?}"),
        }
    }

    /// Pins that a limit past the clock's range builds and never fires, while
    /// the check still runs.
    #[test]
    fn a_limit_past_the_clock_range_never_fires() {
        let clock = TestClock::frozen();
        let (bound, asked) = paced(on(&clock, Duration::MAX));
        let (release, released) = oneshot::channel::<()>();
        let done = on_thread(move || current_thread().block_on(bound.run(released)));
        for _ in 0..3 {
            asked.recv_timeout(DEADLINE).unwrap();
        }
        // Frees a thread waiting to hand over, so the run's join returns.
        drop(asked);
        release.send(()).unwrap();
        match done.recv_timeout(DEADLINE) {
            Ok(Ok(Ok(()))) => {}
            other => panic!("expected the output, got {other:?}"),
        }
    }

    #[test]
    fn debug_names_the_bound() {
        let bound = ThreadBound::new("test-bound", LONG);
        assert_eq!(
            format!("{bound:?}"),
            r#"ThreadBound { name: "test-bound", limit: 600s, .. }"#
        );
    }

    /// A panic drops the channel unsent, the only signal left to end the run.
    #[test]
    fn a_panicking_check_ends_the_work_as_elapsed() {
        let io = current_thread();
        let clock = TestClock::frozen();
        let bound = on(&clock, LONG).with_rejection(|| panic!("the check panicked"));
        let run = hanging(io.handle().clone(), bound);
        match run.done.recv_timeout(DEADLINE) {
            Ok(Err(BoundError::Elapsed)) => {}
            other => panic!("expected the bound to pass, got {other:?}"),
        }
    }

    /// The work ends in the check, so the poll the rejection wakes finds both
    /// ready.
    #[test]
    fn work_finished_when_the_bound_fires_returns_its_output() {
        let clock = TestClock::frozen();
        let finished = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&finished);
        let bound = on(&clock, LONG).with_rejection(move || {
            seen.store(true, Ordering::SeqCst);
            Some("refused".to_owned())
        });
        let work = poll_fn(move |_| {
            if finished.load(Ordering::SeqCst) {
                Poll::Ready(7)
            } else {
                Poll::Pending
            }
        });
        let done = on_thread(move || current_thread().block_on(bound.run(work)));
        match done.recv_timeout(DEADLINE) {
            Ok(Ok(output)) => assert_eq!(output, 7),
            other => panic!("expected the output, got {other:?}"),
        }
    }

    /// Advancing between the call to `run` and its first poll leaves the
    /// whole limit to run.
    #[test]
    fn the_limit_counts_from_the_first_poll() {
        let clock = TestClock::frozen();
        let (bound, asked) = paced(on(&clock, LONG));
        let (release, released) = oneshot::channel::<()>();
        let run = bound.run(released);
        clock.advance(LONG);
        let done = on_thread(move || current_thread().block_on(run));
        for _ in 0..2 {
            asked.recv_timeout(DEADLINE).unwrap();
        }
        // Frees a thread waiting to hand over, so the run's join returns.
        drop(asked);
        release.send(()).unwrap();
        match done.recv_timeout(DEADLINE) {
            Ok(Ok(Ok(()))) => {}
            other => panic!("expected the output, got {other:?}"),
        }
    }

    /// Each poll of the work finds a ready channel and spends the coop budget
    /// before the bound is read.
    #[test]
    fn the_bound_fires_over_work_that_spends_its_whole_budget() {
        let clock = TestClock::frozen();
        let bound = on(&clock, Duration::ZERO);
        let done = on_thread(move || {
            current_thread().block_on(bound.run(async {
                let (tx, mut rx) = unbounded_channel();
                loop {
                    tx.send(()).unwrap();
                    rx.recv().await;
                }
            }))
        });
        match done.recv_timeout(DEADLINE) {
            Ok(Err(BoundError::Elapsed)) => {}
            other => panic!("expected the bound to pass, got {:?}", other.map(|_| ())),
        }
    }

    /// Pins that dropping a run ends its thread: the drop joins it, on a clock
    /// that never moves.
    #[test]
    fn a_dropped_run_ends_its_thread() {
        let clock = TestClock::frozen();
        let bound = on(&clock, LONG);
        let done = on_thread(move || {
            current_thread().block_on(async move {
                tokio::select! {
                    biased;
                    _ = bound.run(std::future::pending::<()>()) => unreachable!("the work hangs"),
                    () = std::future::ready(()) => {}
                }
            });
        });
        done.recv_timeout(DEADLINE).unwrap();
    }
}
