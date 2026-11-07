//! Blocking test-wait helpers: bounded pipeline-exit waits and a generic
//! condition poll. Whole-pipeline tests use these in place of hand-rolled
//! `deadline + thread::sleep` busy-loops.

use spate_core::pipeline::{ExitReport, StartError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// A pipeline running on its own OS thread, with its exit result delivered
/// over a channel so tests can wait with a bounded [`recv_timeout`].
///
/// [`recv_timeout`]: crossbeam_channel::Receiver::recv_timeout
#[derive(Debug)]
pub struct PipelineRun {
    rx: crossbeam_channel::Receiver<Result<ExitReport, StartError>>,
    join: JoinHandle<()>,
}

impl PipelineRun {
    /// Spawn `run` (typically `move || runtime.run()`) on a new thread; its
    /// result is sent over the channel when it returns.
    pub fn spawn(run: impl FnOnce() -> Result<ExitReport, StartError> + Send + 'static) -> Self {
        let (tx, rx) = crossbeam_channel::bounded(1);
        #[expect(
            clippy::disallowed_methods,
            reason = "the thread ends when `run` returns; `PipelineRun::join` joins it, and dropping a run never blocks"
        )]
        let join = std::thread::spawn(move || {
            // The receiver may already be gone (test dropped early); that's fine.
            let _ = tx.send(run());
        });
        Self { rx, join }
    }

    /// Block until the pipeline exits or `timeout` elapses; `None` on timeout.
    ///
    /// Use this to wait for a pipeline that stops on its own (e.g. a
    /// `Fail`-policy fatal) without a manual shutdown trigger.
    pub fn wait_exit(&self, timeout: Duration) -> Option<Result<ExitReport, StartError>> {
        self.rx.recv_timeout(timeout).ok()
    }

    /// Block until the pipeline exits and join its thread, returning the run
    /// result. Panics if the thread panicked.
    pub fn join(self) -> Result<ExitReport, StartError> {
        let report = self.rx.recv().expect("pipeline thread dropped its result");
        self.join.join().expect("pipeline thread panicked");
        report
    }
}

/// How often [`wait_until`] re-checks its predicate. Small enough that the
/// cadence is not itself the latency a test measures, coarse enough not to spin
/// a core.
const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// The bound to pass a wait for a condition a working test always reaches:
/// one minute, far above the time any such condition takes on a loaded
/// machine, so a wait that reaches it means the test hung.
// Under the 120 s kill in this repository's `.config/nextest.toml`, so a hung
// wait fails with its own message, naming the condition.
pub const HANG: Duration = Duration::from_secs(60);

/// Poll `check` until it returns `true`; panics naming `what` if `timeout`
/// passes first.
///
/// `timeout` catches a hang and never decides a result: pass [`HANG`], or a
/// bound as far above the latency a loaded machine shows. A test of how long
/// something takes runs on a test clock or paused tokio time instead. A
/// `timeout` past what [`Instant`] can represent waits as long as `check`
/// takes.
///
/// # Panics
///
/// When `timeout` passes before `check` returns `true`, saying the test hung
/// waiting for `what`.
///
/// # Examples
///
/// ```
/// let mut polls = 0;
/// spate_test::wait_until(spate_test::HANG, "the third poll", || {
///     polls += 1;
///     polls == 3
/// });
/// ```
pub fn wait_until(timeout: Duration, what: &str, mut check: impl FnMut() -> bool) {
    if poll_until(timeout, || check().then_some(())).is_none() {
        panic!("{}", hung(timeout, what));
    }
}

/// The panic message of a wait that reached its bound.
pub(crate) fn hung(timeout: Duration, what: &str) -> String {
    format!("hung after {timeout:?} waiting for: {what}")
}

/// Poll `check` until it returns a value, or `None` once `timeout` elapses.
/// `check` runs at least once.
pub(crate) fn poll_until<T>(timeout: Duration, mut check: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = deadline(timeout);
    loop {
        if let Some(value) = check() {
            return Some(value);
        }
        left(deadline)?;
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// The instant `timeout` from now, or `None` for no deadline when it lies past
/// what [`Instant`] can represent.
pub(crate) fn deadline(timeout: Duration) -> Option<Instant> {
    Instant::now().checked_add(timeout)
}

/// What is left of `deadline`: `None` once it has passed, and
/// [`Duration::MAX`] when there is none.
pub(crate) fn left(deadline: Option<Instant>) -> Option<Duration> {
    match deadline {
        None => Some(Duration::MAX),
        Some(at) => at
            .checked_duration_since(Instant::now())
            .filter(|left| !left.is_zero()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A bound past what `Instant` can represent waits for the condition.
    /// Regression for #1844.
    #[test]
    fn wait_until_takes_a_bound_past_the_clock_range() {
        wait_until(Duration::MAX, "a condition that holds", || true);
    }

    /// A bound past the clock's range is no deadline, and has no end; a
    /// deadline that has passed has nothing left.
    #[test]
    fn no_deadline_never_ends_and_a_passed_one_has_nothing_left() {
        assert_eq!(deadline(Duration::MAX), None);
        assert_eq!(left(None), Some(Duration::MAX));
        assert_eq!(left(deadline(Duration::ZERO)), None);
        assert!(left(deadline(HANG)).is_some_and(|left| left <= HANG));
    }

    /// A condition that already holds passes on a zero bound.
    #[test]
    fn a_condition_that_holds_passes_on_a_zero_bound() {
        wait_until(Duration::ZERO, "a condition that holds", || true);
    }

    /// A wait that reaches its bound says the test hung, naming the condition.
    #[test]
    #[should_panic(expected = "hung after 0ns waiting for: the flag")]
    fn a_wait_that_reaches_its_bound_says_the_test_hung() {
        wait_until(Duration::ZERO, "the flag", || false);
    }
}
