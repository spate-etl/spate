//! Blocking test-wait helpers: bounded pipeline-exit waits and a generic
//! condition poll, so whole-pipeline tests don't hand-roll
//! `deadline + thread::sleep` busy-loops.

use spate_core::pipeline::{ExitReport, StartError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// A pipeline running on its own OS thread, with its exit result delivered
/// over a channel so tests can wait with a bounded [`recv_timeout`] instead of
/// polling [`JoinHandle::is_finished`].
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
/// cadence is not itself the latency a test measures — at the old 250ms every
/// wait paid up to a quarter second of pure sleep, and the suites use dozens of
/// them — but coarse enough not to spin a core.
const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Poll `check` until it returns `true` or `timeout` elapses; panics with `what`
/// on timeout.
pub fn wait_until(timeout: Duration, what: &str, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check() {
            return;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    panic!("timed out after {timeout:?} waiting for: {what}");
}
