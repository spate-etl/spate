//! Assertions over what a component logged.
//!
//! [`LogCapture`] is a writer a `tracing` subscriber can be pointed at, and
//! waits on what it has captured. [`capture_logs`] and [`show_logs`] install
//! one around a call, scoped to the calling thread.

use crate::run::poll_until;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::Level;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::fmt::{MakeWriter, TestWriter};

/// Everything the subscriber holding it has formatted.
#[derive(Clone, Debug, Default)]
pub struct LogCapture(Arc<Mutex<Vec<u8>>>);

impl LogCapture {
    /// An empty capture.
    pub fn new() -> LogCapture {
        LogCapture::default()
    }

    /// The lines formatted so far.
    ///
    /// Panics if a subscriber panicked mid-write and poisoned the buffer.
    pub fn lines(&self) -> Vec<String> {
        String::from_utf8_lossy(&self.0.lock().expect("capture"))
            .lines()
            .map(str::to_string)
            .collect()
    }

    /// Poll `check` until it returns a value, on the cadence of
    /// [`wait_until`](crate::wait_until).
    ///
    /// Panics once `timeout` elapses, naming `what` and carrying every line
    /// captured so far.
    pub fn wait_for<T>(
        &self,
        timeout: Duration,
        what: &str,
        check: impl FnMut() -> Option<T>,
    ) -> T {
        poll_until(timeout, check).unwrap_or_else(|| {
            panic!(
                "timed out after {timeout:?} waiting for: {what}\n--- captured ---\n{}",
                self.lines().join("\n")
            )
        })
    }

    /// The first captured line containing `needle`, waiting up to `timeout`
    /// for one to arrive.
    ///
    /// Panics on timeout as [`wait_for`](Self::wait_for) does.
    pub fn wait_for_line(&self, timeout: Duration, needle: &str) -> String {
        self.wait_for(timeout, &format!("a line containing {needle:?}"), || {
            self.lines().into_iter().find(|l| l.contains(needle))
        })
    }
}

impl std::io::Write for LogCapture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("capture").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for LogCapture {
    type Writer = LogCapture;

    fn make_writer(&'a self) -> LogCapture {
        self.clone()
    }
}

/// Run `f` under a subscriber at `level` and return the lines it formatted.
///
/// The lines reach the test harness as well, so a panic inside `f` carries the
/// log around it. The subscriber is scoped to the calling thread, because
/// `cargo test` shares one process across a binary.
pub fn capture_logs(level: Level, f: impl FnOnce()) -> Vec<String> {
    let capture = LogCapture::new();
    under_subscriber(level, capture.clone().and(TestWriter::default()), f);
    capture.lines()
}

/// Run `f` under a subscriber at `level` writing to the test harness.
///
/// The harness prints those lines when the test fails, so a panic inside `f`
/// carries the log around it. The subscriber is scoped to the calling thread,
/// because `cargo test` shares one process across a binary.
pub fn show_logs(level: Level, f: impl FnOnce()) {
    under_subscriber(level, TestWriter::default(), f);
}

fn under_subscriber<W>(level: Level, writer: W, f: impl FnOnce())
where
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    let subscriber = tracing_subscriber::fmt()
        .with_writer(writer)
        .with_max_level(level)
        .without_time()
        .finish();
    tracing::subscriber::with_default(subscriber, f);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn wait_for_line_returns_the_matching_line() {
        let mut capture = LogCapture::new();
        capture.write_all(b"starting\nready at 1\n").unwrap();
        assert_eq!(
            capture.wait_for_line(Duration::from_secs(1), "ready"),
            "ready at 1"
        );
    }

    /// A timeout's panic carries every line captured so far.
    #[test]
    #[should_panic(expected = "--- captured ---\nstarting")]
    fn a_timeout_panics_with_the_capture() {
        let mut capture = LogCapture::new();
        capture.write_all(b"starting\n").unwrap();
        capture.wait_for_line(Duration::from_millis(20), "ready");
    }
}
