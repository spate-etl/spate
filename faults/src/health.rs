//! The container health poller: while workers run, it checks each container
//! once a second over a connection that crosses no injected fault, and writes
//! each poll to `health.ndjson`.

use std::fs::File;
use std::io::{self, Write as _};
use std::path::Path;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

use crate::journal::now_ms;
use crate::outcome::HealthPoll;

/// One container the poller checks.
pub struct Target<'a> {
    /// Container name in each poll.
    pub container: &'a str,
    /// Whether Docker reports the container running.
    pub running: Box<dyn Fn() -> Result<bool, String> + Sync + 'a>,
    /// A request to the service inside it.
    pub reach: Box<dyn Fn() -> Result<(), String> + Sync + 'a>,
}

impl std::fmt::Debug for Target<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Target")
            .field("container", &self.container)
            .finish_non_exhaustive()
    }
}

impl Target<'_> {
    /// Polls the container: it is up when Docker reports it running and it
    /// answers.
    #[must_use]
    pub fn poll(&self) -> HealthPoll {
        let result = match (self.running)() {
            Ok(true) => (self.reach)(),
            Ok(false) => Err("not running".to_owned()),
            Err(e) => Err(format!("docker: {e}")),
        };
        HealthPoll {
            t_ms: now_ms(),
            container: self.container.to_owned(),
            ok: result.is_ok(),
            error: result.err(),
        }
    }
}

/// Polls every target each `interval` until `stop` receives or disconnects,
/// appending each poll to `path` as one JSON line, and returns the polls.
///
/// # Errors
///
/// Fails when `path` cannot be created or written.
pub fn watch(
    targets: &[Target<'_>],
    path: &Path,
    interval: Duration,
    stop: &Receiver<()>,
) -> io::Result<Vec<HealthPoll>> {
    let mut file = File::create(path)?;
    let mut polls = Vec::new();
    loop {
        for target in targets {
            let poll = target.poll();
            let mut line = serde_json::to_vec(&poll).map_err(io::Error::other)?;
            line.push(b'\n');
            file.write_all(&line)?;
            polls.push(poll);
        }
        match stop.recv_timeout(interval) {
            Err(RecvTimeoutError::Timeout) => {}
            Ok(()) | Err(RecvTimeoutError::Disconnected) => return Ok(polls),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn target<'a>(
        container: &'a str,
        running: Result<bool, String>,
        reach: Result<(), String>,
        reached: &'a AtomicBool,
    ) -> Target<'a> {
        Target {
            container,
            running: Box::new(move || running.clone()),
            reach: Box::new(move || {
                reached.store(true, Ordering::SeqCst);
                reach.clone()
            }),
        }
    }

    /// A container Docker reports running is down when its service does not
    /// answer, and a stopped one is down without being asked.
    #[test]
    fn a_running_container_that_does_not_answer_is_down() {
        let reached = AtomicBool::new(false);
        let poll = target("nats", Ok(true), Err("timed out".to_owned()), &reached).poll();
        assert_eq!((poll.ok, poll.error.as_deref()), (false, Some("timed out")));
        assert!(target("nats", Ok(true), Ok(()), &reached).poll().ok);

        let skipped = AtomicBool::new(false);
        let poll = target("nats", Ok(false), Ok(()), &skipped).poll();
        assert_eq!(
            (poll.ok, poll.error.as_deref()),
            (false, Some("not running"))
        );
        assert!(!skipped.load(Ordering::SeqCst));
    }

    /// The poller writes one line per container per round until stopped, and
    /// returns the polls it wrote.
    #[test]
    fn watch_writes_each_poll_until_stopped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("health.ndjson");
        let (a, b) = (AtomicBool::new(false), AtomicBool::new(false));
        let targets = [
            target("nats", Ok(true), Ok(()), &a),
            target("seaweedfs", Ok(true), Err("refused".to_owned()), &b),
        ];
        let polls = std::thread::scope(|s| {
            // Inside the scope, so a failed wait drops `tx` and the poller ends.
            let (tx, rx) = std::sync::mpsc::channel();
            let (targets, path) = (&targets, &path);
            let poller = s.spawn(move || watch(targets, path, Duration::from_millis(5), &rx));
            spate_test::wait_until(Duration::from_secs(10), "four health lines", || {
                std::fs::read_to_string(path).is_ok_and(|t| t.lines().count() >= 4)
            });
            tx.send(()).unwrap();
            poller.join().unwrap().unwrap()
        });
        let written: Vec<HealthPoll> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(written, polls);
        assert!(polls.len() >= 4 && polls.len() % 2 == 0);
        assert!(polls.chunks(2).all(|p| p[0].container == "nats"
            && p[0].ok
            && p[1].container == "seaweedfs"
            && !p[1].ok));
    }
}
