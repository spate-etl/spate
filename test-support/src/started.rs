//! Waiting for a started container's service to answer, and the report a
//! failed start writes: how the container ended and the tail of its output.

use std::fmt;
use std::time::{Duration, Instant};

use testcontainers::core::ContainerPort;
use testcontainers::{Container, GenericImage};

/// How often a failed probe asks Docker whether the container still runs.
const LIVENESS_EVERY: Duration = Duration::from_secs(1);

/// The lines of each container stream a failed start writes. A service's
/// reason for stopping is the last thing it writes.
const LOG_TAIL: usize = 200;

/// Waits until `probe` succeeds against `container`'s service, named `what`.
///
/// Between failed probes it asks Docker, at most once a second, whether the
/// container still runs. It fails once Docker reports the container exited,
/// or once `deadline` passes, after writing the tail of the container's
/// output to stderr.
pub(crate) fn ready(
    what: &str,
    deadline: Duration,
    probe: impl FnMut() -> Result<(), String>,
    container: &Container<GenericImage>,
) -> Result<(), String> {
    wait(what, deadline, probe, || match container.is_running() {
        Ok(false) => Some(container.exit_code().ok().flatten()),
        // Docker not answering leaves the deadline to decide.
        _ => None,
    })
    .map_err(|failure| report(what, failure, container))
}

/// The host port `container` maps `port` to, named `name` in the error.
///
/// A failed lookup writes the tail of the container's output to stderr, as
/// [`ready`] does.
pub(crate) fn host_port(
    what: &str,
    container: &Container<GenericImage>,
    port: impl Into<ContainerPort>,
    name: &str,
) -> Result<u16, String> {
    container
        .get_host_port_ipv4(port)
        .map_err(|e| report(what, format!("{what} {name}: {e}"), container))
}

/// The polling loop of [`ready`]: `exited` gives the container's exit code
/// once Docker reports it exited, `Some(None)` when Docker gave no code.
fn wait(
    what: &str,
    deadline: Duration,
    mut probe: impl FnMut() -> Result<(), String>,
    mut exited: impl FnMut() -> Option<Option<i64>>,
) -> Result<(), String> {
    let until = Instant::now() + deadline;
    let mut asked: Option<Instant> = None;
    loop {
        let failure = match probe() {
            Ok(()) => return Ok(()),
            Err(failure) => failure,
        };
        let now = Instant::now();
        if now >= until {
            return Err(format!(
                "{what} was not ready within {deadline:?}: {failure}"
            ));
        }
        if asked.is_none_or(|at| now.duration_since(at) >= LIVENESS_EVERY) {
            asked = Some(now);
            match exited() {
                Some(Some(code)) => {
                    return Err(format!(
                        "{what} exited with code {code} before it was ready: {failure}"
                    ));
                }
                Some(None) => return Err(format!("{what} exited before it was ready: {failure}")),
                None => {}
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Writes `failure` and the tail of `container`'s output to stderr, and
/// returns `failure`.
fn report(what: &str, failure: String, container: &Container<GenericImage>) -> String {
    reported(
        what,
        failure,
        container.stdout_to_vec(),
        container.stderr_to_vec(),
    )
}

/// Writes `failure` and the tail of a container's `stdout` and `stderr` to
/// stderr, where the test's captured output keeps its lines, and returns
/// `failure`.
#[expect(
    clippy::print_stderr,
    reason = "a failed start's container output is read from the test's output"
)]
fn reported<E: fmt::Display>(
    what: &str,
    failure: String,
    stdout: Result<Vec<u8>, E>,
    stderr: Result<Vec<u8>, E>,
) -> String {
    eprintln!(
        "{failure}\n{}",
        container_tails(what, &container_output(stdout), &container_output(stderr))
    );
    failure
}

/// A container stream as text, or why it could not be read.
pub(crate) fn container_output<E: fmt::Display>(read: Result<Vec<u8>, E>) -> String {
    match read {
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(e) => format!("(unreadable: {e})"),
    }
}

/// The last [`LOG_TAIL`] lines of a container's stdout and stderr, each
/// under a heading.
pub(crate) fn container_tails(what: &str, stdout: &str, stderr: &str) -> String {
    format!(
        "---- last {LOG_TAIL} lines of {what} stdout ----\n{}\n\
         ---- last {LOG_TAIL} lines of {what} stderr ----\n{}",
        tail(stdout),
        tail(stderr),
    )
}

/// The last [`LOG_TAIL`] lines of `log`.
fn tail(log: &str) -> String {
    let lines: Vec<&str> = log.lines().collect();
    lines[lines.len().saturating_sub(LOG_TAIL)..].join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A probe that never succeeds, with the error a refused connection gives.
    fn refused() -> Result<(), String> {
        Err("connection refused".to_owned())
    }

    /// A service that answers is ready, and Docker is not asked.
    #[test]
    fn an_answering_service_is_ready_without_asking_docker() {
        let mut asked = 0;
        let ready = wait(
            "Store",
            Duration::from_secs(60),
            || Ok(()),
            || {
                asked += 1;
                None
            },
        );
        assert_eq!(ready, Ok(()));
        assert_eq!(asked, 0);
    }

    /// A container Docker reports exited fails the wait with its exit code,
    /// or without one when Docker gave none, and keeps the last probe's error.
    #[test]
    fn an_exited_container_fails_the_wait_with_its_exit_code() {
        let deadline = Duration::from_secs(60);
        assert_eq!(
            wait("Ceph RGW", deadline, refused, || Some(Some(250))),
            Err("Ceph RGW exited with code 250 before it was ready: connection refused".to_owned())
        );
        assert_eq!(
            wait("Ceph RGW", deadline, refused, || Some(None)),
            Err("Ceph RGW exited before it was ready: connection refused".to_owned())
        );
    }

    /// A container still running when the deadline passes fails the wait as
    /// not ready.
    #[test]
    fn a_running_container_past_its_deadline_was_not_ready() {
        assert_eq!(
            wait("ElasticMQ", Duration::ZERO, refused, || None),
            Err("ElasticMQ was not ready within 0ns: connection refused".to_owned())
        );
    }

    /// A probe that fails before the container exits is retried until Docker
    /// reports the exit.
    #[test]
    fn a_failed_probe_is_retried_until_the_container_exits() {
        let mut probes = 0;
        let mut asked = 0;
        let failure = wait(
            "RustFS",
            Duration::from_secs(60),
            || {
                probes += 1;
                refused()
            },
            || {
                asked += 1;
                (asked > 1).then_some(Some(1))
            },
        );
        assert_eq!(
            failure,
            Err("RustFS exited with code 1 before it was ready: connection refused".to_owned())
        );
        assert_eq!(asked, 2);
        assert!(probes >= 2, "{probes} probes");
    }

    /// A failed start's report returns the failure it was given.
    #[test]
    fn a_report_returns_its_failure() {
        let read: Result<Vec<u8>, String> = Ok(b"err line".to_vec());
        assert_eq!(
            reported("Ceph RGW", "failure".to_owned(), Ok(Vec::new()), read),
            "failure"
        );
    }

    /// A container stream reads as its text, or as why it could not be read.
    #[test]
    fn a_container_stream_reads_as_text_or_its_error() {
        let read: Result<Vec<u8>, String> = Ok(b"a line".to_vec());
        assert_eq!(container_output(read), "a line");
        let unread: Result<Vec<u8>, String> = Err("no such container".to_owned());
        assert_eq!(container_output(unread), "(unreadable: no such container)");
    }

    /// The report after a failed start carries each stream under its heading.
    #[test]
    fn the_container_tails_carry_both_streams() {
        assert_eq!(
            container_tails("Garage", "out line", "err line"),
            "---- last 200 lines of Garage stdout ----\nout line\n\
             ---- last 200 lines of Garage stderr ----\nerr line"
        );
    }

    /// A container log longer than the tail keeps only its last lines.
    #[test]
    fn a_long_log_keeps_its_last_lines() {
        let log: Vec<String> = (0..=LOG_TAIL).map(|n| n.to_string()).collect();
        let kept = tail(&log.join("\n"));
        assert_eq!(kept, log[1..].join("\n"));
        assert_eq!(tail("one\ntwo"), "one\ntwo");
    }
}
