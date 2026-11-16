//! Waiting for a started container's service to answer, and the report a
//! failed start writes: how the container ended and the tail of its output.

#[cfg(feature = "container")]
use std::fmt;
#[cfg(feature = "container")]
use std::future::Future;
use std::time::{Duration, Instant};

#[cfg(any(feature = "s3", feature = "elasticmq"))]
use testcontainers::core::ContainerPort;
#[cfg(feature = "container")]
use testcontainers::{Container, ContainerAsync, GenericImage, Image};

#[cfg(feature = "container")]
use crate::container_log::{container_output, container_tails, exit_clause};

/// How often a failed probe asks Docker whether the container still runs.
const LIVENESS_EVERY: Duration = Duration::from_secs(1);

/// The pause between two probes.
const PROBE_EVERY: Duration = Duration::from_millis(50);

/// Waits until `probe` succeeds against `container`'s service, named `what`.
///
/// Between failed probes it asks Docker, at most once a second, whether the
/// container still runs. It sleeps the calling thread between probes.
///
/// # Errors
///
/// Fails once Docker reports the container exited, or once `deadline`
/// passes, with the last probe's error. Before it returns the error it
/// writes it to stderr with the last 200 lines of the container's stdout and
/// stderr.
#[cfg(feature = "container")]
pub fn ready(
    what: &str,
    deadline: Duration,
    probe: impl FnMut() -> Result<(), String>,
    container: &Container<GenericImage>,
) -> Result<(), String> {
    wait(what, deadline, probe, || match container.is_running() {
        Ok(false) => Some(exit_clause(container.exit_code().ok().flatten())),
        // Docker not answering leaves the deadline to decide.
        _ => None,
    })
    .map_err(|failure| report(what, failure, container))
}

/// [`ready`] for a container started through testcontainers' async runner,
/// with a probe that returns a future. It yields to the runtime between
/// probes.
///
/// # Errors
///
/// As [`ready`].
#[cfg(feature = "container")]
pub async fn ready_async<I, F, Fut>(
    what: &str,
    deadline: Duration,
    mut probe: F,
    container: &ContainerAsync<I>,
) -> Result<(), String>
where
    I: Image,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let mut schedule = Schedule::new(what, deadline, Instant::now());
    loop {
        let failure = match probe().await {
            Ok(()) => return Ok(()),
            Err(failure) => failure,
        };
        let failed = match schedule.after_failure(&failure, Instant::now()) {
            Err(failed) => Some(failed),
            Ok(true) => match container.is_running().await {
                Ok(false) => {
                    let code = container.exit_code().await.ok().flatten();
                    Some(schedule.ended(&exit_clause(code), &failure))
                }
                // Docker not answering leaves the deadline to decide.
                _ => None,
            },
            Ok(false) => None,
        };
        if let Some(failed) = failed {
            return Err(reported(
                what,
                failed,
                container.stdout_to_vec().await,
                container.stderr_to_vec().await,
            ));
        }
        tokio::time::sleep(PROBE_EVERY).await;
    }
}

/// The host port `container` maps `port` to, named `name` in the error.
///
/// A failed lookup writes the tail of the container's output to stderr, as
/// [`ready`] does.
#[cfg(any(feature = "s3", feature = "elasticmq"))]
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

/// Waits until `probe` succeeds against the service named `what`.
///
/// Between failed probes it calls `ended`, at most once a second, which says
/// how the service's container ended once it has, such as `exited with code
/// 1`. It fails then, or once `deadline` passes.
pub(crate) fn wait(
    what: &str,
    deadline: Duration,
    mut probe: impl FnMut() -> Result<(), String>,
    mut ended: impl FnMut() -> Option<String>,
) -> Result<(), String> {
    let mut schedule = Schedule::new(what, deadline, Instant::now());
    loop {
        let failure = match probe() {
            Ok(()) => return Ok(()),
            Err(failure) => failure,
        };
        if schedule.after_failure(&failure, Instant::now())?
            && let Some(ended) = ended()
        {
            return Err(schedule.ended(&ended, &failure));
        }
        std::thread::sleep(PROBE_EVERY);
    }
}

/// When one wait gives up, and when it next asks whether its container still
/// runs. The caller reads the clock, so the sync and async waits share it.
struct Schedule<'a> {
    what: &'a str,
    deadline: Duration,
    until: Instant,
    asked: Option<Instant>,
}

impl<'a> Schedule<'a> {
    /// A wait for `what` that started at `now`.
    fn new(what: &'a str, deadline: Duration, now: Instant) -> Self {
        Self {
            what,
            deadline,
            until: now + deadline,
            asked: None,
        }
    }

    /// After a probe failed at `now` with `failure`: the error once the
    /// deadline has passed, otherwise whether the container is due to be
    /// asked whether it still runs. The first failure is always due.
    fn after_failure(&mut self, failure: &str, now: Instant) -> Result<bool, String> {
        if now >= self.until {
            return Err(format!(
                "{} was not ready within {:?}: {failure}",
                self.what, self.deadline
            ));
        }
        let due = self
            .asked
            .is_none_or(|at| now.duration_since(at) >= LIVENESS_EVERY);
        if due {
            self.asked = Some(now);
        }
        Ok(due)
    }

    /// The error once the container has ended, where `ended` says how.
    fn ended(&self, ended: &str, failure: &str) -> String {
        format!("{} {ended} before it was ready: {failure}", self.what)
    }
}

/// Writes `failure` and the tail of `container`'s output to stderr, and
/// returns `failure`.
#[cfg(feature = "container")]
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
#[cfg(feature = "container")]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::container_log::exit_clause;

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
            wait("Ceph RGW", deadline, refused, || Some(exit_clause(Some(
                250
            )))),
            Err("Ceph RGW exited with code 250 before it was ready: connection refused".to_owned())
        );
        assert_eq!(
            wait("Ceph RGW", deadline, refused, || Some(exit_clause(None))),
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
                (asked > 1).then(|| exit_clause(Some(1)))
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
    #[cfg(feature = "container")]
    #[test]
    fn a_report_returns_its_failure() {
        let read: Result<Vec<u8>, String> = Ok(b"err line".to_vec());
        assert_eq!(
            reported("Ceph RGW", "failure".to_owned(), Ok(Vec::new()), read),
            "failure"
        );
    }

    /// A wait's first failed probe asks whether the container still runs.
    #[test]
    fn the_first_failure_asks_at_once() {
        let start = Instant::now();
        let mut schedule = Schedule::new("ClickHouse", Duration::from_secs(60), start);
        assert_eq!(schedule.after_failure("refused", start), Ok(true));
    }

    /// After an ask, the next is due once [`LIVENESS_EVERY`] has passed and
    /// not before.
    #[test]
    fn the_next_ask_waits_for_the_liveness_interval() {
        let start = Instant::now();
        let mut schedule = Schedule::new("ClickHouse", Duration::from_secs(60), start);
        assert_eq!(schedule.after_failure("refused", start), Ok(true));
        let early = start + LIVENESS_EVERY - Duration::from_millis(1);
        assert_eq!(schedule.after_failure("refused", early), Ok(false));
        let due = start + LIVENESS_EVERY;
        assert_eq!(schedule.after_failure("refused", due), Ok(true));
        assert_eq!(schedule.after_failure("refused", due), Ok(false));
    }

    /// Once the deadline has passed the wait fails as not ready, even when
    /// an ask is due.
    #[test]
    fn the_deadline_wins_over_a_due_ask() {
        let start = Instant::now();
        let mut schedule = Schedule::new("ClickHouse", LIVENESS_EVERY, start);
        assert_eq!(schedule.after_failure("refused", start), Ok(true));
        assert_eq!(
            schedule.after_failure("refused", start + LIVENESS_EVERY),
            Err("ClickHouse was not ready within 1s: refused".to_owned())
        );
    }

    /// An ended container's error names how it ended and the last probe's
    /// error.
    #[test]
    fn an_ended_container_names_how_it_ended() {
        let schedule = Schedule::new("ClickHouse", LIVENESS_EVERY, Instant::now());
        assert_eq!(
            schedule.ended("exited with code 232", "refused"),
            "ClickHouse exited with code 232 before it was ready: refused"
        );
    }
}
