//! The outcome a fault scenario reports, and the rules that pick its kind.

use serde::{Deserialize, Serialize};

/// How a scenario ended. Every kind but [`Kind::Pass`] fails the scenario.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// Every check held.
    Pass,
    /// The oracle found a delivery-contract violation.
    Violation,
    /// A worker failed while the oracle found nothing and every container
    /// stayed healthy.
    Worker,
    /// The scenario's own assertion failed.
    Expectation,
    /// Infrastructure failed: setup, a journal write, or a container outage.
    Harness,
}

impl Kind {
    /// The prefix a scenario's result message starts with.
    #[must_use]
    pub fn panic_prefix(self) -> &'static str {
        match self {
            Kind::Pass => "fault-run passed:",
            Kind::Violation => "fault-run violation:",
            Kind::Worker => "fault-run worker failure:",
            Kind::Expectation => "fault-run expectation failed:",
            Kind::Harness => "fault-run harness error:",
        }
    }
}

/// The stage a scenario had reached when it ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    /// Starting containers and creating the store.
    Setup,
    /// Workers running under the fault schedule.
    Running,
    /// Judging the journals and the final sweep.
    Oracle,
}

/// The delivery check a [`Violation`] failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Check {
    /// A generated record reached no sink.
    RecordMissing,
    /// A sink wrote a record the harness never generated.
    RecordUnknown,
    /// A record arrived twice with no fault or replay to explain it.
    UnexplainedDuplicate,
    /// A landed value moved the watermark past rows its sender had not written,
    /// completed below a record of its split, or has no journalled sender.
    AheadOfRows,
    /// A split completed more than once.
    CompletedTwice,
    /// A split was quarantined.
    Quarantined,
    /// A split never completed.
    Unfinished,
    /// A generated record lies in no split.
    RecordInNoSplit,
    /// A generated record lies in two splits.
    RecordInTwoSplits,
    /// A polled store holds no verdict.
    VerdictMissing,
    /// A stopped worker's split was not claimed by its peer within the cap.
    StoppedSplitNotReassigned,
    /// No other instance took the leader key within the cap after the
    /// leader was killed.
    LeaderNotReplaced,
    /// A value at a higher revision carries a lower epoch.
    EpochRegressed,
    /// Two owners hold one epoch.
    TwoOwners,
    /// A watermark moved under an epoch other than the highest already seen.
    StaleEpochCommit,
    /// Two observations of one revision of a key hold different values.
    ValueConflict,
}

impl Check {
    /// The delivery property, numbered 1 to 5, this check belongs to.
    #[must_use]
    pub fn property(self) -> u8 {
        match self {
            Check::RecordMissing | Check::RecordUnknown => 1,
            Check::UnexplainedDuplicate => 2,
            Check::AheadOfRows => 3,
            Check::CompletedTwice
            | Check::Quarantined
            | Check::Unfinished
            | Check::RecordInNoSplit
            | Check::RecordInTwoSplits
            | Check::VerdictMissing
            | Check::StoppedSplitNotReassigned
            | Check::LeaderNotReplaced => 4,
            Check::EpochRegressed
            | Check::TwoOwners
            | Check::StaleEpochCommit
            | Check::ValueConflict => 5,
        }
    }
}

/// One failed delivery check.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Violation {
    /// The check.
    pub check: Check,
    /// Store key involved, when one is.
    pub key: Option<String>,
    /// Revision involved, when one is.
    pub rev: Option<u64>,
    /// Instance id of the writer, when known.
    pub instance: Option<String>,
    /// Pid of the writer's process, when known.
    pub pid: Option<u32>,
    /// What was found.
    pub detail: String,
}

/// How a worker process ended.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerExit {
    /// Instance id.
    pub instance: String,
    /// Pid of the process.
    pub pid: u32,
    /// Exit code, when it exited.
    pub code: Option<i32>,
    /// Terminating signal, when a signal ended it.
    pub signal: Option<i32>,
    /// The harness caused the ending: a kill it sent, or an abort the
    /// process journalled.
    pub scheduled: bool,
}

impl WorkerExit {
    fn failed(&self) -> bool {
        !self.scheduled && self.code != Some(0)
    }

    /// The worker exited 3, the status a worker exits with when a journal
    /// line cannot be written.
    fn journal_failed(&self) -> bool {
        !self.scheduled && self.code == Some(3)
    }
}

/// One container health poll.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthPoll {
    /// Milliseconds since the Unix epoch.
    pub t_ms: u64,
    /// Container name.
    pub container: String,
    /// The container was running and answered.
    pub ok: bool,
    /// Why the poll failed.
    pub error: Option<String>,
}

/// The first container outage in `polls`: two consecutive failed polls of one
/// container, as the container name and the time of the second.
#[must_use]
pub fn container_outage(polls: &[HealthPoll]) -> Option<(&str, u64)> {
    let mut failed_before: Vec<&str> = Vec::new();
    for poll in polls {
        let name = poll.container.as_str();
        if poll.ok {
            failed_before.retain(|c| *c != name);
        } else if failed_before.contains(&name) {
            return Some((name, poll.t_ms));
        } else {
            failed_before.push(name);
        }
    }
    None
}

/// The stop a stopped-writer scenario observed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StopSeen {
    /// The stopped write's key.
    pub key: String,
    /// Pid of the stopped process.
    pub pid: u32,
    /// Revision at which the stopped process's re-send landed, when one did.
    pub resend_rev: Option<u64>,
}

/// What a scenario sets out to show.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scenario {
    /// Every delivery property holds under the schedule.
    Ordinary,
    /// A worker stops itself before a write and resumes after another
    /// instance claimed the split. With `broken_fence` the resumed write is
    /// re-sent at the current revision and the oracle must catch it.
    StoppedWriter {
        /// The fence is broken.
        broken_fence: bool,
        /// The stop, when its journal line appeared.
        stop: Option<StopSeen>,
    },
}

impl Scenario {
    fn is_broken_fence(&self) -> bool {
        matches!(
            self,
            Scenario::StoppedWriter {
                broken_fence: true,
                ..
            }
        )
    }
}

/// The lost-reply faults a scenario drew and what followed them.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LostReplies {
    /// The schedule drew at least one lost reply.
    pub drawn: bool,
    /// `err_after_land` lines across the journals.
    pub lines: usize,
    /// Each line no accepted recovery followed, described.
    pub unexplained: Vec<String>,
}

/// Everything [`classify`] judges.
#[derive(Clone, Debug)]
pub struct Evidence<'a> {
    /// Why setup failed, when it did.
    pub setup_failure: Option<&'a str>,
    /// What the scenario sets out to show.
    pub scenario: &'a Scenario,
    /// How each worker process ended.
    pub worker_exits: &'a [WorkerExit],
    /// Workers were still running at the deadline.
    pub timed_out: bool,
    /// The oracle's violations, plus [`Check::StoppedSplitNotReassigned`] and
    /// [`Check::LeaderNotReplaced`] when the harness recorded them.
    pub violations: &'a [Violation],
    /// Scenario assertions that failed, described.
    pub expectations: &'a [String],
    /// Lost-reply faults and their evidence.
    pub lost_replies: &'a LostReplies,
    /// The health polls taken while workers ran.
    pub health: &'a [HealthPoll],
}

/// Picks the outcome kind and its message. The first rule that applies wins.
///
/// 1. Harness: setup failed, or a worker exited 3 because it could not write
///    its journal. A container outage also makes a run that fails by a later
///    rule harness, unless the run has a property 3 or 5 violation outside a
///    broken-fence scenario.
/// 2. Violation: a stopped split was not reassigned.
/// 3. Expectation: an assertion failed, the stop never fired, a lost reply was
///    drawn but not exercised, or a broken fence produced no matching
///    `EpochRegressed`. Outside a broken-fence scenario a violation outranks
///    it.
/// 4. Pass: a broken-fence scenario whose oracle caught the stale re-send.
/// 5. Violation: the oracle found any violation.
/// 6. Worker: a worker failed or the run timed out.
/// 7. Pass.
#[must_use]
pub fn classify(e: &Evidence<'_>) -> (Kind, String) {
    if let Some(failure) = e.setup_failure {
        return (Kind::Harness, format!("setup failed: {failure}"));
    }
    let unjournalled: Vec<String> = e
        .worker_exits
        .iter()
        .filter(|w| w.journal_failed())
        .map(|w| format!("{} (pid {})", w.instance, w.pid))
        .collect();
    if !unjournalled.is_empty() {
        return (
            Kind::Harness,
            format!("journal write failed: {}", unjournalled.join("; ")),
        );
    }
    let (kind, message) = classify_run(e);
    if kind == Kind::Pass {
        return (kind, message);
    }
    let exempt = !e.scenario.is_broken_fence()
        && e.violations
            .iter()
            .any(|v| matches!(v.check.property(), 3 | 5));
    match container_outage(e.health) {
        Some((container, t_ms)) if !exempt => (
            Kind::Harness,
            format!("container {container} was down at t_ms {t_ms}; the run also found: {message}"),
        ),
        _ => (kind, message),
    }
}

fn classify_run(e: &Evidence<'_>) -> (Kind, String) {
    let violations = describe(e.violations);
    let stopped = matches!(e.scenario, Scenario::StoppedWriter { .. });
    if stopped
        && e.violations
            .iter()
            .any(|v| v.check == Check::StoppedSplitNotReassigned)
    {
        return (Kind::Violation, violations);
    }

    let mut expectations: Vec<String> = e.expectations.to_vec();
    if e.lost_replies.drawn && e.lost_replies.lines == 0 {
        expectations.push("fault not exercised: no err_after_land line".to_owned());
    }
    expectations.extend(
        e.lost_replies
            .unexplained
            .iter()
            .map(|u| format!("fault not exercised: {u}")),
    );
    let mut caught = false;
    if let Scenario::StoppedWriter { broken_fence, stop } = e.scenario {
        match stop {
            None => expectations.push("fault not exercised: the stop never fired".to_owned()),
            Some(stop) if *broken_fence => {
                caught = e.violations.iter().any(|v| {
                    v.check == Check::EpochRegressed
                        && v.key.as_deref() == Some(stop.key.as_str())
                        && v.rev.is_some()
                        && v.rev == stop.resend_rev
                        && v.pid == Some(stop.pid)
                });
                if !caught {
                    expectations.push(format!(
                        "no EpochRegressed on {} at the stopped process's re-send",
                        stop.key
                    ));
                }
            }
            Some(_) => {}
        }
    }
    if !expectations.is_empty() {
        let expectations = expectations.join("; ");
        if e.scenario.is_broken_fence() || e.violations.is_empty() {
            return (Kind::Expectation, expectations);
        }
        return (
            Kind::Violation,
            format!("{violations}; expectations also failed: {expectations}"),
        );
    }
    if caught {
        return (Kind::Pass, "the oracle caught the broken fence".to_owned());
    }
    if !e.violations.is_empty() {
        return (Kind::Violation, violations);
    }
    let failed: Vec<String> = e
        .worker_exits
        .iter()
        .filter(|w| w.failed())
        .map(|w| match (w.code, w.signal) {
            (_, Some(signal)) => format!("{} (pid {}) ended on signal {signal}", w.instance, w.pid),
            (code, None) => format!("{} (pid {}) exited {code:?}", w.instance, w.pid),
        })
        .collect();
    if !failed.is_empty() || e.timed_out {
        let mut message = failed.join("; ");
        if e.timed_out {
            if !message.is_empty() {
                message.push_str("; ");
            }
            message.push_str("workers were still running at the deadline");
        }
        return (Kind::Worker, message);
    }
    (Kind::Pass, "every check held".to_owned())
}

fn describe(violations: &[Violation]) -> String {
    violations
        .iter()
        .map(|v| format!("P{} {:?}: {}", v.check.property(), v.check, v.detail))
        .collect::<Vec<_>>()
        .join("; ")
}

/// What one scenario writes to `outcome.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outcome {
    /// Scenario name.
    pub scenario: String,
    /// Store kind.
    pub store: String,
    /// Worker instances.
    pub instances: u32,
    /// The run seed.
    pub seed: u64,
    /// The command that replays the fault schedule.
    pub replay: String,
    /// The stage the scenario reached.
    pub stage: Stage,
    /// The outcome kind.
    pub kind: Kind,
    /// What [`classify`] reported.
    pub message: String,
    /// The violations found. Each is written with a `property` field holding
    /// [`Check::property`], which reading ignores.
    #[serde(serialize_with = "with_property")]
    pub violations: Vec<Violation>,
    /// The scenario assertions that failed.
    pub expectations: Vec<String>,
    /// Each fault applied to a process incarnation.
    pub faults_fired: Vec<FaultFired>,
}

fn with_property<S: serde::Serializer>(violations: &[Violation], s: S) -> Result<S::Ok, S::Error> {
    #[derive(Serialize)]
    struct Numbered<'a> {
        property: u8,
        #[serde(flatten)]
        violation: &'a Violation,
    }
    s.collect_seq(violations.iter().map(|violation| Numbered {
        property: violation.check.property(),
        violation,
    }))
}

/// A fault applied to one process incarnation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FaultFired {
    /// Instance id and incarnation, as `<instance>-<n>`.
    pub incarnation: String,
    /// The fault drawn.
    pub fault: String,
    /// Whether its target was live when it fell.
    pub fired: bool,
}

#[cfg(test)]
mod tests;
