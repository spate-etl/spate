//! The stopped-writer rendezvous: the peer leads, the sleeper stops itself
//! inside a commit, the peer claims the split, and the sleeper is resumed.

use super::{
    Env, Event, FaultFired, Faults, Journal, Keyspace, POLL, RUN_DEADLINE, Run, SETUP_DEADLINE,
    SLEEPER, STOP_CONFIRM, STORE_CALL, Tuning, Workers, journal, journal_holds,
};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::journal::Progress;

/// The instance that starts first and leads.
const PEER: u32 = 0;
/// Cap on the sleeper journalling its stop, from its start.
const STOP_LINE: Duration = Duration::from_secs(60);
/// How long a broken-fence run waits for its workers after the release.
const BROKEN_FENCE_WAIT: Duration = Duration::from_secs(90);
/// How far past one lease from the stop the release falls at the earliest.
const RELEASE_MARGIN_MS: u64 = 250;
/// Working-set bound for both workers, above any data set's split count, so
/// the peer has a free lane for every split the sleeper holds.
pub(super) const WORKING_SET: u32 = 32;
const _: () =
    assert!(WORKING_SET as u64 >= super::OBJECTS * super::MAX_OBJECT.div_ceil(super::MIB));

/// The stop a stopped-writer run saw.
#[derive(Clone, Debug)]
pub(super) struct Stopped {
    /// The stopped commit's key.
    pub(super) key: String,
    /// The revision the stopped commit replaces.
    pub(super) expected: u64,
    /// Pid of the stopped process.
    pub(super) pid: u32,
    /// The peer claimed the split before the cap.
    pub(super) reassigned: bool,
}

/// What [`Run::drive_stopped`] returns: whether a worker was still running
/// at the end, the sleeper's stop plan and whether it fired, and the stop.
type Driven = (bool, Vec<FaultFired>, Option<Stopped>);

impl Run<'_> {
    /// Starts the peer, waits until it leads, then starts the sleeper and
    /// waits for its `stop` line. Once the sleeper reports stopped it polls
    /// the store until the peer holds the split at a higher epoch, and sends
    /// SIGCONT once that holds and a lease plus [`RELEASE_MARGIN_MS`] has
    /// passed since the `stop` line, or four leases from it. Then it waits
    /// for the workers: [`BROKEN_FENCE_WAIT`] with a broken fence, else until
    /// [`RUN_DEADLINE`] from the start.
    ///
    /// # Errors
    ///
    /// Fails when a worker cannot be started or signalled, the peer does not
    /// lead within [`SETUP_DEADLINE`], or the sleeper journals its stop and
    /// does not report stopped.
    pub(super) fn drive_stopped(
        &self,
        workers: &mut Workers,
        processes: &mut Vec<(String, u32, PathBuf)>,
        env: &Env,
        rt: &tokio::runtime::Runtime,
        tuning: &Tuning,
        faults_path: &Path,
    ) -> Result<Driven, String> {
        let faults =
            Journal::open(faults_path).map_err(|e| format!("{}: {e}", faults_path.display()))?;
        let status = |e: std::io::Error| format!("signal or read a worker: {e}");
        let until = Instant::now() + RUN_DEADLINE;
        let plan = self
            .schedule
            .stop_at
            .ok_or("a stopped-writer schedule carries a stop plan")?;
        processes.push(self.spawn(workers, env, PEER, 1, tuning)?);
        self.await_leader(workers, env, rt)?;
        let sleeper = self.spawn(workers, env, SLEEPER, 1, tuning)?;
        let (name, pid, path) = sleeper.clone();
        processes.push(sleeper);

        let line = stop_line(workers, &name, &path).map_err(status)?;
        let fired = vec![FaultFired {
            incarnation: format!("{name}-1"),
            fault: plan.to_string(),
            fired: line.is_some(),
        }];
        let Some((stop_ms, key, expected, epoch)) = line else {
            let timed_out = workers
                .wait(until.saturating_duration_since(Instant::now()))
                .map_err(status)?;
            return Ok((timed_out, fired, None));
        };
        if !workers
            .confirm_stopped(&name, STOP_CONFIRM)
            .map_err(status)?
        {
            return Err(format!(
                "{name} (pid {pid}) journalled its stop and did not report stopped"
            ));
        }
        let cap = stop_ms + 4 * tuning.lease_ms;
        let mut claimed = false;
        loop {
            claimed = claimed || peer_claimed(env, rt, &key, epoch);
            let now = journal::now_ms();
            if release_due(stop_ms, claimed, now, tuning.lease_ms) || now >= cap {
                break;
            }
            std::thread::sleep(POLL);
        }
        if workers.resume(&name, pid).map_err(status)? {
            faults
                .append(Event::Sigcont {
                    instance: name.clone(),
                    pid,
                })
                .map_err(|e| format!("{}: {e}", faults_path.display()))?;
        }
        let broken_fence = matches!(
            self.spec.faults,
            Faults::StoppedWriter { broken_fence: true }
        );
        let wait = if broken_fence {
            BROKEN_FENCE_WAIT
        } else {
            until.saturating_duration_since(Instant::now())
        };
        let timed_out = workers.wait(wait).map_err(status)?;
        let stopped = Stopped {
            key,
            expected,
            pid,
            reassigned: claimed,
        };
        Ok((timed_out, fired, Some(stopped)))
    }

    /// Waits until the leader key names the peer.
    fn await_leader(
        &self,
        workers: &mut Workers,
        env: &Env,
        rt: &tokio::runtime::Runtime,
    ) -> Result<(), String> {
        let until = Instant::now() + SETUP_DEADLINE;
        let peer = format!("w{PEER}");
        loop {
            let leader = rt.block_on(async {
                tokio::time::timeout(STORE_CALL, env.direct.get(Keyspace::Ephemeral, "leader"))
                    .await
            });
            if let Ok(Ok(Some(entry))) = leader
                && serde_json::from_slice::<serde_json::Value>(&entry.value)
                    .is_ok_and(|v| v["owner"] == peer.as_str())
            {
                return Ok(());
            }
            let live = workers
                .live(&peer)
                .map_err(|e| format!("read {peer}: {e}"))?;
            if live.is_none() || Instant::now() >= until {
                return Err(format!("{peer} did not lead within a minute of its start"));
            }
            std::thread::sleep(POLL);
        }
    }
}

/// The `stop` line in the journal `instance` writes at `path`, as its time,
/// key, expected revision and epoch. `None` when the process exits or
/// [`STOP_LINE`] passes first.
fn stop_line(
    workers: &mut Workers,
    instance: &str,
    path: &Path,
) -> std::io::Result<Option<(u64, String, u64, u64)>> {
    let until = Instant::now() + STOP_LINE;
    loop {
        if journal_holds(path, "stop")
            && let Ok(lines) = journal::read(path)
            && let Some(found) = lines.into_iter().find_map(|l| match l.event {
                Event::Stop {
                    key,
                    expected,
                    epoch,
                } => Some((l.t_ms, key, expected, epoch)),
                _ => None,
            })
        {
            return Ok(Some(found));
        }
        if workers.live(instance)?.is_none() || Instant::now() >= until {
            return Ok(None);
        }
        std::thread::sleep(POLL);
    }
}

/// Whether the store shows the peer holding `key` at an epoch above `epoch`.
fn peer_claimed(env: &Env, rt: &tokio::runtime::Runtime, key: &str, epoch: u64) -> bool {
    let entry = rt.block_on(async {
        tokio::time::timeout(STORE_CALL, env.direct.get(Keyspace::Durable, key)).await
    });
    let Ok(Ok(Some(entry))) = entry else {
        return false;
    };
    Progress::parse(&entry.value)
        .is_ok_and(|p| p.epoch > epoch && p.owner.as_deref() == Some(format!("w{PEER}").as_str()))
}

/// Whether the stopped process may be resumed at `now_ms`: the peer's claim
/// was seen, and one lease plus [`RELEASE_MARGIN_MS`] has passed since the
/// stop at `stop_ms`.
fn release_due(stop_ms: u64, claimed: bool, now_ms: u64, lease_ms: u64) -> bool {
    claimed && now_ms >= stop_ms + lease_ms + RELEASE_MARGIN_MS
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The release is due only once the peer's claim was seen and a lease
    /// plus the margin has passed since the stop; a claim seen at 0.99 lease
    /// does not release.
    #[test]
    fn release_waits_a_lease_from_the_stop_and_for_the_claim() {
        let (stop, lease) = (10_000, 2_000);
        assert!(!release_due(stop, true, stop + 1_980, lease));
        assert!(!release_due(stop, true, stop + lease + 249, lease));
        assert!(release_due(stop, true, stop + lease + 250, lease));
        assert!(!release_due(stop, false, stop + 4 * lease, lease));
    }
}
