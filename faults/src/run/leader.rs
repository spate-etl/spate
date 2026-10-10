//! The leader-kill run: the first worker process to reach one stage of a
//! leader's work stops there and is killed, and another instance must take
//! the leader key before the killed instance's replacement starts.

use super::{
    Env, Event, FaultFired, Journal, LeaderAtKill, POLL, RUN_DEADLINE, Run, STOP_CONFIRM, Tuning,
    Workers, journal, journal_holds, kill_fault_text, millis, read_leader,
};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::outcome::{Check, Violation};

/// The token file, in the run directory, whose creator is the one process
/// that stops.
pub(super) const TOKEN: &str = "leader-stop.token";
/// Cap on a `leader_stop` line, from the workers' start.
const STOP_LINE: Duration = Duration::from_secs(60);
/// Leases from the kill within which another instance must hold the key.
const TAKEOVER_LEASES: u64 = 4;

/// What a leader-kill run found besides the oracle's checks.
#[derive(Debug, Default)]
pub(super) struct Killed {
    /// The takeover that did not come within the cap.
    pub(super) violation: Option<Violation>,
    /// The scenario's own assertions that failed.
    pub(super) expectations: Vec<String>,
}

/// The process that journalled a `leader_stop` line.
struct Stopped {
    index: u32,
    instance: String,
    pid: u32,
}

impl Run<'_> {
    /// Starts every worker at once and waits for a `leader_stop` line. Once
    /// that process reports stopped it reads the leader key, journals the
    /// kill with what it read, kills the process, and polls the key until
    /// another instance holds it or [`TAKEOVER_LEASES`] leases pass. It
    /// starts the killed instance's replacement the drawn delay after that,
    /// then waits for the workers until [`RUN_DEADLINE`] from the start.
    ///
    /// # Errors
    ///
    /// Fails when a worker cannot be started, signalled or killed, a journal
    /// line cannot be written, or the stopped process journals its stop and
    /// does not report stopped.
    pub(super) fn drive_leader(
        &self,
        workers: &mut Workers,
        processes: &mut Vec<(String, u32, PathBuf)>,
        env: &Env,
        rt: &tokio::runtime::Runtime,
        tuning: &Tuning,
        faults_path: &Path,
    ) -> Result<(bool, Vec<FaultFired>, Killed), String> {
        let faults =
            Journal::open(faults_path).map_err(|e| format!("{}: {e}", faults_path.display()))?;
        let log = |event| {
            faults
                .append(event)
                .map_err(|e| format!("{}: {e}", faults_path.display()))
        };
        let status = |e: std::io::Error| format!("signal or read a worker: {e}");
        let plan = self
            .schedule
            .leader
            .ok_or("a leader-kill schedule carries a leader plan")?;
        let start = Instant::now();
        let until = start + RUN_DEADLINE;
        let first: Vec<(String, u32, PathBuf)> = (0..self.spec.instances)
            .map(|i| self.spawn(workers, env, i, 1, tuning))
            .collect::<Result<_, _>>()?;
        processes.extend(first.iter().cloned());

        let stopped = leader_stop(workers, &first).map_err(status)?;
        let mut fired: Vec<FaultFired> = first
            .iter()
            .map(|(name, pid, _)| FaultFired {
                incarnation: format!("{name}-1"),
                fault: plan.stop.to_string(),
                fired: stopped.as_ref().is_some_and(|s| s.pid == *pid),
            })
            .collect();
        let mut killed = Killed::default();
        let Some(Stopped {
            index,
            instance: name,
            pid,
        }) = stopped
        else {
            killed.expectations.push(format!(
                "fault not exercised: no leader reached {}",
                plan.stop
            ));
            let timed_out = workers
                .wait(until.saturating_duration_since(Instant::now()))
                .map_err(status)?;
            return Ok((timed_out, fired, killed));
        };
        if !workers
            .confirm_stopped(&name, STOP_CONFIRM)
            .map_err(status)?
        {
            return Err(format!(
                "{name} (pid {pid}) journalled its leader stop and did not report stopped"
            ));
        }

        let read = read_leader(rt, &env.direct);
        if !matches!(&read, LeaderAtKill::Held { owner, .. } if *owner == name) {
            killed.expectations.push(format!(
                "the stopped process {name} (pid {pid}) did not hold the leader key: read {read:?}"
            ));
        }
        let kill_ms = millis(start.elapsed());
        log(Event::Kill {
            instance: name.clone(),
            pid,
            leader: read.clone(),
        })?;
        workers
            .kill(&name)
            .map_err(|e| format!("kill {name}: {e}"))?;
        fired.push(FaultFired {
            incarnation: format!("{name}-1"),
            fault: kill_fault_text(kill_ms, &read),
            fired: true,
        });

        let cap_ms = journal::now_ms() + TAKEOVER_LEASES * tuning.lease_ms;
        killed.violation = await_takeover(
            &name,
            pid,
            cap_ms,
            || read_leader(rt, &env.direct),
            journal::now_ms,
            || std::thread::sleep(POLL),
        );

        std::thread::sleep(Duration::from_millis(plan.respawn_after_ms));
        let process = self.spawn(workers, env, index, 2, tuning)?;
        log(Event::Respawn {
            instance: process.0.clone(),
            pid: process.1,
        })?;
        processes.push(process);
        let timed_out = workers
            .wait(until.saturating_duration_since(Instant::now()))
            .map_err(status)?;
        Ok((timed_out, fired, killed))
    }
}

/// Polls `read` until it shows the leader key held by an instance other than
/// `killed`, calling `sleep` between polls. A [`Check::LeaderNotReplaced`]
/// violation when `now` reaches `cap_ms` first.
fn await_takeover(
    killed: &str,
    pid: u32,
    cap_ms: u64,
    mut read: impl FnMut() -> LeaderAtKill,
    mut now: impl FnMut() -> u64,
    mut sleep: impl FnMut(),
) -> Option<Violation> {
    while !leadership_moved(killed, &read()) {
        if now() >= cap_ms {
            return Some(Violation {
                check: Check::LeaderNotReplaced,
                key: Some("leader".to_owned()),
                rev: None,
                instance: Some(killed.to_owned()),
                pid: Some(pid),
                detail: format!(
                    "no other instance held the leader key within {TAKEOVER_LEASES} leases \
                     of the kill"
                ),
            });
        }
        sleep();
    }
    None
}

/// The first of `first`, in start order, whose journal holds a `leader_stop`
/// line. `None` once every one has exited or [`STOP_LINE`] passes first.
fn leader_stop(
    workers: &mut Workers,
    first: &[(String, u32, PathBuf)],
) -> std::io::Result<Option<Stopped>> {
    let until = Instant::now() + STOP_LINE;
    loop {
        for (index, (instance, pid, path)) in (0..).zip(first) {
            let stopped = journal_holds(path, "leader_stop")
                && journal::read(path).is_ok_and(|lines| {
                    lines
                        .iter()
                        .any(|l| matches!(l.event, Event::LeaderStop { .. }))
                });
            if stopped {
                return Ok(Some(Stopped {
                    index,
                    instance: instance.clone(),
                    pid: *pid,
                }));
            }
        }
        let mut live = false;
        for (instance, _, _) in first {
            live |= workers.live(instance)?.is_some();
        }
        if !live || Instant::now() >= until {
            return Ok(None);
        }
        std::thread::sleep(POLL);
    }
}

/// Whether `read` shows the leader key held by an instance other than
/// `killed`.
fn leadership_moved(killed: &str, read: &LeaderAtKill) -> bool {
    matches!(read, LeaderAtKill::Held { owner, .. } if owner != killed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn held(owner: &str, generation: u64) -> LeaderAtKill {
        LeaderAtKill::Held {
            owner: owner.to_owned(),
            generation,
            digest: 7,
        }
    }

    /// Leadership moved when another instance holds the key, at any
    /// generation; the killed instance at a higher generation, an absent key
    /// and a failed read do not count.
    #[test]
    fn leadership_moved_compares_owner_not_generation() {
        assert!(leadership_moved("w1", &held("w2", 3)));
        assert!(!leadership_moved("w1", &held("w1", 4)));
        assert!(!leadership_moved("w1", &LeaderAtKill::Vacant));
        assert!(!leadership_moved("w1", &LeaderAtKill::Unread));
    }

    /// The wait ends at the first read naming another owner; the killed
    /// owner at a higher generation, an absent key and a failed read until
    /// the cap give a `LeaderNotReplaced` violation at exactly the cap.
    #[test]
    fn takeover_comes_from_another_owner_by_the_cap() {
        let now = std::cell::Cell::new(0);
        let mut reads = [held("w1", 2), LeaderAtKill::Vacant, held("w2", 2)].into_iter();
        let found = await_takeover(
            "w1",
            9,
            200,
            || reads.next().expect("polled past the takeover"),
            || now.get(),
            || now.set(now.get() + 50),
        );
        assert_eq!((found, now.get()), (None, 100));

        now.set(0);
        let mut reads = [held("w1", 3), LeaderAtKill::Vacant, LeaderAtKill::Unread]
            .into_iter()
            .cycle();
        let found = await_takeover(
            "w1",
            9,
            200,
            || reads.next().expect("cycles"),
            || now.get(),
            || now.set(now.get() + 50),
        );
        assert_eq!(
            (found.map(|v| (v.check, v.instance, v.pid)), now.get()),
            (
                Some((Check::LeaderNotReplaced, Some("w1".to_owned()), Some(9))),
                200
            )
        );
    }
}
