//! The seeded script by which each DynamoDB worker's fault proxy answers,
//! and the scenario that drives a DynamoDB store through dropped replies.

use std::collections::BTreeMap;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use spate_coordination::store::{
    CasOutcome, CoordinationStore as _, Keyspace, Revision, StoreError,
};
use spate_test_support::{Call, DynamoDbFaultProxy, Fault, retried_status};

use super::{
    Faults, Run, STORE_CALL, Spec, await_ready, dynamodb_local, panic_text, run_root, run_seed,
    through,
};
use crate::journal::{Event, Line};
use crate::oracle::StoreKind;
use crate::outcome::{self, Evidence, FaultFired, LostReplies, Outcome, Scenario, Stage};
use crate::schedule::Schedule;
use crate::seed::SplitMix64;
use crate::store::AbortMode;
use crate::worker::{Tuning, dynamodb_store};

/// Longest delay the proxy holds a call for.
pub(super) const MAX_DELAY_MS: u64 = 100;

/// How the proxy in front of one worker process answers each call: a draw
/// indexed by the call's sequence number, so a seed replays the decisions.
#[derive(Clone, Debug)]
pub(super) struct ProxyScript {
    seed: u64,
    process: String,
    faults: bool,
}

impl ProxyScript {
    /// The script for `instance`'s `incarnation`. The process carrying the
    /// `ErrAfterLand` plan answers every call `pass`.
    pub(super) fn new(
        seed: u64,
        schedule: &Schedule,
        instance: u32,
        incarnation: u32,
    ) -> ProxyScript {
        ProxyScript {
            seed,
            process: format!("w{instance}-{incarnation}"),
            faults: link_faults(schedule, instance, incarnation),
        }
    }

    /// The answer to `call`. A durable `split.*` update draws a fault or a
    /// delay more often than any other call, and is the only call that draws
    /// a fault after it lands.
    pub(super) fn decide(&self, call: &Call) -> Fault {
        if !self.faults {
            return Fault::Pass;
        }
        let rng =
            &mut SplitMix64::for_scenario(self.seed, &format!("{}/{}", self.process, call.seq));
        let split_write = call.op == "UpdateItem"
            && call
                .key
                .as_ref()
                .is_some_and(|(pk, sk)| pk.ends_with("#d") && sk.starts_with("split."));
        match (split_write, rng.in_range(0, 99)) {
            (true, 0..=1) | (false, 0) => Fault::Throttle,
            (true, 2..=3) | (false, 1) => Fault::ThroughputExceeded,
            (true, 4..=5) | (false, 2) => Fault::ServerError(retried_status(rng.next_u64())),
            (true, 6..=7) => Fault::ErrorAfterLand(retried_status(rng.next_u64())),
            (true, 8..=10) => Fault::DropAfterLand,
            (true, 11..=15) | (false, 3..=4) => {
                Fault::Delay(Duration::from_millis(rng.in_range(1, MAX_DELAY_MS)))
            }
            _ => Fault::Pass,
        }
    }
}

/// Whether `instance`'s `incarnation` gets faults on its store link: every
/// process but the one carrying the `ErrAfterLand` plan does.
pub(super) fn link_faults(schedule: &Schedule, instance: u32, incarnation: u32) -> bool {
    schedule
        .plan_for(instance, incarnation)
        .is_none_or(|p| p.plan.mode != AbortMode::ErrAfterLand)
}

/// `script` with each answer other than `pass` handed to `append` as a
/// `proxy_fault` event against `instance` and the pid `pid` will hold. A call
/// whose event `append` fails to write is answered `pass`, and the first such
/// failure is kept in `failed`.
pub(super) fn logged(
    script: ProxyScript,
    instance: String,
    pid: Arc<OnceLock<u32>>,
    append: impl Fn(Event) -> io::Result<()> + Send + Sync + 'static,
    failed: Arc<OnceLock<String>>,
) -> impl Fn(&Call) -> Fault + Send + Sync + 'static {
    move |call| {
        let fault = script.decide(call);
        if fault == Fault::Pass {
            return fault;
        }
        let event = Event::ProxyFault {
            instance: instance.clone(),
            pid: *pid.wait(),
            fault: fault.to_string(),
            key: call.key.as_ref().map(|(_, sk)| sk.clone()),
        };
        match append(event) {
            Ok(()) => fault,
            Err(e) => {
                let _ = failed.set(format!("a {fault} proxy fault was not journalled: {e}"));
                Fault::Pass
            }
        }
    }
}

/// The failed expectation of a run that put fault proxies in front of its
/// workers when `faults` holds no `proxy_fault` line.
pub(super) fn unexercised(proxied: bool, faults: &[Line]) -> Option<String> {
    let fired = faults
        .iter()
        .any(|line| matches!(line.event, Event::ProxyFault { .. }));
    (proxied && !fired).then(|| "fault not exercised: no proxy_fault line".to_owned())
}

/// One entry per process and proxy fault kind in `faults`, naming how often
/// it fired. `processes` maps each pid to its journal, named after its
/// incarnation.
pub(super) fn fired(faults: &[Line], processes: &[(String, u32, PathBuf)]) -> Vec<FaultFired> {
    let mut counts: BTreeMap<(String, String), u32> = BTreeMap::new();
    for line in faults {
        let Event::ProxyFault { pid, fault, .. } = &line.event else {
            continue;
        };
        let Some((_, _, path)) = processes.iter().find(|(_, p, _)| p == pid) else {
            continue;
        };
        let incarnation = path
            .file_stem()
            .map_or_else(String::new, |s| s.to_string_lossy().into_owned());
        let kind = fault.split('(').next().unwrap_or(fault).to_owned();
        *counts.entry((incarnation, kind)).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|((incarnation, kind), n)| FaultFired {
            incarnation,
            fault: format!("proxy {kind} x{n}"),
            fired: true,
        })
        .collect()
}

/// A durable `split.*` update through a fault proxy that drops the reply to
/// its first attempt returns `Won` and lands its value, and one whose every
/// attempt is dropped returns `Retryable` with its value landed.
///
/// # Panics
///
/// Panics with the outcome kind's prefix when the scenario fails.
pub fn drop_after_land_then_pass_wins(name: &str) -> Outcome {
    let spec = Spec {
        name,
        store: StoreKind::DynamoDb,
        instances: 0,
        worker: Path::new(""),
        sink_delay_ms: 0,
        faults: Faults::None,
    };
    let seed = run_seed();
    let dir = run_root().join(format!("{name}-{seed:016x}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("create {}: {e}", dir.display()));
    let run = Run {
        spec: &spec,
        seed,
        dir,
        schedule: Schedule::default(),
        proxy_seed: None,
        links: Vec::new(),
    };
    let tuning = Tuning::dynamodb();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("harness runtime");
    let retried_attempts = Arc::new(AtomicU32::new(0));
    let setup = catch_unwind(AssertUnwindSafe(|| -> Result<_, String> {
        let _runtime = rt.enter();
        let (local, config, direct, upstream) = dynamodb_local(&tuning, None)?;
        await_ready(&rt, || direct.get(Keyspace::Durable, "plan"))?;
        let attempts = Arc::clone(&retried_attempts);
        let proxy = DynamoDbFaultProxy::start(upstream, move |call| drop_script(call, &attempts))
            .map_err(|e| format!("start the fault proxy: {e}"))?;
        let proxied = dynamodb_store(&through(&config, proxy.addr()), &tuning)?;
        Ok((local, direct, proxy, proxied))
    }));
    let (_local, direct, _proxy, proxied) = match setup {
        Ok(Ok(env)) => env,
        Ok(Err(failure)) => return run.harness(Stage::Setup, &failure),
        Err(panic) => return run.harness(Stage::Setup, &panic_text(&*panic)),
    };
    let mut expectations = Vec::new();
    for (key, dropped) in [(RETRIED, false), (DROPPED, true)] {
        let created = match timed(&rt, direct.create(Keyspace::Durable, key, b"0".to_vec())) {
            Ok(Ok(CasOutcome::Won(rev))) => rev,
            other => return run.harness(Stage::Setup, &format!("create {key}: {other:?}")),
        };
        let updated = timed(
            &rt,
            proxied.update(Keyspace::Durable, key, b"1".to_vec(), created),
        );
        let landed = timed(&rt, direct.get(Keyspace::Durable, key));
        expectations.extend(judge(key, dropped, created, &updated, &landed));
    }
    let attempts = retried_attempts.load(Ordering::SeqCst);
    if attempts != 2 {
        expectations.push(format!(
            "{RETRIED} took {attempts} attempts through the proxy, not 2"
        ));
    }
    let (kind, message) = outcome::classify(&Evidence {
        setup_failure: None,
        scenario: &Scenario::Ordinary,
        worker_exits: &[],
        timed_out: false,
        violations: &[],
        expectations: &expectations,
        lost_replies: &LostReplies::default(),
        health: &[],
    });
    run.finish(
        Stage::Oracle,
        kind,
        message,
        Vec::new(),
        expectations,
        Vec::new(),
    )
}

/// The key whose first attempt's reply is dropped.
const RETRIED: &str = "split.retried";
/// The key whose every attempt's reply is dropped.
const DROPPED: &str = "split.dropped";

/// Drops the reply to every update of [`DROPPED`] and to the first attempt
/// of [`RETRIED`], counting the updates of [`RETRIED`] in `attempts`.
fn drop_script(call: &Call, attempts: &AtomicU32) -> Fault {
    let Some((_, key)) = call.key.as_ref().filter(|_| call.op == "UpdateItem") else {
        return Fault::Pass;
    };
    match key.as_str() {
        DROPPED => Fault::DropAfterLand,
        RETRIED => {
            attempts.fetch_add(1, Ordering::SeqCst);
            if call.attempt == 1 {
                Fault::DropAfterLand
            } else {
                Fault::Pass
            }
        }
        _ => Fault::Pass,
    }
}

type Timed<T> = Result<Result<T, StoreError>, tokio::time::error::Elapsed>;

fn timed<T>(
    rt: &tokio::runtime::Runtime,
    call: impl std::future::Future<Output = Result<T, StoreError>>,
) -> Timed<T> {
    rt.block_on(async { tokio::time::timeout(STORE_CALL, call).await })
}

/// The failed expectations for `key`'s update from `created`: `Won` at the
/// next revision when only its first reply was dropped, `Retryable` when
/// every reply was, and the value landed either way.
fn judge(
    key: &str,
    dropped: bool,
    created: Revision,
    updated: &Timed<CasOutcome>,
    landed: &Timed<Option<spate_coordination::store::Entry>>,
) -> Vec<String> {
    let mut failed = Vec::new();
    let next = Revision(created.0 + 1);
    let replied = match updated {
        Ok(Ok(CasOutcome::Won(rev))) => !dropped && *rev == next,
        Ok(Err(StoreError::Retryable(_))) => dropped,
        _ => false,
    };
    if !replied {
        let want = if dropped {
            "Retryable".to_owned()
        } else {
            format!("Won({})", next.0)
        };
        failed.push(format!(
            "the update of {key} returned {updated:?}, not {want}"
        ));
    }
    match landed {
        Ok(Ok(Some(entry))) if entry.value == b"1" && entry.revision == next => {}
        other => failed.push(format!(
            "{key} holds {other:?} after the update, not its value at revision {}",
            next.0
        )),
    }
    failed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{Event, Journal, LeaderAtKill};

    const LEASE: u64 = 3_000;

    fn call(seq: u64, split_write: bool) -> Call {
        Call {
            op: if split_write { "UpdateItem" } else { "Query" }.to_owned(),
            seq,
            key: split_write.then(|| ("faults#d".to_owned(), "split.a".to_owned())),
            attempt: 1,
        }
    }

    /// Across seeds and calls, every delay the script draws is at most
    /// 100 ms, and delays are drawn.
    #[test]
    fn dynamodb_proxy_delay_draws_stay_under_100_ms() {
        let mut delays = 0;
        for seed in 0..50 {
            let schedule = Schedule::draw(&mut SplitMix64::new(seed), 3, LEASE);
            let script = ProxyScript::new(seed, &schedule, 2, 2);
            for seq in 0..400 {
                for split_write in [true, false] {
                    if let Fault::Delay(d) = script.decide(&call(seq, split_write)) {
                        assert!(
                            d <= Duration::from_millis(MAX_DELAY_MS),
                            "seed {seed}: {d:?}"
                        );
                        delays += 1;
                    }
                }
            }
        }
        assert!(delays > 0);
    }

    /// Across seeds, the process carrying the `ErrAfterLand` plan answers
    /// every call `pass` and opens no Toxiproxy window, and every other
    /// process draws faults and opens its windows.
    #[test]
    fn err_after_land_incarnations_draw_no_link_faults() {
        use super::super::link::{LinkKind, Links, Window};
        for instances in [1, 3] {
            for seed in 0..50 {
                let schedule = Schedule::draw(&mut SplitMix64::new(seed), instances, LEASE);
                let lost = schedule.lost_reply().expect("a lost reply is drawn");
                let windows: Vec<Window> = (0..instances)
                    .map(|instance| Window {
                        at_ms: 1_000,
                        instance,
                        kind: LinkKind::Blackhole,
                        duration_ms: 100,
                    })
                    .collect();
                let mut links = Links::new(&schedule, &windows);
                let mut incarnations = vec![1; instances as usize];
                let mut opened = Vec::new();
                while let Some((_, proxy, _)) = links.next_open(1_000, &incarnations, |_| Some(7)) {
                    opened.push(proxy);
                }
                let others: Vec<String> = (0..instances)
                    .filter(|i| *i != lost)
                    .map(|i| format!("w{i}-1"))
                    .collect();
                assert_eq!(opened, others, "seed {seed}");
                incarnations[lost as usize] = 2;
                let replaced = links.next_open(1_000, &incarnations, |_| Some(7));
                assert_eq!(replaced.map(|o| o.1), Some(format!("w{lost}-2")));
                for instance in 0..instances {
                    for incarnation in 1..=3 {
                        let script = ProxyScript::new(seed, &schedule, instance, incarnation);
                        let faults = (0..400)
                            .flat_map(|seq| [call(seq, true), call(seq, false)])
                            .filter(|c| script.decide(c) != Fault::Pass)
                            .count();
                        let carries = instance == lost && incarnation == 1;
                        assert_eq!(
                            faults == 0,
                            carries,
                            "seed {seed}: w{instance}-{incarnation}"
                        );
                    }
                }
            }
        }
    }

    /// Faults after a call lands fall only on durable `split.*` updates, and
    /// the script draws each fault kind.
    #[test]
    fn after_land_faults_fall_only_on_split_writes() {
        let script = ProxyScript::new(7, &Schedule::default(), 0, 1);
        let mut kinds = std::collections::BTreeSet::new();
        for seq in 0..2_000 {
            for split_write in [true, false] {
                let fault = script.decide(&call(seq, split_write));
                let after_land = matches!(fault, Fault::ErrorAfterLand(_) | Fault::DropAfterLand);
                assert!(split_write || !after_land, "{fault} on a read");
                kinds.insert(fault.to_string().split('(').next().unwrap().to_owned());
            }
        }
        assert_eq!(kinds.len(), 7, "{kinds:?}");
    }

    /// Only a durable `split.*` update draws a fault after it lands, and it
    /// draws a fault or a delay more often than any other keyed call.
    #[test]
    fn after_land_faults_fall_only_on_durable_split_updates() {
        let script = ProxyScript::new(7, &Schedule::default(), 0, 1);
        let keyed = |op: &str, pk: &str, sk: &str, seq| Call {
            op: op.to_owned(),
            seq,
            key: Some((pk.to_owned(), sk.to_owned())),
            attempt: 1,
        };
        let drawn = |op, pk, sk| {
            (0..2_000)
                .map(|seq| script.decide(&keyed(op, pk, sk, seq)))
                .collect::<Vec<_>>()
        };
        let split = drawn("UpdateItem", "faults#d", "split.a");
        let faulted = |answers: &[Fault]| answers.iter().filter(|f| **f != Fault::Pass).count();
        for (op, pk, sk) in [
            ("UpdateItem", "faults#e", "split.a"),
            ("UpdateItem", "faults#d", "plan"),
            ("GetItem", "faults#d", "split.a"),
        ] {
            let other = drawn(op, pk, sk);
            assert!(
                !other
                    .iter()
                    .any(|f| matches!(f, Fault::ErrorAfterLand(_) | Fault::DropAfterLand)),
                "after-land fault on {op} {pk}/{sk}"
            );
            assert!(faulted(&split) > faulted(&other), "{op} {pk}/{sk}");
        }
    }

    /// The same seed and call replay the same answer.
    #[test]
    fn proxy_answers_replay_from_the_seed() {
        let schedule = Schedule::default();
        let answers = |seed| {
            let script = ProxyScript::new(seed, &schedule, 0, 1);
            (0..500)
                .map(|seq| script.decide(&call(seq, true)))
                .collect::<Vec<_>>()
        };
        assert_eq!(answers(5), answers(5));
        assert_ne!(answers(5), answers(6));
    }

    /// Every answer but `pass` is journalled against the process with its
    /// call's key, under the name the oracle reads.
    #[test]
    fn proxy_faults_are_journalled_and_passes_are_not() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("faults.ndjson");
        let journal = Arc::new(Journal::open(&path).unwrap());
        let pid = Arc::new(OnceLock::new());
        pid.set(42).unwrap();
        let script = ProxyScript::new(3, &Schedule::default(), 1, 2);
        let failed = Arc::new(OnceLock::new());
        let answer = logged(
            script.clone(),
            "w1".to_owned(),
            pid,
            move |event| journal.append(event),
            Arc::clone(&failed),
        );
        let calls: Vec<Call> = (0..300).map(|seq| call(seq, true)).collect();
        let answers: Vec<Fault> = calls.iter().map(&answer).collect();
        let lines = crate::journal::read(&path).unwrap();
        let logged: Vec<_> = lines
            .iter()
            .map(|l| match &l.event {
                Event::ProxyFault {
                    instance,
                    pid,
                    fault,
                    key,
                } => {
                    assert_eq!(
                        (instance.as_str(), *pid, key.as_deref()),
                        ("w1", 42, Some("split.a"))
                    );
                    fault.clone()
                }
                other => panic!("{other:?}"),
            })
            .collect();
        let expected: Vec<String> = answers
            .iter()
            .filter(|f| **f != Fault::Pass)
            .map(ToString::to_string)
            .collect();
        assert!(!expected.is_empty());
        assert_eq!(logged, expected);
        assert_eq!(failed.get(), None);
    }

    /// A fault whose `proxy_fault` line cannot be written is answered `pass`,
    /// and the failure is kept for the run to report.
    #[test]
    fn an_unjournalled_proxy_fault_is_not_applied() {
        let pid = Arc::new(OnceLock::new());
        pid.set(42).unwrap();
        let failed = Arc::new(OnceLock::new());
        let script = ProxyScript::new(3, &Schedule::default(), 1, 2);
        let answer = logged(
            script.clone(),
            "w1".to_owned(),
            pid,
            |_| Err(io::Error::other("disk full")),
            Arc::clone(&failed),
        );
        let calls: Vec<Call> = (0..300).map(|seq| call(seq, true)).collect();
        assert!(calls.iter().any(|c| script.decide(c) != Fault::Pass));
        assert!(calls.iter().all(|c| answer(c) == Fault::Pass));
        let failure = failed.get().expect("the failure is kept");
        assert!(
            failure.ends_with("was not journalled: disk full"),
            "{failure}"
        );
    }

    /// A proxied run with no `proxy_fault` line fails its expectation; one
    /// with a line, or a run with no proxies, does not.
    #[test]
    fn a_proxied_run_without_a_proxy_fault_is_not_exercised() {
        let kill = Line {
            t_ms: 1,
            event: Event::Kill {
                instance: "w0".to_owned(),
                pid: 10,
                leader: LeaderAtKill::Unread,
            },
        };
        let proxied = Line {
            t_ms: 2,
            event: Event::ProxyFault {
                instance: "w0".to_owned(),
                pid: 10,
                fault: "throttle".to_owned(),
                key: None,
            },
        };
        assert_eq!(
            unexercised(true, std::slice::from_ref(&kill)).as_deref(),
            Some("fault not exercised: no proxy_fault line")
        );
        assert_eq!(unexercised(true, &[kill.clone(), proxied]), None);
        assert_eq!(unexercised(false, &[kill]), None);
    }

    /// Fired proxy faults are counted per process and kind, whatever their
    /// parameters, and a pid no process holds is skipped.
    #[test]
    fn proxy_faults_fired_counts_each_kind_per_process() {
        let line = |pid, fault: &str| Line {
            t_ms: 1,
            event: Event::ProxyFault {
                instance: "w0".to_owned(),
                pid,
                fault: fault.to_owned(),
                key: None,
            },
        };
        let processes = [
            ("w0".to_owned(), 10, PathBuf::from("/r/w0-1.ndjson")),
            ("w0".to_owned(), 11, PathBuf::from("/r/w0-2.ndjson")),
        ];
        let faults = [
            line(10, "server_error(500)"),
            line(10, "server_error(503)"),
            line(11, "throttle"),
            line(99, "throttle"),
        ];
        let fired: Vec<_> = fired(&faults, &processes)
            .into_iter()
            .map(|f| (f.incarnation, f.fault, f.fired))
            .collect();
        assert_eq!(
            fired,
            [
                ("w0-1".to_owned(), "proxy server_error x2".to_owned(), true),
                ("w0-2".to_owned(), "proxy throttle x1".to_owned(), true),
            ]
        );
    }

    /// A reply dropped once then passed must come back `Won` at the next
    /// revision, a reply dropped on every attempt `Retryable`, and the value
    /// must land in both cases.
    #[test]
    fn drop_after_land_judges_the_reply_and_the_landed_value() {
        let entry = |value: &[u8], rev| {
            Ok(Ok(Some(spate_coordination::store::Entry {
                key: "split.a".to_owned(),
                value: value.to_vec(),
                revision: Revision(rev),
            })))
        };
        let won = |rev| Ok(Ok(CasOutcome::Won(Revision(rev))));
        let retryable = || Ok(Err(StoreError::Retryable("x".to_owned())));
        assert!(judge("k", false, Revision(4), &won(5), &entry(b"1", 5)).is_empty());
        assert!(judge("k", true, Revision(4), &retryable(), &entry(b"1", 5)).is_empty());
        assert_eq!(
            judge("k", false, Revision(4), &retryable(), &entry(b"1", 5)).len(),
            1
        );
        assert_eq!(
            judge("k", true, Revision(4), &won(5), &entry(b"1", 5)).len(),
            1
        );
        assert_eq!(
            judge("k", false, Revision(4), &won(6), &entry(b"1", 5)).len(),
            1
        );
        assert_eq!(
            judge("k", false, Revision(4), &won(5), &entry(b"0", 4)).len(),
            1
        );
    }
}
