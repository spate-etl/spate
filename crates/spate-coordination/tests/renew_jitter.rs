//! Split lease renewal under the heartbeat jitter.

mod support;

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{CasOutcome, CoordinationStore, Keyspace, Revision, StoreError};
use spate_coordination::{CoordinationEvent, SplitCoordinator, StoreCoordinator};
use spate_core::clock::tokio::Clock;
use std::hash::BuildHasher as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use support::tap::{Op, TapStore};
use support::{DEADLINE, Fleet, Held, PhasedPlanner, TestClock, config, runtime};

/// The clock step between polls.
const STEP: Duration = Duration::from_millis(5);

/// The worker's tick seed, as the coordinator derives it from its identity.
fn seed(instance: &str, nonce: &str) -> u64 {
    foldhash::fast::FixedState::with_seed(0).hash_one(format!("{instance}/{nonce}").as_str())
}

/// The heartbeat delay armed after round `round`.
fn jitter(seed: u64, round: u64, base: Duration) -> Duration {
    let h = foldhash::fast::FixedState::with_seed(seed).hash_one(round) % 1024;
    base.mul_f64(0.8 + 0.4 * (h as f64) / 1024.0)
}

fn r0_lost(event: &CoordinationEvent) -> bool {
    matches!(event, CoordinationEvent::Lost { split } if split.as_str() == "r0")
}

/// A solo worker holding split `r0` on a frozen clock, with its heartbeats,
/// `r0` renewal attempts and `r0` claims recorded by clock instant.
struct Rig {
    worker: StoreCoordinator<TapStore<MemoryStore>>,
    fleet: Fleet,
    clock: Arc<TestClock>,
    inner: MemoryStore,
    seed: u64,
    renew: Duration,
    lease: Duration,
    ticks: Arc<Mutex<Vec<tokio::time::Instant>>>,
    /// Each attempt's instant, and whether the injected failure took it.
    renewals: Arc<Mutex<Vec<(tokio::time::Instant, bool)>>>,
    claims: Arc<Mutex<Vec<tokio::time::Instant>>>,
    /// Fails the next `r0` renewal attempt.
    fail_next: Arc<AtomicBool>,
    rt: tokio::runtime::Runtime,
}

impl Rig {
    fn start() -> Rig {
        let rt = runtime();
        let clock = TestClock::frozen();
        let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
        let tap = TapStore::new(inner.clone());
        let cfg = config(Some("solo"));
        let (renew, lease) = (cfg.renew_interval(), cfg.lease_duration);

        let ticks: Arc<Mutex<Vec<tokio::time::Instant>>> = Arc::default();
        let renewals: Arc<Mutex<Vec<(tokio::time::Instant, bool)>>> = Arc::default();
        let claims: Arc<Mutex<Vec<tokio::time::Instant>>> = Arc::default();
        let fail_next = Arc::new(AtomicBool::new(false));
        {
            let (ticks, renewals, claims, fail_next, clock) = (
                ticks.clone(),
                renewals.clone(),
                claims.clone(),
                fail_next.clone(),
                clock.clone(),
            );
            tap.on_write(move |w| {
                if w.ks != Keyspace::Ephemeral {
                    return None;
                }
                match (w.op, w.key) {
                    (Op::Create, "split.r0") => claims.lock().unwrap().push(clock.now()),
                    (Op::Update, "worker.solo") => ticks.lock().unwrap().push(clock.now()),
                    (Op::Update, "split.r0") => {
                        let fail = fail_next.swap(false, Ordering::AcqRel);
                        renewals.lock().unwrap().push((clock.now(), fail));
                        if fail {
                            return Some(StoreError::Retryable("injected: renewal failed".into()));
                        }
                    }
                    _ => {}
                }
                None
            });
        }

        let mut worker = StoreCoordinator::with_clock(
            tap,
            cfg,
            rt.handle().clone(),
            None,
            clock.clone() as Arc<dyn Clock>,
        )
        .expect("coordinator");
        worker
            .start(Box::new(PhasedPlanner::one_final(
                "renew-jitter:v1",
                &["r0"],
            )))
            .unwrap();
        let mut fleet = Fleet::new(&inner, rt.handle());
        fleet.join(&worker);
        let mut held = Held::default();
        support::drive(&mut worker, &mut held, "claiming r0", |h| {
            h.splits.len() == 1
        });

        let entry = rt
            .block_on(inner.get(Keyspace::Ephemeral, "split.r0"))
            .unwrap()
            .expect("r0 is leased");
        let lease_val: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        let seed = seed("solo", lease_val["nonce"].as_str().expect("nonce"));
        Rig {
            worker,
            fleet,
            clock,
            inner,
            seed,
            renew,
            lease,
            ticks,
            renewals,
            claims,
            fail_next,
            rt,
        }
    }

    /// Advances the clock one [`STEP`] and polls the worker.
    fn step(&mut self) -> Vec<CoordinationEvent> {
        self.fleet.step(&self.clock, STEP);
        self.worker.poll().expect("poll")
    }

    /// The heartbeat delay armed after round `round`.
    fn jitter(&self, round: u64) -> Duration {
        jitter(self.seed, round, self.renew)
    }

    /// The heartbeats so far, each checked to come one jitter delay after the
    /// one before, rounded up to the next step.
    fn ticks(&self) -> Vec<tokio::time::Instant> {
        let ticks = self.ticks.lock().unwrap().clone();
        for (i, pair) in ticks.windows(2).enumerate() {
            let want = self.jitter(i as u64 + 1);
            let got = pair[1] - pair[0];
            assert!(
                got >= want && got < want + STEP,
                "tick {} came {got:?} after the last, computed {want:?}",
                i + 2
            );
        }
        ticks
    }

    fn lease_revision(&self) -> Revision {
        self.rt
            .block_on(self.inner.get(Keyspace::Ephemeral, "split.r0"))
            .unwrap()
            .expect("r0 is leased")
            .revision
    }

    /// The instants after `from` in `at`, as ages since `from`.
    fn ages<T: Copy>(
        from: tokio::time::Instant,
        at: &[(tokio::time::Instant, T)],
    ) -> Vec<(Duration, T)> {
        at.iter()
            .filter(|(t, _)| *t >= from)
            .map(|(t, v)| (*t - from, *v))
            .collect()
    }
}

/// A split keeps its lease through one failed renewal when the next heartbeat
/// after a confirmed renewal comes under one renew interval and the three
/// heartbeats from that renewal span a lease. Regression for #979.
#[test]
fn one_failed_renewal_after_an_early_tick_keeps_the_split() {
    let mut rig = Rig::start();
    let (renew, lease) = (rig.renew, rig.lease);

    // Step until a tick renews r0 and the next three delays are an early
    // tick, then two that bring the third tick to a lease or later.
    let deadline = Instant::now() + DEADLINE * 6;
    let mut seen = 0;
    let (round, renewed_at) = loop {
        assert!(Instant::now() < deadline, "no losing window found");
        let events = rig.step();
        assert!(!events.iter().any(r0_lost), "lost before the fault");
        let ticks = rig.ticks();
        if ticks.len() == seen {
            continue;
        }
        seen = ticks.len();
        let at = *ticks.last().unwrap();
        let renewed = rig.renewals.lock().unwrap().last().copied() == Some((at, false));
        let round = seen as u64;
        let js = [0, 1, 2].map(|d| rig.jitter(round + d));
        if renewed && js[0] + STEP <= renew && js.iter().sum::<Duration>() >= lease {
            rig.fail_next.store(true, Ordering::Release);
            break (round, at);
        }
    };
    let revision_at_fault = rig.lease_revision();

    let mut lost_at = None;
    while rig.clock.now() < renewed_at + lease + renew {
        let events = rig.step();
        if lost_at.is_none() && events.iter().any(r0_lost) {
            lost_at = Some(rig.clock.now() - renewed_at);
        }
    }

    let ticks: Vec<Duration> = rig
        .ticks()
        .into_iter()
        .filter(|t| *t >= renewed_at)
        .map(|t| t - renewed_at)
        .collect();
    let renewals = Rig::ages(renewed_at, &rig.renewals.lock().unwrap());
    let timeline = format!(
        "round {round}; ages after the last confirmed renewal: ticks {ticks:?}, \
         renewal attempts (age, injected failure) {renewals:?}, Lost at {lost_at:?}"
    );
    assert_eq!(
        renewals.iter().filter(|(_, failed)| *failed).count(),
        1,
        "exactly one renewal failed: {timeline}"
    );
    assert!(
        lost_at.is_none(),
        "one failed renewal cost the split: {timeline}"
    );
    let reclaimed = rig.claims.lock().unwrap().iter().any(|t| *t >= renewed_at);
    assert!(
        !reclaimed && rig.lease_revision() > revision_at_fault,
        "no renewal won after the failure: {timeline}"
    );
}

/// The first heartbeat after a claim renews the split however soon after
/// the claim it comes, so a failed renewal there leaves a second attempt
/// inside the lease. Regression for #979.
#[test]
fn the_first_heartbeat_after_a_claim_renews_the_lease() {
    let mut rig = Rig::start();

    let deadline = Instant::now() + DEADLINE;
    while rig.ticks().is_empty() {
        assert!(Instant::now() < deadline, "no heartbeat");
        rig.step();
    }
    let ticks = rig.ticks();
    let n = ticks.len();
    let due = *ticks.last().unwrap() + rig.jitter(n as u64);
    while rig.clock.now() + STEP * 2 < due {
        rig.step();
    }
    assert_eq!(rig.ticks().len(), n, "a heartbeat came before {due:?}");

    // Deleting the lease drops r0 and the worker claims it again.
    let deleted_at = rig.clock.now();
    let deleted = rig
        .rt
        .block_on(rig.inner.delete(Keyspace::Ephemeral, "split.r0", None))
        .unwrap();
    assert!(matches!(deleted, CasOutcome::Won(_)), "delete r0's lease");
    let claimed_at = loop {
        assert!(
            rig.clock.now() < due,
            "r0 was not claimed again before the next heartbeat"
        );
        rig.step();
        let claims = rig.claims.lock().unwrap().clone();
        if let Some(at) = claims.into_iter().find(|t| *t >= deleted_at) {
            break at;
        }
    };

    while rig.ticks().len() == n {
        rig.step();
    }
    let ticks = rig.ticks();
    let tick = ticks[n];
    let renewals = Rig::ages(claimed_at, &rig.renewals.lock().unwrap());
    let ticks: Vec<Duration> = ticks
        .into_iter()
        .filter(|t| *t >= claimed_at)
        .map(|t| t - claimed_at)
        .collect();
    assert!(
        renewals.iter().any(|(age, _)| *age == tick - claimed_at),
        "no renewal at the heartbeat {:?} after the claim; ages after the claim: ticks \
         {ticks:?}, renewal attempts {renewals:?}",
        tick - claimed_at
    );
}
