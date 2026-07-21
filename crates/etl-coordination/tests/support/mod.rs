//! Shared harness for the multi-worker integration suites: real
//! coordinators over one shared in-memory store, driven through the
//! public synchronous API exactly as a source would drive them.
// Each test binary compiles this module independently and uses a subset.
#![allow(dead_code, unreachable_pub)]

use etl_coordination::store::memory::MemoryStore;
use etl_coordination::{
    Clock, CoordinationConfig, CoordinationError, CoordinationEvent, MemoryCoordinator,
    PlanContext, PlanFinality, PlannedSplit, SplitCoordinator, SplitId, SplitPlan, SplitPlanner,
    SplitProgress, SplitSpec, StoreCoordinator,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Test lease: short enough for fast takeover tests, far above the
/// mechanical floor, and wide enough that "faster than a lease wait"
/// upper-bound assertions survive a CI scheduler stall. All timing
/// assertions use multiples of this.
pub const LEASE: Duration = Duration::from_millis(1500);

/// How long `drive` waits before declaring a scenario stuck.
/// Pacing for the direct-drive helpers below, which poll a coordinator
/// without a driver to park on.
pub const POLL_INTERVAL: Duration = Duration::from_millis(5);

pub const DEADLINE: Duration = Duration::from_secs(20);

pub fn config(instance_id: Option<&str>) -> CoordinationConfig {
    CoordinationConfig {
        lease_duration: LEASE,
        op_timeout: Duration::from_millis(200),
        instance_id: instance_id.map(str::to_string),
        replan_interval: LEASE,
        reconcile_interval: Duration::from_millis(300),
        // Both defaults are sized against the 30s production lease and
        // would swamp a 1.5s test one — `drain_deadline` would not even
        // validate. Scaled here rather than in `Default` so production
        // keeps the documented values.
        drain_deadline: LEASE / 2,
        // Zero by default: most tests assert that a dead worker's splits
        // flow back promptly, and a grace window would just add latency to
        // every one of them. The tests that care about the window set it.
        rebalance_delay: Duration::ZERO,
        ..CoordinationConfig::default()
    }
}

pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("test runtime")
}

pub fn store() -> MemoryStore {
    MemoryStore::new(LEASE)
}

/// A frozen (optionally advanceable) [`Clock`] for deterministic lease
/// fencing. `now()` is a fixed base plus an atomic offset, so a real
/// wall-clock stall (a CI scheduler starving the renewal task) can never
/// move it — the starvation self-fence and store expiry only fire when the
/// test advances time, not when the process is merely slow. Share one
/// instance between the store and the coordinator so both stay coherent.
#[derive(Debug)]
pub struct TestClock {
    base: tokio::time::Instant,
    offset_nanos: AtomicU64,
}

impl TestClock {
    /// A clock frozen at construction. Build it after [`runtime`] exists so
    /// `tokio::time::Instant::now()` reads the runtime's clock.
    pub fn frozen() -> Arc<TestClock> {
        Arc::new(TestClock {
            base: tokio::time::Instant::now(),
            offset_nanos: AtomicU64::new(0),
        })
    }

    /// Move the clock forward. Unused by the current suite — here so genuine
    /// expiry/takeover tests can drive time deterministically.
    pub fn advance(&self, by: Duration) {
        self.offset_nanos
            .fetch_add(by.as_nanos() as u64, Ordering::Relaxed);
    }
}

impl Clock for TestClock {
    fn now(&self) -> tokio::time::Instant {
        self.base + Duration::from_nanos(self.offset_nanos.load(Ordering::Relaxed))
    }
}

pub fn worker(
    store: &MemoryStore,
    io: &tokio::runtime::Handle,
    instance_id: Option<&str>,
) -> MemoryCoordinator {
    StoreCoordinator::new(store.clone(), config(instance_id), io.clone(), None)
        .expect("coordinator")
}

/// A worker with a non-default drain deadline. A long deadline makes the
/// forced revocation effectively never fire inside a test window (every
/// move must complete cooperatively); a very short one forces it, which is
/// how the replaying path is exercised on purpose.
pub fn worker_drain_deadline(
    store: &MemoryStore,
    io: &tokio::runtime::Handle,
    instance_id: Option<&str>,
    drain_deadline: Duration,
) -> MemoryCoordinator {
    let mut config = config(instance_id);
    config.drain_deadline = drain_deadline;
    StoreCoordinator::new(store.clone(), config, io.clone(), None).expect("coordinator")
}

/// A worker with a non-default rebalance delay. Tests that assert a
/// restart keeps its own work need a delay longer than the restart takes;
/// tests that assert prompt reassignment set it to zero.
pub fn worker_rebalance_delay(
    store: &MemoryStore,
    io: &tokio::runtime::Handle,
    instance_id: Option<&str>,
    rebalance_delay: Duration,
) -> MemoryCoordinator {
    let mut config = config(instance_id);
    config.rebalance_delay = rebalance_delay;
    StoreCoordinator::new(store.clone(), config, io.clone(), None).expect("coordinator")
}

/// A worker with a pinned lane budget, so a test can force the leader to
/// leave work queued or to spread it across members.
pub fn worker_max_in_flight(
    store: &MemoryStore,
    io: &tokio::runtime::Handle,
    instance_id: Option<&str>,
    max_in_flight: u32,
) -> MemoryCoordinator {
    let mut config = config(instance_id);
    config.max_in_flight = max_in_flight;
    StoreCoordinator::new(store.clone(), config, io.clone(), None).expect("coordinator")
}

/// A deterministic, cursor-driven planner: phase `n` returns
/// `phases[n]` and persists cursor `n + 1`; past the end it returns an
/// empty plan with the last phase's finality. Cursor-keyed, so replans
/// and leader changes are idempotent by construction.
pub struct PhasedPlanner {
    pub fingerprint: String,
    pub phases: Vec<(Vec<PlannedSplit>, PlanFinality)>,
}

impl PhasedPlanner {
    pub fn one_final(fingerprint: &str, ids: &[&str]) -> PhasedPlanner {
        PhasedPlanner {
            fingerprint: fingerprint.to_string(),
            phases: vec![(splits(ids), PlanFinality::Final)],
        }
    }
}

impl SplitPlanner for PhasedPlanner {
    fn fingerprint(&self) -> String {
        self.fingerprint.clone()
    }

    fn plan(&mut self, ctx: PlanContext<'_>) -> Result<SplitPlan, CoordinationError> {
        let index: usize = ctx
            .planner_state
            .map(|bytes| String::from_utf8_lossy(bytes).parse().expect("cursor"))
            .unwrap_or(0);
        let (splits, finality) = match self.phases.get(index) {
            Some((splits, finality)) => (splits.clone(), *finality),
            None => (
                Vec::new(),
                self.phases.last().map_or(PlanFinality::Open, |(_, f)| *f),
            ),
        };
        let next = (index + 1).min(self.phases.len());
        Ok(SplitPlan::new(splits, finality).with_planner_state(next.to_string().into_bytes()))
    }
}

pub fn splits(ids: &[&str]) -> Vec<PlannedSplit> {
    ids.iter()
        .map(|id| {
            PlannedSplit::new(SplitSpec::new(
                SplitId::new(*id).expect("test split id"),
                format!("descriptor:{id}").into_bytes(),
            ))
        })
        .collect()
}

pub fn split_id(id: &str) -> SplitId {
    SplitId::new(id).expect("test split id")
}

/// What a worker currently believes it holds, folded from its events.
#[derive(Default)]
pub struct Held {
    pub splits: BTreeMap<String, (u64, Option<SplitProgress>)>,
    pub all_complete: bool,
    pub stalled: Option<(u64, u64)>,
    pub quarantined: Vec<(String, u32)>,
    /// Revocations the leader has asked for and this worker has not yet
    /// answered. Recorded rather than acted on, because whether a source
    /// consents is exactly what several tests are varying: ignoring the
    /// request is the declining source, and `consent_to_revocations` is
    /// the cooperating one.
    pub revoke_requests: Vec<String>,
}

impl Held {
    pub fn fold(&mut self, events: Vec<CoordinationEvent>) {
        for event in events {
            match event {
                CoordinationEvent::Gained {
                    split,
                    epoch,
                    progress,
                } => {
                    self.splits
                        .insert(split.id.as_str().to_string(), (epoch.0, progress));
                }
                CoordinationEvent::Lost { split } => {
                    self.splits.remove(split.as_str());
                }
                CoordinationEvent::Quarantined { split, attempts } => {
                    self.splits.remove(split.as_str());
                    self.quarantined
                        .push((split.as_str().to_string(), attempts));
                }
                CoordinationEvent::RevokeRequested { split } => {
                    let id = split.as_str().to_string();
                    if !self.revoke_requests.contains(&id) {
                        self.revoke_requests.push(id);
                    }
                }
                CoordinationEvent::AllComplete => self.all_complete = true,
                CoordinationEvent::Stalled {
                    completed,
                    quarantined,
                } => self.stalled = Some((completed, quarantined)),
                _ => {}
            }
        }
    }
}

/// Drive one worker until `done` returns true, folding events as they
/// arrive. Panics (with `what`) on the deadline.
pub fn drive(
    coordinator: &mut impl SplitCoordinator,
    held: &mut Held,
    what: &str,
    mut done: impl FnMut(&Held) -> bool,
) {
    let deadline = Instant::now() + DEADLINE;
    while !done(held) {
        assert!(Instant::now() < deadline, "timed out: {what}");
        let events = coordinator
            .poll()
            .unwrap_or_else(|e| panic!("poll failed while {what}: {e}"));
        if events.is_empty() {
            // `poll` no longer blocks — the driver owns the wait in the
            // real pipeline. These helpers drive a coordinator directly,
            // so pace the predicate check rather than spinning hot.
            std::thread::sleep(POLL_INTERVAL);
        }
        held.fold(events);
    }
}

/// The watermark [`commit_held`] writes: what a running data plane has
/// durably committed before a rebalance starts.
pub const BASE_WATERMARK: i64 = 1;

/// The watermark [`consent_to_revocations`] commits as the drained tail.
/// Deliberately distinct from [`BASE_WATERMARK`] so a test can tell a
/// cooperative move from a forced one: a forced revocation hands the next
/// owner the base watermark, a drained one hands it this.
pub const DRAINED_WATERMARK: i64 = 42;

/// Answer every outstanding revocation the way a cooperating source does:
/// commit the split's drained tail to manufacture a resume point, then
/// release it. The release is what makes the transfer replay-free — the
/// next owner starts from a watermark covering everything this one
/// emitted, which is exactly what [`DRAINED_WATERMARK`] lets a test see.
pub fn consent_to_revocations(coordinator: &mut impl SplitCoordinator, held: &mut Held) {
    for id in std::mem::take(&mut held.revoke_requests) {
        if !held.splits.contains_key(&id) {
            continue; // already gone: forced, completed, or fenced
        }
        // A `Fenced` commit is normal — the lease may legitimately have
        // moved — and leaves the release below a no-op. Clamped upward for
        // the same reason `commit_held` is: a split can be drained more
        // than once, and watermarks never move backwards.
        let watermark = held.splits[&id]
            .1
            .as_ref()
            .map_or(DRAINED_WATERMARK, |p| p.watermark.max(DRAINED_WATERMARK));
        let _ = coordinator.commit(&split_id(&id), &SplitProgress::new(watermark, vec![]));
        let _ = coordinator.release_drained(&[split_id(&id)]);
        held.splits.remove(&id);
    }
}

/// Commit a first watermark ([`BASE_WATERMARK`]) for everything a worker
/// holds — what a running data plane does at its first checkpoint. A
/// `Fenced` commit is normal here (the lease may legitimately have moved)
/// and is ignored.
///
/// Never regresses: a split that arrived carrying a drained tail is
/// already past [`BASE_WATERMARK`], and the backend rejects a regressing
/// watermark as a source bug — correctly, so the helper must behave like a
/// real data plane and not walk backwards.
pub fn commit_held(coordinator: &mut impl SplitCoordinator, held: &Held) {
    for (id, (_, carried)) in &held.splits {
        let watermark = carried
            .as_ref()
            .map_or(BASE_WATERMARK, |p| p.watermark.max(BASE_WATERMARK));
        match coordinator.commit(&split_id(id), &SplitProgress::new(watermark, vec![])) {
            Ok(()) => {}
            Err(e) if e.kind == etl_coordination::CoordinationErrorKind::Fenced => {}
            Err(e) => panic!("commit failed: {e}"),
        }
    }
}

/// Drive two workers together until `done`. Panics on the deadline.
pub fn drive_pair<C: SplitCoordinator>(
    a: (&mut C, &mut Held),
    b: (&mut C, &mut Held),
    what: &str,
    mut done: impl FnMut(&Held, &Held) -> bool,
) {
    let deadline = Instant::now() + DEADLINE;
    while !done(a.1, b.1) {
        assert!(Instant::now() < deadline, "timed out: {what}");
        for (coordinator, held) in [(&mut *a.0, &mut *a.1), (&mut *b.0, &mut *b.1)] {
            let events = coordinator
                .poll()
                .unwrap_or_else(|e| panic!("poll failed while {what}: {e}"));
            if events.is_empty() {
                std::thread::sleep(POLL_INTERVAL);
            }
            held.fold(events);
        }
    }
}

/// Simulate a crash: the worker's runtime is torn down (its task dies
/// mid-heartbeat) and the handle is forgotten so no drop-time release
/// runs. Its leases must expire like a real dead process's.
pub fn crash<C: SplitCoordinator + 'static>(runtime: tokio::runtime::Runtime, coordinator: C) {
    runtime.shutdown_background();
    std::mem::forget(coordinator);
}
