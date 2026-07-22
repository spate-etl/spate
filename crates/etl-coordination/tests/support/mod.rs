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

/// The deterministic clock lives in the crate, behind its `testing`
/// feature — these are external test binaries, so a `#[cfg(test)]` item
/// would be invisible to them. Re-exported here so the suites read
/// unchanged. Share one instance between the store and the coordinator so
/// expiry, the self-fence, and the control-loop cadence stay coherent.
pub use etl_coordination::clock::TestClock;

/// A store whose ephemeral expiry runs on `clock` rather than wall time.
pub fn store_with_clock(clock: Arc<dyn Clock>) -> MemoryStore {
    MemoryStore::with_clock(LEASE, clock)
}

/// A worker whose protocol timing runs on `clock`. Pass the same clock the
/// store was built with.
pub fn worker_with_clock(
    store: &MemoryStore,
    io: &tokio::runtime::Handle,
    instance_id: Option<&str>,
    clock: Arc<dyn Clock>,
) -> MemoryCoordinator {
    StoreCoordinator::with_clock(store.clone(), config(instance_id), io.clone(), None, clock)
        .expect("coordinator")
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

/// [`worker_rebalance_delay`] on an injected clock, so a grace-window test
/// can advance time deterministically instead of waiting the window out in
/// wall-clock. Build the store with [`store_with_clock`] and the same clock.
pub fn worker_rebalance_delay_clock(
    store: &MemoryStore,
    io: &tokio::runtime::Handle,
    instance_id: Option<&str>,
    rebalance_delay: Duration,
    clock: Arc<dyn Clock>,
) -> MemoryCoordinator {
    let mut config = config(instance_id);
    config.rebalance_delay = rebalance_delay;
    StoreCoordinator::with_clock(store.clone(), config, io.clone(), None, clock)
        .expect("coordinator")
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

/// Like [`drive`], but for a worker on a **frozen** clock: advance `clock`
/// a fraction of a renew-interval each poll so the protocol keeps making
/// progress (heartbeat, reconcile, replan, and the completion sweep all read
/// the clock now, so a never-advanced frozen clock stalls them all).
///
/// The step is small enough that a live worker renews inside it — the "advance
/// to settle" pattern from [`etl_coordination::clock`] — so a genuine
/// self-fence never fires merely because the test moved time in one jump.
pub fn drive_clocked(
    coordinator: &mut impl SplitCoordinator,
    clock: &TestClock,
    held: &mut Held,
    what: &str,
    mut done: impl FnMut(&Held) -> bool,
) {
    let step = LEASE / 12;
    let deadline = Instant::now() + DEADLINE;
    while !done(held) {
        assert!(Instant::now() < deadline, "timed out: {what}");
        clock.advance(step);
        std::thread::sleep(POLL_INTERVAL);
        held.fold(
            coordinator
                .poll()
                .unwrap_or_else(|e| panic!("poll failed while {what}: {e}")),
        );
    }
}

/// Poll `check` until it returns true or `timeout` elapses; panics with
/// `what` on timeout. Mirrors `etl_test::wait_until`, which this crate
/// cannot reach (`etl-test` is not a dev-dependency here). Use it instead
/// of sleeping a guessed interval: a sleep long enough for a loaded CI
/// scheduler is dead time on every other run, and one that is not is a
/// flake.
pub fn wait_until(timeout: Duration, what: &str, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check() {
            return;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    panic!("timed out after {timeout:?} waiting for: {what}");
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
/// Like [`commit_held`], but silent about `skip`.
///
/// A committing split is a *live* one as far as the coordinator is
/// concerned — a landed commit is the only progress signal a draining split
/// gives it, and what a cancelled revocation's watchdog is armed against.
/// Excluding a split here is therefore how a test stages a wedged drain: it
/// keeps the worker healthy while one split goes quiet.
pub fn commit_held_except(coordinator: &mut impl SplitCoordinator, held: &Held, skip: &[String]) {
    for (id, (_, carried)) in &held.splits {
        if skip.contains(id) {
            continue;
        }
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

/// Drive two **cooperating** workers on a frozen `clock` until the fleet
/// reaches `done` **and stays there without being asked to give anything
/// up**. Returns with both revocation slates provably empty.
///
/// Unlike [`drive_pair`], this one answers revocations: it commits and
/// releases on both sides every iteration, so a hand-off completes the
/// moment the leader asks rather than waiting out `drain_deadline`. Use it
/// to reach a settled baseline before asserting on what a fleet does next
/// — a mid-rebalance fleet revokes for its own reasons, and those requests
/// linger in [`Held::revoke_requests`], which nothing but
/// [`consent_to_revocations`] ever drains. A test that needs a declining
/// source wants [`drive_pair`] instead.
///
/// Reaching `done` is not enough on its own: the balancer can satisfy a
/// split-count predicate mid-rebalance and revoke again a step later. So
/// after `done` first holds this runs [`QUIET_ROUNDS`] further rounds
/// *without* consenting — if a request arrives, the fleet was not settled,
/// so it consents and goes back to converging. Because the quiet rounds
/// drain nothing, an empty slate on exit is a measurement rather than an
/// artefact of having just drained it.
///
/// The clock step matches [`drive_clocked`]'s: a fraction of a
/// renew-interval, so both workers stay live across it (advance to settle).
pub fn settle_pair_clocked<C: SplitCoordinator>(
    a: (&mut C, &mut Held),
    b: (&mut C, &mut Held),
    clock: &TestClock,
    what: &str,
    mut done: impl FnMut(&Held, &Held) -> bool,
) {
    let step = LEASE / 12;
    let deadline = Instant::now() + DEADLINE;
    // One clocked round: advance, let the tasks run, fold, commit. Consent
    // is the caller's choice, because whether requests are drained is
    // exactly what distinguishes converging from proving quiescence.
    let round = |a: (&mut C, &mut Held), b: (&mut C, &mut Held), consent: bool| {
        clock.advance(step);
        std::thread::sleep(POLL_INTERVAL);
        for (coordinator, held) in [(a.0, a.1), (b.0, b.1)] {
            held.fold(
                coordinator
                    .poll()
                    .unwrap_or_else(|e| panic!("poll failed while {what}: {e}")),
            );
            commit_held(coordinator, held);
            if consent {
                consent_to_revocations(coordinator, held);
            }
        }
    };
    loop {
        assert!(Instant::now() < deadline, "timed out: {what}");
        if !done(a.1, b.1) {
            round((&mut *a.0, &mut *a.1), (&mut *b.0, &mut *b.1), true);
            continue;
        }
        // `done` holds. Hold the fleet still and see whether it stays that
        // way without us answering anything.
        for _ in 0..QUIET_ROUNDS {
            round((&mut *a.0, &mut *a.1), (&mut *b.0, &mut *b.1), false);
        }
        let disturbed =
            !a.1.revoke_requests.is_empty() || !b.1.revoke_requests.is_empty() || !done(a.1, b.1);
        if !disturbed {
            return;
        }
        // Still rebalancing: answer what it asked for and converge again.
        for (coordinator, held) in [(&mut *a.0, &mut *a.1), (&mut *b.0, &mut *b.1)] {
            consent_to_revocations(coordinator, held);
        }
    }
}

/// How many revocation-free rounds prove a fleet has stopped moving. Each
/// round is a clock step of `LEASE / 12`, so this spans a renew-interval —
/// long enough for the leader to have reconciled and republished, which is
/// when a spurious revocation would surface.
pub const QUIET_ROUNDS: usize = 4;

/// Simulate a crash: the worker's runtime is torn down (its task dies
/// mid-heartbeat) and the handle is forgotten so no drop-time release
/// runs. Its leases must expire like a real dead process's.
pub fn crash<C: SplitCoordinator + 'static>(runtime: tokio::runtime::Runtime, coordinator: C) {
    runtime.shutdown_background();
    std::mem::forget(coordinator);
}
