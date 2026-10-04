//! A poison report refused by the store and queued in the driver must not
//! fail a later tenancy of the same split. Real `StoreCoordinator` over a
//! `MemoryStore` on a test clock, driven by the real `CoordinationDriver`.

mod support;

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchStream,
};
use spate_coordination::{
    ControlWaker, CoordinationError, CoordinationEvent, LeaseEpoch, SplitCoordinator, SplitPlanner,
};
use spate_coordination::{
    CoordinationErrorKind, SplitId, SplitProgress, SplitSpec, StoreCoordinator,
};
use spate_core::checkpoint::AckRef;
use spate_core::clock::tokio::Clock;
use spate_core::coordination::driver::{CoordinationDriver, SplitOpening, SplitSource};
use spate_core::error::{ErrorClass, SourceError};
use spate_core::record::{PartitionId, RawPayload};
use spate_core::source::{LaneId, PayloadBatch, SourceEvent, SourceLane};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use support::{DEADLINE, Held, LEASE, PhasedPlanner, TestClock, config, split_id};

#[derive(Clone)]
struct RefuseStore {
    inner: MemoryStore,
    /// While set, durable `split.` updates fail Retryable, as a store outage does.
    refuse: Arc<AtomicBool>,
    /// Durable `split.` updates refused so far.
    refused: Arc<AtomicU64>,
}

impl CoordinationStore for RefuseStore {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }
    async fn create(&self, ks: Keyspace, key: &str, v: Vec<u8>) -> Result<CasOutcome, StoreError> {
        self.inner.create(ks, key, v).await
    }
    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        v: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        if ks == Keyspace::Durable
            && key.starts_with("split.")
            && self.refuse.load(Ordering::Acquire)
        {
            self.refused.fetch_add(1, Ordering::AcqRel);
            return Err(StoreError::Retryable("injected: store unavailable".into()));
        }
        self.inner.update(ks, key, v, expected).await
    }
    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        self.inner.get(ks, key).await
    }
    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.delete(ks, key, expected).await
    }
    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }
    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
}

enum NoBatch {}
impl<'b> PayloadBatch<'b> for NoBatch {
    fn next_payload(&mut self) -> Option<RawPayload<'b>> {
        match *self {}
    }
    fn ack(&self) -> &AckRef {
        match *self {}
    }
}
struct Lane(LaneId, PartitionId);
impl SourceLane for Lane {
    type Batch<'a> = NoBatch;
    fn id(&self) -> LaneId {
        self.0
    }
    fn partition(&self) -> PartitionId {
        self.1
    }
    fn poll(&mut self, _: usize, _: Duration) -> Result<Option<NoBatch>, SourceError> {
        Ok(None)
    }
}

/// Refuses the carried progress of exactly one gain once armed.
struct Source {
    reject_next_resume: Cell<bool>,
    partitions: Vec<PartitionId>,
}
impl SplitSource for Source {
    type Lane = Lane;
    fn open_split(&mut self, o: SplitOpening<'_>) -> Result<Lane, SourceError> {
        self.partitions.push(o.partition);
        Ok(Lane(o.lane, o.partition))
    }
    fn validate_resume(&self, _: &SplitSpec, _: &SplitProgress) -> Result<(), SourceError> {
        if self.reject_next_resume.replace(false) {
            return Err(SourceError::Client {
                class: ErrorClass::Retryable,
                reason: "resume drift".into(),
            });
        }
        Ok(())
    }
    fn encode_commit(&mut self, _: &SplitId, w: i64) -> Result<SplitProgress, SourceError> {
        Ok(SplitProgress::new(w, vec![]))
    }
    fn sweep(&mut self, _: &SplitId) -> Result<Option<SplitProgress>, SourceError> {
        Ok(None)
    }
    fn close_split(&mut self, _: &SplitId) {}
}

type Hook = Arc<Mutex<Option<Box<dyn FnOnce() + Send>>>>;

/// Forwards every method to `inner`; `poll` runs the armed hook once, after the inner drain returns.
struct Hooked<C> {
    inner: C,
    hook: Hook,
}

impl<C: SplitCoordinator> SplitCoordinator for Hooked<C> {
    fn start(&mut self, planner: Box<dyn SplitPlanner>) -> Result<(), CoordinationError> {
        self.inner.start(planner)
    }
    fn set_waker(&mut self, waker: ControlWaker) {
        self.inner.set_waker(waker)
    }
    fn poll(&mut self) -> Result<Vec<CoordinationEvent>, CoordinationError> {
        let out = self.inner.poll();
        let hook = self.hook.lock().unwrap().take();
        if let Some(hook) = hook {
            hook();
        }
        out
    }
    fn commit(
        &mut self,
        split: &SplitId,
        progress: &SplitProgress,
    ) -> Result<(), CoordinationError> {
        self.inner.commit(split, progress)
    }
    fn fail(
        &mut self,
        split: &SplitId,
        epoch: LeaseEpoch,
        reason: &str,
    ) -> Result<(), CoordinationError> {
        self.inner.fail(split, epoch, reason)
    }
    fn release(&mut self, splits: &[SplitId]) -> Result<(), CoordinationError> {
        self.inner.release(splits)
    }
    fn depart(&mut self, held: &[SplitId]) -> Result<(), CoordinationError> {
        self.inner.depart(held)
    }
    fn release_drained(&mut self, splits: &[SplitId]) -> Result<(), CoordinationError> {
        self.inner.release_drained(splits)
    }
    fn decline_revoke(&mut self, split: &SplitId) -> Result<(), CoordinationError> {
        self.inner.decline_revoke(split)
    }
}

/// Where the loss of the rejected gain's tenancy and the next regain land.
#[derive(Clone, Copy)]
enum Window {
    /// The rejected gain is polled first; the loss and regain share a later batch.
    LaterBatch,
    /// The rejected gain, the loss of its tenancy and the regain drain in one batch, with
    /// the store refusing split writes during that poll while `outage` is set.
    SameBatch { outage: bool },
    /// The loss and regain land between a later poll's drain and its re-offer of the queued report.
    AfterDrain,
}

/// The split's state after the driver polls the regained tenancy.
struct Outcome {
    /// The durable record's attempts when the regained tenancy was claimed.
    before: u64,
    after: u64,
    record: serde_json::Value,
    lease_held: bool,
    assigned: bool,
}

fn run(window: Window) -> Outcome {
    let rt = support::runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(LEASE, clock.clone());
    let refuse = Arc::new(AtomicBool::new(false));
    let refused = Arc::new(AtomicU64::new(0));
    let store = RefuseStore {
        inner: inner.clone(),
        refuse: refuse.clone(),
        refused: refused.clone(),
    };
    let mut cfg = config(Some("solo"));
    cfg.max_attempts = 50;
    let worker = StoreCoordinator::with_clock(
        store,
        cfg,
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    let mut fleet = support::Fleet::new(&inner, rt.handle());
    fleet.join(&worker);
    let mut hook_fleet = support::Fleet::new(&inner, rt.handle());
    hook_fleet.join(&worker);
    let hook: Hook = Arc::new(Mutex::new(None));
    let mut d = CoordinationDriver::new(Box::new(Hooked {
        inner: worker,
        hook: hook.clone(),
    }));
    let mut src = Source {
        reject_next_resume: Cell::new(false),
        partitions: vec![],
    };
    let _: SourceEvent<Lane> = d
        .start(Box::new(PhasedPlanner::one_final(
            "stale-poison:v1",
            &["a"],
        )))
        .unwrap();

    let record = || -> serde_json::Value {
        let e = rt
            .block_on(inner.get(Keyspace::Durable, "split.a"))
            .unwrap()
            .unwrap();
        serde_json::from_slice(&e.value).unwrap()
    };
    let lease_gone = || {
        rt.block_on(inner.get(Keyspace::Ephemeral, "split.a"))
            .unwrap()
            .is_none()
    };
    let step_until = |what: &str, mut cond: Box<dyn FnMut() -> bool + '_>| {
        let deadline = Instant::now() + DEADLINE;
        while !cond() {
            assert!(Instant::now() < deadline, "timed out: {what}");
            fleet.step(&clock, LEASE / 12);
        }
    };
    let expire_lease = || {
        rt.block_on(inner.delete(Keyspace::Ephemeral, "split.a", None))
            .unwrap()
    };

    // Tenancy 1, with committed progress.
    let deadline = Instant::now() + DEADLINE;
    while d.assignments().is_empty() {
        assert!(Instant::now() < deadline, "no gain");
        fleet.step(&clock, LEASE / 12);
        let _ = d.poll_events(&mut src, Duration::ZERO);
    }
    let first = src.partitions[0];
    d.commit(&mut src, &[(first, 5)]).unwrap();

    // The lease lapses; the store reclaims the split (carrying progress) while the driver is not polling.
    let epoch = record()["epoch"].as_u64().unwrap();
    let _ = expire_lease();
    step_until(
        "regain 2",
        Box::new(|| record()["epoch"].as_u64().unwrap() > epoch && !lease_gone()),
    );

    let before = Arc::new(AtomicU64::new(u64::MAX));
    let hook_ran = Arc::new(AtomicBool::new(false));
    match window {
        Window::SameBatch { outage } => {
            // The lease lapses again and the store regains the split (epoch 3), all before the driver polls.
            let epoch = record()["epoch"].as_u64().unwrap();
            let _ = expire_lease();
            step_until(
                "regain 3",
                Box::new(|| record()["epoch"].as_u64().unwrap() > epoch && !lease_gone()),
            );
            // The driver rejects the epoch 2 gain and reports it inside this poll, while the
            // batch also ends that tenancy and regains the split.
            before.store(record()["attempts"].as_u64().unwrap(), Ordering::Release);
            refuse.store(outage, Ordering::Release);
            src.reject_next_resume.set(true);
            assert!(
                d.poll_events(&mut src, Duration::ZERO).is_err(),
                "gain rejected"
            );
            refuse.store(false, Ordering::Release);
        }
        Window::LaterBatch | Window::AfterDrain => {
            // The store goes unavailable; the driver rejects that gain, its report is refused and queued.
            refuse.store(true, Ordering::Release);
            src.reject_next_resume.set(true);
            assert!(
                d.poll_events(&mut src, Duration::ZERO).is_err(),
                "gain rejected"
            );
            // Deliver the staged revoke of tenancy 1; the report is re-offered and refused each time.
            for _ in 0..3 {
                let _ = d.poll_events(&mut src, Duration::ZERO);
            }
        }
    }
    match window {
        Window::SameBatch { .. } => {}
        Window::LaterBatch => {
            // Still unavailable: the lease lapses again and the store drops the split (Lost is queued).
            let epoch = record()["epoch"].as_u64().unwrap();
            let _ = expire_lease();
            // A refused claim attempt means the task has dropped the split and queued Lost.
            let seen = refused.load(Ordering::Acquire);
            let deadline = Instant::now() + DEADLINE;
            while refused.load(Ordering::Acquire) == seen {
                assert!(Instant::now() < deadline, "timed out: loss");
                clock.advance(LEASE / 12);
                std::thread::sleep(support::POLL_INTERVAL);
            }
            // The store recovers and reclaims the split (epoch + 1).
            refuse.store(false, Ordering::Release);
            step_until(
                "regain 3",
                Box::new(|| record()["epoch"].as_u64().unwrap() > epoch && !lease_gone()),
            );
            before.store(record()["attempts"].as_u64().unwrap(), Ordering::Release);
        }
        Window::AfterDrain => {
            // The store recovers. Between the next poll's drain and its re-offer, the lease
            // lapses and the store regains the split at the next epoch.
            refuse.store(false, Ordering::Release);
            let inner = inner.clone();
            let io = rt.handle().clone();
            let clock = clock.clone();
            let before = before.clone();
            let hook_ran = hook_ran.clone();
            *hook.lock().unwrap() = Some(Box::new(move || {
                let record = || -> serde_json::Value {
                    let e = io
                        .block_on(inner.get(Keyspace::Durable, "split.a"))
                        .unwrap()
                        .unwrap();
                    serde_json::from_slice(&e.value).unwrap()
                };
                let lease_gone = || {
                    io.block_on(inner.get(Keyspace::Ephemeral, "split.a"))
                        .unwrap()
                        .is_none()
                };
                let epoch = record()["epoch"].as_u64().unwrap();
                let _ = io
                    .block_on(inner.delete(Keyspace::Ephemeral, "split.a", None))
                    .unwrap();
                let deadline = Instant::now() + DEADLINE;
                while record()["epoch"].as_u64().unwrap() <= epoch || lease_gone() {
                    assert!(Instant::now() < deadline, "timed out: regain in hook");
                    hook_fleet.step(&clock, LEASE / 12);
                }
                before.store(record()["attempts"].as_u64().unwrap(), Ordering::Release);
                hook_ran.store(true, Ordering::Release);
            }));
        }
    }

    for _ in 0..4 {
        let _ = d.poll_events(&mut src, Duration::ZERO);
    }
    if let Window::AfterDrain = window {
        assert!(hook_ran.load(Ordering::Acquire), "the hook ran");
    }
    let record = record();
    Outcome {
        before: before.load(Ordering::Acquire),
        after: record["attempts"].as_u64().unwrap(),
        record,
        lease_held: !lease_gone(),
        assigned: d.assignments().iter().any(|(id, _)| id.as_str() == "a"),
    }
}

/// The loss of a split and its regain drain in one batch; the report queued
/// before the loss must not fail the regained tenancy.
#[test]
fn stale_report_does_not_fail_a_regained_split_in_one_batch() {
    let out = run(Window::LaterBatch);
    assert_eq!(
        out.after, out.before,
        "the stale report failed the regained tenancy"
    );
}

/// A gain rejected in the batch that also loses and regains its split queues a
/// report that must not fail the regained tenancy. Regression for #904.
#[test]
fn report_queued_by_the_batch_that_loses_its_split_is_dropped() {
    let out = run(Window::SameBatch { outage: true });
    assert_eq!(
        out.after, out.before,
        "the stale report failed the regained tenancy"
    );
}

/// A rejected gain's report, made on a healthy store while the same batch
/// already regains the split, leaves the regained tenancy's attempts and lane
/// alone. Regression for #909.
#[test]
fn immediate_report_for_an_earlier_tenancy_does_not_fail_the_regained_split() {
    let out = run(Window::SameBatch { outage: false });
    assert_eq!(
        out.after, out.before,
        "the rejected gain's immediate report failed the regained tenancy"
    );
    assert!(out.assigned, "the regained split stays assigned");
}

/// A report queued for one tenancy and re-offered after a loss and regain that
/// land between the drain and the re-offer leaves the regained tenancy whole.
/// Regression for #909.
#[test]
fn report_queued_before_a_loss_and_regain_after_the_drain_does_not_fail_the_regained_split() {
    let out = run(Window::AfterDrain);
    assert_eq!(
        out.after, out.before,
        "the stale report failed the regained tenancy"
    );
    assert_eq!(out.record["owner"], "solo", "record {}", out.record);
    assert!(out.lease_held, "the regained tenancy's lease is held");
    assert!(out.assigned, "the regained split stays assigned");
}

/// A report for another tenancy of a held split is `Fenced`: it writes
/// nothing, emits no `Lost` and leaves the lease, and a report for the held
/// tenancy then applies.
#[test]
fn a_report_for_another_tenancy_is_fenced_and_keeps_the_split() {
    let rt = support::runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(LEASE, clock.clone());
    let mut cfg = config(Some("solo"));
    cfg.max_attempts = 50;
    let mut worker = StoreCoordinator::with_clock(
        inner.clone(),
        cfg,
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final(
            "stale-poison:v1",
            &["a"],
        )))
        .unwrap();
    let mut fleet = support::Fleet::new(&inner, rt.handle());
    fleet.join(&worker);
    let mut held = Held::default();
    let mut step_until = |what: &str, held: &mut Held, done: &dyn Fn(&Held) -> bool| {
        let deadline = Instant::now() + DEADLINE;
        while !done(held) {
            assert!(Instant::now() < deadline, "timed out: {what}");
            fleet.step(&clock, LEASE / 12);
            held.fold(worker.poll().expect("poll"));
        }
    };
    step_until("claiming a", &mut held, &|h| h.splits.contains_key("a"));
    // The lease lapses and the worker regains the split at the next epoch.
    let first = held.splits["a"].0;
    let _ = rt
        .block_on(inner.delete(Keyspace::Ephemeral, "split.a", None))
        .unwrap();
    step_until("regaining a", &mut held, &|h| {
        h.splits.get("a").is_some_and(|(e, _)| *e > first)
    });
    let epoch = held.splits["a"].0;
    assert_eq!(epoch, 2);

    let record = || -> serde_json::Value {
        let e = rt
            .block_on(inner.get(Keyspace::Durable, "split.a"))
            .unwrap()
            .unwrap();
        serde_json::from_slice(&e.value).unwrap()
    };
    let lease_held = || {
        rt.block_on(inner.get(Keyspace::Ephemeral, "split.a"))
            .unwrap()
            .is_some()
    };
    let attempts = record()["attempts"].as_u64().unwrap();
    for other in [epoch - 1, epoch + 1] {
        let r = worker.fail(&split_id("a"), LeaseEpoch(other), "poison");
        assert!(
            matches!(&r, Err(e) if e.kind == CoordinationErrorKind::Fenced),
            "epoch {other}: {r:?}"
        );
    }
    let events = worker.poll().expect("poll");
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, CoordinationEvent::Lost { split } if split.as_str() == "a")),
        "{events:?}"
    );
    let after = record();
    assert_eq!(after["attempts"], attempts, "record {after}");
    assert_eq!(after["owner"], "solo", "record {after}");
    assert!(lease_held(), "the lease is kept");

    worker
        .fail(&split_id("a"), LeaseEpoch(epoch), "poison")
        .expect("the held tenancy's report applies");
    assert_eq!(record()["attempts"], attempts + 1);
}
