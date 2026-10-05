//! Fault-injection regressions: scripted store failures at the exact
//! writes whose loss used to wedge the protocol. Both scenarios run over
//! the real coordinator through the public API; the fault store is a
//! [`CoordinationStore`] like any custom backend.

mod support;

use futures_util::StreamExt;
use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchStream,
};
use spate_coordination::{
    CoordinationConfig, CoordinationEvent, LeaseEpoch, SplitCoordinator, SplitProgress,
    StoreCoordinator,
};
use spate_core::clock::tokio::Clock;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use support::tap::TapStore;
use support::{DEADLINE, Held, PhasedPlanner, TestClock, config, runtime, split_id};

/// The clock a stalled write advances, and by how much.
type Stall = (Arc<TestClock>, Duration);

/// A slot of the first reconcile under [`SLOTTED_RECONCILE`].
const RECONCILE_SLOT: Duration = support::LEASE.saturating_mul(8);

/// A `reconcile_interval` whose first reconcile, at
/// `protocol::spread(seed, interval)`, falls on a whole multiple of
/// [`RECONCILE_SLOT`], whatever the seed.
const SLOTTED_RECONCILE: Duration = RECONCILE_SLOT.saturating_mul(1024);

/// What [`FaultStore`] does with the next create of the leader key.
enum LeaderCreate {
    /// Applies, then returns an error.
    Lands,
    /// Applies, then returns `Lost`.
    LandsAsLost,
    /// Creates the key with this peer's value, then returns an error.
    PeerFirst(Vec<u8>),
    /// Creates the key with this peer's value, then returns `Lost`.
    PeerFirstLost(Vec<u8>),
    /// Applies, advances the clock, then returns the outcome.
    Slow(Stall),
    /// Writes nothing and returns an error; the next leader-key read
    /// answers with this entry.
    StaleRead(Entry),
}

/// One split-lease update as [`FaultStore`] received it.
#[derive(Debug)]
#[expect(dead_code, reason = "read through `Debug` in assertion messages")]
struct LeaseUpdate {
    epoch: Option<u64>,
    owner: Option<String>,
    expected: Revision,
    after_act: bool,
}

/// A [`MemoryStore`] with scripted faults on specific writes.
#[derive(Clone)]
struct FaultStore {
    inner: MemoryStore,
    /// Per-call script for durable updates of the plan record: `true`
    /// fails the call (Retryable, nothing written). Exhausted = pass.
    plan_update_script: Arc<Mutex<VecDeque<bool>>>,
    /// Once: the next ephemeral split-lease update WRITES but returns an
    /// error, the maybe-landed renewal.
    lease_maybe_land: Arc<AtomicBool>,
    /// Split-lease updates that won since the maybe-landed renewal.
    renewals_after_fault: Arc<AtomicU64>,
    /// The split lease the maybe-landed renewal wrote.
    lease_fault_key: Arc<Mutex<Option<String>>>,
    /// Split-lease update calls per key, in arrival order.
    lease_updates: Arc<Mutex<BTreeMap<String, Vec<LeaseUpdate>>>>,
    /// Set once the test's act has returned; stamped on each lease update.
    act_returned: Arc<AtomicBool>,
    /// The split lease before the maybe-landed renewal, for
    /// `lease_stale_reads`.
    lease_before: Arc<Mutex<Option<Entry>>>,
    /// Reads of the faulted split lease still to answer with the lease as
    /// it stood before the maybe-landed renewal.
    lease_stale_reads: Arc<Mutex<u64>>,
    /// Once: the next delete of the faulted split lease at its current
    /// revision fails without applying.
    lease_delete_fails: Arc<AtomicBool>,
    /// Once: the next read of the faulted split lease after the act returns
    /// fails.
    lease_settle_read_fails: Arc<AtomicBool>,
    /// Reads of the faulted split lease after the act returns.
    lease_reads_after_act: Arc<AtomicU64>,
    /// When set, the maybe-landed renewal advances this clock by this much
    /// before it returns, as a reply lost to `op_timeout` does.
    lease_stall: Arc<Mutex<Option<Stall>>>,
    /// The clock when a stalled maybe-landed renewal wrote.
    lease_fault_at: Arc<Mutex<Option<tokio::time::Instant>>>,
    /// While set: the first update of the faulted split lease after the
    /// maybe-landed renewal passes, and every later one fails with nothing
    /// written.
    lease_refuse_after_adopt: Arc<AtomicBool>,
    /// The outcome of the update `lease_refuse_after_adopt` passed.
    lease_adopting_cas: Arc<Mutex<Option<Result<CasOutcome, String>>>>,
    /// While armed: the next durable split-record write that clears the
    /// owner (a graceful release, or a revocation's final hand-back) is
    /// dropped (Retryable, nothing written), disarming afterward.
    /// Owner-setting writes (claims, commits) are untouched, so it targets
    /// exactly the release CAS.
    drop_owner_clear: Arc<AtomicBool>,
    /// Once: the next durable `assign.` write is dropped, so the leader
    /// believes it has told a worker something it never heard. Nothing is
    /// wedged by that alone; the point under test is that the leader
    /// republishes rather than treating the fleet as informed.
    drop_assignment_publish: Arc<AtomicBool>,
    /// Once: the next leader-key update writes but returns an error.
    leader_maybe_land: Arc<AtomicBool>,
    /// When set, the maybe-landed leader write advances this clock by this
    /// much before it returns, as a reply lost to `op_timeout` does.
    leader_stall: Arc<Mutex<Option<Stall>>>,
    /// Once: the next leader-key read after the maybe-landed write answers
    /// with the key as it stood before that write.
    leader_stale_read: Arc<AtomicBool>,
    /// The entry `leader_stale_read` serves: the leader key before the
    /// maybe-landed write, or a [`LeaderCreate::StaleRead`] entry.
    leader_before: Arc<Mutex<Option<Entry>>>,
    /// Once: the next leader-key read fails.
    leader_read_fails: Arc<AtomicBool>,
    /// Once: after a leader-key read, the next leader-key update fails
    /// without writing.
    leader_update_after_read_fails: Arc<AtomicBool>,
    /// Armed by the read `leader_update_after_read_fails` waits for.
    leader_update_fail_armed: Arc<AtomicBool>,
    /// Once: the fault on the next leader-key create.
    leader_create: Arc<Mutex<Option<LeaderCreate>>>,
}

impl FaultStore {
    fn new(inner: MemoryStore) -> FaultStore {
        FaultStore {
            inner,
            plan_update_script: Arc::new(Mutex::new(VecDeque::new())),
            lease_maybe_land: Arc::new(AtomicBool::new(false)),
            renewals_after_fault: Arc::new(AtomicU64::new(0)),
            lease_fault_key: Arc::default(),
            lease_updates: Arc::default(),
            act_returned: Arc::default(),
            lease_before: Arc::default(),
            lease_stale_reads: Arc::default(),
            lease_delete_fails: Arc::default(),
            lease_settle_read_fails: Arc::default(),
            lease_reads_after_act: Arc::default(),
            lease_stall: Arc::default(),
            lease_fault_at: Arc::default(),
            lease_refuse_after_adopt: Arc::default(),
            lease_adopting_cas: Arc::default(),
            drop_owner_clear: Arc::new(AtomicBool::new(false)),
            drop_assignment_publish: Arc::new(AtomicBool::new(false)),
            leader_maybe_land: Arc::default(),
            leader_stall: Arc::default(),
            leader_stale_read: Arc::default(),
            leader_before: Arc::default(),
            leader_read_fails: Arc::default(),
            leader_update_after_read_fails: Arc::default(),
            leader_update_fail_armed: Arc::default(),
            leader_create: Arc::default(),
        }
    }
}

impl CoordinationStore for FaultStore {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        let fault = if ks == Keyspace::Ephemeral && key == "leader" {
            self.leader_create.lock().expect("leader create").take()
        } else {
            None
        };
        let lost = || StoreError::Retryable("injected: election reply lost".into());
        match fault {
            None => self.inner.create(ks, key, value).await,
            Some(LeaderCreate::Lands) => {
                let _ = self.inner.create(ks, key, value).await?;
                Err(lost())
            }
            Some(LeaderCreate::LandsAsLost) => {
                let _ = self.inner.create(ks, key, value).await?;
                Ok(CasOutcome::Lost)
            }
            Some(LeaderCreate::PeerFirst(peer)) => {
                let _ = self.inner.create(ks, key, peer).await?;
                Err(lost())
            }
            Some(LeaderCreate::PeerFirstLost(peer)) => {
                let _ = self.inner.create(ks, key, peer).await?;
                Ok(CasOutcome::Lost)
            }
            Some(LeaderCreate::Slow((clock, by))) => {
                let outcome = self.inner.create(ks, key, value).await;
                clock.advance(by);
                outcome
            }
            Some(LeaderCreate::StaleRead(entry)) => {
                *self.leader_before.lock().expect("before") = Some(entry);
                self.leader_stale_read.store(true, Ordering::Release);
                Err(lost())
            }
        }
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        if ks == Keyspace::Ephemeral && key.starts_with("split.") {
            let written = serde_json::from_slice::<serde_json::Value>(&value).ok();
            let update = LeaseUpdate {
                epoch: written.as_ref().and_then(|v| v["epoch"].as_u64()),
                owner: written
                    .as_ref()
                    .and_then(|v| v["owner"].as_str().map(String::from)),
                expected,
                after_act: self.act_returned.load(Ordering::Acquire),
            };
            self.lease_updates
                .lock()
                .expect("updates")
                .entry(key.to_string())
                .or_default()
                .push(update);
        }
        if ks == Keyspace::Durable
            && key == "plan"
            && self
                .plan_update_script
                .lock()
                .expect("script")
                .pop_front()
                .unwrap_or(false)
        {
            return Err(StoreError::Retryable("injected: plan write dropped".into()));
        }
        if ks == Keyspace::Durable
            && key.starts_with("split.")
            && self.drop_owner_clear.load(Ordering::Acquire)
            && serde_json::from_slice::<serde_json::Value>(&value)
                .ok()
                .and_then(|v| v.get("owner").map(serde_json::Value::is_null))
                .unwrap_or(false)
        {
            self.drop_owner_clear.store(false, Ordering::Release);
            return Err(StoreError::Retryable(
                "injected: revocation release write dropped".into(),
            ));
        }
        if ks == Keyspace::Durable
            && key.starts_with("assign.")
            && self.drop_assignment_publish.swap(false, Ordering::AcqRel)
        {
            // The leader's assignment write never lands. Nothing is
            // wedged by this on its own; the leader must notice and
            // republish rather than believing the fleet was told.
            return Err(StoreError::Retryable(
                "injected: assignment publish dropped".into(),
            ));
        }
        if ks == Keyspace::Ephemeral
            && key.starts_with("split.")
            && self.lease_maybe_land.swap(false, Ordering::AcqRel)
        {
            // The write LANDS but the caller sees a failure, the
            // maybe-landed renewal a flaky round-trip produces.
            *self.lease_before.lock().expect("before") = self.inner.get(ks, key).await?;
            let stall = self.lease_stall.lock().expect("stall").clone();
            if let Some((clock, _)) = &stall {
                *self.lease_fault_at.lock().expect("fault at") = Some(clock.now());
            }
            let _ = self.inner.update(ks, key, value, expected).await?;
            self.renewals_after_fault.store(0, Ordering::Release);
            *self.lease_fault_key.lock().expect("fault key") = Some(key.to_string());
            if let Some((clock, by)) = stall {
                clock.advance(by);
            }
            return Err(StoreError::Retryable(
                "injected: renewal reply lost after the write landed".into(),
            ));
        }
        if ks == Keyspace::Ephemeral
            && key == "leader"
            && self.leader_update_fail_armed.swap(false, Ordering::AcqRel)
        {
            return Err(StoreError::Retryable(
                "injected: leader update failed".into(),
            ));
        }
        if ks == Keyspace::Ephemeral
            && key == "leader"
            && self.leader_maybe_land.swap(false, Ordering::AcqRel)
        {
            *self.leader_before.lock().expect("before") = self.inner.get(ks, key).await?;
            let _ = self.inner.update(ks, key, value, expected).await?;
            if let Some((clock, by)) = self.leader_stall.lock().expect("stall").clone() {
                clock.advance(by);
            }
            return Err(StoreError::Retryable(
                "injected: leader renewal reply lost after the write landed".into(),
            ));
        }
        if ks == Keyspace::Ephemeral
            && self.lease_refuse_after_adopt.load(Ordering::Acquire)
            && self.lease_fault_key.lock().expect("fault key").as_deref() == Some(key)
        {
            if self.lease_adopting_cas.lock().expect("adopting").is_some() {
                return Err(StoreError::Retryable(
                    "injected: lease renewal refused after the adoption".into(),
                ));
            }
            let outcome = self.inner.update(ks, key, value, expected).await;
            *self.lease_adopting_cas.lock().expect("adopting") =
                Some(outcome.as_ref().map(|o| *o).map_err(ToString::to_string));
            return outcome;
        }
        let outcome = self.inner.update(ks, key, value, expected).await?;
        if ks == Keyspace::Ephemeral
            && key.starts_with("split.")
            && matches!(outcome, CasOutcome::Won(_))
        {
            self.renewals_after_fault.fetch_add(1, Ordering::AcqRel);
        }
        Ok(outcome)
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        if ks == Keyspace::Ephemeral
            && self.lease_fault_key.lock().expect("fault key").as_deref() == Some(key)
            && {
                let mut left = self.lease_stale_reads.lock().expect("stale reads");
                let serve = *left > 0;
                *left = left.saturating_sub(1);
                serve
            }
        {
            return Ok(self.lease_before.lock().expect("before").clone());
        }
        if ks == Keyspace::Ephemeral
            && self.lease_fault_key.lock().expect("fault key").as_deref() == Some(key)
            && self.act_returned.load(Ordering::Acquire)
        {
            self.lease_reads_after_act.fetch_add(1, Ordering::AcqRel);
        }
        if ks == Keyspace::Ephemeral
            && self.lease_fault_key.lock().expect("fault key").as_deref() == Some(key)
            && self.act_returned.load(Ordering::Acquire)
            && self.lease_settle_read_fails.swap(false, Ordering::AcqRel)
        {
            return Err(StoreError::Retryable("injected: lease read failed".into()));
        }
        if ks == Keyspace::Ephemeral && key == "leader" {
            if self.leader_read_fails.swap(false, Ordering::AcqRel) {
                return Err(StoreError::Retryable("injected: leader read failed".into()));
            }
            if self
                .leader_update_after_read_fails
                .swap(false, Ordering::AcqRel)
            {
                self.leader_update_fail_armed.store(true, Ordering::Release);
            }
            let before = self.leader_before.lock().expect("before").take();
            if let Some(before) = before
                && self.leader_stale_read.swap(false, Ordering::AcqRel)
            {
                return Ok(Some(before));
            }
        }
        self.inner.get(ks, key).await
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        if ks == Keyspace::Ephemeral
            && self.lease_fault_key.lock().expect("fault key").as_deref() == Some(key)
            && self.lease_delete_fails.load(Ordering::Acquire)
            && self.inner.get(ks, key).await?.map(|e| e.revision) == expected
        {
            self.lease_delete_fails.store(false, Ordering::Release);
            return Err(StoreError::Retryable(
                "injected: lease delete failed".into(),
            ));
        }
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
}

/// A failed plan publish must not desynchronize terminal detection: the
/// splits were already seeded, so `planned` must be recounted from the
/// store on the next run. Accounting by `planned += creates won this run`
/// made the re-run count zero, the totals never matched again, and a
/// bounded job idled forever instead of draining.
#[test]
fn failed_plan_publish_heals_and_the_job_still_completes() {
    let rt = runtime();
    let store = FaultStore::new(MemoryStore::new(support::LEASE));
    // Plan-record updates: [generation bump: ok, first publish: FAILS].
    store
        .plan_update_script
        .lock()
        .expect("script")
        .extend([false, true]);

    let planner = Box::new(PhasedPlanner::one_final("publish-fault:v1", &["p0", "p1"]));
    let mut worker = StoreCoordinator::new(store, config(Some("solo")), rt.handle().clone(), None)
        .expect("coordinator");
    worker.start(planner).unwrap();

    let mut held = Held::default();
    support::drive(&mut worker, &mut held, "claiming both splits", |h| {
        h.splits.len() == 2
    });
    for id in ["p0", "p1"] {
        worker
            .commit(&split_id(id), &SplitProgress::completed(100, vec![]))
            .unwrap();
    }
    // The replan tick recounts the seeded records and publishes Final;
    // without the recount this drive times out; nothing ever fires.
    support::drive(&mut worker, &mut held, "healing the failed publish", |h| {
        h.all_complete
    });
}

/// A renewal whose write lands but whose reply is lost must be ADOPTED on
/// the next heartbeat (the lease still carries our owner+nonce), not
/// treated as a fence. Dropping the split as Lost and re-acquiring it
/// through an attempt-consuming reclaim let four flakes quarantine a
/// healthy split.
#[test]
fn maybe_landed_renewal_is_adopted_not_fenced() {
    let rt = runtime();
    // Freeze time. Every protocol deadline reads this clock (lease expiry,
    // the self-fence, AND the renewal cadence), so nothing fires until the
    // test advances it. That makes the negative assertion below meaningful:
    // a Lost/Quarantined can only come from mishandling the maybe-landed
    // renewal, never from a CI scheduler stall. We drive the renewals
    // ourselves by stepping the clock, one fraction of a renew-interval at a
    // time so a live worker always gets to renew before the self-fence would
    // fire (the "advance to settle" pattern; see `support::TestClock`).
    let clock = support::TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let lease_maybe_land = store.lease_maybe_land.clone();
    let renewals_after_fault = store.renewals_after_fault.clone();

    let planner = Box::new(PhasedPlanner::one_final("renewal-fault:v1", &["r0"]));
    let mut worker = StoreCoordinator::with_clock(
        store,
        config(Some("solo")),
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker.start(planner).unwrap();
    let mut fleet = support::Fleet::new(&inner, rt.handle());
    fleet.join(&worker);

    let mut held = Held::default();
    support::drive(&mut worker, &mut held, "claiming the split", |h| {
        h.splits.len() == 1
    });

    // Poll the worker once, folding events and asserting the split is never
    // dropped. `RefCell` so it can be called from inside the `advance_stepped`
    // step closure, which already borrows the clock.
    let held = std::cell::RefCell::new(held);
    let worker = std::cell::RefCell::new(worker);
    let pump = || {
        fleet.settle(&clock);
        for event in worker.borrow_mut().poll().expect("poll") {
            assert!(
                !matches!(
                    event,
                    CoordinationEvent::Lost { .. } | CoordinationEvent::Quarantined { .. }
                ),
                "a maybe-landed renewal must not cost the split: {event:?}"
            );
            held.borrow_mut().fold(vec![event]);
        }
    };

    // Arm the fault: the next lease renewal writes but reports an error, and
    // the one after loses its CAS against that landed write and must ADOPT it
    // (not fence). Step a renew-interval per iteration until the fault has
    // fired (the flag clears), then until a renewal wins against the adopted
    // revision, all under the no-loss assertion. A step of a quarter
    // renew-interval keeps `last_ok_write` within a lease throughout, so a
    // genuine self-fence never masquerades as the loss we are refuting.
    lease_maybe_land.store(true, Ordering::Release);
    let renew = support::LEASE / 3;
    let deadline = Instant::now() + DEADLINE;
    while lease_maybe_land.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "the fault never fired");
        clock.advance_stepped(renew, renew / 4, pump);
    }
    while renewals_after_fault.load(Ordering::Acquire) == 0 {
        assert!(Instant::now() < deadline, "no renewal won after the fault");
        clock.advance_stepped(renew, renew / 4, pump);
    }

    let mut held = held.into_inner();
    let mut worker = worker.into_inner();
    assert_eq!(held.splits.len(), 1, "still held");
    // The tenancy is intact: the fenced commit path would reject this.
    worker
        .commit(&split_id("r0"), &SplitProgress::completed(7, vec![]))
        .unwrap();
    // The completion sweep runs on a reconcile tick, which is clock-driven;
    // keep stepping the frozen clock so it fires.
    support::drive_clocked(
        &mut worker,
        &clock,
        &mut held,
        "completing after the flake",
        |h| h.all_complete,
    );
}

/// A solo worker on a frozen clock holding `r0` and `r1`, stepped until a
/// lease renewal applies with its reply lost, then `act` on that split's id,
/// then stepped for `settle`. The first `stale_reads` reads of the lease
/// after the fault answer from before the renewal, and `arm` adds further
/// faults. Returns the lease, the split record and the splits reported lost
/// while settling.
fn after_unseen_lease_renewal(
    stale_reads: u64,
    settle: Duration,
    arm: impl FnOnce(&FaultStore),
    act: impl FnOnce(&mut StoreCoordinator<FaultStore>, &str, LeaseEpoch),
) -> (Option<String>, serde_json::Value, Vec<String>) {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let maybe_land = store.lease_maybe_land.clone();
    let fault_key = store.lease_fault_key.clone();
    let updates = store.lease_updates.clone();
    let act_returned = store.act_returned.clone();
    let stale = store.lease_stale_reads.clone();
    arm(&store);
    let mut worker = StoreCoordinator::with_clock(
        store,
        config(Some("solo")),
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final(
            "lease-fault:v1",
            &["r0", "r1"],
        )))
        .unwrap();
    let mut fleet = support::Fleet::new(&inner, rt.handle());
    fleet.join(&worker);
    let mut held = Held::default();
    support::drive(&mut worker, &mut held, "claiming both splits", |h| {
        h.splits.len() == 2
    });

    // Step a twelfth of a lease at a time, so no second heartbeat runs
    // between the fault and `act`.
    maybe_land.store(true, Ordering::Release);
    let deadline = Instant::now() + DEADLINE;
    while maybe_land.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "the fault never fired");
        fleet.step(&clock, support::LEASE / 12);
        held.fold(worker.poll().expect("poll"));
    }
    let key = fault_key
        .lock()
        .unwrap()
        .clone()
        .expect("the faulted lease");
    let (at_fault, fault_epoch) = {
        let log = updates.lock().unwrap();
        let log = &log[&key];
        (log.len(), log.last().expect("the faulted update").epoch)
    };
    *stale.lock().unwrap() = stale_reads;
    let id = key.strip_prefix("split.").expect("a split key");
    let epoch = LeaseEpoch(held.splits[id].0);
    act(&mut worker, id, epoch);
    act_returned.store(true, Ordering::Release);

    // Only a write of the faulted epoch is a renewal; a claim of the
    // handed-back split writes a later one.
    {
        let log = updates.lock().unwrap();
        let since = &log[&key][at_fault..];
        assert!(
            since.iter().all(|u| u.epoch != fault_epoch),
            "a renewal ran between the fault and the act: {since:?}"
        );
    }
    let mut lost = Vec::new();
    for _ in 0..settle.as_nanos() / (support::LEASE / 12).as_nanos() {
        fleet.step(&clock, support::LEASE / 12);
        for event in worker.poll().expect("poll") {
            if let CoordinationEvent::Lost { split } = &event {
                lost.push(split.as_str().to_string());
            }
            held.fold(vec![event]);
        }
    }
    assert_eq!(*stale.lock().unwrap(), 0, "the stale reads were not served");
    let lease = rt
        .block_on(inner.get(Keyspace::Ephemeral, &key))
        .unwrap()
        .map(|e| String::from_utf8_lossy(&e.value).into_owned());
    let record = rt
        .block_on(inner.get(Keyspace::Durable, &key))
        .unwrap()
        .expect("split record");
    (lease, record_json(&record.value), lost)
}

/// A release right after a lease renewal that applied with its reply lost
/// deletes that lease.
/// Regression for #865.
#[test]
fn a_release_after_an_unseen_lease_renewal_deletes_the_lease() {
    let (lease, record, _) = after_unseen_lease_renewal(
        0,
        Duration::ZERO,
        |_| {},
        |worker, _, _| {
            worker
                .release(&[split_id("r0"), split_id("r1")])
                .expect("release");
        },
    );
    assert!(lease.is_none(), "the lease was left: {lease:?}");
    assert!(record["owner"].is_null());
}

/// A completing commit right after a lease renewal that applied with its
/// reply lost deletes that lease.
/// Regression for #865.
#[test]
fn a_completion_after_an_unseen_lease_renewal_deletes_the_lease() {
    let (lease, ..) = after_unseen_lease_renewal(
        0,
        Duration::ZERO,
        |_| {},
        |worker, id, _| {
            worker
                .commit(&split_id(id), &SplitProgress::completed(1, vec![]))
                .expect("complete");
        },
    );
    assert!(lease.is_none(), "the lease was left: {lease:?}");
}

/// A release whose lease read answers from before an unseen renewal leaves
/// the lease until the next heartbeat.
#[test]
fn a_release_whose_lease_read_lags_the_renewal_leaves_the_lease_until_the_next_heartbeat() {
    let (lease, record, _) = after_unseen_lease_renewal(
        1,
        Duration::ZERO,
        |_| {},
        |worker, _, _| {
            worker
                .release(&[split_id("r0"), split_id("r1")])
                .expect("release");
        },
    );
    assert!(lease.is_some(), "the lease was deleted");
    assert!(record["owner"].is_null());
}

/// A release whose lease read answers from before an unseen renewal deletes
/// the lease on the next heartbeat.
/// Regression for #886.
#[test]
fn a_release_whose_lease_read_lags_deletes_the_lease_on_the_next_heartbeat() {
    let (lease, record, _) = after_unseen_lease_renewal(
        1,
        support::LEASE / 2,
        |_| {},
        |worker, _, _| {
            worker
                .release(&[split_id("r0"), split_id("r1")])
                .expect("release");
        },
    );
    assert!(lease.is_none(), "the lease was left: {lease:?}");
    assert!(record["owner"].is_null());
}

/// A release whose lease read lags twice deletes the lease on a later
/// heartbeat.
#[test]
fn a_release_whose_lease_read_lags_twice_deletes_the_lease_on_a_later_heartbeat() {
    let (lease, record, _) = after_unseen_lease_renewal(
        2,
        support::LEASE * 5 / 6,
        |_| {},
        |worker, _, _| {
            worker
                .release(&[split_id("r0"), split_id("r1")])
                .expect("release");
        },
    );
    assert!(lease.is_none(), "the lease was left: {lease:?}");
    assert!(record["owner"].is_null());
}

/// A release whose second lease delete fails deletes the lease on the next
/// heartbeat.
#[test]
fn a_release_whose_second_lease_delete_fails_deletes_the_lease_on_the_next_heartbeat() {
    let (lease, record, _) = after_unseen_lease_renewal(
        0,
        support::LEASE / 2,
        |store| store.lease_delete_fails.store(true, Ordering::Release),
        |worker, _, _| {
            worker
                .release(&[split_id("r0"), split_id("r1")])
                .expect("release");
        },
    );
    assert!(lease.is_none(), "the lease was left: {lease:?}");
    assert!(record["owner"].is_null());
}

/// A release whose lease read lags, and whose first retry cannot read the
/// lease, deletes the lease on a later heartbeat.
#[test]
fn a_release_whose_owed_lease_read_fails_deletes_the_lease_on_a_later_heartbeat() {
    let mut read_fails = None;
    let (lease, record, _) = after_unseen_lease_renewal(
        1,
        support::LEASE * 5 / 6,
        |store| {
            store.lease_settle_read_fails.store(true, Ordering::Release);
            read_fails = Some(store.lease_settle_read_fails.clone());
        },
        |worker, _, _| {
            worker
                .release(&[split_id("r0"), split_id("r1")])
                .expect("release");
        },
    );
    assert!(
        !read_fails.expect("armed").load(Ordering::Acquire),
        "the failing read was not served"
    );
    assert!(lease.is_none(), "the lease was left: {lease:?}");
    assert!(record["owner"].is_null());
}

/// A release whose lease read lags, and whose first retry cannot delete the
/// lease, deletes the lease on a later heartbeat.
#[test]
fn a_release_whose_owed_lease_delete_fails_deletes_the_lease_on_a_later_heartbeat() {
    let mut delete_fails = None;
    let (lease, record, _) = after_unseen_lease_renewal(
        1,
        support::LEASE * 5 / 6,
        |store| {
            store.lease_delete_fails.store(true, Ordering::Release);
            delete_fails = Some(store.lease_delete_fails.clone());
        },
        |worker, _, _| {
            worker
                .release(&[split_id("r0"), split_id("r1")])
                .expect("release");
        },
    );
    assert!(
        !delete_fails.expect("armed").load(Ordering::Acquire),
        "the failing delete was not served"
    );
    assert!(lease.is_none(), "the lease was left: {lease:?}");
    assert!(record["owner"].is_null());
}

/// A failure report whose lease read lags leaves the lease of this worker's
/// later claim of the split in place.
#[test]
fn a_failure_report_whose_lease_read_lags_keeps_the_reclaimed_lease() {
    let (lease, _, lost) = after_unseen_lease_renewal(
        1,
        support::LEASE / 2,
        |_| {},
        |worker, id, epoch| {
            worker.fail(&split_id(id), epoch, "injected").expect("fail");
        },
    );
    let lease: serde_json::Value =
        serde_json::from_str(&lease.expect("the re-claimed lease")).expect("lease json");
    assert_eq!(lease["epoch"], 2, "{lease}");
    assert_eq!(lease["owner"], "solo", "{lease}");
    assert!(lost.is_empty(), "splits lost: {lost:?}");
}

/// A failure report whose lease read lags stops retrying the owed lease once
/// the split is claimed again: later heartbeats do not read it.
#[test]
fn a_failure_report_whose_lease_read_lags_stops_retrying_the_reclaimed_lease() {
    let reads_over = |settle| {
        let mut reads = None;
        after_unseen_lease_renewal(
            1,
            settle,
            |store| reads = Some(store.lease_reads_after_act.clone()),
            |worker, id, epoch| {
                worker.fail(&split_id(id), epoch, "injected").expect("fail");
            },
        );
        reads.expect("armed").load(Ordering::Acquire)
    };
    let short = reads_over(support::LEASE / 2);
    let long = reads_over(support::LEASE * 2);
    assert_eq!(long, short, "the owed lease was read on later heartbeats");
}

/// Whether `event` reports `r0` lost.
fn r0_lost(event: &CoordinationEvent) -> bool {
    matches!(event, CoordinationEvent::Lost { split } if split.as_str() == "r0")
}

/// A split whose lease renewal applied with its reply lost to `op_timeout`,
/// half a lease after the write, stays held.
/// Regression for #886.
#[test]
fn a_lease_renewal_whose_reply_timed_out_keeps_the_split() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    *store.lease_stall.lock().unwrap() = Some((clock.clone(), support::LEASE / 2));
    let mut cfg = config(Some("solo"));
    cfg.op_timeout = support::LEASE / 2;
    let mut worker = StoreCoordinator::with_clock(
        store.clone(),
        cfg,
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final(
            "lease-timeout:v1",
            &["r0"],
        )))
        .unwrap();
    let mut fleet = support::Fleet::new(&inner, rt.handle());
    fleet.join(&worker);
    let mut held = Held::default();
    support::drive(&mut worker, &mut held, "claiming the split", |h| {
        h.splits.len() == 1
    });

    let mut step = || {
        fleet.step(&clock, support::LEASE / 12);
        for event in worker.poll().expect("poll") {
            assert!(
                !matches!(
                    event,
                    CoordinationEvent::Lost { .. } | CoordinationEvent::Quarantined { .. }
                ),
                "a renewal whose reply timed out cost the split: {event:?}"
            );
            held.fold(vec![event]);
        }
    };
    store.lease_maybe_land.store(true, Ordering::Release);
    let deadline = Instant::now() + DEADLINE;
    while store.lease_maybe_land.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "the fault never fired");
        step();
    }
    for _ in 0..24 {
        step();
    }

    worker
        .commit(&split_id("r0"), &SplitProgress::completed(7, vec![]))
        .expect("the tenancy is intact");
}

/// A split whose lease renewal applied with its reply lost to `op_timeout`,
/// and whose every renewal after the adoption fails, self-fences between one
/// and one and a half leases after the lost write, with the expiry hidden
/// from the watch. The bound assumes a renewal confirmed before the fault.
/// Regression for #886.
#[test]
fn a_self_fence_after_an_adopted_renewal_counts_from_the_lost_write() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let tap = TapStore::new(store.clone());
    let mut cfg = config(Some("solo"));
    cfg.op_timeout = support::LEASE / 2;
    cfg.reconcile_interval = SLOTTED_RECONCILE;
    let t_start = clock.now();
    let listings: Arc<Mutex<Vec<(Keyspace, tokio::time::Instant)>>> = Arc::default();
    {
        let (listings, clock) = (listings.clone(), clock.clone());
        tap.on_list(move |ks, _| {
            listings.lock().unwrap().push((ks, clock.now()));
            None
        });
    }
    let mut worker = StoreCoordinator::with_clock(
        tap.clone(),
        cfg,
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final("self-fence:v1", &["r0"])))
        .unwrap();
    let mut fleet = support::Fleet::new(&inner, rt.handle());
    fleet.join(&worker);
    let mut held = Held::default();
    support::drive_clocked(&mut worker, &clock, &mut held, "claiming r0", |h| {
        h.splits.len() == 1
    });

    *store.lease_stall.lock().unwrap() = Some((clock.clone(), support::LEASE / 2));
    store
        .lease_refuse_after_adopt
        .store(true, Ordering::Release);
    {
        let (store, updates) = (store.clone(), AtomicU64::new(0));
        tap.on_write(move |w| {
            if w.op == support::tap::Op::Update
                && w.ks == Keyspace::Ephemeral
                && w.key == "split.r0"
                && updates.fetch_add(1, Ordering::AcqRel) == 1
            {
                store.lease_maybe_land.store(true, Ordering::Release);
            }
            None
        });
    }
    // With no listing in the window, the hide stands for a view that lags
    // the lease's expiry.
    tap.hide(|ks, key| ks == Keyspace::Ephemeral && key == "split.r0");
    let deadline = Instant::now() + DEADLINE;
    let t = loop {
        assert!(Instant::now() < deadline, "the fault never fired");
        fleet.step(&clock, support::LEASE / 12);
        let events = worker.poll().expect("poll");
        assert!(!events.iter().any(r0_lost), "lost before the fault");
        if let Some(t) = *store.lease_fault_at.lock().unwrap() {
            break t;
        }
    };
    let mut first_lost = None;
    while clock.now() < t + support::LEASE * 3 / 2 {
        fleet.step(&clock, support::LEASE / 12);
        let events = worker.poll().expect("poll");
        if first_lost.is_none() && events.iter().any(r0_lost) {
            first_lost = Some(clock.now() - t);
        }
    }

    assert_eq!(
        *store.lease_adopting_cas.lock().unwrap(),
        Some(Ok(CasOutcome::Lost)),
        "the renewal after the fault did not lose to it"
    );
    let late: Vec<_> = listings
        .lock()
        .unwrap()
        .iter()
        .filter(|(ks, at)| *ks == Keyspace::Ephemeral && *at >= t)
        .copied()
        .collect();
    assert!(
        late.is_empty(),
        "a reconcile listed in the window: {late:?}"
    );
    assert!(
        clock.now() - t_start < RECONCILE_SLOT,
        "the test ran into a later reconcile slot"
    );
    assert!(
        first_lost.is_some_and(|at| at >= support::LEASE && at <= support::LEASE * 3 / 2),
        "r0 lost at {first_lost:?} after the lost write"
    );
}

/// A renewal that failed with nothing written, then one that applied with its
/// reply lost and was adopted, with every later renewal failing: the split
/// self-fences between one and four thirds of a lease after the first failure.
/// The bound assumes a renewal confirmed before the first failure.
#[test]
fn a_self_fence_after_an_adopted_renewal_counts_from_the_first_failed_one() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let tap = TapStore::new(store.clone());
    let mut worker = StoreCoordinator::with_clock(
        tap.clone(),
        config(Some("solo")),
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final(
            "first-failed:v1",
            &["r0"],
        )))
        .unwrap();
    let mut fleet = support::Fleet::new(&inner, rt.handle());
    fleet.join(&worker);
    let mut held = Held::default();
    support::drive_clocked(&mut worker, &clock, &mut held, "claiming r0", |h| {
        h.splits.len() == 1
    });

    let first_failed: Arc<Mutex<Option<tokio::time::Instant>>> = Arc::default();
    {
        let (first_failed, store, clock) = (first_failed.clone(), store.clone(), clock.clone());
        let updates = AtomicU64::new(0);
        tap.on_write(move |w| {
            if w.op != support::tap::Op::Update
                || w.ks != Keyspace::Ephemeral
                || w.key != "split.r0"
                || updates.fetch_add(1, Ordering::AcqRel) == 0
            {
                return None;
            }
            let mut first_failed = first_failed.lock().unwrap();
            if first_failed.is_some() {
                return None;
            }
            *first_failed = Some(clock.now());
            store
                .lease_refuse_after_adopt
                .store(true, Ordering::Release);
            store.lease_maybe_land.store(true, Ordering::Release);
            Some(StoreError::Retryable(
                "injected: renewal failed unwritten".into(),
            ))
        });
    }
    let deadline = Instant::now() + DEADLINE;
    let s = loop {
        assert!(Instant::now() < deadline, "the refusal never fired");
        fleet.step(&clock, support::LEASE / 12);
        assert!(
            !worker.poll().expect("poll").iter().any(r0_lost),
            "lost at the refusal"
        );
        if let Some(s) = *first_failed.lock().unwrap() {
            break s;
        }
    };
    let mut first_lost = None;
    while clock.now() < s + support::LEASE * 2 {
        fleet.step(&clock, support::LEASE / 12);
        let events = worker.poll().expect("poll");
        if first_lost.is_none() && events.iter().any(r0_lost) {
            first_lost = Some(clock.now() - s);
        }
    }

    assert_eq!(
        *store.lease_adopting_cas.lock().unwrap(),
        Some(Ok(CasOutcome::Lost)),
        "the renewal after the fault did not lose to it"
    );
    assert!(
        first_lost.is_some_and(|at| at >= support::LEASE && at < support::LEASE * 4 / 3),
        "r0 lost at {first_lost:?} after the first failed renewal"
    );
}

/// A renewal that failed with nothing written, followed by renewals that won,
/// does not count toward the self-fence of a renewal adopted later. It assumes
/// a renewal confirmed before the failure, so the renewals after it win.
#[test]
fn a_self_fence_after_an_adopted_renewal_ignores_a_failure_before_a_confirmed_one() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let tap = TapStore::new(store.clone());
    let mut worker = StoreCoordinator::with_clock(
        tap.clone(),
        config(Some("solo")),
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final("confirmed:v1", &["r0"])))
        .unwrap();
    let mut fleet = support::Fleet::new(&inner, rt.handle());
    fleet.join(&worker);
    let mut held = Held::default();
    support::drive_clocked(&mut worker, &clock, &mut held, "claiming r0", |h| {
        h.splits.len() == 1
    });

    let refuse = Arc::new(AtomicBool::new(true));
    {
        let (refuse, updates) = (refuse.clone(), AtomicU64::new(0));
        tap.on_write(move |w| {
            (w.op == support::tap::Op::Update
                && w.ks == Keyspace::Ephemeral
                && w.key == "split.r0"
                && updates.fetch_add(1, Ordering::AcqRel) == 1
                && refuse.swap(false, Ordering::AcqRel))
            .then(|| StoreError::Retryable("injected: renewal failed unwritten".into()))
        });
    }
    let deadline = Instant::now() + DEADLINE;
    while refuse.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "the refusal never fired");
        fleet.step(&clock, support::LEASE / 12);
        assert!(
            !worker.poll().expect("poll").iter().any(r0_lost),
            "lost at the refusal"
        );
    }
    for _ in 0..36 {
        fleet.step(&clock, support::LEASE / 12);
        assert!(
            !worker.poll().expect("poll").iter().any(r0_lost),
            "lost while renewals won"
        );
    }

    *store.lease_stall.lock().unwrap() = Some((clock.clone(), Duration::ZERO));
    store
        .lease_refuse_after_adopt
        .store(true, Ordering::Release);
    store.lease_maybe_land.store(true, Ordering::Release);
    let deadline = Instant::now() + DEADLINE;
    let t = loop {
        assert!(Instant::now() < deadline, "the fault never fired");
        fleet.step(&clock, support::LEASE / 12);
        assert!(
            !worker.poll().expect("poll").iter().any(r0_lost),
            "lost before the fault"
        );
        if let Some(t) = *store.lease_fault_at.lock().unwrap() {
            break t;
        }
    };
    let mut first_lost = None;
    while clock.now() < t + support::LEASE * 3 / 2 {
        fleet.step(&clock, support::LEASE / 12);
        let events = worker.poll().expect("poll");
        if first_lost.is_none() && events.iter().any(r0_lost) {
            first_lost = Some(clock.now() - t);
        }
    }

    assert_eq!(
        *store.lease_adopting_cas.lock().unwrap(),
        Some(Ok(CasOutcome::Lost)),
        "the renewal after the fault did not lose to it"
    );
    assert!(
        first_lost.is_some_and(|at| at >= support::LEASE),
        "r0 lost at {first_lost:?} after the adopted write"
    );
}

/// A split whose lease renewal applied with its reply lost, and whose
/// adopting read answers with the lease as it stood before that renewal,
/// stays held.
#[test]
fn an_adoption_read_behind_the_lost_write_keeps_the_split() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let mut worker = StoreCoordinator::with_clock(
        store.clone(),
        config(Some("solo")),
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final(
            "read-behind:v1",
            &["r0"],
        )))
        .unwrap();
    let mut fleet = support::Fleet::new(&inner, rt.handle());
    fleet.join(&worker);
    let mut held = Held::default();
    support::drive_clocked(&mut worker, &clock, &mut held, "claiming r0", |h| {
        h.splits.len() == 1
    });

    *store.lease_stale_reads.lock().unwrap() = 1;
    store.lease_maybe_land.store(true, Ordering::Release);
    let deadline = Instant::now() + DEADLINE;
    while store.lease_maybe_land.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "the fault never fired");
        fleet.step(&clock, support::LEASE / 12);
        assert!(
            !worker.poll().expect("poll").iter().any(r0_lost),
            "lost before the fault"
        );
    }
    for _ in 0..24 {
        fleet.step(&clock, support::LEASE / 12);
        assert!(
            !worker.poll().expect("poll").iter().any(r0_lost),
            "lost after an adoption read behind the write"
        );
    }

    assert_eq!(
        *store.lease_stale_reads.lock().unwrap(),
        0,
        "no adopting read was served the older lease"
    );
    worker
        .commit(&split_id("r0"), &SplitProgress::completed(7, vec![]))
        .expect("the tenancy is intact");
}

/// A renewal that applied with its reply lost, an adopting read behind it,
/// and every later renewal failing or losing: the split is lost between one
/// and four thirds of a lease after the lost write, with the expiry hidden
/// from the watch.
#[test]
fn an_adoption_read_behind_the_lost_write_keeps_the_self_fence() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let tap = TapStore::new(store.clone());
    let mut cfg = config(Some("solo"));
    cfg.reconcile_interval = SLOTTED_RECONCILE;
    let mut worker = StoreCoordinator::with_clock(
        tap.clone(),
        cfg,
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final(
            "read-behind-fence:v1",
            &["r0"],
        )))
        .unwrap();
    let mut fleet = support::Fleet::new(&inner, rt.handle());
    fleet.join(&worker);
    let mut held = Held::default();
    support::drive_clocked(&mut worker, &clock, &mut held, "claiming r0", |h| {
        h.splits.len() == 1
    });

    // Lease updates in order: applied with the reply lost (the next read
    // answers with the lease before it), lost, failed unwritten, lost, then
    // failed unwritten.
    let sent: Arc<Mutex<Option<tokio::time::Instant>>> = Arc::default();
    let reads = Arc::new(AtomicU64::new(0));
    {
        let (sent, store, clock) = (sent.clone(), store.clone(), clock.clone());
        let updates = AtomicU64::new(0);
        tap.on_write(move |w| {
            if w.op != support::tap::Op::Update || w.key != "split.r0" {
                return None;
            }
            match updates.fetch_add(1, Ordering::AcqRel) {
                0 => {
                    *sent.lock().unwrap() = Some(clock.now());
                    *store.lease_stale_reads.lock().unwrap() = 1;
                    store.lease_maybe_land.store(true, Ordering::Release);
                    None
                }
                1 | 3 => None,
                _ => Some(StoreError::Retryable(
                    "injected: renewal failed unwritten".into(),
                )),
            }
        });
        let reads = reads.clone();
        tap.on_get(move |ks, key| {
            if ks == Keyspace::Ephemeral && key == "split.r0" {
                reads.fetch_add(1, Ordering::AcqRel);
            }
            None
        });
    }
    tap.hide(|ks, key| ks == Keyspace::Ephemeral && key == "split.r0");
    let deadline = Instant::now() + DEADLINE;
    let s = loop {
        assert!(Instant::now() < deadline, "the fault never fired");
        fleet.step(&clock, support::LEASE / 12);
        assert!(
            !worker.poll().expect("poll").iter().any(r0_lost),
            "lost before the fault"
        );
        if let Some(s) = *sent.lock().unwrap() {
            break s;
        }
    };
    let mut first_lost = None;
    while clock.now() < s + support::LEASE * 2 {
        fleet.step(&clock, support::LEASE / 12);
        let events = worker.poll().expect("poll");
        if first_lost.is_none() && events.iter().any(r0_lost) {
            first_lost = Some(clock.now() - s);
        }
    }

    assert_eq!(
        *store.lease_stale_reads.lock().unwrap(),
        0,
        "no adopting read was served the older lease"
    );
    assert!(
        reads.load(Ordering::Acquire) >= 2,
        "the lease was not read again after the read behind"
    );
    assert!(
        first_lost.is_some_and(|at| at >= support::LEASE && at < support::LEASE * 4 / 3),
        "r0 lost at {first_lost:?} after the lost write"
    );
}

/// A renewal that applied with its reply lost, a failed renewal after it, and
/// every read of the lease answered with the lease before it: the split
/// self-fences between one and four thirds of a lease after the lost write,
/// with the expiry hidden from the watch.
#[test]
fn a_lease_read_behind_after_a_failed_renewal_self_fences() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let tap = TapStore::new(store.clone());
    let mut cfg = config(Some("solo"));
    cfg.reconcile_interval = SLOTTED_RECONCILE;
    let mut worker = StoreCoordinator::with_clock(
        tap.clone(),
        cfg,
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final(
            "read-behind-failed:v1",
            &["r0"],
        )))
        .unwrap();
    let mut fleet = support::Fleet::new(&inner, rt.handle());
    fleet.join(&worker);
    let mut held = Held::default();
    support::drive_clocked(&mut worker, &clock, &mut held, "claiming r0", |h| {
        h.splits.len() == 1
    });

    // Lease updates in order: applied with the reply lost, lost, failed
    // unwritten, then lost. Every read answers with the lease before the
    // first.
    let sent: Arc<Mutex<Option<tokio::time::Instant>>> = Arc::default();
    let reads = Arc::new(AtomicU64::new(0));
    {
        let (sent, store, clock) = (sent.clone(), store.clone(), clock.clone());
        let updates = AtomicU64::new(0);
        tap.on_write(move |w| {
            if w.op != support::tap::Op::Update || w.key != "split.r0" {
                return None;
            }
            match updates.fetch_add(1, Ordering::AcqRel) {
                0 => {
                    *sent.lock().unwrap() = Some(clock.now());
                    *store.lease_stale_reads.lock().unwrap() = u64::MAX;
                    store.lease_maybe_land.store(true, Ordering::Release);
                    None
                }
                2 => Some(StoreError::Retryable(
                    "injected: renewal failed unwritten".into(),
                )),
                _ => None,
            }
        });
        let reads = reads.clone();
        tap.on_get(move |ks, key| {
            if ks == Keyspace::Ephemeral && key == "split.r0" {
                reads.fetch_add(1, Ordering::AcqRel);
            }
            None
        });
    }
    tap.hide(|ks, key| ks == Keyspace::Ephemeral && key == "split.r0");
    let deadline = Instant::now() + DEADLINE;
    let s = loop {
        assert!(Instant::now() < deadline, "the fault never fired");
        fleet.step(&clock, support::LEASE / 12);
        assert!(
            !worker.poll().expect("poll").iter().any(r0_lost),
            "lost before the fault"
        );
        if let Some(s) = *sent.lock().unwrap() {
            break s;
        }
    };
    let mut first_lost = None;
    while clock.now() < s + support::LEASE * 2 {
        fleet.step(&clock, support::LEASE / 12);
        let events = worker.poll().expect("poll");
        if first_lost.is_none() && events.iter().any(r0_lost) {
            first_lost = Some(clock.now() - s);
        }
    }

    assert_eq!(
        u64::MAX - *store.lease_stale_reads.lock().unwrap(),
        reads.load(Ordering::Acquire),
        "a read of the lease was not served the lease before the lost write"
    );
    assert!(
        first_lost.is_some_and(|at| at >= support::LEASE && at < support::LEASE * 4 / 3),
        "r0 lost at {first_lost:?} after the lost write"
    );
    assert!(
        reads.load(Ordering::Acquire) >= 2,
        "the lease was not read behind after the failed renewal"
    );
}

/// A split whose adopting beat's renewal also applied with its reply lost,
/// then one renewal that failed with nothing written, an adoption, and every
/// later renewal failing: the split self-fences between one and four thirds
/// of a lease after the adopting beat's renewal, with the expiry hidden from
/// the watch.
#[test]
fn a_self_fence_after_an_adopted_renewal_counts_from_a_failed_same_beat_one() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let tap = TapStore::new(store.clone());
    let mut cfg = config(Some("solo"));
    cfg.reconcile_interval = SLOTTED_RECONCILE;
    let mut worker = StoreCoordinator::with_clock(
        tap.clone(),
        cfg,
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final("same-beat:v1", &["r0"])))
        .unwrap();
    let mut fleet = support::Fleet::new(&inner, rt.handle());
    fleet.join(&worker);
    let mut held = Held::default();
    support::drive_clocked(&mut worker, &clock, &mut held, "claiming r0", |h| {
        h.splits.len() == 1
    });

    // Renewals in order: applied with the reply lost, lost, applied with the
    // reply lost, failed unwritten, lost, then failed unwritten.
    let sent: Arc<Mutex<Option<tokio::time::Instant>>> = Arc::default();
    let reads = Arc::new(AtomicU64::new(0));
    {
        let (sent, store, clock) = (sent.clone(), store.clone(), clock.clone());
        let updates = AtomicU64::new(0);
        tap.on_write(move |w| {
            if w.op != support::tap::Op::Update || w.key != "split.r0" {
                return None;
            }
            match updates.fetch_add(1, Ordering::AcqRel) {
                n @ (0 | 2) => {
                    if n == 2 {
                        *sent.lock().unwrap() = Some(clock.now());
                    }
                    store.lease_maybe_land.store(true, Ordering::Release);
                    None
                }
                1 | 4 => None,
                _ => Some(StoreError::Retryable(
                    "injected: renewal failed unwritten".into(),
                )),
            }
        });
        let reads = reads.clone();
        tap.on_get(move |ks, key| {
            if ks == Keyspace::Ephemeral && key == "split.r0" {
                reads.fetch_add(1, Ordering::AcqRel);
            }
            None
        });
    }
    tap.hide(|ks, key| ks == Keyspace::Ephemeral && key == "split.r0");
    let deadline = Instant::now() + DEADLINE;
    let s = loop {
        assert!(Instant::now() < deadline, "the same-beat fault never fired");
        fleet.step(&clock, support::LEASE / 12);
        assert!(
            !worker.poll().expect("poll").iter().any(r0_lost),
            "lost before the same-beat fault"
        );
        if let Some(s) = *sent.lock().unwrap() {
            break s;
        }
    };
    let mut first_lost = None;
    while clock.now() < s + support::LEASE * 2 {
        fleet.step(&clock, support::LEASE / 12);
        let events = worker.poll().expect("poll");
        if first_lost.is_none() && events.iter().any(r0_lost) {
            first_lost = Some(clock.now() - s);
        }
    }

    assert_eq!(
        reads.load(Ordering::Acquire),
        2,
        "the worker did not adopt twice"
    );
    assert!(
        first_lost.is_some_and(|at| at >= support::LEASE && at < support::LEASE * 4 / 3),
        "r0 lost at {first_lost:?} after the same-beat renewal"
    );
}

/// A split whose adopting beat's renewal applied with its reply lost to
/// `op_timeout`, then an adoption, and every later renewal failing: the split
/// self-fences between one and one and a half leases after that renewal was
/// sent, with the expiry hidden from the watch.
#[test]
fn a_self_fence_after_an_adopted_renewal_counts_from_a_timed_out_same_beat_one() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let tap = TapStore::new(store.clone());
    let mut cfg = config(Some("solo"));
    cfg.reconcile_interval = SLOTTED_RECONCILE;
    cfg.op_timeout = support::LEASE / 2;
    let mut worker = StoreCoordinator::with_clock(
        tap.clone(),
        cfg,
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final(
            "timed-out-same-beat:v1",
            &["r0"],
        )))
        .unwrap();
    let mut fleet = support::Fleet::new(&inner, rt.handle());
    fleet.join(&worker);
    let mut held = Held::default();
    support::drive_clocked(&mut worker, &clock, &mut held, "claiming r0", |h| {
        h.splits.len() == 1
    });

    // Renewals in order: applied with the reply lost, lost, applied with the
    // reply lost to `op_timeout`, lost, then failed unwritten.
    let sent: Arc<Mutex<Option<tokio::time::Instant>>> = Arc::default();
    let reads = Arc::new(AtomicU64::new(0));
    {
        let (sent, store, clock) = (sent.clone(), store.clone(), clock.clone());
        let updates = AtomicU64::new(0);
        tap.on_write(move |w| {
            if w.op != support::tap::Op::Update || w.key != "split.r0" {
                return None;
            }
            match updates.fetch_add(1, Ordering::AcqRel) {
                n @ (0 | 2) => {
                    if n == 2 {
                        *sent.lock().unwrap() = Some(clock.now());
                        *store.lease_stall.lock().unwrap() =
                            Some((clock.clone(), support::LEASE / 2));
                    }
                    store.lease_maybe_land.store(true, Ordering::Release);
                    None
                }
                1 | 3 => None,
                _ => Some(StoreError::Retryable(
                    "injected: renewal failed unwritten".into(),
                )),
            }
        });
        let reads = reads.clone();
        tap.on_get(move |ks, key| {
            if ks == Keyspace::Ephemeral && key == "split.r0" {
                reads.fetch_add(1, Ordering::AcqRel);
            }
            None
        });
    }
    tap.hide(|ks, key| ks == Keyspace::Ephemeral && key == "split.r0");
    let deadline = Instant::now() + DEADLINE;
    let s = loop {
        assert!(Instant::now() < deadline, "the same-beat fault never fired");
        fleet.step(&clock, support::LEASE / 12);
        assert!(
            !worker.poll().expect("poll").iter().any(r0_lost),
            "lost before the same-beat fault"
        );
        if let Some(s) = *sent.lock().unwrap() {
            break s;
        }
    };
    let mut first_lost = None;
    while clock.now() < s + support::LEASE * 2 {
        fleet.step(&clock, support::LEASE / 12);
        let events = worker.poll().expect("poll");
        if first_lost.is_none() && events.iter().any(r0_lost) {
            first_lost = Some(clock.now() - s);
        }
    }

    assert_eq!(
        reads.load(Ordering::Acquire),
        2,
        "the worker did not adopt twice"
    );
    assert!(
        first_lost.is_some_and(|at| at >= support::LEASE && at < support::LEASE * 3 / 2),
        "r0 lost at {first_lost:?} after the same-beat renewal"
    );
}

/// A solo leader whose next leader-key renewal applies with its reply lost,
/// faulted further by `arm`, then stepped for two leases: asserts that the
/// plan generation did not move and the leader key still names the worker.
fn leader_keeps_leading_after(
    tune: impl FnOnce(&mut CoordinationConfig),
    arm: impl FnOnce(&FaultStore, &Arc<TestClock>),
) {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let maybe_land = store.leader_maybe_land.clone();
    arm(&store, &clock);

    let mut cfg = config(Some("solo"));
    tune(&mut cfg);
    let planner = Box::new(PhasedPlanner::one_final("leader-fault:v1", &["r0"]));
    let mut worker = StoreCoordinator::with_clock(
        store,
        cfg,
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker.start(planner).unwrap();
    let mut fleet = support::Fleet::new(&inner, rt.handle());
    fleet.join(&worker);
    let mut held = Held::default();
    support::drive(&mut worker, &mut held, "claiming the split", |h| {
        h.splits.len() == 1
    });
    let generation = plan_generation(&rt, &inner);

    let mut pump = || {
        fleet.settle(&clock);
        held.fold(worker.poll().expect("poll"));
    };
    maybe_land.store(true, Ordering::Release);
    let renew = support::LEASE / 3;
    let deadline = Instant::now() + DEADLINE;
    while maybe_land.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "the fault never fired");
        clock.advance_stepped(renew, renew / 4, &mut pump);
    }
    clock.advance_stepped(support::LEASE * 2, renew / 4, &mut pump);

    assert_eq!(
        plan_generation(&rt, &inner),
        generation,
        "the leader gave up leadership and was re-elected"
    );
    let leader = rt
        .block_on(inner.get(Keyspace::Ephemeral, "leader"))
        .unwrap()
        .expect("leader key");
    assert_eq!(record_json(&leader.value)["owner"], "solo");
}

fn plan_generation(rt: &tokio::runtime::Runtime, store: &MemoryStore) -> u64 {
    let plan = rt
        .block_on(store.get(Keyspace::Durable, "plan"))
        .unwrap()
        .expect("plan record");
    record_json(&plan.value)["generation"].as_u64().unwrap()
}

/// A leader whose renewal applied with its reply lost keeps leading.
/// Regression for #865.
#[test]
fn a_leader_whose_renewal_applied_unseen_keeps_leading() {
    leader_keeps_leading_after(|_| {}, |_, _| {});
}

/// A leader whose renewal reply was lost to `op_timeout`, half a lease after
/// the write applied, renews before the key expires.
/// Regression for #865.
#[test]
fn a_leader_whose_renewal_reply_timed_out_keeps_leading() {
    leader_keeps_leading_after(
        |cfg| cfg.op_timeout = support::LEASE / 2,
        |store, clock| {
            *store.leader_stall.lock().unwrap() = Some((clock.clone(), support::LEASE / 2));
        },
    );
}

/// A leader whose read-back after a lost renewal comes from a replica behind
/// the applied write keeps leading.
/// Regression for #865.
#[test]
fn a_leader_whose_read_back_lags_keeps_leading() {
    leader_keeps_leading_after(
        |_| {},
        |store, _| store.leader_stale_read.store(true, Ordering::Release),
    );
}

/// A leader whose read-back after a lost renewal fails keeps leading.
#[test]
fn a_leader_whose_read_back_fails_keeps_leading() {
    leader_keeps_leading_after(
        |_| {},
        |store, _| store.leader_read_fails.store(true, Ordering::Release),
    );
}

/// A leader whose renewal after the read-back of a lost renewal fails keeps
/// leading.
#[test]
fn a_leader_whose_renewal_after_the_read_back_fails_keeps_leading() {
    leader_keeps_leading_after(
        |_| {},
        |store, _| {
            store
                .leader_update_after_read_fails
                .store(true, Ordering::Release)
        },
    );
}

/// A solo worker over `store` on `clock`, planning `ids`, with a fleet over
/// `inner` that it has joined.
fn solo(
    rt: &tokio::runtime::Runtime,
    inner: &MemoryStore,
    store: FaultStore,
    clock: &Arc<TestClock>,
    ids: &[&str],
) -> (StoreCoordinator<FaultStore>, support::Fleet) {
    let mut worker = StoreCoordinator::with_clock(
        store,
        config(Some("solo")),
        rt.handle().clone(),
        None,
        clock.clone() as Arc<dyn Clock>,
    )
    .expect("coordinator");
    worker
        .start(Box::new(PhasedPlanner::one_final("leader-key:v1", ids)))
        .unwrap();
    let mut fleet = support::Fleet::new(inner, rt.handle());
    fleet.join(&worker);
    (worker, fleet)
}

/// The leader key's JSON, if the key exists.
fn leader_json(rt: &tokio::runtime::Runtime, inner: &MemoryStore) -> Option<serde_json::Value> {
    rt.block_on(inner.get(Keyspace::Ephemeral, "leader"))
        .unwrap()
        .map(|e| record_json(&e.value))
}

/// Waits until the armed leader-key create fault has fired.
fn wait_leader_create_fired(store: &FaultStore) {
    spate_test::wait_until(DEADLINE, "the election write", || {
        store.leader_create.lock().unwrap().is_none()
    });
}

/// A solo leader holding `r0`, stepped until a leader-key renewal applies
/// with its reply lost, then `arm`ed and released from `r0`, its last split.
fn released_after_unseen_leader_renewal(
    arm: impl FnOnce(&FaultStore),
) -> (
    tokio::runtime::Runtime,
    MemoryStore,
    StoreCoordinator<FaultStore>,
) {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let (mut worker, fleet) = solo(&rt, &inner, store.clone(), &clock, &["r0"]);
    let mut held = Held::default();
    support::drive(&mut worker, &mut held, "claiming the split", |h| {
        h.splits.len() == 1
    });

    // Step a twelfth of a lease at a time, so no second heartbeat runs
    // between the fault and the release.
    store.leader_maybe_land.store(true, Ordering::Release);
    let deadline = Instant::now() + DEADLINE;
    while store.leader_maybe_land.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "the fault never fired");
        fleet.step(&clock, support::LEASE / 12);
        held.fold(worker.poll().expect("poll"));
    }
    arm(&store);
    worker.release(&[split_id("r0")]).expect("release");
    (rt, inner, worker)
}

/// Releasing the last split after a leader renewal that applied with its
/// reply lost deletes the leader key.
/// Regression for #886.
#[test]
fn a_release_after_an_unseen_leader_renewal_deletes_the_leader_key() {
    let (rt, inner, _worker) = released_after_unseen_leader_renewal(|_| {});
    let leader = leader_json(&rt, &inner);
    assert!(leader.is_none(), "the leader key was left: {leader:?}");
}

/// A release whose read-back of the leader key answers from before an unseen
/// renewal deletes the key.
/// Regression for #886.
#[test]
fn a_release_whose_leader_read_lags_leaves_no_leader_key() {
    let (rt, inner, _worker) = released_after_unseen_leader_renewal(|store| {
        store.leader_stale_read.store(true, Ordering::Release);
    });
    let leader = leader_json(&rt, &inner);
    assert!(leader.is_none(), "the leader key was left: {leader:?}");
}

/// A departure deletes a leader key of its own that the release before it
/// could not read back.
#[test]
fn a_departure_deletes_an_own_leader_key_the_release_could_not_read_back() {
    let (rt, inner, mut worker) = released_after_unseen_leader_renewal(|store| {
        store.leader_read_fails.store(true, Ordering::Release);
    });
    assert!(
        leader_json(&rt, &inner).is_some(),
        "the release deleted the leader key without reading it back"
    );
    let result = worker.depart(&[]);
    let leader = leader_json(&rt, &inner);
    assert!(
        leader.is_none(),
        "depart returned {result:?}; the leader key was left: {leader:?}"
    );
}

/// A worker whose election write applied with its reply lost leads and
/// claims well within the lease of the key it wrote.
/// Regression for #886.
#[test]
fn an_election_whose_reply_was_lost_still_leads() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    *store.leader_create.lock().unwrap() = Some(LeaderCreate::Lands);
    let (mut worker, fleet) = solo(&rt, &inner, store.clone(), &clock, &["e0"]);
    wait_leader_create_fired(&store);

    let mut held = Held::default();
    for _ in 0..9 {
        fleet.step(&clock, support::LEASE / 12);
        held.fold(worker.poll().expect("poll"));
    }
    assert_eq!(
        held.splits.len(),
        1,
        "no split claimed 0.75 lease after the election; leader key: {:?}",
        leader_json(&rt, &inner)
    );
    assert_eq!(plan_generation(&rt, &inner), 1);
}

/// A worker whose election create applied and was reported lost leads and
/// claims well within the lease of the key it wrote.
/// Regression for #899.
#[test]
fn an_election_reported_lost_over_its_own_key_still_leads() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    *store.leader_create.lock().unwrap() = Some(LeaderCreate::LandsAsLost);
    let (mut worker, fleet) = solo(&rt, &inner, store.clone(), &clock, &["e0"]);
    wait_leader_create_fired(&store);

    let mut held = Held::default();
    for _ in 0..9 {
        fleet.step(&clock, support::LEASE / 12);
        held.fold(worker.poll().expect("poll"));
    }
    assert_eq!(
        held.splits.len(),
        1,
        "no split claimed 0.75 lease after the election; leader key: {:?}",
        leader_json(&rt, &inner)
    );
    assert_eq!(plan_generation(&rt, &inner), 1);
}

/// A worker whose election create lost to a peer's key leaves the key to
/// the peer and the plan generation where it was.
#[test]
fn an_election_lost_to_a_peers_key_does_not_lead() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let peer = serde_json::to_vec(&serde_json::json!({
        "schema": 3, "owner": "peer", "nonce": "peer-nonce", "generation": 1
    }))
    .unwrap();
    *store.leader_create.lock().unwrap() = Some(LeaderCreate::PeerFirstLost(peer.clone()));
    let (mut worker, fleet) = solo(&rt, &inner, store.clone(), &clock, &["e0"]);
    wait_leader_create_fired(&store);

    let mut held = Held::default();
    for _ in 0..9 {
        fleet.step(&clock, support::LEASE / 12);
        held.fold(worker.poll().expect("poll"));
    }
    let leader = rt
        .block_on(inner.get(Keyspace::Ephemeral, "leader"))
        .unwrap();
    assert!(
        leader.as_ref().is_some_and(|e| e.value == peer),
        "the peer's leader key was replaced: {:?}",
        leader.map(|e| record_json(&e.value))
    );
    assert_eq!(
        plan_generation(&rt, &inner),
        0,
        "the worker led under a peer's key"
    );
}

/// A worker whose election write failed under a peer's key leaves the key
/// to the peer and the plan generation where it was.
#[test]
fn an_election_that_failed_under_a_peers_key_does_not_lead() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let peer = serde_json::to_vec(&serde_json::json!({
        "schema": 3, "owner": "peer", "nonce": "peer-nonce", "generation": 1
    }))
    .unwrap();
    *store.leader_create.lock().unwrap() = Some(LeaderCreate::PeerFirst(peer.clone()));
    let (mut worker, fleet) = solo(&rt, &inner, store.clone(), &clock, &["e0"]);
    wait_leader_create_fired(&store);

    let mut held = Held::default();
    for _ in 0..9 {
        fleet.step(&clock, support::LEASE / 12);
        held.fold(worker.poll().expect("poll"));
    }
    let leader = rt
        .block_on(inner.get(Keyspace::Ephemeral, "leader"))
        .unwrap();
    assert!(
        leader.as_ref().is_some_and(|e| e.value == peer),
        "the peer's leader key was replaced: {:?}",
        leader.map(|e| record_json(&e.value))
    );
    assert_eq!(
        plan_generation(&rt, &inner),
        0,
        "the worker led under a peer's key"
    );
}

/// An election whose write failed is not adopted from a read-back that
/// returns this worker's key from an earlier term.
#[test]
fn an_election_read_back_ignores_this_workers_earlier_key() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    let (mut worker, fleet) = solo(&rt, &inner, store.clone(), &clock, &["s0"]);
    let mut held = Held::default();
    support::drive(&mut worker, &mut held, "claiming the split", |h| {
        h.splits.len() == 1
    });
    fleet.settle(&clock);
    assert_eq!(plan_generation(&rt, &inner), 1);
    let earlier = rt
        .block_on(inner.get(Keyspace::Ephemeral, "leader"))
        .unwrap()
        .expect("leader key");

    *store.leader_create.lock().unwrap() = Some(LeaderCreate::StaleRead(earlier));
    let _ = rt
        .block_on(inner.delete(Keyspace::Ephemeral, "leader", None))
        .unwrap();
    fleet.settle(&clock);
    assert!(
        store.leader_create.lock().unwrap().is_none(),
        "the worker never ran for election again"
    );
    // The failed election runs again at the worker's next step, which the
    // markers of this settle drive.
    fleet.settle(&clock);

    let leader = leader_json(&rt, &inner).expect("the worker leads without a leader key");
    assert_eq!(leader["owner"], "solo");
    assert_eq!(leader["generation"], 2);
    assert_eq!(plan_generation(&rt, &inner), 2);
}

/// A first election that takes most of a lease keeps the presence key and
/// the leader key, and the worker leads at its first generation throughout.
/// Regression for #886.
#[test]
fn a_slow_first_election_keeps_presence_and_the_leader_key() {
    let rt = runtime();
    let clock = TestClock::frozen();
    let inner = MemoryStore::with_clock(support::LEASE, clock.clone());
    let store = FaultStore::new(inner.clone());
    *store.leader_create.lock().unwrap() =
        Some(LeaderCreate::Slow((clock.clone(), support::LEASE * 9 / 10)));
    let (mut worker, fleet) = solo(&rt, &inner, store.clone(), &clock, &["w0"]);
    wait_leader_create_fired(&store);
    fleet.settle(&clock);

    let mut held = Held::default();
    for step in 1..=9 {
        fleet.step(&clock, support::LEASE / 12);
        held.fold(worker.poll().expect("poll"));
        let presence = rt
            .block_on(inner.get(Keyspace::Ephemeral, "worker.solo"))
            .unwrap();
        let leader = leader_json(&rt, &inner);
        let generation = plan_generation(&rt, &inner);
        assert!(
            presence.is_some() && leader.as_ref().is_some_and(|l| l["owner"] == "solo"),
            "step {step}: presence present: {}; leader key: {leader:?}",
            presence.is_some()
        );
        assert_eq!(generation, 1, "step {step}: the worker was elected again");
    }
}

/// A dropped assignment publish must be republished, not treated as
/// delivered. The leader caches the revision it believes each
/// `assign.{instance}` record holds so it can CAS the next one; if a
/// failed write left that cache believing a write it never made, every
/// later publish for that instance would CAS against a revision the store
/// never had and lose forever, and the worker, never having seen a
/// record, would hold nothing and claim nothing.
#[test]
fn a_dropped_assignment_publish_is_republished() {
    let rt = runtime();
    let store = FaultStore::new(MemoryStore::new(support::LEASE));
    let drop_publish = store.drop_assignment_publish.clone();
    let ids = ["a0", "a1"];
    let planner = || Box::new(PhasedPlanner::one_final("assign-fault:v1", &ids));

    // Arm before the worker starts, so the very first publish is the one
    // that vanishes.
    drop_publish.store(true, Ordering::Release);
    let mut worker = StoreCoordinator::new(store, config(Some("solo")), rt.handle().clone(), None)
        .expect("coordinator");
    worker.start(planner()).unwrap();

    let mut held = Held::default();
    support::drive(
        &mut worker,
        &mut held,
        "republishing the lost assignment",
        |h| h.splits.len() == ids.len(),
    );
    assert!(
        !drop_publish.load(Ordering::Acquire),
        "the fault never fired, so this proved nothing"
    );
}

/// A revocation release whose owner-clear write is dropped still gives the
/// split up: the lease key goes with it, so a peer takes over on expiry
/// rather than the split staying pinned to a worker the leader has already
/// stopped assigning it to. The replay that costs is the price of the
/// dropped write, not of the protocol.
#[test]
fn a_dropped_release_write_still_surrenders_the_split() {
    let rt = runtime();
    let store = FaultStore::new(MemoryStore::new(support::LEASE));
    let drop_clear = store.drop_owner_clear.clone();
    let ids = ["d0", "d1"];
    let planner = || Box::new(PhasedPlanner::one_final("release-fault:v1", &ids));

    let mut a = StoreCoordinator::new(
        store.clone(),
        config(Some("worker-a")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    support::drive(&mut a, &mut held_a, "worker-a takes the plan", |h| {
        h.splits.len() == ids.len()
    });
    support::commit_held(&mut a, &held_a);

    // B joins: the leader revokes one split, and A's release write is the
    // one that gets dropped.
    drop_clear.store(true, Ordering::Release);
    let mut b = StoreCoordinator::new(store, config(Some("worker-b")), rt.handle().clone(), None)
        .expect("coordinator");
    b.start(planner()).unwrap();
    let mut held_b = Held::default();

    let deadline = Instant::now() + DEADLINE;
    while held_b.splits.is_empty() {
        assert!(
            Instant::now() < deadline,
            "a dropped release write pinned the split to worker-a forever"
        );
        held_a.fold(a.poll().expect("poll a"));
        held_b.fold(b.poll().expect("poll b"));
        support::commit_held(&mut a, &held_a);
        support::consent_to_revocations(&mut a, &mut held_a);
        std::thread::sleep(support::POLL_INTERVAL);
    }
    assert!(
        !drop_clear.load(Ordering::Acquire),
        "the fault never fired, so this proved nothing"
    );
    assert!(
        held_a.splits.keys().all(|k| !held_b.splits.contains_key(k)),
        "the split is held twice: a={:?} b={:?}",
        held_a.splits.keys().collect::<Vec<_>>(),
        held_b.splits.keys().collect::<Vec<_>>()
    );
}

/// How [`ReleaseStore`] treats the lease delete of its armed split key.
#[derive(Clone, Copy)]
enum LeaseDeleteFault {
    /// Delete, then hold the call until another worker owns the split
    /// record.
    HoldUntilClaimed,
    /// Return an error without deleting.
    Fail,
}

/// A [`MemoryStore`] that applies one [`LeaseDeleteFault`] to the lease of
/// the armed split key.
#[derive(Clone)]
struct ReleaseStore {
    inner: MemoryStore,
    fault: LeaseDeleteFault,
    armed: Arc<Mutex<Option<String>>>,
    fired: Arc<AtomicBool>,
    /// The foreign owner the hold observed before returning.
    claimed_by: Arc<Mutex<Option<String>>>,
}

impl ReleaseStore {
    fn new(inner: MemoryStore, fault: LeaseDeleteFault) -> ReleaseStore {
        ReleaseStore {
            inner,
            fault,
            armed: Arc::new(Mutex::new(None)),
            fired: Arc::new(AtomicBool::new(false)),
            claimed_by: Arc::new(Mutex::new(None)),
        }
    }

    /// The first owner other than worker-a that the split record carries.
    async fn foreign_owner(&self, key: &str) -> Option<String> {
        let mut watch = self.inner.watch(Keyspace::Durable, key).await.ok()?;
        while let Some(Ok(event)) = watch.next().await {
            if let WatchEvent::Put(entry) = event
                && entry.key == key
                && let Some(owner) = record_json(&entry.value)["owner"].as_str()
                && owner != "worker-a"
            {
                return Some(owner.to_string());
            }
        }
        None
    }
}

impl CoordinationStore for ReleaseStore {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.create(ks, key, value).await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.update(ks, key, value, expected).await
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
        let armed =
            ks == Keyspace::Ephemeral && self.armed.lock().expect("armed").as_deref() == Some(key);
        if !armed {
            return self.inner.delete(ks, key, expected).await;
        }
        self.fired.store(true, Ordering::Release);
        match self.fault {
            LeaseDeleteFault::Fail => Err(StoreError::Retryable(
                "injected: lease delete dropped".into(),
            )),
            LeaseDeleteFault::HoldUntilClaimed => {
                let outcome = self.inner.delete(ks, key, expected).await?;
                *self.claimed_by.lock().expect("claimed by") = self.foreign_owner(key).await;
                Ok(outcome)
            }
        }
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
}

fn record_json(value: &[u8]) -> serde_json::Value {
    serde_json::from_slice(value).expect("record json")
}

/// Starts worker-a on `store` and worker-b on the store beneath it, both on
/// `clock`, arms the fault on the split the leader revokes from worker-a,
/// and drops worker-a so the direct release hands it back. Returns the
/// split and its record once worker-b holds it.
fn drop_during_revocation(
    store: &ReleaseStore,
    clock: &Arc<TestClock>,
) -> (String, serde_json::Value) {
    let rt = runtime();
    let ids = ["r0", "r1"];
    let planner = || Box::new(PhasedPlanner::one_final("direct-release:v1", &ids));
    // Both workers are alive while the clock moves, so each step stays
    // under a renew interval.
    let step = support::LEASE / 12;

    // A drain deadline the test never reaches, so the revocation is not
    // forced through the task before the drop.
    let mut cfg_a = config(Some("worker-a"));
    cfg_a.drain_deadline = DEADLINE;
    let mut a = StoreCoordinator::with_clock(
        store.clone(),
        cfg_a,
        rt.handle().clone(),
        None,
        clock.clone(),
    )
    .expect("coordinator");
    a.start(planner()).unwrap();
    let mut fleet = support::Fleet::new(&store.inner, rt.handle());
    fleet.join(&a);
    let mut held_a = Held::default();
    let deadline = Instant::now() + DEADLINE;
    while held_a.splits.len() < ids.len() {
        assert!(Instant::now() < deadline, "worker-a never took the plan");
        fleet.step(clock, step);
        held_a.fold(a.poll().expect("poll a"));
    }
    support::commit_held(&mut a, &held_a);

    let mut b =
        support::worker_with_clock(&store.inner, rt.handle(), Some("worker-b"), clock.clone());
    b.start(planner()).unwrap();
    fleet.join(&b);
    let mut held_b = Held::default();
    while held_a.revoke_requests.is_empty() {
        assert!(Instant::now() < deadline, "no revocation was requested");
        fleet.step(clock, step);
        held_a.fold(a.poll().expect("poll a"));
        held_b.fold(b.poll().expect("poll b"));
    }
    let moved = held_a.revoke_requests[0].clone();
    let key = format!("split.{moved}");
    *store.armed.lock().expect("armed") = Some(key.clone());

    // `Drop` runs the direct release and blocks, so it gets its own thread.
    // The clock does not move until it returns.
    fleet.forget("worker-a");
    let (dropped_tx, dropped) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        drop(a);
        let _ = dropped_tx.send(());
    });
    dropped
        .recv_timeout(DEADLINE)
        .expect("the direct release never returned");
    assert!(
        store.fired.load(Ordering::Acquire),
        "the direct release never deleted the lease of {moved}, so this proved nothing"
    );

    while !held_b.splits.contains_key(&moved) {
        assert!(Instant::now() < deadline, "worker-b never took {moved}");
        fleet.step(clock, step);
        held_b.fold(b.poll().expect("poll b"));
    }
    let entry = rt
        .block_on(store.inner.get(Keyspace::Durable, &key))
        .expect("get")
        .expect("split record");
    (moved, record_json(&entry.value))
}

/// A peer that claims a split before the direct release handing it back
/// returns consumes no delivery attempt. Regression for #759.
#[test]
fn a_claim_during_a_direct_release_consumes_no_attempt() {
    let clock = TestClock::frozen();
    let store = ReleaseStore::new(
        support::store_with_clock(clock.clone()),
        LeaseDeleteFault::HoldUntilClaimed,
    );
    let (moved, record) = drop_during_revocation(&store, &clock);
    assert_eq!(
        store.claimed_by.lock().expect("claimed by").as_deref(),
        Some("worker-b"),
        "worker-b did not claim {moved} before the direct release returned, so this proved nothing"
    );
    assert_eq!(
        record["attempts"], 0,
        "a graceful direct release charged {moved} a delivery attempt: {record}"
    );
}

/// A direct release whose lease delete fails still hands the split back
/// without an attempt: the peer claims it once the lease expires.
#[test]
fn a_direct_release_whose_lease_delete_fails_consumes_no_attempt() {
    let clock = TestClock::frozen();
    let store = ReleaseStore::new(
        support::store_with_clock(clock.clone()),
        LeaseDeleteFault::Fail,
    );
    let (moved, record) = drop_during_revocation(&store, &clock);
    assert_eq!(
        record["attempts"], 0,
        "a direct release with a failed lease delete charged {moved} a delivery attempt: {record}"
    );
}

/// A store that opens a TCP connection on every operation once `io` is set,
/// as a client that dials per request does.
#[derive(Clone)]
struct DialingStore {
    inner: MemoryStore,
    io: Arc<AtomicBool>,
    peer: std::net::SocketAddr,
}

impl DialingStore {
    async fn dial(&self) -> Result<(), StoreError> {
        if self.io.load(Ordering::Acquire) {
            tokio::net::TcpStream::connect(self.peer)
                .await
                .map_err(|e| StoreError::Retryable(format!("dial: {e}")))?;
        }
        Ok(())
    }
}

impl CoordinationStore for DialingStore {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        self.dial().await?;
        self.inner.create(ks, key, value).await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        self.dial().await?;
        self.inner.update(ks, key, value, expected).await
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        self.dial().await?;
        self.inner.get(ks, key).await
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.dial().await?;
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.dial().await?;
        self.inner.list(ks, prefix).await
    }
}

/// Dropping a coordinator hands its splits back through a store whose
/// client opens connections, so a peer claims them without waiting out the
/// lease.
#[test]
fn a_drop_time_release_can_open_connections() {
    let rt = runtime();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let store = DialingStore {
        inner: MemoryStore::new(support::LEASE),
        io: Arc::new(AtomicBool::new(false)),
        peer: listener.local_addr().unwrap(),
    };
    let mut w = StoreCoordinator::new(
        store.clone(),
        config(Some("worker-a")),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    w.start(Box::new(PhasedPlanner::one_final("dial:v1", &["d0"])))
        .unwrap();
    support::drive(&mut w, &mut Held::default(), "claiming d0", |h| {
        h.splits.len() == 1
    });

    store.io.store(true, Ordering::Release);
    drop(w);
    let record = rt
        .block_on(store.inner.get(Keyspace::Durable, "split.d0"))
        .unwrap()
        .expect("record");
    let record = record_json(&record.value);
    assert!(
        record["owner"].is_null(),
        "the split was not released: {record}"
    );
}
