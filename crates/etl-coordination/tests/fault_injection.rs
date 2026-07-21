//! Fault-injection regressions: scripted store failures at the exact
//! writes whose loss used to wedge the protocol. Both scenarios run over
//! the real coordinator through the public API — the fault store is a
//! [`CoordinationStore`] like any custom backend.

mod support;

use etl_coordination::store::memory::MemoryStore;
use etl_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchStream,
};
use etl_coordination::{
    Clock, CoordinationEvent, SplitCoordinator, SplitProgress, StoreCoordinator,
};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use support::{DEADLINE, Held, PhasedPlanner, config, runtime, split_id};

/// A [`MemoryStore`] with scripted faults on specific writes.
#[derive(Clone)]
struct FaultStore {
    inner: MemoryStore,
    /// Per-call script for durable updates of the plan record: `true`
    /// fails the call (Retryable, nothing written). Exhausted = pass.
    plan_update_script: Arc<Mutex<VecDeque<bool>>>,
    /// Once: the next ephemeral split-lease update WRITES but returns an
    /// error — the maybe-landed renewal.
    lease_maybe_land: Arc<AtomicBool>,
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
}

impl FaultStore {
    fn new(inner: MemoryStore) -> FaultStore {
        FaultStore {
            inner,
            plan_update_script: Arc::new(Mutex::new(VecDeque::new())),
            lease_maybe_land: Arc::new(AtomicBool::new(false)),
            drop_owner_clear: Arc::new(AtomicBool::new(false)),
            drop_assignment_publish: Arc::new(AtomicBool::new(false)),
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
        self.inner.create(ks, key, value).await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
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
            // wedged by this on its own — the point is that the leader
            // must notice and republish rather than believing the fleet
            // was told, which is the failure the old grant-annotation
            // knob covered in the negotiated protocol.
            return Err(StoreError::Retryable(
                "injected: assignment publish dropped".into(),
            ));
        }
        if ks == Keyspace::Ephemeral
            && key.starts_with("split.")
            && self.lease_maybe_land.swap(false, Ordering::AcqRel)
        {
            // The write LANDS but the caller sees a failure — the
            // maybe-landed renewal a flaky round-trip produces.
            let _ = self.inner.update(ks, key, value, expected).await?;
            return Err(StoreError::Retryable(
                "injected: renewal reply lost after the write landed".into(),
            ));
        }
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
/// store on the next run — under the pre-fix accounting (`planned +=
/// creates won this run`) the re-run counted zero, the totals never
/// matched again, and a bounded job idled forever instead of draining.
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
    // without the recount this drive times out — nothing ever fires.
    support::drive(&mut worker, &mut held, "healing the failed publish", |h| {
        h.all_complete
    });
}

/// A renewal whose write lands but whose reply is lost must be ADOPTED on
/// the next heartbeat (the lease still carries our owner+nonce), not
/// treated as a fence: the pre-fix path dropped the split as Lost and
/// re-acquired it through an attempt-consuming reclaim — four flakes
/// quarantined a healthy split.
#[test]
fn maybe_landed_renewal_is_adopted_not_fenced() {
    let rt = runtime();
    // Freeze time. The self-fence (task) and lease expiry (store) both read
    // this clock, so a real wall-clock stall — a renewal task starved past
    // the lease TTL under parallel CI load — can no longer expire the lease:
    // `clock.now() - last_ok_write` stays 0 while frozen. Only a genuine peer
    // fence could drop the split, and there is no peer here. Renewal *cadence*
    // still runs on real `.elapsed()`, so the maybe-landed fault below is
    // really exercised. This removes the wall-clock flake (#45) without
    // weakening what the test asserts.
    let clock: Arc<dyn Clock> = support::TestClock::frozen();
    let store = FaultStore::new(MemoryStore::with_clock(support::LEASE, clock.clone()));
    let lease_maybe_land = store.lease_maybe_land.clone();

    let planner = Box::new(PhasedPlanner::one_final("renewal-fault:v1", &["r0"]));
    let mut worker = StoreCoordinator::with_clock(
        store,
        config(Some("solo")),
        rt.handle().clone(),
        None,
        clock,
    )
    .expect("coordinator");
    worker.start(planner).unwrap();

    let mut held = Held::default();
    support::drive(&mut worker, &mut held, "claiming the split", |h| {
        h.splits.len() == 1
    });
    // Arm the fault: the next lease renewal writes but reports an error, and
    // the one after loses its CAS against the landed write and must ADOPT it
    // (not fence). Drive until the fault has fired (the flag clears), then a
    // settle window so the adopting renewal runs — asserting no
    // Lost/Quarantined throughout. Frozen time makes this robust: the
    // negative assertion can only be tripped by a real mishandling of the
    // maybe-landed renewal, never by a scheduler stall.
    lease_maybe_land.store(true, Ordering::Release);
    let deadline = Instant::now() + DEADLINE;
    let mut settle_until: Option<Instant> = None;
    loop {
        assert!(Instant::now() < deadline, "adoption never settled");
        for event in worker.poll().expect("poll") {
            assert!(
                !matches!(
                    event,
                    CoordinationEvent::Lost { .. } | CoordinationEvent::Quarantined { .. }
                ),
                "a maybe-landed renewal must not cost the split: {event:?}"
            );
            held.fold(vec![event]);
        }
        match settle_until {
            // Fault has fired: watch a few more renewal intervals so the
            // adopting renewal executes under the no-loss assertion.
            None if !lease_maybe_land.load(Ordering::Acquire) => {
                settle_until = Some(Instant::now() + support::LEASE);
            }
            Some(until) if Instant::now() >= until => break,
            _ => {}
        }
    }
    assert_eq!(held.splits.len(), 1, "still held");
    // The tenancy is intact: the fenced commit path would reject this.
    worker
        .commit(&split_id("r0"), &SplitProgress::completed(7, vec![]))
        .unwrap();
    support::drive(&mut worker, &mut held, "completing after the flake", |h| {
        h.all_complete
    });
}

/// A dropped assignment publish must be republished, not treated as
/// delivered. The leader caches the revision it believes each
/// `assign.{instance}` record holds so it can CAS the next one; if a
/// failed write left that cache believing a write it never made, every
/// later publish for that instance would CAS against a revision the store
/// never had and lose forever — and the worker, never having seen a
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
