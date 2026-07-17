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
}

impl FaultStore {
    fn new(inner: MemoryStore) -> FaultStore {
        FaultStore {
            inner,
            plan_update_script: Arc::new(Mutex::new(VecDeque::new())),
            lease_maybe_land: Arc::new(AtomicBool::new(false)),
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
        for event in worker.poll(Duration::from_millis(25)).expect("poll") {
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
