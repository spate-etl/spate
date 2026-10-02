//! A poison report refused by the store and queued in the driver must not
//! fail a later tenancy of the same split. Real `StoreCoordinator` over a
//! `MemoryStore` on a test clock, driven by the real `CoordinationDriver`.

mod support;

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchStream,
};
use spate_coordination::{SplitId, SplitProgress, SplitSpec, StoreCoordinator};
use spate_core::checkpoint::AckRef;
use spate_core::clock::tokio::Clock;
use spate_core::coordination::driver::{CoordinationDriver, SplitOpening, SplitSource};
use spate_core::error::{ErrorClass, SourceError};
use spate_core::record::{PartitionId, RawPayload};
use spate_core::source::{LaneId, PayloadBatch, SourceEvent, SourceLane};
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use support::{DEADLINE, LEASE, PhasedPlanner, TestClock, config};

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

/// Returns the durable record's attempts before and after the driver polls the
/// regained tenancy. With `regain_in_batch` a rejected gain, the loss of its
/// tenancy and the next regain drain in one batch; otherwise the gain is polled
/// first and the loss and regain share a later batch.
fn run(regain_in_batch: bool) -> (u64, u64) {
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
    let mut d = CoordinationDriver::new(Box::new(worker));
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

    if regain_in_batch {
        // The lease lapses again and the store regains the split (epoch 3), all before the driver polls.
        let epoch = record()["epoch"].as_u64().unwrap();
        let _ = expire_lease();
        step_until(
            "regain 3",
            Box::new(|| record()["epoch"].as_u64().unwrap() > epoch && !lease_gone()),
        );
        // The store goes unavailable; the driver rejects the epoch 2 gain and its report is
        // refused and queued, then the batch's loss ends that tenancy.
        refuse.store(true, Ordering::Release);
        src.reject_next_resume.set(true);
        assert!(
            d.poll_events(&mut src, Duration::ZERO).is_err(),
            "gain rejected"
        );
        refuse.store(false, Ordering::Release);
    } else {
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
    }

    let before = record()["attempts"].as_u64().unwrap();
    for _ in 0..4 {
        let _ = d.poll_events(&mut src, Duration::ZERO);
    }
    let after = record()["attempts"].as_u64().unwrap();
    (before, after)
}

/// The loss of a split and its regain drain in one batch; the report queued
/// before the loss must not fail the regained tenancy.
#[test]
fn stale_report_does_not_fail_a_regained_split_in_one_batch() {
    let (before, after) = run(false);
    assert_eq!(
        after, before,
        "the stale report failed the regained tenancy"
    );
}

/// A gain rejected in the batch that also loses and regains its split queues a
/// report that must not fail the regained tenancy. Regression for #904.
#[test]
fn report_queued_by_the_batch_that_loses_its_split_is_dropped() {
    let (before, after) = run(true);
    assert_eq!(
        after, before,
        "the stale report failed the regained tenancy"
    );
}
