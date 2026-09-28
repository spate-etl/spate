//! Regressions for store reads that run beside the task loop: a listing or
//! a seeding run that outlasts a lease leaves renewals running, and a
//! listing older than what the view learned meanwhile neither drops a claim
//! made since nor restores a lease deleted since.

mod support;

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchStream,
};
use spate_coordination::{
    CoordinationConfig, CoordinationEvent, SplitCoordinator, SplitProgress, StoreCoordinator,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use support::{
    CountingStore, DEADLINE, Held, LEASE, PhasedPlanner, config, drive, runtime, split_id, store,
};

/// A store whose ephemeral listings of `""` are held back. The first is
/// read at once and delivered when [`release`](Self::release) is called;
/// later ones never finish, so only the first can change a view.
#[derive(Clone)]
struct HeldListing {
    inner: MemoryStore,
    read: Arc<AtomicBool>,
    released: Arc<tokio::sync::Notify>,
    calls: Arc<AtomicU64>,
}

impl HeldListing {
    fn new(inner: MemoryStore) -> HeldListing {
        HeldListing {
            inner,
            read: Arc::default(),
            released: Arc::default(),
            calls: Arc::default(),
        }
    }

    fn release(&self) {
        self.released.notify_one();
    }
}

impl CoordinationStore for HeldListing {
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
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        if ks != Keyspace::Ephemeral || !prefix.is_empty() {
            return self.inner.list(ks, prefix).await;
        }
        if self.calls.fetch_add(1, Ordering::AcqRel) > 0 {
            return std::future::pending().await;
        }
        let listed = self.inner.list(ks, prefix).await?;
        self.read.store(true, Ordering::Release);
        self.released.notified().await;
        Ok(listed)
    }
}

/// A store whose listings of `prefix` in `ks` each take `delay`.
#[derive(Clone)]
struct SlowListing {
    inner: MemoryStore,
    ks: Keyspace,
    prefix: &'static str,
    delay: Duration,
}

impl CoordinationStore for SlowListing {
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
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        if ks == self.ks && prefix == self.prefix {
            tokio::time::sleep(self.delay).await;
        }
        self.inner.list(ks, prefix).await
    }
}

fn tuned(instance: &str, tune: impl FnOnce(&mut CoordinationConfig)) -> CoordinationConfig {
    let mut config = config(Some(instance));
    tune(&mut config);
    config
}

/// Poll `worker` for `window`, failing on any `Lost`.
fn hold_for(worker: &mut impl SplitCoordinator, held: &mut Held, window: Duration, what: &str) {
    let until = Instant::now() + window;
    while Instant::now() < until {
        for event in worker.poll().unwrap_or_else(|e| panic!("{what}: {e}")) {
            assert!(
                !matches!(event, CoordinationEvent::Lost { .. }),
                "{what}: {event:?}"
            );
            held.fold(vec![event]);
        }
        std::thread::sleep(support::POLL_INTERVAL);
    }
}

/// A numeric field of the plan record, 0 before the record exists.
fn plan_field(rt: &tokio::runtime::Runtime, store: &MemoryStore, field: &str) -> u64 {
    rt.block_on(store.get(Keyspace::Durable, "plan"))
        .unwrap()
        .map_or(0, |plan| {
            serde_json::from_slice::<serde_json::Value>(&plan.value).unwrap()[field]
                .as_u64()
                .unwrap()
        })
}

/// A reconcile listing that takes longer than a lease leaves renewals
/// running, so the worker keeps its split.
#[test]
fn a_listing_slower_than_a_lease_leaves_renewals_running() {
    let rt = runtime();
    let inner = store();
    let slow = SlowListing {
        inner: inner.clone(),
        ks: Keyspace::Ephemeral,
        prefix: "",
        delay: LEASE * 2,
    };
    let mut w = StoreCoordinator::new(slow, config(Some("worker-a")), rt.handle().clone(), None)
        .expect("coordinator");
    w.start(Box::new(PhasedPlanner::one_final("slow-list:v1", &["x"])))
        .unwrap();
    let mut held = Held::default();
    drive(&mut w, &mut held, "claiming x", |h| h.splits.len() == 1);
    hold_for(
        &mut w,
        &mut held,
        LEASE * 3,
        "holding x through slow listings",
    );
    assert_eq!(held.splits.len(), 1);
}

/// A verdict listing that takes longer than a lease leaves renewals
/// running, so the worker's presence never expires while it waits.
#[test]
fn a_verdict_listing_slower_than_a_lease_leaves_renewals_running() {
    let rt = runtime();
    let inner = store();
    let expired = Arc::new(AtomicBool::new(false));
    let mut presence = rt
        .block_on(inner.watch(Keyspace::Ephemeral, "worker."))
        .unwrap();
    let seen = Arc::clone(&expired);
    rt.spawn(async move {
        use futures_util::StreamExt as _;
        while let Some(Ok(event)) = presence.next().await {
            if matches!(event, WatchEvent::Delete { .. }) {
                seen.store(true, Ordering::Release);
            }
        }
    });
    let slow = SlowListing {
        inner: inner.clone(),
        ks: Keyspace::Durable,
        prefix: "split.",
        delay: LEASE * 2,
    };
    let mut w = StoreCoordinator::new(slow, config(Some("worker-a")), rt.handle().clone(), None)
        .expect("coordinator");
    w.start(Box::new(PhasedPlanner::one_final(
        "slow-verdict:v1",
        &["x"],
    )))
    .unwrap();
    let mut held = Held::default();
    drive(&mut w, &mut held, "claiming x", |h| h.splits.len() == 1);
    w.commit(&split_id("x"), &SplitProgress::completed(1, vec![]))
        .unwrap();
    drive(&mut w, &mut held, "the verdict", |h| h.all_complete);
    assert!(
        !expired.load(Ordering::Acquire),
        "the worker's presence expired while a listing ran"
    );
}

/// A seeding run that takes longer than a lease leaves the leader renewing,
/// so it plans under the generation it was elected with. Each create stays
/// inside `op_timeout`; the run is long because there are many of them.
#[test]
fn seeding_slower_than_a_lease_keeps_the_leadership() {
    let rt = runtime();
    let inner = store();
    let counting = CountingStore::new(inner.clone()).with_create_delay(LEASE / 10);
    let ids: Vec<String> = (0..704).map(|i| format!("s{i}")).collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    let mut w = counting.worker(rt.handle(), "worker-a");
    w.start(Box::new(PhasedPlanner::one_final("slow-seed:v1", &ids)))
        .unwrap();
    let mut held = Held::default();
    let deadline = Instant::now() + DEADLINE;
    while plan_field(&rt, &inner, "planned") < ids.len() as u64 {
        assert!(Instant::now() < deadline, "the plan was never published");
        held.fold(w.poll().expect("poll"));
        std::thread::sleep(support::POLL_INTERVAL);
    }
    assert_eq!(
        plan_field(&rt, &inner, "generation"),
        1,
        "the leader was re-elected while seeding"
    );
}

/// A listing read before this worker claimed a split omits the split's
/// lease. Applied after the claim, it leaves the claim alone.
#[test]
fn a_listing_older_than_a_claim_does_not_drop_it() {
    let rt = runtime();
    let inner = store();
    let listing = HeldListing::new(inner.clone());
    let mut w = StoreCoordinator::new(
        listing.clone(),
        tuned("worker-a", |c| c.max_in_flight = 1),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    w.start(Box::new(PhasedPlanner::one_final(
        "older-listing:v1",
        &["a", "b"],
    )))
    .unwrap();
    let mut held = Held::default();
    drive(&mut w, &mut held, "claiming one split", |h| {
        h.splits.len() == 1
    });
    spate_test::wait_until(DEADLINE, "the listing read", || {
        listing.read.load(Ordering::Acquire)
    });

    let first = held.splits.keys().next().cloned().expect("held");
    w.commit(&split_id(&first), &SplitProgress::completed(1, vec![]))
        .unwrap();
    held.splits.remove(&first);
    drive(&mut w, &mut held, "claiming the other split", |h| {
        h.splits.len() == 1
    });

    listing.release();
    hold_for(&mut w, &mut held, LEASE, "after the older listing landed");
    assert_eq!(held.splits.len(), 1);
}

/// A listing read while a lease was live, applied after that lease was
/// deleted, does not bring it back, so the split stays claimable.
#[test]
fn a_listing_does_not_restore_a_lease_deleted_since() {
    let rt = runtime();
    let inner = store();
    let listing = HeldListing::new(inner.clone());
    let mut w = StoreCoordinator::new(
        listing.clone(),
        tuned("worker-a", |c| c.max_in_flight = 1),
        rt.handle().clone(),
        None,
    )
    .expect("coordinator");
    w.start(Box::new(PhasedPlanner::one_final(
        "deleted-since:v1",
        &["a", "b"],
    )))
    .unwrap();
    let mut held = Held::default();
    drive(&mut w, &mut held, "claiming one split", |h| {
        h.splits.len() == 1
    });
    let first = held.splits.keys().next().cloned().expect("held");
    let other = if first == "a" { "b" } else { "a" };

    // A peer's lease on the queued split, live when the listing reads it.
    let foreign = serde_json::json!({
        "schema": 3, "owner": "worker-z", "nonce": "z", "epoch": 1
    });
    let lease_key = format!("split.{other}");
    let rev = rt
        .block_on(inner.create(
            Keyspace::Ephemeral,
            &lease_key,
            serde_json::to_vec(&foreign).unwrap(),
        ))
        .unwrap()
        .won()
        .expect("foreign lease");
    spate_test::wait_until(DEADLINE, "the listing read", || {
        listing.read.load(Ordering::Acquire)
    });
    let deleted = rt
        .block_on(inner.delete(Keyspace::Ephemeral, &lease_key, Some(rev)))
        .unwrap();
    assert!(matches!(deleted, CasOutcome::Won(_)));
    listing.release();
    hold_for(&mut w, &mut held, LEASE / 2, "after the listing landed");

    w.commit(&split_id(&first), &SplitProgress::completed(1, vec![]))
        .unwrap();
    held.splits.remove(&first);
    drive(
        &mut w,
        &mut held,
        "claiming the split whose lease was deleted",
        |h| h.splits.contains_key(other),
    );
}
