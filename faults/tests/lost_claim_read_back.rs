//! The real coordinator after a lost claim reply whose read-back fails once,
//! judged by the lost-reply check.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchStream,
};
use spate_coordination::{
    CoordinationError, CoordinationEvent, PlanContext, PlanFinality, PlannedSplit,
    SplitCoordinator, SplitId, SplitPlan, SplitPlanner, SplitSpec, StoreCoordinator,
};
use spate_faults::classify::{Classifier, WriteKind};
use spate_faults::expect::lost_replies;
use spate_faults::journal::{self, Event, Journal};
use spate_faults::oracle::ProcessJournal;
use spate_faults::store::{AbortAt, AbortMode, AbortPlan, JournalStore};
use spate_faults::worker::Tuning;

const IDLE: u8 = 0;
const ARMED: u8 = 1;
const SPENT: u8 = 2;

/// How the first durable `split.*` read after the first landed durable
/// `split.*` update fails.
#[derive(Clone, Copy)]
enum ReadBack {
    Error,
    Hang,
}

/// A [`MemoryStore`] whose read-back of a landed claim fails once.
#[derive(Clone)]
struct FailReadBack {
    inner: MemoryStore,
    how: ReadBack,
    state: Arc<AtomicU8>,
}

impl CoordinationStore for FailReadBack {
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
        let out = self.inner.update(ks, key, value, expected).await;
        if ks == Keyspace::Durable
            && key.starts_with("split.")
            && matches!(out, Ok(CasOutcome::Won(_)))
        {
            let _ = self
                .state
                .compare_exchange(IDLE, ARMED, Ordering::SeqCst, Ordering::SeqCst);
        }
        out
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        if ks == Keyspace::Durable
            && key.starts_with("split.")
            && self
                .state
                .compare_exchange(ARMED, SPENT, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            match self.how {
                ReadBack::Error => return Err(StoreError::Retryable("read failed".to_owned())),
                ReadBack::Hang => std::future::pending::<()>().await,
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
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
}

struct OneSplit;

impl SplitPlanner for OneSplit {
    fn fingerprint(&self) -> String {
        "fp".to_owned()
    }

    fn plan(&mut self, ctx: PlanContext<'_>) -> Result<SplitPlan, CoordinationError> {
        let splits = if ctx.planner_state.is_some() {
            Vec::new()
        } else {
            vec![PlannedSplit::new(SplitSpec::new(
                SplitId::new("a").unwrap(),
                b"descriptor:a".to_vec(),
            ))]
        };
        Ok(SplitPlan::new(splits, PlanFinality::Final).with_planner_state(b"1".to_vec()))
    }
}

fn run(how: ReadBack) -> (Vec<journal::Line>, spate_faults::outcome::LostReplies) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let tuning = Tuning::nats();
    let lease = Duration::from_millis(tuning.lease_ms);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("w0-1.ndjson");
    let journal = Arc::new(Journal::open(&path).unwrap());
    let classifier = Arc::new(Classifier::new("w0"));
    let inner = FailReadBack {
        inner: MemoryStore::new(lease),
        how,
        state: Arc::new(AtomicU8::new(IDLE)),
    };
    let store = JournalStore::new(inner, Arc::clone(&journal), Arc::clone(&classifier));
    let plan = AbortPlan {
        kind: WriteKind::Claim,
        n: 1,
        mode: AbortMode::ErrAfterLand,
    };
    let store = AbortAt::new(store, plan, Arc::clone(&journal), classifier);
    let mut coordinator =
        StoreCoordinator::new(store, tuning.coordination("w0"), rt.handle().clone(), None).unwrap();
    coordinator.start(Box::new(OneSplit)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut gained = false;
    while !gained {
        assert!(Instant::now() < deadline, "the split was never gained");
        for event in coordinator.poll().unwrap() {
            if matches!(event, CoordinationEvent::Gained { .. }) {
                gained = true;
            }
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    drop(coordinator);
    let lines = journal::read(&path).unwrap();
    let pj = ProcessJournal {
        instance: "w0".to_owned(),
        pid: 1,
        lines: lines.clone(),
    };
    let verdict = lost_replies(&[pj], true);
    rt.shutdown_timeout(Duration::from_secs(1));
    (lines, verdict)
}

/// A read-back that returns an error is journalled, and the reclaim after it
/// passes the check.
#[test]
fn a_failed_read_back_licenses_the_reclaim() {
    let (lines, verdict) = run(ReadBack::Error);
    assert!(
        lines
            .iter()
            .any(|l| matches!(l.event, Event::ErrAfterLand { .. }))
    );
    assert_eq!(verdict.lines, 1);
    assert!(verdict.unexplained.is_empty(), "{:?}", verdict.unexplained);
}

/// A read-back that runs past `op_timeout` is journalled, and the reclaim
/// after it passes the check.
#[test]
fn a_timed_out_read_back_licenses_the_reclaim() {
    let (lines, verdict) = run(ReadBack::Hang);
    assert!(
        lines
            .iter()
            .any(|l| matches!(l.event, Event::ErrAfterLand { .. }))
    );
    assert_eq!(verdict.lines, 1);
    assert!(verdict.unexplained.is_empty(), "{:?}", verdict.unexplained);
}
