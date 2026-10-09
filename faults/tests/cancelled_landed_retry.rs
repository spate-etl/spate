//! The real coordinator after a lost commit reply whose retry lands with its
//! reply cancelled at `op_timeout`, judged by the lost-reply check.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchMode, WatchStream,
};
use spate_coordination::{
    CoordinationConfig, CoordinationError, CoordinationErrorKind, CoordinationEvent, PlanContext,
    PlanFinality, PlannedSplit, SplitCoordinator as _, SplitId, SplitPlan, SplitPlanner,
    SplitProgress, SplitSpec, StoreCoordinator,
};
use spate_faults::classify::{Classifier, WriteKind};
use spate_faults::expect::lost_replies;
use spate_faults::journal::{self, Event, Journal, Source};
use spate_faults::oracle::ProcessJournal;
use spate_faults::store::{AbortAt, AbortMode, AbortPlan, JournalStore};

const LEASE: Duration = Duration::from_millis(3000);
const OP_TIMEOUT: Duration = Duration::from_millis(300);

/// Once armed, the next durable `split.*` update that wins is held past
/// `op_timeout` after it lands.
#[derive(Clone)]
struct Stall {
    inner: MemoryStore,
    armed: Arc<AtomicBool>,
}

impl CoordinationStore for Stall {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }
    fn watch_mode(&self) -> WatchMode {
        self.inner.watch_mode()
    }
    async fn create(&self, ks: Keyspace, key: &str, v: Vec<u8>) -> Result<CasOutcome, StoreError> {
        self.inner.create(ks, key, v).await
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
            && self.armed.swap(false, Ordering::SeqCst)
        {
            tokio::time::sleep(OP_TIMEOUT * 3).await;
        }
        out
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

struct OnePlan;

impl SplitPlanner for OnePlan {
    fn fingerprint(&self) -> String {
        "fp".to_owned()
    }
    fn plan(&mut self, ctx: PlanContext<'_>) -> Result<SplitPlan, CoordinationError> {
        let splits = if ctx.planner_state.is_some() {
            Vec::new()
        } else {
            vec![PlannedSplit::new(SplitSpec::new(
                SplitId::new("x0").unwrap(),
                b"d".to_vec(),
            ))]
        };
        Ok(SplitPlan::new(splits, PlanFinality::Final).with_planner_state(b"1".to_vec()))
    }
}

fn config() -> CoordinationConfig {
    let mut cfg = CoordinationConfig::default();
    cfg.lease_duration = LEASE;
    cfg.op_timeout = OP_TIMEOUT;
    cfg.instance_id = Some("w0".to_owned());
    cfg.replan_interval = LEASE;
    cfg.reconcile_interval = LEASE / 5;
    cfg.drain_deadline = LEASE / 2;
    cfg.rebalance_delay = Duration::ZERO;
    cfg
}

fn pump(c: &mut impl spate_coordination::SplitCoordinator, gained: &mut bool) {
    for e in c.poll().expect("poll") {
        if let CoordinationEvent::Gained { split, .. } = e
            && split.id.as_str() == "x0"
        {
            *gained = true;
        }
    }
    std::thread::sleep(Duration::from_millis(5));
}

/// A commit sent from the cancelled retry's revision that wins passes the
/// check.
#[test]
fn coordinator_recovery_through_a_cancelled_landed_retry_passes_the_check() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("w0-1.ndjson");
    let journal = Arc::new(Journal::open(&path).unwrap());
    let classifier = Arc::new(Classifier::new("w0"));
    let armed = Arc::new(AtomicBool::new(false));
    let inner = Stall {
        inner: MemoryStore::new(LEASE),
        armed: Arc::clone(&armed),
    };
    let journalled = JournalStore::new(inner, Arc::clone(&journal), Arc::clone(&classifier));
    let plan = AbortPlan {
        kind: WriteKind::Commit,
        n: 1,
        mode: AbortMode::ErrAfterLand,
    };
    let store = AbortAt::new(
        journalled,
        plan,
        Arc::clone(&journal),
        Arc::clone(&classifier),
    );
    let mut c = StoreCoordinator::new(store, config(), rt.handle().clone(), None).unwrap();
    c.start(Box::new(OnePlan)).unwrap();
    let split = SplitId::new("x0").unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut gained = false;
    while !gained {
        assert!(Instant::now() < deadline, "never gained x0");
        pump(&mut c, &mut gained);
    }

    // The lost reply: the commit lands and its caller sees `Retryable`.
    let e = c
        .commit(&split, &SplitProgress::new(10, vec![]))
        .expect_err("lost reply");
    assert_eq!(e.kind, CoordinationErrorKind::Retryable, "{e}");
    let lines = journal::read(&path).unwrap();
    let rev = lines
        .iter()
        .find_map(|l| match &l.event {
            Event::ErrAfterLand { rev, .. } => Some(*rev),
            _ => None,
        })
        .expect("err_after_land journalled");

    // Wait for the watch echo of the landed commit, then a little longer so
    // the task folds it.
    loop {
        assert!(
            Instant::now() < deadline,
            "no watch echo of the landed commit"
        );
        pump(&mut c, &mut gained);
        let lines = journal::read(&path).unwrap();
        if lines.iter().any(
            |l| matches!(&l.event, Event::Seen { rev: r, from: Source::Watch, .. } if *r == rev),
        ) {
            break;
        }
    }
    for _ in 0..20 {
        pump(&mut c, &mut gained);
    }

    // The retry lands and is cancelled at `op_timeout`.
    armed.store(true, Ordering::SeqCst);
    let e = c
        .commit(&split, &SplitProgress::new(20, vec![]))
        .expect_err("cancelled retry");
    assert_eq!(e.kind, CoordinationErrorKind::Retryable, "{e}");
    for _ in 0..20 {
        pump(&mut c, &mut gained);
    }
    c.commit(&split, &SplitProgress::new(30, vec![]))
        .expect("the next commit lands");

    let lines = journal::read(&path).unwrap();
    let judged = lost_replies(
        &[ProcessJournal {
            instance: "w0".to_owned(),
            pid: 1,
            lines,
        }],
        true,
    );
    assert_eq!(judged.lines, 1);
    assert_eq!(judged.unexplained, Vec::<String>::new());
}
