use std::time::Duration;

use futures_util::FutureExt as _;
use spate_coordination::store::memory::MemoryStore;

use super::*;
use crate::journal::{Line, Status};

const LEASE: Duration = Duration::from_secs(2);

fn record_bytes(epoch: u64, owner: Option<&str>) -> Vec<u8> {
    record_at(epoch, owner, None)
}

fn record_at(epoch: u64, owner: Option<&str>, watermark: Option<i64>) -> Vec<u8> {
    serde_json::json!({
        "schema": 3, "id": "a", "fp": 1, "epoch": epoch, "status": "runnable",
        "owner": owner, "attempts": 0, "watermark": watermark, "state": null,
        "completed": false, "written_at_ms": 5,
    })
    .to_string()
    .into_bytes()
}

fn progress(epoch: u64, owner: Option<&str>) -> Progress {
    Progress {
        schema: 3,
        epoch,
        owner: owner.map(str::to_owned),
        watermark: None,
        completed: false,
        status: Status::Runnable,
        attempts: 0,
    }
}

/// A store whose updates never complete and whose reads fail, with a polled
/// watch and an `op_timeout`, over a [`MemoryStore`].
#[derive(Clone)]
struct Hang(MemoryStore);

impl CoordinationStore for Hang {
    fn lease_ttl(&self) -> Duration {
        LEASE
    }

    fn watch_mode(&self) -> WatchMode {
        WatchMode::Polled {
            interval: Duration::from_millis(300),
        }
    }

    fn op_timeout(&self) -> Option<Duration> {
        Some(Duration::from_millis(700))
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        self.0.create(ks, key, value).await
    }

    async fn update(
        &self,
        _ks: Keyspace,
        _key: &str,
        _value: Vec<u8>,
        _expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        std::future::pending().await
    }

    async fn get(&self, _ks: Keyspace, _key: &str) -> Result<Option<Entry>, StoreError> {
        Err(StoreError::Retryable("read failed".to_owned()))
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.0.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.0.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.0.list(ks, prefix).await
    }
}

fn journalled<S>(inner: S) -> (JournalStore<S>, tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("w0-1.ndjson");
    let journal = Arc::new(Journal::open(&path).unwrap());
    (
        JournalStore::new(inner, journal, Arc::new(Classifier::new("w0"))),
        dir,
        path,
    )
}

fn events(path: &std::path::Path) -> Vec<Event> {
    crate::journal::read(path)
        .unwrap()
        .into_iter()
        .map(|l: Line| l.event)
        .collect()
}

/// An update dropped before it returns, as `op_timeout` drops one, is
/// journalled as `done: cancelled` against its `send`.
#[tokio::test]
async fn journal_store_writes_cancelled_on_drop() {
    let (store, _dir, path) = journalled(Hang(MemoryStore::new(LEASE)));
    let created = store
        .create(Keyspace::Durable, "split.a", record_bytes(1, None))
        .await
        .unwrap();
    let Some(rev) = created.won() else {
        panic!("create lost")
    };
    let update = store.update(
        Keyspace::Durable,
        "split.a",
        record_bytes(2, Some("w0")),
        rev,
    );
    assert!(
        update.now_or_never().is_none(),
        "the update never completes"
    );

    assert_eq!(
        events(&path),
        [
            Event::Send {
                call: 1,
                op: WriteOp::Create,
                key: "split.a".to_owned(),
                expected: None,
                value: progress(1, None),
            },
            Event::Done {
                call: 1,
                key: "split.a".to_owned(),
                reply: Reply::Won(rev.0),
            },
            Event::Send {
                call: 2,
                op: WriteOp::Update,
                key: "split.a".to_owned(),
                expected: Some(rev.0),
                value: progress(2, Some("w0")),
            },
            Event::Done {
                call: 2,
                key: "split.a".to_owned(),
                reply: Reply::Cancelled,
            },
        ]
    );
}

/// Every wrapper reports the inner store's lease, watch mode and
/// `op_timeout`, which the coordinator checks against its config.
#[test]
fn wrappers_forward_op_timeout_and_watch_mode() {
    let (store, _dir, _path) = journalled(Hang(MemoryStore::new(LEASE)));
    let journal = Arc::clone(&store.journal);
    let classifier = Arc::clone(&store.classifier);
    let plan = plan(WriteKind::Commit, 1, AbortMode::Before);
    let aborting = AbortAt::new(
        store.clone(),
        plan,
        Arc::clone(&journal),
        Arc::clone(&classifier),
    );
    let fence = Arc::new(Fence::default());
    let broken = BrokenFence::new(store.clone(), Arc::clone(&fence));
    let stopping = StopAt::new(store.clone(), None, Some(fence), None, journal, classifier);
    for (lease, op_timeout, watch) in [
        (store.lease_ttl(), store.op_timeout(), store.watch_mode()),
        (
            aborting.lease_ttl(),
            aborting.op_timeout(),
            aborting.watch_mode(),
        ),
        (broken.lease_ttl(), broken.op_timeout(), broken.watch_mode()),
        (
            stopping.lease_ttl(),
            stopping.op_timeout(),
            stopping.watch_mode(),
        ),
    ] {
        assert_eq!(lease, LEASE);
        assert_eq!(op_timeout, Some(Duration::from_millis(700)));
        assert_eq!(
            watch,
            WatchMode::Polled {
                interval: Duration::from_millis(300)
            }
        );
    }
}

fn plan(kind: WriteKind, n: u32, mode: AbortMode) -> AbortPlan {
    AbortPlan { kind, n, mode }
}

/// A plan counts only writes of its kind: a `Before` plan fires at the `n`th
/// such write and no other, and a plan that waits for a landed write is armed
/// from the `n`th on until it fires, once.
#[test]
fn abort_at_counts_only_its_kind() {
    let kinds = [
        Some(WriteKind::Quarantine),
        Some(WriteKind::Claim),
        Some(WriteKind::Complete),
        Some(WriteKind::Commit),
        Some(WriteKind::Release),
        Some(WriteKind::FailReport),
        Some(WriteKind::Renew),
        None,
    ];
    let armed = |trigger: &Trigger, rounds| {
        let mut armed = Vec::new();
        for round in 0..rounds {
            for kind in kinds {
                if let Some(n) = trigger.arm(kind) {
                    armed.push((round, kind, n));
                }
            }
        }
        armed
    };
    let before = Trigger::new(plan(WriteKind::Commit, 2, AbortMode::Before));
    assert_eq!(armed(&before, 4), [(1, Some(WriteKind::Commit), 2)]);

    let after = Trigger::new(plan(WriteKind::Renew, 2, AbortMode::After));
    assert_eq!(
        armed(&after, 3),
        [
            (1, Some(WriteKind::Renew), 2),
            (2, Some(WriteKind::Renew), 3)
        ]
    );
    assert!(after.fire());
    assert!(!after.fire(), "a plan fires once");
    assert_eq!(armed(&after, 2), []);
}

/// A lost-reply plan on a commit also counts completions, so a process whose
/// splits each finish in one write reaches it; an abort on a commit does not.
#[test]
fn err_after_land_on_commit_counts_a_completion() {
    let complete = Some(WriteKind::Complete);
    let lost = Trigger::new(plan(WriteKind::Commit, 2, AbortMode::ErrAfterLand));
    assert_eq!(lost.arm(complete), None);
    assert_eq!(lost.arm(Some(WriteKind::Commit)), Some(2));
    assert_eq!(lost.arm(complete), Some(3));
    let abort = Trigger::new(plan(WriteKind::Commit, 1, AbortMode::After));
    assert_eq!(abort.arm(complete), None);
}

/// An `ErrAfterLand` commit lands in the store and is journalled `won`, then
/// its caller gets a retryable error after the `err_after_land` line; the next
/// commit is forwarded untouched.
#[tokio::test]
async fn err_after_land_forwards_then_returns_retryable() {
    let (journalled, _dir, path) = journalled(MemoryStore::new(LEASE));
    let journal = Arc::clone(&journalled.journal);
    let classifier = Arc::clone(&journalled.classifier);
    let plan = plan(WriteKind::Commit, 1, AbortMode::ErrAfterLand);
    let store = AbortAt::new(journalled, plan, journal, classifier);
    let claimed = store
        .create(Keyspace::Durable, "split.a", record_at(1, Some("w0"), None))
        .await
        .unwrap()
        .won()
        .unwrap();

    let lost = store
        .update(
            Keyspace::Durable,
            "split.a",
            record_at(1, Some("w0"), Some(10)),
            claimed,
        )
        .await;
    assert!(matches!(lost, Err(StoreError::Retryable(_))), "{lost:?}");
    let landed = store
        .inner
        .inner
        .get(Keyspace::Durable, "split.a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        Progress::parse(&landed.value).unwrap().watermark,
        Some(10),
        "the write landed"
    );
    let next = store
        .update(
            Keyspace::Durable,
            "split.a",
            record_at(1, Some("w0"), Some(20)),
            landed.revision,
        )
        .await
        .unwrap();
    assert!(next.won().is_some());

    let tail: Vec<Event> = events(&path).into_iter().skip(2).take(3).collect();
    assert_eq!(
        tail,
        [
            Event::Send {
                call: 2,
                op: WriteOp::Update,
                key: "split.a".to_owned(),
                expected: Some(claimed.0),
                value: Progress {
                    watermark: Some(10),
                    ..progress(1, Some("w0"))
                },
            },
            Event::Done {
                call: 2,
                key: "split.a".to_owned(),
                reply: Reply::Won(landed.revision.0),
            },
            Event::ErrAfterLand {
                key: "split.a".to_owned(),
                rev: landed.revision.0,
            },
        ]
    );
}

/// Durable `split.*` entries from a `get`, a `list` and a watch are
/// journalled as `seen`. Writes to other keys or to the ephemeral keyspace,
/// and reads of them, are not journalled, even with a progress record as the
/// value.
#[tokio::test]
async fn journals_durable_split_traffic_only() {
    let (store, _dir, path) = journalled(MemoryStore::new(LEASE));
    let split = store
        .inner
        .create(Keyspace::Durable, "split.a", record_bytes(1, None))
        .await
        .unwrap()
        .won()
        .unwrap();
    let _ = store
        .create(Keyspace::Durable, "plan", record_bytes(1, None))
        .await
        .unwrap();
    let _ = store
        .create(Keyspace::Ephemeral, "split.a", record_bytes(1, None))
        .await
        .unwrap();

    let _ = store.get(Keyspace::Durable, "split.a").await.unwrap();
    let _ = store.get(Keyspace::Durable, "plan").await.unwrap();
    let _ = store.get(Keyspace::Ephemeral, "split.a").await.unwrap();
    let _ = store.list(Keyspace::Durable, "").await.unwrap();
    let _ = store.list(Keyspace::Ephemeral, "").await.unwrap();
    let mut watch = store.watch(Keyspace::Durable, "").await.unwrap();
    while let Some(event) = watch.next().await {
        if matches!(event, Ok(WatchEvent::SnapshotDone)) {
            break;
        }
    }
    let mut ephemeral = store.watch(Keyspace::Ephemeral, "").await.unwrap();
    while let Some(event) = ephemeral.next().await {
        if matches!(event, Ok(WatchEvent::SnapshotDone)) {
            break;
        }
    }

    let seen = |from| Event::Seen {
        key: "split.a".to_owned(),
        rev: split.0,
        value: progress(1, None),
        from,
    };
    assert_eq!(
        events(&path),
        [seen(Source::Get), seen(Source::List), seen(Source::Watch)]
    );
}

/// `done` carries `lost` for a lost CAS and the class of a store error.
#[test]
fn done_carries_lost_and_the_error_class() {
    let (store, _dir, path) = journalled(MemoryStore::new(LEASE));
    let results = [
        Ok(CasOutcome::Lost),
        Err(StoreError::Retryable("x".to_owned())),
        Err(StoreError::Fatal("x".to_owned())),
    ];
    for result in &results {
        store
            .send(
                Keyspace::Durable,
                WriteOp::Update,
                "split.a",
                &record_bytes(1, None),
                Some(1),
            )
            .unwrap()
            .finish(result);
    }
    let replies: Vec<Reply> = events(&path)
        .into_iter()
        .filter_map(|e| match e {
            Event::Done { reply, .. } => Some(reply),
            _ => None,
        })
        .collect();
    assert_eq!(
        replies,
        [
            Reply::Lost,
            Reply::Err("retryable".to_owned()),
            Reply::Err("fatal".to_owned()),
        ]
    );
}

/// A failed `get` of a durable `split.*` key is journalled as `read_failed`;
/// a failed read of another key or of the ephemeral keyspace is not.
#[tokio::test]
async fn a_failed_split_read_is_journalled() {
    let (store, _dir, path) = journalled(Hang(MemoryStore::new(LEASE)));
    for (ks, key) in [
        (Keyspace::Durable, "split.a"),
        (Keyspace::Durable, "plan"),
        (Keyspace::Ephemeral, "split.a"),
    ] {
        assert!(store.get(ks, key).await.is_err(), "{key}");
    }
    assert_eq!(
        events(&path),
        [Event::ReadFailed {
            key: "split.a".to_owned()
        }]
    );
}

/// An ephemeral `split.*` update through `AbortAt` counts as a renewal.
#[tokio::test]
async fn an_ephemeral_split_update_counts_as_a_renewal() {
    let (journalled, _dir, _path) = journalled(MemoryStore::new(LEASE));
    let journal = Arc::clone(&journalled.journal);
    let classifier = Arc::clone(&journalled.classifier);
    let plan = plan(WriteKind::Renew, 1, AbortMode::ErrAfterLand);
    let store = AbortAt::new(journalled, plan, journal, classifier);
    let lease = store
        .create(Keyspace::Ephemeral, "split.a", b"lease".to_vec())
        .await
        .unwrap()
        .won()
        .unwrap();
    let renewed = store
        .update(Keyspace::Ephemeral, "split.a", b"lease".to_vec(), lease)
        .await;
    assert!(
        matches!(renewed, Err(StoreError::Retryable(_))),
        "{renewed:?}"
    );
}

const ABORT_CHILD: &str = "SPATE_FAULTS_ABORT_CHILD";

/// In a child process: claims `split.a` and sends one commit through an
/// `AbortAt` carrying a commit plan in `mode`.
fn abort_child(mode: &str, path: &std::path::Path) {
    let mode = match mode {
        "before" => AbortMode::Before,
        _ => AbortMode::After,
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let journal = Arc::new(Journal::open(path).unwrap());
        let classifier = Arc::new(Classifier::new("w0"));
        let inner = JournalStore::new(
            MemoryStore::new(LEASE),
            Arc::clone(&journal),
            Arc::clone(&classifier),
        );
        let store = AbortAt::new(inner, plan(WriteKind::Commit, 1, mode), journal, classifier);
        let claimed = store
            .create(Keyspace::Durable, "split.a", record_at(1, Some("w0"), None))
            .await
            .unwrap()
            .won()
            .unwrap();
        let _ = store
            .update(
                Keyspace::Durable,
                "split.a",
                record_at(1, Some("w0"), Some(10)),
                claimed,
            )
            .await;
    });
}

/// A `Before` plan aborts the process before its write is sent and an `After`
/// plan once it lands, each with its `abort` line in the journal.
#[cfg(unix)]
#[test]
fn abort_plans_end_the_process_on_sigabrt() {
    use std::os::unix::process::ExitStatusExt as _;
    if let Ok(mode) = std::env::var(ABORT_CHILD) {
        // The child's abort writes no core file.
        let no_core = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: `setrlimit` reads one valid `rlimit` and changes only this
        // process's limit.
        let set = unsafe { libc::setrlimit(libc::RLIMIT_CORE, &raw const no_core) };
        assert_eq!(set, 0);
        abort_child(
            &mode,
            std::path::Path::new(&std::env::var("SPATE_FAULTS_ABORT_JOURNAL").unwrap()),
        );
        return;
    }
    for (mode, at) in [("before", AbortPoint::Before), ("after", AbortPoint::After)] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w0-1.ndjson");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "store::tests::abort_plans_end_the_process_on_sigabrt",
                "--nocapture",
            ])
            .env(ABORT_CHILD, mode)
            .env("SPATE_FAULTS_ABORT_JOURNAL", &path)
            .status()
            .unwrap();
        assert_eq!(status.signal(), Some(libc::SIGABRT), "{mode}: {status:?}");
        let events = events(&path);
        let sends = events
            .iter()
            .filter(|e| matches!(e, Event::Send { .. }))
            .count();
        assert_eq!(
            sends,
            if at == AbortPoint::Before { 1 } else { 2 },
            "{mode}: {events:?}"
        );
        assert_eq!(
            events.last(),
            Some(&Event::Abort {
                key: "split.a".to_owned(),
                kind: WriteKind::Commit,
                n: 1,
                at,
            }),
            "{mode}"
        );
    }
}

/// A store whose first `get` never completes, over a [`MemoryStore`].
#[derive(Clone)]
struct SlowFirstGet(MemoryStore, Arc<AtomicBool>);

impl CoordinationStore for SlowFirstGet {
    fn lease_ttl(&self) -> Duration {
        LEASE
    }
    fn watch_mode(&self) -> WatchMode {
        self.0.watch_mode()
    }
    fn op_timeout(&self) -> Option<Duration> {
        None
    }
    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        self.0.create(ks, key, value).await
    }
    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        self.0.update(ks, key, value, expected).await
    }
    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        if !self.1.swap(true, Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        self.0.get(ks, key).await
    }
    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.0.delete(ks, key, expected).await
    }
    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.0.watch(ks, prefix).await
    }
    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.0.list(ks, prefix).await
    }
}

/// A `get` dropped at the coordinator's `op_timeout` is journalled as a failed read.
#[tokio::test]
async fn a_get_dropped_at_op_timeout_is_journalled_as_read_failed() {
    let (store, _dir, path) = journalled(SlowFirstGet(
        MemoryStore::new(LEASE),
        Arc::new(AtomicBool::new(false)),
    ));
    let read = tokio::time::timeout(
        Duration::from_millis(50),
        store.get(Keyspace::Durable, "split.a"),
    )
    .await;
    assert!(read.is_err(), "the get timed out");
    assert_eq!(
        events(&path),
        [Event::ReadFailed {
            key: "split.a".to_owned()
        }]
    );
}

/// The coordinator's claim fallback after a lost claim reply whose read-back
/// times out at `op_timeout` passes the lost-reply check.
#[tokio::test]
async fn claim_fallback_after_a_timed_out_read_back_is_a_recovery() {
    let (journalled, _dir, path) = journalled(SlowFirstGet(
        MemoryStore::new(LEASE),
        Arc::new(AtomicBool::new(false)),
    ));
    let journal = Arc::clone(&journalled.journal);
    let classifier = Arc::clone(&journalled.classifier);
    let store = AbortAt::new(
        journalled,
        plan(WriteKind::Claim, 1, AbortMode::ErrAfterLand),
        journal,
        classifier,
    );
    // The leader's unowned record.
    let unowned = store
        .create(Keyspace::Durable, "split.a", record_bytes(1, None))
        .await
        .unwrap()
        .won()
        .unwrap();
    // The claim lands; its reply is lost.
    let lost = store
        .update(
            Keyspace::Durable,
            "split.a",
            record_bytes(2, Some("w0")),
            unowned,
        )
        .await;
    assert!(matches!(lost, Err(StoreError::Retryable(_))), "{lost:?}");
    // The read-back, bounded as Metered bounds it, times out.
    let readback = tokio::time::timeout(
        Duration::from_millis(50),
        store.get(Keyspace::Durable, "split.a"),
    )
    .await;
    assert!(readback.is_err());
    // The coordinator releases the lease and claims again from its stale view.
    let again = store
        .update(
            Keyspace::Durable,
            "split.a",
            record_bytes(2, Some("w0")),
            unowned,
        )
        .await
        .unwrap();
    assert!(again.won().is_none(), "the stale claim loses");
    let fresh = store
        .get(Keyspace::Durable, "split.a")
        .await
        .unwrap()
        .unwrap();
    let won = store
        .update(
            Keyspace::Durable,
            "split.a",
            record_bytes(3, Some("w0")),
            fresh.revision,
        )
        .await
        .unwrap();
    assert!(won.won().is_some());

    let lines = crate::journal::read(&path).unwrap();
    let journal = crate::oracle::ProcessJournal {
        instance: "w0".to_owned(),
        pid: 1,
        lines,
    };
    let judged = crate::expect::lost_replies(&[journal], true);
    assert_eq!(judged.lines, 1);
    assert_eq!(judged.unexplained, Vec::<String>::new());
}

/// An `After` plan does not abort on a write that loses its CAS.
#[tokio::test]
async fn an_after_plan_ignores_a_lost_write() {
    let (journalled, _dir, path) = journalled(MemoryStore::new(LEASE));
    let peer = journalled.clone();
    let journal = Arc::clone(&journalled.journal);
    let classifier = Arc::clone(&journalled.classifier);
    let store = AbortAt::new(
        journalled,
        plan(WriteKind::Commit, 1, AbortMode::After),
        journal,
        classifier,
    );
    let claimed = store
        .create(Keyspace::Durable, "split.a", record_at(1, Some("w0"), None))
        .await
        .unwrap()
        .won()
        .unwrap();
    peer.update(
        Keyspace::Durable,
        "split.a",
        record_at(1, Some("w0"), Some(5)),
        claimed,
    )
    .await
    .unwrap()
    .won()
    .unwrap();
    let stale = store
        .update(
            Keyspace::Durable,
            "split.a",
            record_at(1, Some("w0"), Some(10)),
            claimed,
        )
        .await;
    assert!(matches!(stale, Ok(CasOutcome::Lost)), "{stale:?}");
    assert!(
        !events(&path)
            .iter()
            .any(|e| matches!(e, Event::Abort { .. }))
    );
}

/// A [`StopAt`] over a journalled [`MemoryStore`] holding a claimed
/// `split.a`, stopping at `plan` through `stop`, with a [`BrokenFence`]
/// below it that it arms when `broken`.
async fn stopping(
    plan: StopPlan,
    stop: fn(),
    broken: bool,
) -> (
    StopAt<BrokenFence<JournalStore<MemoryStore>>>,
    Revision,
    tempfile::TempDir,
    std::path::PathBuf,
) {
    let (journalled, dir, path) = journalled(MemoryStore::new(LEASE));
    let journal = Arc::clone(&journalled.journal);
    let classifier = Arc::clone(&journalled.classifier);
    let fence = Arc::new(Fence::default());
    let mut store = StopAt::new(
        BrokenFence::new(journalled, Arc::clone(&fence)),
        Some(plan),
        broken.then_some(fence),
        None,
        journal,
        classifier,
    );
    store.stop = stop;
    let claimed = store
        .create(Keyspace::Durable, "split.a", record_at(1, Some("w0"), None))
        .await
        .unwrap()
        .won()
        .unwrap();
    (store, claimed, dir, path)
}

static HOOK_STOPS: AtomicU32 = AtomicU32::new(0);

fn count_hook_stop() {
    HOOK_STOPS.fetch_add(1, Ordering::SeqCst);
}

/// The stop runs, after its `stop` line and with the fence armed for the
/// commit's key, when the stopped commit's `update` future is built, before
/// anything polls it; the commit is sent only once the future is polled.
#[tokio::test]
async fn stop_at_hook_runs_at_construction_not_poll() {
    let plan = StopPlan {
        kind: WriteKind::Commit,
        n: 1,
    };
    let (store, claimed, _dir, path) = stopping(plan, count_hook_stop, true).await;
    let update = store.update(
        Keyspace::Durable,
        "split.a",
        record_at(1, Some("w0"), Some(10)),
        claimed,
    );
    assert_eq!(
        HOOK_STOPS.load(Ordering::SeqCst),
        1,
        "stopped at construction"
    );
    assert!(store.inner.fence.take("split.a"), "the fence was armed");
    assert_eq!(
        events(&path).last(),
        Some(&Event::Stop {
            key: "split.a".to_owned(),
            expected: claimed.0,
            epoch: 1,
        })
    );
    assert!(update.await.unwrap().won().is_some());
    assert!(matches!(
        events(&path).as_slice(),
        [
            ..,
            Event::Stop { .. },
            Event::Send { .. },
            Event::Done { .. }
        ]
    ));
}

static COUNTED_STOPS: AtomicU32 = AtomicU32::new(0);

fn count_counted_stop() {
    COUNTED_STOPS.fetch_add(1, Ordering::SeqCst);
}

/// A plan on commits counts commits and completions only, and stops once,
/// at its `n`th.
#[tokio::test]
async fn stop_at_counts_commits_and_completions_and_stops_once() {
    let plan = StopPlan {
        kind: WriteKind::Commit,
        n: 2,
    };
    let (store, claimed, _dir, _path) = stopping(plan, count_counted_stop, false).await;
    let renew = store
        .update(
            Keyspace::Ephemeral,
            "split.a",
            record_at(1, Some("w0"), None),
            Revision(1),
        )
        .await;
    drop(renew);
    assert_eq!(COUNTED_STOPS.load(Ordering::SeqCst), 0, "a renewal");
    let mut rev = claimed;
    let complete = serde_json::json!({
        "schema": 3, "id": "a", "fp": 1, "epoch": 2, "status": "runnable",
        "owner": "w0", "attempts": 0, "watermark": 20, "state": null,
        "completed": true, "written_at_ms": 5,
    })
    .to_string()
    .into_bytes();
    // A claim, a commit, the completion that is the second counted write,
    // and a later commit.
    for (i, (value, stops)) in [
        (record_at(2, Some("w0"), None), 0),
        (record_at(2, Some("w0"), Some(10)), 0),
        (complete, 1),
        (record_at(2, Some("w0"), Some(30)), 1),
    ]
    .into_iter()
    .enumerate()
    {
        rev = store
            .update(Keyspace::Durable, "split.a", value, rev)
            .await
            .unwrap()
            .won()
            .unwrap();
        assert_eq!(COUNTED_STOPS.load(Ordering::SeqCst), stops, "write {i}");
    }
}

fn no_stop() {}

/// A fence armed for `split.a` re-sends a commit there that lost its CAS at
/// the current revision, where it lands; a lost commit on another key while
/// it is armed, or with no fence armed, comes back `Lost`.
#[tokio::test]
async fn broken_fence_resends_only_on_its_armed_key() {
    let plan = StopPlan {
        kind: WriteKind::Commit,
        n: 99,
    };
    for broken in [true, false] {
        let (store, claimed, _dir, path) = stopping(plan, no_stop, broken).await;
        if broken {
            store.inner.fence.arm("split.a");
        }
        let other = store
            .create(Keyspace::Durable, "split.b", record_at(1, Some("w0"), None))
            .await
            .unwrap()
            .won()
            .unwrap();
        let inner = &store.inner.inner;
        let peer_a = inner
            .update(
                Keyspace::Durable,
                "split.a",
                record_at(2, Some("w1"), None),
                claimed,
            )
            .await
            .unwrap()
            .won()
            .unwrap();
        inner
            .update(
                Keyspace::Durable,
                "split.b",
                record_at(2, Some("w1"), None),
                other,
            )
            .await
            .unwrap()
            .won()
            .unwrap();

        let stale = |key| {
            store.update(
                Keyspace::Durable,
                key,
                record_at(1, Some("w0"), Some(10)),
                if key == "split.a" { claimed } else { other },
            )
        };
        let b = stale("split.b").await.unwrap();
        let a = stale("split.a").await.unwrap();
        assert_eq!(b, CasOutcome::Lost, "an unarmed key");
        if !broken {
            assert_eq!(a, CasOutcome::Lost, "no fence armed");
            continue;
        }
        let Some(rev) = a.won() else {
            panic!("the re-send landed: {a:?}")
        };
        assert!(rev > peer_a);
        let landed = inner
            .get(Keyspace::Durable, "split.a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(Progress::parse(&landed.value).unwrap().epoch, 1);
        assert!(events(&path).iter().any(|e| matches!(
            e,
            Event::Send { key, expected: Some(x), .. } if key == "split.a" && *x == peer_a.0
        )));
        let again = store
            .update(
                Keyspace::Durable,
                "split.a",
                record_at(1, Some("w0"), Some(20)),
                claimed,
            )
            .await
            .unwrap();
        assert_eq!(again, CasOutcome::Lost, "the fence fires once");
    }
}

/// Through the worker's own layering, a stopped commit that loses its CAS is
/// re-sent with a `send` line at the peer's revision when `broken_fence` is
/// set, and comes back `Lost` after its one `send` when it is not.
#[tokio::test]
async fn worker_layering_journals_the_resend_only_with_a_broken_fence() {
    let plan = StopPlan {
        kind: WriteKind::Commit,
        n: 1,
    };
    for broken in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w0-1.ndjson");
        let journal = Arc::new(Journal::open(&path).unwrap());
        let classifier = Arc::new(Classifier::new("w0"));
        let peer = MemoryStore::new(LEASE);
        let mut store = crate::worker::layered(
            peer.clone(),
            Some(plan),
            broken,
            None,
            &journal,
            &classifier,
            |s| s,
        );
        store.stop = no_stop;
        let claimed = store
            .create(Keyspace::Durable, "split.a", record_at(1, Some("w0"), None))
            .await
            .unwrap()
            .won()
            .unwrap();
        let moved = peer
            .update(
                Keyspace::Durable,
                "split.a",
                record_at(2, Some("w1"), None),
                claimed,
            )
            .await
            .unwrap()
            .won()
            .unwrap();
        let stale = store
            .update(
                Keyspace::Durable,
                "split.a",
                record_at(1, Some("w0"), Some(10)),
                claimed,
            )
            .await
            .unwrap();
        let sends: Vec<Option<u64>> = events(&path)
            .into_iter()
            .filter_map(|e| match e {
                Event::Send {
                    op: WriteOp::Update,
                    expected,
                    ..
                } => Some(expected),
                _ => None,
            })
            .collect();
        if broken {
            assert!(stale.won().is_some(), "the re-send landed: {stale:?}");
            assert_eq!(sends, [Some(claimed.0), Some(moved.0)]);
        } else {
            assert_eq!(stale, CasOutcome::Lost);
            assert_eq!(sends, [Some(claimed.0)]);
        }
    }
}

const STOP_CHILD: &str = "SPATE_FAULTS_STOP_CHILD";

/// In a child process with the real stop: claims `split.a` and sends one
/// commit through a [`StopAt`] that stops at it.
fn stop_child(path: &std::path::Path) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let journal = Arc::new(Journal::open(path).unwrap());
        let classifier = Arc::new(Classifier::new("w0"));
        let inner = JournalStore::new(
            MemoryStore::new(LEASE),
            Arc::clone(&journal),
            Arc::clone(&classifier),
        );
        let plan = StopPlan {
            kind: WriteKind::Commit,
            n: 1,
        };
        let store = StopAt::new(inner, Some(plan), None, None, journal, classifier);
        let claimed = store
            .create(Keyspace::Durable, "split.a", record_at(1, Some("w0"), None))
            .await
            .unwrap()
            .won()
            .unwrap();
        let won = store
            .update(
                Keyspace::Durable,
                "split.a",
                record_at(1, Some("w0"), Some(10)),
                claimed,
            )
            .await
            .unwrap();
        assert!(won.won().is_some());
    });
}

/// The stop halts the whole process after its `stop` line and before the
/// commit is sent, and the commit lands once the process is continued.
#[cfg(unix)]
#[test]
fn stop_at_stops_the_process_before_sending() {
    if let Some(path) = std::env::var_os(STOP_CHILD) {
        stop_child(std::path::Path::new(&path));
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("w0-1.ndjson");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "store::tests::stop_at_stops_the_process_before_sending",
            "--nocapture",
        ])
        .env(STOP_CHILD, &path)
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    spate_test::wait_until(Duration::from_secs(60), "the child stopped", || {
        assert!(
            child.try_wait().unwrap().is_none(),
            "the child exited unstopped"
        );
        crate::workers::stopped(child.id())
    });
    let stopped = events(&path);
    assert!(
        matches!(stopped.as_slice(), [.., Event::Stop { .. }]),
        "{stopped:?}"
    );
    let pid = libc::pid_t::try_from(child.id()).unwrap();
    // SAFETY: `kill` takes no pointers; `pid` is our unreaped child.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGCONT) }, 0);
    let status = child.wait().unwrap();
    assert!(status.success(), "{status:?}");
    assert!(matches!(
        events(&path).as_slice(),
        [
            ..,
            Event::Stop { .. },
            Event::Send { .. },
            Event::Done {
                reply: Reply::Won(_),
                ..
            }
        ]
    ));
}

/// An armed fence passes an ephemeral update of its key through untouched
/// and stays armed for the durable one.
#[tokio::test]
async fn broken_fence_leaves_ephemeral_updates_alone() {
    let fence = Arc::new(Fence::default());
    let store = BrokenFence::new(MemoryStore::new(LEASE), Arc::clone(&fence));
    fence.arm("split.a");
    let renew = store
        .update(
            Keyspace::Ephemeral,
            "split.a",
            b"lease".to_vec(),
            Revision(7),
        )
        .await;
    assert!(!matches!(renew, Ok(CasOutcome::Won(_))), "{renew:?}");
    assert!(fence.take("split.a"), "still armed");
}

/// A [`StopAt`] over a journalled [`MemoryStore`], stopping at `plan` through
/// `stop` with the token `once`, and arming the fence below it at the stop.
fn leader_stopping(
    plan: StopPlan,
    stop: fn(),
    once: Option<std::path::PathBuf>,
) -> (
    StopAt<BrokenFence<JournalStore<MemoryStore>>>,
    tempfile::TempDir,
    std::path::PathBuf,
) {
    let (journalled, dir, path) = journalled(MemoryStore::new(LEASE));
    let journal = Arc::clone(&journalled.journal);
    let classifier = Arc::clone(&journalled.classifier);
    let fence = Arc::new(Fence::default());
    let mut store = StopAt::new(
        BrokenFence::new(journalled, Arc::clone(&fence)),
        Some(plan),
        Some(fence),
        once,
        journal,
        classifier,
    );
    store.stop = stop;
    (store, dir, path)
}

fn plan_bytes(planned: u64) -> Vec<u8> {
    serde_json::json!({"schema": 3, "generation": 1, "planned": planned})
        .to_string()
        .into_bytes()
}

fn assign_bytes(splits: &[&str]) -> Vec<u8> {
    serde_json::json!({"schema": 3, "generation": 1, "splits": splits})
        .to_string()
        .into_bytes()
}

/// Creates `key` holding `value` through `store`, and returns its revision.
async fn create<S: CoordinationStore>(store: &S, key: &str, value: Vec<u8>) -> Revision {
    store
        .create(Keyspace::Durable, key, value)
        .await
        .unwrap()
        .won()
        .unwrap()
}

/// Updates `key` from `rev` to `value` through `store`, and returns the new
/// revision.
async fn update<S: CoordinationStore>(
    store: &S,
    key: &str,
    value: Vec<u8>,
    rev: Revision,
) -> Revision {
    store
        .update(Keyspace::Durable, key, value, rev)
        .await
        .unwrap()
        .won()
        .unwrap()
}

/// The last `leader_stop` line in the journal at `path`.
fn leader_stop_line(path: &std::path::Path) -> Option<Event> {
    events(path)
        .into_iter()
        .rfind(|e| matches!(e, Event::LeaderStop { .. }))
}

static SEED_STOPS: AtomicU32 = AtomicU32::new(0);

fn count_seed_stop() {
    SEED_STOPS.fetch_add(1, Ordering::SeqCst);
}

/// A seed create stops when its future is built, before anything polls it,
/// after its `leader_stop` line and with the fence armed for its key.
#[tokio::test]
async fn stop_at_stops_on_a_seed_create_at_construction() {
    let plan = StopPlan {
        kind: WriteKind::Seed,
        n: 1,
    };
    let (store, _dir, path) = leader_stopping(plan, count_seed_stop, None);
    let seed = store.create(Keyspace::Durable, "split.a", record_at(0, None, None));
    assert_eq!(
        SEED_STOPS.load(Ordering::SeqCst),
        1,
        "stopped at construction"
    );
    assert!(store.inner.fence.take("split.a"), "the fence was armed");
    let value: serde_json::Value = serde_json::from_slice(&record_at(0, None, None)).unwrap();
    assert_eq!(
        events(&path),
        [Event::LeaderStop {
            key: "split.a".to_owned(),
            kind: WriteKind::Seed,
            n: 1,
            value,
            published: false,
        }]
    );
    drop(seed);
}

static ASSIGN_STOPS: AtomicU32 = AtomicU32::new(0);

fn count_assign_stop() {
    ASSIGN_STOPS.fetch_add(1, Ordering::SeqCst);
}

/// An assignment plan counts only `assign.*` writes whose value names a
/// split: under the second-assignment plan, an empty create and one
/// non-empty update pass, and the next non-empty update stops.
#[tokio::test]
async fn stop_at_counts_only_assign_writes_that_name_a_split() {
    let plan = StopPlan {
        kind: WriteKind::Assign,
        n: 2,
    };
    let (store, _dir, path) = leader_stopping(plan, count_assign_stop, None);
    let rev = create(&store, "assign.a", assign_bytes(&[])).await;
    let rev = update(&store, "assign.a", assign_bytes(&["s0"]), rev).await;
    assert_eq!(ASSIGN_STOPS.load(Ordering::SeqCst), 0, "no stop yet");
    update(&store, "assign.a", assign_bytes(&["s0", "s1"]), rev).await;
    assert_eq!(ASSIGN_STOPS.load(Ordering::SeqCst), 1, "stopped once");
    let Some(Event::LeaderStop { key, n, value, .. }) = leader_stop_line(&path) else {
        panic!("no leader_stop line: {:?}", events(&path));
    };
    assert_eq!((key.as_str(), n), ("assign.a", 2));
    assert_eq!(value["splits"], serde_json::json!(["s0", "s1"]));
}

static FIRST_ASSIGN_STOPS: AtomicU32 = AtomicU32::new(0);

fn count_first_assign_stop() {
    FIRST_ASSIGN_STOPS.fetch_add(1, Ordering::SeqCst);
}

static MID_ASSIGN_STOPS: AtomicU32 = AtomicU32::new(0);

fn count_mid_assign_stop() {
    MID_ASSIGN_STOPS.fetch_add(1, Ordering::SeqCst);
}

/// A leader stop records whether the process had sent its publish, the first
/// `plan` update after a seed sent through any clone: not at a first
/// assignment during seeding, and yes at a non-empty create of a peer's
/// record after the publish.
#[tokio::test]
async fn stop_at_records_whether_the_publish_was_sent() {
    let first = StopPlan {
        kind: WriteKind::Assign,
        n: 1,
    };
    let (store, _dir, path) = leader_stopping(first, count_first_assign_stop, None);
    let plan = create(&store, "plan", plan_bytes(0)).await;
    update(&store, "plan", plan_bytes(0), plan).await;
    let assign = create(&store, "assign.a", assign_bytes(&[])).await;
    create(&store, "split.a", record_at(0, None, None)).await;
    update(&store, "assign.a", assign_bytes(&["a"]), assign).await;
    assert_eq!(FIRST_ASSIGN_STOPS.load(Ordering::SeqCst), 1);
    assert!(
        matches!(
            leader_stop_line(&path),
            Some(Event::LeaderStop {
                published: false,
                ..
            })
        ),
        "{:?}",
        events(&path)
    );

    let mid = StopPlan {
        kind: WriteKind::Assign,
        n: 2,
    };
    let (store, _dir, path) = leader_stopping(mid, count_mid_assign_stop, None);
    let seeding = store.clone();
    let plan = create(&store, "plan", plan_bytes(0)).await;
    let plan = update(&store, "plan", plan_bytes(0), plan).await;
    let assign = create(&store, "assign.a", assign_bytes(&[])).await;
    create(&seeding, "split.a", record_at(0, None, None)).await;
    update(&store, "assign.a", assign_bytes(&["a"]), assign).await;
    update(&store, "plan", plan_bytes(1), plan).await;
    assert_eq!(MID_ASSIGN_STOPS.load(Ordering::SeqCst), 0, "no stop yet");
    create(&store, "assign.b", assign_bytes(&["b"])).await;
    assert_eq!(MID_ASSIGN_STOPS.load(Ordering::SeqCst), 1);
    let Some(Event::LeaderStop {
        key, n, published, ..
    }) = leader_stop_line(&path)
    else {
        panic!("no leader_stop line: {:?}", events(&path));
    };
    assert_eq!((key.as_str(), n, published), ("assign.b", 2, true));
}

static PUBLISH_STOPS: AtomicU32 = AtomicU32::new(0);

fn count_publish_stop() {
    PUBLISH_STOPS.fetch_add(1, Ordering::SeqCst);
}

/// A publish plan stops at the first `plan` update after a seed sent through
/// a clone, so a retried bump before seeding does not count.
#[tokio::test]
async fn stop_at_publish_skips_a_retried_bump() {
    let plan = StopPlan {
        kind: WriteKind::Publish,
        n: 1,
    };
    let (store, _dir, path) = leader_stopping(plan, count_publish_stop, None);
    let seeding = store.clone();
    let rev = create(&store, "plan", plan_bytes(0)).await;
    let rev = update(&store, "plan", plan_bytes(0), rev).await;
    let rev = update(&store, "plan", plan_bytes(0), rev).await;
    create(&seeding, "split.a", record_at(0, None, None)).await;
    assert_eq!(
        PUBLISH_STOPS.load(Ordering::SeqCst),
        0,
        "no stop before seeding"
    );
    update(&store, "plan", plan_bytes(1), rev).await;
    assert_eq!(PUBLISH_STOPS.load(Ordering::SeqCst), 1);
    let Some(Event::LeaderStop { value, .. }) = leader_stop_line(&path) else {
        panic!("no leader_stop line: {:?}", events(&path));
    };
    assert_eq!(value["planned"], 1);
}

static ONCE_STOPS: AtomicU32 = AtomicU32::new(0);

fn count_once_stop() {
    ONCE_STOPS.fetch_add(1, Ordering::SeqCst);
}

/// Of two processes sharing a token path, only the first to reach its stop
/// stops and journals it.
#[tokio::test]
async fn stop_once_lets_only_the_first_process_stop() {
    let plan = StopPlan {
        kind: WriteKind::Seed,
        n: 1,
    };
    let token_dir = tempfile::tempdir().unwrap();
    let token = token_dir.path().join("leader-stop.token");
    let (first, _a, first_path) = leader_stopping(plan, count_once_stop, Some(token.clone()));
    let (second, _b, second_path) = leader_stopping(plan, count_once_stop, Some(token));
    create(&first, "split.a", record_at(0, None, None)).await;
    create(&second, "split.a", record_at(0, None, None)).await;
    assert_eq!(ONCE_STOPS.load(Ordering::SeqCst), 1);
    assert!(leader_stop_line(&first_path).is_some());
    assert_eq!(leader_stop_line(&second_path), None);
}
