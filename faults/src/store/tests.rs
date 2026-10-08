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

/// Both wrappers report the inner store's lease, watch mode and
/// `op_timeout`, which the coordinator checks against its config.
#[test]
fn wrappers_forward_op_timeout_and_watch_mode() {
    let (store, _dir, _path) = journalled(Hang(MemoryStore::new(LEASE)));
    let journal = Arc::clone(&store.journal);
    let classifier = Arc::clone(&store.classifier);
    let plan = plan(WriteKind::Commit, 1, AbortMode::Before);
    let aborting = AbortAt::new(store.clone(), plan, journal, classifier);
    for (lease, op_timeout, watch) in [
        (store.lease_ttl(), store.op_timeout(), store.watch_mode()),
        (
            aborting.lease_ttl(),
            aborting.op_timeout(),
            aborting.watch_mode(),
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
