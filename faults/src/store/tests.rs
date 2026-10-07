use std::time::Duration;

use futures_util::FutureExt as _;
use spate_coordination::store::memory::MemoryStore;

use super::*;
use crate::journal::{Line, Status};

const LEASE: Duration = Duration::from_secs(2);

fn record_bytes(epoch: u64, owner: Option<&str>) -> Vec<u8> {
    serde_json::json!({
        "schema": 3, "id": "a", "fp": 1, "epoch": epoch, "status": "runnable",
        "owner": owner, "attempts": 0, "watermark": null, "state": null,
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

/// A store whose updates never complete, with a polled watch and an
/// `op_timeout`, over a [`MemoryStore`].
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

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
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

fn journalled<S>(inner: S) -> (JournalStore<S>, tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("w0-1.ndjson");
    let journal = Arc::new(Journal::open(&path).unwrap());
    (JournalStore::new(inner, journal), dir, path)
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

/// The wrapper reports the inner store's lease, watch mode and
/// `op_timeout`, which the coordinator checks against its config.
#[test]
fn wrappers_forward_op_timeout_and_watch_mode() {
    let (store, _dir, _path) = journalled(Hang(MemoryStore::new(LEASE)));
    assert_eq!(store.lease_ttl(), LEASE);
    assert_eq!(store.op_timeout(), Some(Duration::from_millis(700)));
    assert_eq!(
        store.watch_mode(),
        WatchMode::Polled {
            interval: Duration::from_millis(300)
        }
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
