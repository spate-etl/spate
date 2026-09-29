//! The [`CoordinationStore`] contract as assertions any store must pass.
//!
//! Each check uses its own key prefix, so [`all`] runs them against one
//! store in sequence.

use super::delete_contract;
use futures_util::StreamExt as _;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, WatchEvent, WatchStream,
};
use std::time::Duration;

/// How long a check waits for one watch event.
const EVENT_DEADLINE: Duration = Duration::from_secs(10);

/// Every check below, in order. `advance` moves the store's clock forward:
/// a test clock for an in-process store, a real sleep for a server.
pub async fn all<S: CoordinationStore>(store: &S, advance: impl AsyncFn(Duration)) {
    delete_contract(store).await;
    create_and_update(store).await;
    revisions_across_delete_and_recreate(store).await;
    listing(store).await;
    watching(store).await;
    expiry(store, advance).await;
}

async fn next_event(stream: &mut WatchStream, what: &str) -> WatchEvent {
    tokio::time::timeout(EVENT_DEADLINE, stream.next())
        .await
        .unwrap_or_else(|_| panic!("no watch event within {EVENT_DEADLINE:?}: {what}"))
        .unwrap_or_else(|| panic!("watch stream ended: {what}"))
        .unwrap_or_else(|e| panic!("watch failed: {what}: {e}"))
}

/// The live keys a fresh watch delivers up to `SnapshotDone`. A replayed
/// delete of a key that is already gone is skipped, as the task skips it.
async fn snapshot(stream: &mut WatchStream, what: &str) -> Vec<Entry> {
    let mut entries = Vec::new();
    loop {
        match next_event(stream, what).await {
            WatchEvent::Put(entry) => entries.push(entry),
            WatchEvent::Delete { .. } => {}
            WatchEvent::SnapshotDone => return entries,
            other => panic!("{what}: unexpected snapshot event {other:?}"),
        }
    }
}

fn won(outcome: CasOutcome, what: &str) -> Revision {
    outcome.won().unwrap_or_else(|| panic!("{what} lost"))
}

/// Create is create-if-absent; update is compare-and-swap on the revision,
/// and a winning update's revision is above the one it replaced.
pub async fn create_and_update<S: CoordinationStore>(store: &S) {
    for ks in [Keyspace::Durable, Keyspace::Ephemeral] {
        let r1 = won(
            store.create(ks, "cu.k", b"v1".to_vec()).await.unwrap(),
            "first create",
        );
        assert_eq!(
            store.create(ks, "cu.k", b"dup".to_vec()).await.unwrap(),
            CasOutcome::Lost,
            "{ks:?}: a duplicate create"
        );
        let r2 = won(
            store.update(ks, "cu.k", b"v2".to_vec(), r1).await.unwrap(),
            "matched update",
        );
        assert!(r2 > r1, "{ks:?}: update revision {r2:?} not above {r1:?}");
        assert_eq!(
            store.update(ks, "cu.k", b"v3".to_vec(), r1).await.unwrap(),
            CasOutcome::Lost,
            "{ks:?}: a stale update"
        );
        assert_eq!(
            store
                .update(ks, "cu.absent", b"v".to_vec(), r1)
                .await
                .unwrap(),
            CasOutcome::Lost,
            "{ks:?}: an update of an absent key"
        );
        let entry = store.get(ks, "cu.k").await.unwrap().expect("live key");
        assert_eq!(
            (entry.value, entry.revision),
            (b"v2".to_vec(), r2),
            "{ks:?}"
        );
    }
}

/// A key's revisions strictly increase across its whole history: a watch's
/// delete sits above the last put it reported, and a re-created key above
/// the delete.
pub async fn revisions_across_delete_and_recreate<S: CoordinationStore>(store: &S) {
    for ks in [Keyspace::Durable, Keyspace::Ephemeral] {
        let mut watch = store.watch(ks, "rc.").await.unwrap();
        assert!(snapshot(&mut watch, "empty prefix").await.is_empty());
        let r1 = won(
            store.create(ks, "rc.k", b"a".to_vec()).await.unwrap(),
            "create",
        );
        let r2 = won(
            store.update(ks, "rc.k", b"b".to_vec(), r1).await.unwrap(),
            "update",
        );
        // A store may coalesce writes, so wait for the last one; a watch
        // reports the delete of a key it has reported.
        loop {
            match next_event(&mut watch, "the put of rc.k").await {
                WatchEvent::Put(entry) if entry.revision == r2 => break,
                WatchEvent::Put(entry) => assert!(entry.revision < r2, "{ks:?}"),
                other => panic!("{ks:?}: unexpected event {other:?}"),
            }
        }
        assert!(matches!(
            store.delete(ks, "rc.k", Some(r2)).await.unwrap(),
            CasOutcome::Won(_)
        ));
        let deleted = match next_event(&mut watch, "the delete of rc.k").await {
            WatchEvent::Delete { key, revision } if key == "rc.k" => revision,
            other => panic!("{ks:?}: unexpected event {other:?}"),
        };
        assert!(
            deleted > r2,
            "{ks:?}: delete revision {deleted:?} not above the last put {r2:?}"
        );
        let r3 = won(
            store.create(ks, "rc.k", b"c".to_vec()).await.unwrap(),
            "re-create",
        );
        assert!(
            r3 > deleted,
            "{ks:?}: re-created revision {r3:?} not above the delete {deleted:?}"
        );
    }
}

/// A listing returns exactly the live keys under its prefix in its own
/// keyspace, with full values, including a result larger than a megabyte.
pub async fn listing<S: CoordinationStore>(store: &S) {
    for key in ["ls.a", "ls.b", "other.c"] {
        won(
            store
                .create(Keyspace::Durable, key, key.as_bytes().to_vec())
                .await
                .unwrap(),
            key,
        );
    }
    won(
        store
            .create(Keyspace::Ephemeral, "ls.e", b"e".to_vec())
            .await
            .unwrap(),
        "ls.e",
    );
    let keys = |entries: Vec<Entry>| {
        let mut keys: Vec<String> = entries.into_iter().map(|e| e.key).collect();
        keys.sort();
        keys
    };
    assert_eq!(
        keys(store.list(Keyspace::Durable, "ls.").await.unwrap()),
        ["ls.a", "ls.b"]
    );
    assert_eq!(
        keys(store.list(Keyspace::Ephemeral, "ls.").await.unwrap()),
        ["ls.e"]
    );
    assert!(matches!(
        store.delete(Keyspace::Durable, "ls.a", None).await.unwrap(),
        CasOutcome::Won(_)
    ));
    assert_eq!(
        keys(store.list(Keyspace::Durable, "ls.").await.unwrap()),
        ["ls.b"],
        "a deleted key is absent from a listing"
    );

    let big: Vec<u8> = (0..=u8::MAX).cycle().take(300 * 1024).collect();
    for i in 0..4 {
        won(
            store
                .create(Keyspace::Durable, &format!("big.{i}"), big.clone())
                .await
                .unwrap(),
            "big value",
        );
    }
    let listed = store.list(Keyspace::Durable, "big.").await.unwrap();
    assert_eq!(listed.len(), 4, "a listing larger than a megabyte");
    assert!(listed.iter().all(|e| e.value == big), "values arrive whole");
}

/// A watch delivers its prefix's live keys, then `SnapshotDone`, then the
/// puts and deletes under that prefix and nothing outside it.
pub async fn watching<S: CoordinationStore>(store: &S) {
    let ks = Keyspace::Durable;
    let a = won(
        store.create(ks, "wt.a", b"a".to_vec()).await.unwrap(),
        "wt.a",
    );
    let mut watch = store.watch(ks, "wt.").await.unwrap();
    let first = snapshot(&mut watch, "the first snapshot").await;
    assert_eq!(first.len(), 1);
    assert_eq!((first[0].key.as_str(), first[0].revision), ("wt.a", a));

    won(
        store.create(ks, "wt.b", b"b".to_vec()).await.unwrap(),
        "wt.b",
    );
    won(
        store.create(ks, "zz.c", b"c".to_vec()).await.unwrap(),
        "zz.c",
    );
    assert!(matches!(
        store.delete(ks, "wt.a", Some(a)).await.unwrap(),
        CasOutcome::Won(_)
    ));
    let (mut put_b, mut deleted_a) = (false, false);
    while !(put_b && deleted_a) {
        match next_event(&mut watch, "wt.b's put and wt.a's delete").await {
            WatchEvent::Put(entry) => {
                assert_eq!(entry.key, "wt.b", "an event outside the prefix");
                put_b = true;
            }
            WatchEvent::Delete { key, revision } => {
                assert_eq!(key, "wt.a", "an event outside the prefix");
                assert!(revision > a);
                deleted_a = true;
            }
            WatchEvent::SnapshotDone => panic!("a second SnapshotDone"),
            other => panic!("unexpected event {other:?}"),
        }
    }

    let mut again = store.watch(ks, "wt.").await.unwrap();
    let second = snapshot(&mut again, "the re-watch snapshot").await;
    assert_eq!(
        second.iter().map(|e| e.key.as_str()).collect::<Vec<_>>(),
        ["wt.b"]
    );

    let mut none = store.watch(ks, "none.").await.unwrap();
    assert!(
        snapshot(&mut none, "a prefix with no live key beside other keys")
            .await
            .is_empty()
    );

    // A prefix that ends mid-token is a string prefix like any other.
    for key in ["wp1", "wp2", "wq"] {
        won(store.create(ks, key, b"p".to_vec()).await.unwrap(), key);
    }
    let mut partial = store.watch(ks, "wp").await.unwrap();
    let mut keys: Vec<String> = snapshot(&mut partial, "the wp snapshot")
        .await
        .into_iter()
        .map(|e| e.key)
        .collect();
    keys.sort();
    assert_eq!(keys, ["wp1", "wp2"]);
    won(store.create(ks, "wq2", b"q".to_vec()).await.unwrap(), "wq2");
    won(store.create(ks, "wp3", b"p".to_vec()).await.unwrap(), "wp3");
    match next_event(&mut partial, "wp3's put").await {
        WatchEvent::Put(entry) => assert_eq!(entry.key, "wp3", "an event outside the prefix"),
        other => panic!("unexpected event {other:?}"),
    }
}

/// An ephemeral key expires a lease after its last write: a key rewritten
/// every quarter lease survives three leases, an untouched one reads as
/// absent and reaches watchers as a delete.
pub async fn expiry<S: CoordinationStore>(store: &S, advance: impl AsyncFn(Duration)) {
    let ks = Keyspace::Ephemeral;
    let lease = store.lease_ttl();
    let idle = won(
        store.create(ks, "ex.idle", b"i".to_vec()).await.unwrap(),
        "ex.idle",
    );
    let mut kept = won(
        store.create(ks, "ex.kept", b"k".to_vec()).await.unwrap(),
        "ex.kept",
    );
    let mut watch = store.watch(ks, "ex.").await.unwrap();
    assert_eq!(snapshot(&mut watch, "the expiry snapshot").await.len(), 2);

    for _ in 0..12 {
        advance(lease / 4).await;
        kept = store
            .update(ks, "ex.kept", b"k".to_vec(), kept)
            .await
            .unwrap()
            .won()
            .expect("a key rewritten every quarter lease expired");
    }
    assert!(
        store.get(ks, "ex.idle").await.unwrap().is_none(),
        "an untouched key outlived three leases"
    );
    let listed: Vec<String> = store
        .list(ks, "ex.")
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.key)
        .collect();
    assert_eq!(listed, ["ex.kept"]);
    loop {
        match next_event(&mut watch, "the expiry of ex.idle").await {
            WatchEvent::Delete { key, revision } if key == "ex.idle" => {
                assert!(revision > idle);
                break;
            }
            WatchEvent::Delete { key, .. } => panic!("{key} expired while rewritten"),
            WatchEvent::Put(_) | WatchEvent::SnapshotDone => {}
            other => panic!("unexpected event {other:?}"),
        }
    }
}
