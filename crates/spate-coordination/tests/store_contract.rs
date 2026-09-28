//! The [`CoordinationStore`] contract held by the in-memory store and by the
//! polling test double.
//!
//! `nats_integration` runs the same checks against a real NATS server.

mod support;

use futures_util::StreamExt as _;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, WatchEvent,
};
use std::collections::BTreeMap;
use support::polled::{PolledStore, diff};
use support::{LEASE, TestClock, contract, store_with_clock};

#[tokio::test]
async fn the_contract_holds_on_the_memory_store() {
    let clock = TestClock::frozen();
    let store = store_with_clock(clock.clone());
    contract::all(&store, async |by| clock.advance(by)).await;
}

#[tokio::test]
async fn the_contract_holds_on_a_polled_store() {
    let clock = TestClock::frozen();
    let store = PolledStore::new(store_with_clock(clock.clone()), LEASE / 10);
    contract::all(&store, async |by| clock.advance(by)).await;
}

/// A guarded delete of an expired lease wins.
#[tokio::test]
async fn guarded_delete_of_an_expired_lease_wins() {
    let clock = TestClock::frozen();
    let store = store_with_clock(clock.clone());
    let rev = store
        .create(Keyspace::Ephemeral, "lease", b"v".to_vec())
        .await
        .unwrap()
        .won()
        .unwrap();
    clock.advance(LEASE * 2);
    assert!(
        store
            .get(Keyspace::Ephemeral, "lease")
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        store
            .delete(Keyspace::Ephemeral, "lease", Some(rev))
            .await
            .unwrap(),
        CasOutcome::Won(_)
    ));
}

/// Between two listings, a key that moved, a new key and a vanished key each
/// produce one event and an unchanged key none; the vanished key's delete
/// sits one above its last put.
#[test]
fn a_listing_diff_reports_each_change_once() {
    let entry = |key: &str, revision: u64| Entry {
        key: key.to_string(),
        value: Vec::new(),
        revision: Revision(revision),
    };
    let mut seen: BTreeMap<String, Revision> = [("same", 1), ("moved", 2), ("gone", 3)]
        .into_iter()
        .map(|(key, revision)| (key.to_string(), Revision(revision)))
        .collect();
    let listed = || vec![entry("same", 1), entry("moved", 5), entry("new", 6)];
    assert_eq!(
        diff(&mut seen, listed(), |_| None),
        vec![
            WatchEvent::Put(entry("moved", 5)),
            WatchEvent::Put(entry("new", 6)),
            WatchEvent::Delete {
                key: "gone".into(),
                revision: Revision(4),
            },
        ]
    );
    assert!(diff(&mut seen, listed(), |_| None).is_empty());
}

/// A key this handle rewrote after the watch last reported it is deleted one
/// above the rewrite, so a consumer holding the rewrite's revision applies
/// the delete.
#[tokio::test(start_paused = true)]
async fn a_polled_delete_sits_above_the_handles_own_rewrite() {
    let store = PolledStore::new(support::store(), LEASE / 10);
    let ks = Keyspace::Durable;
    let r1 = store
        .create(ks, "k.a", b"1".to_vec())
        .await
        .unwrap()
        .won()
        .unwrap();
    let mut watch = store.watch(ks, "k.").await.unwrap();
    assert!(matches!(
        watch.next().await.unwrap().unwrap(),
        WatchEvent::Put(e) if e.revision == r1
    ));
    assert_eq!(
        watch.next().await.unwrap().unwrap(),
        WatchEvent::SnapshotDone
    );
    let r2 = store
        .update(ks, "k.a", b"2".to_vec(), r1)
        .await
        .unwrap()
        .won()
        .unwrap();
    assert!(matches!(
        store.delete(ks, "k.a", Some(r2)).await.unwrap(),
        CasOutcome::Won(_)
    ));
    match watch.next().await.unwrap().unwrap() {
        WatchEvent::Delete { revision, .. } => {
            assert!(revision > r2, "delete {revision:?} is not above {r2:?}");
        }
        other => panic!("expected the delete of k.a, got {other:?}"),
    }
}

/// A key created and deleted between two listings never reaches a polled
/// watch; the next listing reports only what is live.
#[tokio::test(start_paused = true)]
async fn a_polled_watch_misses_a_key_that_lives_between_two_listings() {
    let store = PolledStore::new(support::store(), LEASE / 10);
    let mut watch = store.watch(Keyspace::Durable, "m.").await.unwrap();
    assert_eq!(
        watch.next().await.unwrap().unwrap(),
        WatchEvent::SnapshotDone
    );
    let short = store
        .create(Keyspace::Durable, "m.short", b"s".to_vec())
        .await
        .unwrap()
        .won()
        .unwrap();
    assert!(matches!(
        store
            .delete(Keyspace::Durable, "m.short", Some(short))
            .await
            .unwrap(),
        CasOutcome::Won(_)
    ));
    store
        .create(Keyspace::Durable, "m.long", b"l".to_vec())
        .await
        .unwrap()
        .won()
        .unwrap();
    match watch.next().await.unwrap().unwrap() {
        WatchEvent::Put(entry) => assert_eq!(entry.key, "m.long"),
        other => panic!("expected m.long's put, got {other:?}"),
    }
}
