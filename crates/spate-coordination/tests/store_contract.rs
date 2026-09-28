//! The [`CoordinationStore`] contract held by the in-memory store and by the
//! polling test double.
//!
//! `nats_integration` runs the same checks against a real NATS server.

mod support;

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
        diff(&mut seen, listed()),
        vec![
            WatchEvent::Put(entry("moved", 5)),
            WatchEvent::Put(entry("new", 6)),
            WatchEvent::Delete {
                key: "gone".into(),
                revision: Revision(4),
            },
        ]
    );
    assert!(diff(&mut seen, listed()).is_empty());
}
