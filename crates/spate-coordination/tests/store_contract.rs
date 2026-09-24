//! The [`CoordinationStore`] contract held by the in-memory store.
//!
//! `nats_integration` runs the same assertions against a real NATS server.

mod support;

use spate_coordination::store::{CasOutcome, CoordinationStore, Keyspace};
use support::{LEASE, TestClock, delete_contract, store, store_with_clock};

#[tokio::test]
async fn delete_outcomes_match_the_trait() {
    delete_contract(&store()).await;
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
