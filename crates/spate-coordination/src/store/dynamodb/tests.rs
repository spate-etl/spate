use super::fake::good_shape;
use super::table::{Cond, KeyAttr, Meta, Shape, Status, Ttl, Write};
use super::*;
use crate::store::WatchEvent;
use futures_util::{FutureExt as _, StreamExt as _};
use spate_core::clock::tokio::TestClock;

const TTL: Duration = Duration::from_millis(1500);
const POLL: Duration = Duration::from_millis(150);
const OP_TIMEOUT: Duration = Duration::from_millis(200);
const E: Keyspace = Keyspace::Ephemeral;
const D: Keyspace = Keyspace::Durable;

fn config() -> DynamoDbConfig {
    let mut config = DynamoDbConfig::new("spate-test", "job");
    config.poll_interval = POLL;
    config
}

fn handle_with(table: &FakeTable, clock: &Arc<TestClock>, config: DynamoDbConfig) -> DynamoDbStore {
    DynamoDbStore::over_fake_table(config, TTL, OP_TIMEOUT, clock.clone(), table).expect("store")
}

fn handle(table: &FakeTable, clock: &Arc<TestClock>) -> DynamoDbStore {
    handle_with(table, clock, config())
}

fn won(outcome: CasOutcome) -> Revision {
    outcome.won().expect("the write won")
}

async fn next(watch: &mut WatchStream) -> WatchEvent {
    tokio::time::timeout(TTL * 20, watch.next())
        .await
        .expect("a watch event")
        .expect("an open watch")
        .expect("a watch event, not an error")
}

async fn snapshot(watch: &mut WatchStream) -> Vec<Entry> {
    let mut entries = Vec::new();
    loop {
        match next(watch).await {
            WatchEvent::Put(entry) => entries.push(entry),
            WatchEvent::SnapshotDone => return entries,
            other => panic!("unexpected snapshot event {other:?}"),
        }
    }
}

/// The revision of the first put of `key`, failing on any delete before it.
async fn put_without_delete(watch: &mut WatchStream, key: &str) -> Revision {
    loop {
        match next(watch).await {
            WatchEvent::Put(entry) if entry.key == key => return entry.revision,
            WatchEvent::Delete { key: k, revision } if k == key => {
                panic!("{key} deleted at {revision:?} before its put")
            }
            _ => {}
        }
    }
}

/// A durable key re-created after 1,000 updates and a delete sits above
/// them all, whatever the wall clock reads.
#[tokio::test]
async fn durable_revisions_rise_across_delete_and_recreate() {
    let table = FakeTable::new();
    table.freeze_wall(1_000);
    let store = handle(&table, &TestClock::frozen());
    let mut rev = won(store.create(D, "k", b"v".to_vec()).await.unwrap());
    for _ in 0..1_000 {
        rev = won(store.update(D, "k", b"v".to_vec(), rev).await.unwrap());
    }
    let deleted = store.delete(D, "k", Some(rev)).await.unwrap();
    let recreated = won(store.create(D, "k", b"v".to_vec()).await.unwrap());
    assert!(
        recreated > rev && Some(recreated) > deleted.won(),
        "re-created at {recreated:?}, after {rev:?} and a delete at {deleted:?}"
    );
}

/// A key deleted and re-created at a lower revision between two polls
/// reaches the watch as a delete above what it held, then the new put.
#[tokio::test(start_paused = true)]
async fn a_recreated_key_between_two_polls_is_repaired_on_the_watch() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let (a, b, c) = (
        handle(&table, &clock),
        handle(&table, &clock),
        handle(&table, &clock),
    );
    table.freeze_wall(10_000);
    let high = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    let mut watch = a.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut watch).await[0].revision, high);
    table.freeze_wall(5_000);
    won(b.delete(E, "k", Some(high)).await.unwrap());
    let low = won(c.create(E, "k", b"c".to_vec()).await.unwrap());
    assert!(low < high);
    match next(&mut watch).await {
        WatchEvent::Delete { revision, .. } => assert!(revision > high, "{revision:?}"),
        other => panic!("expected the repair delete, got {other:?}"),
    }
    assert!(matches!(next(&mut watch).await, WatchEvent::Put(e) if e.revision == low));
}

/// A poll that read a key's absence before this handle re-created it
/// emits no delete, which would sit above the create.
#[tokio::test(start_paused = true)]
async fn a_vanish_decision_older_than_an_own_create_is_dropped() {
    let table = FakeTable::new();
    table.freeze_wall(1_000);
    let clock = TestClock::frozen();
    let (a, b) = (handle(&table, &clock), handle(&table, &clock));
    let first = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    let mut watch = a.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut watch).await.len(), 1);
    won(b.delete(E, "k", Some(first)).await.unwrap());
    let mut gate = table.hold_next_query();
    gate.reached().await;
    let own = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    gate.release();
    assert_eq!(put_without_delete(&mut watch, "k").await, own);
}

/// A poll that read a lease one TTL old before its owner renewed it emits
/// no delete for it.
#[tokio::test(start_paused = true)]
async fn an_expiry_decision_older_than_an_own_renewal_is_dropped() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let a = handle(&table, &clock);
    let first = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    let mut watch = a.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut watch).await.len(), 1);
    clock.advance(TTL + Duration::from_millis(1));
    let mut gate = table.hold_next_query();
    gate.reached().await;
    let renewed = won(a.update(E, "k", b"a".to_vec(), first).await.unwrap());
    gate.release();
    assert_eq!(put_without_delete(&mut watch, "k").await, renewed);
}

/// A key live throughout a watch's first read is in its snapshot, even
/// when an own write lands while that read runs.
#[tokio::test(start_paused = true)]
async fn a_snapshot_lists_a_key_an_own_write_touched_during_the_read() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let a = handle(&table, &clock);
    let rev = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    let mut gate = table.hold_next_query();
    let watcher = a.clone();
    let watching = tokio::spawn(async move { watcher.watch(E, "").await });
    gate.reached().await;
    let renewed = won(a.update(E, "k", b"a".to_vec(), rev).await.unwrap());
    gate.release();
    let mut watch = watching.await.unwrap().unwrap();
    let snap = snapshot(&mut watch).await;
    assert_eq!(snap.len(), 1, "k was live throughout the read: {snap:?}");
    assert_eq!(put_without_delete(&mut watch, "k").await, renewed);
}

/// A key an own write deleted while the first read ran is in the snapshot,
/// and the subscriber then receives its delete.
#[tokio::test(start_paused = true)]
async fn a_snapshot_key_deleted_by_an_own_write_during_the_read_is_deleted_for_the_subscriber() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let a = handle(&table, &clock);
    let rev = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    let mut gate = table.hold_next_query();
    let watcher = a.clone();
    let watching = tokio::spawn(async move { watcher.watch(E, "").await });
    gate.reached().await;
    won(a.delete(E, "k", Some(rev)).await.unwrap());
    gate.release();
    let mut watch = watching.await.unwrap().unwrap();
    assert_eq!(snapshot(&mut watch).await.len(), 1);
    match next(&mut watch).await {
        WatchEvent::Delete { key, revision } => {
            assert_eq!(key, "k");
            assert!(revision > rev);
        }
        other => panic!("expected the delete, got {other:?}"),
    }
}

/// A subscribing read that lists a lease already deleted for expiry sends
/// no put below that delete to the existing subscribers.
#[tokio::test(start_paused = true)]
async fn a_subscribing_read_sends_no_put_below_an_expiry_delete() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let a = handle(&table, &clock);
    won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    let mut first = a.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut first).await.len(), 1);
    clock.advance(TTL + Duration::from_millis(1));
    let deleted = match next(&mut first).await {
        WatchEvent::Delete { revision, .. } => revision,
        other => panic!("expected the expiry delete, got {other:?}"),
    };
    let mut gate = table.hold_next_query();
    let watcher = a.clone();
    let watching = tokio::spawn(async move { watcher.watch(E, "").await });
    gate.reached().await;
    let renewed = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    gate.release();
    let _second = watching.await.unwrap().unwrap();
    match next(&mut first).await {
        WatchEvent::Put(e) => assert!(e.revision == renewed && renewed > deleted, "{e:?}"),
        other => panic!("expected the renewed put, got {other:?}"),
    }
}

/// A subscribing read leaves a key an existing subscriber already holds
/// to the next poll.
#[tokio::test(start_paused = true)]
async fn a_subscribing_read_sends_no_put_for_a_delivered_key() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let (a, b) = (handle(&table, &clock), handle(&table, &clock));
    let first = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    let mut held = a.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut held).await.len(), 1);
    let second = won(b.update(E, "k", b"b".to_vec(), first).await.unwrap());
    let mut gate = table.hold_next_query();
    let watcher = a.clone();
    let watching = tokio::spawn(async move { watcher.watch(E, "").await });
    gate.reached().await;
    won(a.update(E, "k", b"a".to_vec(), second).await.unwrap());
    gate.release();
    let _second = watching.await.unwrap().unwrap();
    assert!(held.next().now_or_never().is_none());
}

/// An existing subscriber receives a key the subscribing read caught and an
/// own write touched, even when the key's revision is unchanged.
#[tokio::test(start_paused = true)]
async fn a_subscribing_read_delivers_an_own_touched_key_to_existing_subscribers() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let (a, b) = (handle(&table, &clock), handle(&table, &clock));
    let mut held = a.watch(E, "").await.unwrap();
    assert!(snapshot(&mut held).await.is_empty());
    let created = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    let mut gate = table.hold_next_query();
    let watcher = a.clone();
    let watching = tokio::spawn(async move { watcher.watch(E, "").await });
    gate.reached().await;
    assert!(
        a.create(E, "k", b"a".to_vec())
            .await
            .unwrap()
            .won()
            .is_none()
    );
    gate.release();
    let _second = watching.await.unwrap().unwrap();
    assert!(matches!(next(&mut held).await, WatchEvent::Put(e) if e.revision == created));
}

/// Each conditional write whose first attempt landed, then reported its
/// condition failed, resolves as won at the revision it wrote.
#[tokio::test]
async fn a_write_that_landed_resolves_won() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let (a, b) = (handle(&table, &clock), handle(&table, &clock));
    let landed = |outcome: CasOutcome, pk: &str, key: &str, what: &str| {
        let stored = table.item(pk, key).expect("item").v;
        assert_eq!(outcome, CasOutcome::Won(Revision(stored)), "{what}");
    };
    a.get(D, "warm").await.unwrap();
    b.get(D, "warm").await.unwrap();

    table.land_then_fail_next_write();
    let created = a.create(D, "d", b"v".to_vec()).await.unwrap();
    landed(created, "job#d", "d", "durable create");
    table.land_then_fail_next_write();
    let updated = a.update(D, "d", b"v".to_vec(), won(created)).await.unwrap();
    landed(updated, "job#d", "d", "durable update");
    table.land_then_fail_next_write();
    let deleted = a.delete(D, "d", Some(won(updated))).await.unwrap();
    landed(deleted, "job#d", "d", "durable delete");

    table.land_then_fail_next_write();
    let created = a.create(E, "e", b"v".to_vec()).await.unwrap();
    landed(created, "job#e", "e", "ephemeral create");
    table.land_then_fail_next_write();
    let updated = a.update(E, "e", b"v".to_vec(), won(created)).await.unwrap();
    landed(updated, "job#e", "e", "ephemeral update");

    assert!(b.get(E, "e").await.unwrap().is_some());
    clock.advance(TTL);
    assert!(b.get(E, "e").await.unwrap().is_none(), "b judges e expired");
    table.land_then_fail_next_write();
    let taken = b.create(E, "e", b"b".to_vec()).await.unwrap();
    landed(taken, "job#e", "e", "takeover");
}

/// A guarded delete whose key was deleted and re-created since loses.
#[tokio::test]
async fn a_retried_physical_delete_that_meets_a_recreate_loses() {
    let table = FakeTable::new();
    table.freeze_wall(1_000);
    let clock = TestClock::frozen();
    let (a, b) = (handle(&table, &clock), handle(&table, &clock));
    let first = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    won(b.delete(E, "k", Some(first)).await.unwrap());
    table.freeze_wall(2_000);
    won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    assert_eq!(
        a.delete(E, "k", Some(first)).await.unwrap(),
        CasOutcome::Lost
    );
}

/// Deleting a key this handle judged expired wins without a call at a
/// stale revision, and removes the item at the expired revision.
#[tokio::test(start_paused = true)]
async fn deleting_an_observed_expired_key_at_a_stale_revision_wins_without_a_call() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let (a, b) = (handle(&table, &clock), handle(&table, &clock));
    let rev = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    let mut watch = b.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut watch).await.len(), 1);
    clock.advance(TTL);
    assert!(matches!(next(&mut watch).await, WatchEvent::Delete { .. }));

    let writes = table.count(FakeOp::Write);
    let stale = Some(Revision(rev.0 - 1));
    assert!(b.delete(E, "k", stale).await.unwrap().won().is_some());
    assert_eq!(table.count(FakeOp::Write), writes, "a call was made");
    assert!(b.delete(E, "k", Some(rev)).await.unwrap().won().is_some());
    assert!(
        table.item("job#e", "k").is_none(),
        "the expired item stayed"
    );
}

/// The delete that removes an expired item is conditional on the expired
/// revision, so it spares an incarnation another handle wrote since.
#[tokio::test(start_paused = true)]
async fn a_cleanup_delete_spares_a_newer_incarnation() {
    let table = FakeTable::new();
    table.freeze_wall(1_000);
    let clock = TestClock::frozen();
    let (a, b, c) = (
        handle(&table, &clock),
        handle(&table, &clock),
        handle(&table, &clock),
    );
    let old = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    let mut watch = b.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut watch).await.len(), 1);
    clock.advance(TTL);
    assert!(matches!(next(&mut watch).await, WatchEvent::Delete { .. }));
    won(c.delete(E, "k", Some(old)).await.unwrap());
    table.freeze_wall(2_000);
    let new = won(c.create(E, "k", b"c".to_vec()).await.unwrap());
    assert!(b.delete(E, "k", Some(old)).await.unwrap().won().is_some());
    assert_eq!(table.item("job#e", "k").map(|i| i.v), Some(new.0));
}

/// An owner's renewal that lands between an observer's expiry judgment and
/// its takeover stands, and the observer loses.
#[tokio::test]
async fn a_takeover_is_conditional_on_the_expired_version() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let (a, b) = (handle(&table, &clock), handle(&table, &clock));
    let rev = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    assert!(b.get(E, "k").await.unwrap().is_some());
    clock.advance(TTL);
    assert!(b.get(E, "k").await.unwrap().is_none());
    let renewal = Write::Put {
        v: rev.0 + 1,
        b: b"a".to_vec(),
        w: [7; 16],
        x: None,
        cond: Cond::VersionIs(rev.0),
    };
    table.interpose(1, "job#e", "k", renewal);
    assert_eq!(
        b.create(E, "k", b"b".to_vec()).await.unwrap(),
        CasOutcome::Lost
    );
    assert_eq!(table.item("job#e", "k").map(|i| i.w), Some(Some([7; 16])));
}

/// Polls that fail while the clock passes a lease judge no expiry: the
/// first successful poll reports the renewed lease.
#[tokio::test(start_paused = true)]
async fn expiry_needs_a_confirming_read() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let (a, b) = (handle(&table, &clock), handle(&table, &clock));
    let mut rev = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    let mut watch = b.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut watch).await.len(), 1);
    table.fail(FakeOp::Query, true);
    for _ in 0..8 {
        clock.advance(TTL / 4);
        rev = won(a.update(E, "k", b"a".to_vec(), rev).await.unwrap());
        tokio::time::sleep(POLL).await;
    }
    table.fail(FakeOp::Query, false);
    assert_eq!(put_without_delete(&mut watch, "k").await, rev);
}

/// A handle that first reads a lease long after its last write judges it
/// expired one TTL after that read.
#[tokio::test]
async fn a_fresh_observer_expires_a_stale_lease_one_ttl_after_its_first_read() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let a = handle(&table, &clock);
    won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    clock.advance(TTL * 5);
    let b = handle(&table, &clock);
    b.get(D, "warm").await.unwrap();
    clock.advance(TTL * 5);
    assert!(b.get(E, "k").await.unwrap().is_some(), "at the first read");
    clock.advance(TTL - Duration::from_millis(1));
    assert!(b.get(E, "k").await.unwrap().is_some(), "inside one TTL");
    clock.advance(Duration::from_millis(1));
    assert!(b.get(E, "k").await.unwrap().is_none(), "one TTL on");
}

/// An eventually consistent durable poll that returns an older revision or
/// omits a new key reports neither.
#[tokio::test(start_paused = true)]
async fn a_durable_ec_poll_emits_no_delete_on_absence_or_older_put() {
    let table = FakeTable::new();
    let a = handle(&table, &TestClock::frozen());
    let first = won(a.create(D, "d.a", b"1".to_vec()).await.unwrap());
    won(a.update(D, "d.a", b"2".to_vec(), first).await.unwrap());
    won(a.create(D, "d.b", b"b".to_vec()).await.unwrap());
    let mut watch = a.watch(D, "d.").await.unwrap();
    assert_eq!(snapshot(&mut watch).await.len(), 2);
    table.set_stale_reads(true);
    let queries = table.count(FakeOp::Query);
    while table.count(FakeOp::Query) < queries + 3 {
        tokio::time::sleep(POLL).await;
    }
    table.set_stale_reads(false);
    won(a.create(D, "d.c", b"c".to_vec()).await.unwrap());
    match next(&mut watch).await {
        WatchEvent::Put(entry) => assert_eq!(entry.key, "d.c"),
        other => panic!("expected d.c's put, got {other:?}"),
    }
}

/// After a durable watch reports a delete, an eventually consistent read of
/// the item from before it puts nothing.
#[tokio::test(start_paused = true)]
async fn durable_watch_puts_nothing_below_its_delete() {
    let table = FakeTable::new();
    let a = handle(&table, &TestClock::frozen());
    let v1 = won(a.create(D, "assign.x", b"old".to_vec()).await.unwrap());
    let mut watch = a.watch(D, "assign.").await.unwrap();
    assert_eq!(snapshot(&mut watch).await[0].revision, v1);
    let v2 = won(a.delete(D, "assign.x", Some(v1)).await.unwrap());
    assert!(
        matches!(next(&mut watch).await, WatchEvent::Delete { revision, .. } if revision == v2)
    );
    table.set_stale_reads(true);
    let after = tokio::time::timeout(POLL * 5, watch.next()).await;
    assert!(after.is_err(), "after the delete at {v2:?}: {after:?}");
}

/// A durable watch that starts after a key's delete never puts the item
/// from before the delete.
#[tokio::test(start_paused = true)]
async fn a_durable_watch_started_after_a_delete_puts_nothing_below_it() {
    let table = FakeTable::new();
    let a = handle(&table, &TestClock::frozen());
    let v1 = won(a.create(D, "assign.x", b"old".to_vec()).await.unwrap());
    let v2 = won(a.delete(D, "assign.x", Some(v1)).await.unwrap());
    let mut watch = a.watch(D, "assign.").await.unwrap();
    assert!(snapshot(&mut watch).await.is_empty());
    table.set_stale_reads(true);
    let after = tokio::time::timeout(POLL * 5, watch.next()).await;
    assert!(after.is_err(), "deleted at {v2:?}: {after:?}");
}

/// A durable key created and deleted between two polls never reaches the
/// watch as a put.
#[tokio::test(start_paused = true)]
async fn a_durable_key_deleted_between_polls_is_not_put() {
    let table = FakeTable::new();
    let a = handle(&table, &TestClock::frozen());
    let mut watch = a.watch(D, "assign.").await.unwrap();
    assert!(snapshot(&mut watch).await.is_empty());
    let v1 = won(a.create(D, "assign.x", b"old".to_vec()).await.unwrap());
    let v2 = won(a.delete(D, "assign.x", Some(v1)).await.unwrap());
    let polls = table.count(FakeOp::Query);
    while table.count(FakeOp::Query) < polls + 1 {
        tokio::time::sleep(POLL / 2).await;
    }
    table.set_stale_reads(true);
    let after = tokio::time::timeout(POLL * 5, watch.next()).await;
    assert!(after.is_err(), "deleted at {v2:?}: {after:?}");
}

/// An eventually consistent poll that omits a tombstoned key keeps its
/// floor, so a later stale read of the item from before the delete puts
/// nothing.
#[tokio::test(start_paused = true)]
async fn an_eventually_consistent_omission_keeps_the_floor() {
    let table = FakeTable::new();
    let a = handle(&table, &TestClock::frozen());
    let mut watch = a.watch(D, "assign.").await.unwrap();
    assert!(snapshot(&mut watch).await.is_empty());
    let polled = async |table: &FakeTable| {
        let polls = table.count(FakeOp::Query);
        while table.count(FakeOp::Query) < polls + 1 {
            tokio::time::sleep(POLL / 2).await;
        }
    };
    let v1 = won(a.create(D, "assign.x", b"old".to_vec()).await.unwrap());
    let v2 = won(a.delete(D, "assign.x", Some(v1)).await.unwrap());
    polled(&table).await;
    table.set_unseen("job#d", "assign.x", true);
    polled(&table).await;
    table.set_unseen("job#d", "assign.x", false);
    table.set_stale_reads(true);
    let after = tokio::time::timeout(POLL * 5, watch.next()).await;
    assert!(after.is_err(), "deleted at {v2:?}: {after:?}");
}

/// A key's second incarnation, created and deleted between two polls,
/// raises its floor to the newer tombstone, so a stale read of that
/// incarnation puts nothing.
#[tokio::test(start_paused = true)]
async fn a_floor_rises_to_the_newest_tombstone_read() {
    let table = FakeTable::new();
    let a = handle(&table, &TestClock::frozen());
    let v1 = won(a.create(D, "assign.x", b"old".to_vec()).await.unwrap());
    won(a.delete(D, "assign.x", Some(v1)).await.unwrap());
    let mut watch = a.watch(D, "assign.").await.unwrap();
    assert!(snapshot(&mut watch).await.is_empty());
    let v3 = won(a.create(D, "assign.x", b"mid".to_vec()).await.unwrap());
    let v4 = won(a.delete(D, "assign.x", Some(v3)).await.unwrap());
    let polls = table.count(FakeOp::Query);
    while table.count(FakeOp::Query) < polls + 1 {
        tokio::time::sleep(POLL / 2).await;
    }
    table.set_stale_reads(true);
    let after = tokio::time::timeout(POLL * 5, watch.next()).await;
    assert!(after.is_err(), "deleted at {v4:?}: {after:?}");
}

/// Once a consistent read no longer lists a tombstone, as after native TTL
/// collects it, a key re-created below its revision reaches the watch.
#[tokio::test(start_paused = true)]
async fn a_collected_tombstone_leaves_no_floor() {
    let table = FakeTable::new();
    table.freeze_wall(1_000);
    let a = handle(&table, &TestClock::frozen());
    let v1 = won(a.create(D, "assign.x", b"old".to_vec()).await.unwrap());
    let tomb = won(a.delete(D, "assign.x", Some(v1)).await.unwrap());
    let mut watch = a.watch(D, "assign.").await.unwrap();
    assert!(snapshot(&mut watch).await.is_empty());
    table
        .write("job#d", "assign.x", Write::Remove { expected: None })
        .await
        .unwrap();
    let mut again = a.watch(D, "assign.").await.unwrap();
    assert!(snapshot(&mut again).await.is_empty());
    let recreated = won(a.create(D, "assign.x", b"new".to_vec()).await.unwrap());
    assert!(recreated <= tomb, "{recreated:?} above {tomb:?}");
    assert!(matches!(next(&mut watch).await, WatchEvent::Put(e) if e.revision == recreated));
}

/// A key this handle creates, then deletes while a poll that read it is in
/// flight, never reaches the watch as a put.
#[tokio::test(start_paused = true)]
async fn a_key_deleted_during_a_poll_is_not_put() {
    let table = FakeTable::new();
    let a = handle(&table, &TestClock::frozen());
    let mut watch = a.watch(E, "").await.unwrap();
    assert!(snapshot(&mut watch).await.is_empty());
    let mut gate = table.hold_next_query();
    let v = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    gate.reached().await;
    won(a.delete(E, "k", Some(v)).await.unwrap());
    gate.release();
    let after = tokio::time::timeout(POLL * 5, watch.next()).await;
    assert!(after.is_err(), "{after:?}");
}

/// A listing of four 300 KiB values reads more than one page, as a Query
/// returns at most 1 MB of items per page.
#[tokio::test]
async fn pages_of_a_1_2_mb_listing() {
    let table = FakeTable::new();
    let a = handle(&table, &TestClock::frozen());
    for i in 0..4 {
        won(a
            .create(D, &format!("big.{i}"), vec![7; 300 * 1024])
            .await
            .unwrap());
    }
    let before = table.count(FakeOp::Query);
    assert_eq!(a.list(D, "big.").await.unwrap().len(), 4);
    let pages = table.count(FakeOp::Query) - before;
    assert!(pages >= 2, "a 1.2 MB listing came back in {pages} page(s)");
}

/// A key this handle re-creates sits above the delete its watch emitted.
#[tokio::test(start_paused = true)]
async fn a_recreated_key_on_one_handle_sits_above_its_watch_delete() {
    let table = FakeTable::new();
    table.freeze_wall(1_000);
    let a = handle(&table, &TestClock::frozen());
    let mut watch = a.watch(E, "").await.unwrap();
    assert!(snapshot(&mut watch).await.is_empty());
    let rev = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    assert!(matches!(next(&mut watch).await, WatchEvent::Put(e) if e.revision == rev));
    won(a.delete(E, "k", Some(rev)).await.unwrap());
    let deleted = match next(&mut watch).await {
        WatchEvent::Delete { revision, .. } => revision,
        other => panic!("expected k's delete, got {other:?}"),
    };
    let recreated = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    assert!(recreated > deleted, "{recreated:?} <= {deleted:?}");
}

/// A value up to 384 KiB is written; one byte more is Fatal, names the key,
/// and makes no call.
#[tokio::test]
async fn values_above_the_cap_fail_without_a_call() {
    let table = FakeTable::new();
    let a = handle(&table, &TestClock::frozen());
    won(a.create(D, "fits", vec![0; MAX_VALUE_BYTES]).await.unwrap());
    let writes = table.count(FakeOp::Write);
    for ks in [D, E] {
        let err = a.create(ks, "big", vec![0; MAX_VALUE_BYTES + 1]).await;
        assert!(
            matches!(&err, Err(StoreError::Fatal(m)) if m.contains("big")),
            "{err:?}"
        );
    }
    assert_eq!(table.count(FakeOp::Write), writes);
}

/// Ephemeral writes stamp `x` a day ahead and tombstones a week ahead; a
/// live durable item carries none.
#[tokio::test]
async fn items_carry_their_collection_time() {
    let table = FakeTable::new();
    table.freeze_wall(1_000_000);
    let a = handle(&table, &TestClock::frozen());
    let rev = won(a.create(E, "e", b"v".to_vec()).await.unwrap());
    assert_eq!(table.item("job#e", "e").unwrap().x, Some(1_000 + 86_400));
    table.freeze_wall(2_000_000);
    won(a.update(E, "e", b"v".to_vec(), rev).await.unwrap());
    assert_eq!(table.item("job#e", "e").unwrap().x, Some(2_000 + 86_400));
    let rev = won(a.create(D, "d", b"v".to_vec()).await.unwrap());
    assert_eq!(table.item("job#d", "d").unwrap().x, None);
    won(a.delete(D, "d", Some(rev)).await.unwrap());
    assert_eq!(
        table.item("job#d", "d").unwrap().x,
        Some(2_000 + 7 * 86_400)
    );
}

/// A job's lease and item layout are fixed by the first handle that
/// starts it; its poll interval is not.
#[tokio::test]
async fn a_meta_mismatch_is_fatal() {
    let clock = TestClock::frozen();
    let table = FakeTable::new();
    handle(&table, &clock).get(D, "k").await.unwrap();
    let mut slower = config();
    slower.poll_interval = POLL * 2;
    handle_with(&table, &clock, slower)
        .get(D, "k")
        .await
        .unwrap();
    let longer =
        DynamoDbStore::over_fake_table(config(), TTL * 2, OP_TIMEOUT, clock.clone(), &table)
            .unwrap();
    let err = longer.get(D, "k").await;
    assert!(matches!(err, Err(StoreError::Fatal(_))), "{err:?}");

    let table = FakeTable::new();
    let other = Meta {
        lease_ms: u64::try_from(TTL.as_millis()).unwrap(),
        layout: startup::LAYOUT + 1,
    };
    table.put_meta("job#m", other).await.unwrap();
    let err = handle(&table, &clock).get(D, "k").await;
    assert!(matches!(err, Err(StoreError::Fatal(_))), "{err:?}");
}

/// A table with the wrong key schema, a local index, replicas or a status
/// it cannot serve from is refused before any item call.
#[tokio::test]
async fn startup_rejects_the_wrong_schema_an_lsi_and_replicas() {
    let clock = TestClock::frozen();
    let key = |name: &str, hash, kind: &str| KeyAttr {
        name: name.to_string(),
        hash,
        kind: kind.to_string(),
    };
    let mut shapes: Vec<Shape> = [
        vec![key("pk", true, "S")],
        vec![key("pk", true, "S"), key("sk", false, "N")],
        vec![key("id", true, "S"), key("sk", false, "S")],
    ]
    .into_iter()
    .map(|keys| Shape {
        keys,
        ..good_shape()
    })
    .collect();
    shapes.push(Shape {
        local_indexes: 1,
        ..good_shape()
    });
    shapes.push(Shape {
        replicas: 2,
        ..good_shape()
    });
    shapes.push(Shape {
        status: Status::Other("DELETING".into()),
        ..good_shape()
    });
    for shape in shapes {
        let table = FakeTable::new();
        table.set_shape(Some(shape.clone()));
        let err = handle(&table, &clock).get(D, "k").await;
        assert!(
            matches!(err, Err(StoreError::Fatal(_))),
            "{shape:?}: {err:?}"
        );
        assert_eq!(table.count(FakeOp::Get), 0, "{shape:?}");
    }
    let table = FakeTable::new();
    table.set_shape(Some(Shape {
        status: Status::Creating,
        ..good_shape()
    }));
    let err = handle(&table, &clock).get(D, "k").await;
    assert!(matches!(err, Err(StoreError::Retryable(_))), "{err:?}");
    table.set_shape(Some(Shape {
        status: Status::Updating,
        global_indexes: 1,
        ..good_shape()
    }));
    handle(&table, &clock).get(D, "k").await.unwrap();
}

/// A missing table is Fatal unless the config allows creating it; then it
/// is created with TTL on `x`, and the next attempt proceeds.
#[tokio::test]
async fn a_missing_table_is_created_only_when_allowed() {
    let clock = TestClock::frozen();
    let table = FakeTable::new();
    table.set_shape(None);
    let err = handle(&table, &clock).get(D, "k").await;
    assert!(matches!(err, Err(StoreError::Fatal(_))), "{err:?}");

    let mut creating = config();
    creating.create_table = true;
    let store = handle_with(&table, &clock, creating);
    let err = store.get(D, "k").await;
    assert!(matches!(err, Err(StoreError::Retryable(_))), "{err:?}");
    store.get(D, "k").await.unwrap();
    assert_eq!(table.describe_ttl().await.unwrap(), Ttl::On("x".into()));
}

/// A table whose TTL is off, on another attribute or unreadable is used,
/// with a warning.
#[test]
fn startup_warns_without_ttl() {
    for ttl in [Ttl::Off, Ttl::On("y".into()), Ttl::Unknown("denied".into())] {
        let table = FakeTable::new();
        table.set_ttl(ttl.clone());
        let lines = spate_test::capture_logs(tracing::Level::WARN, || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(handle(&table, &TestClock::frozen()).get(D, "k"))
                .unwrap();
        });
        let warned = lines.iter().any(|l| l.contains("time to live"));
        assert!(warned, "{ttl:?}: {lines:?}");
    }
}

/// Listings, watch snapshots and polls read every page of a prefix.
#[tokio::test(start_paused = true)]
async fn listings_and_polls_follow_every_page() {
    let table = FakeTable::new();
    table.set_page_bytes(1024);
    let a = handle(&table, &TestClock::frozen());
    for ks in [D, E] {
        for i in 0..20 {
            won(a
                .create(ks, &format!("p.{i:02}"), vec![0; 200])
                .await
                .unwrap());
        }
        assert_eq!(a.list(ks, "p.").await.unwrap().len(), 20, "{ks:?}");
        let mut watch = a.watch(ks, "p.").await.unwrap();
        assert_eq!(snapshot(&mut watch).await.len(), 20, "{ks:?}");
        won(a.create(ks, "p.99", vec![0; 200]).await.unwrap());
        assert!(matches!(next(&mut watch).await, WatchEvent::Put(e) if e.key == "p.99"));
    }
}

/// Every watch of one prefix on a handle shares one poller, which stops
/// once no stream remains.
#[tokio::test(start_paused = true)]
async fn one_poller_serves_every_watch_of_a_prefix_and_stops_with_the_last() {
    let table = FakeTable::new();
    let a = handle(&table, &TestClock::frozen());
    let mut first = a.watch(E, "").await.unwrap();
    let mut second = a.watch(E, "").await.unwrap();
    snapshot(&mut first).await;
    snapshot(&mut second).await;
    let queries = table.count(FakeOp::Query);
    tokio::time::sleep(POLL * 5 + POLL / 2).await;
    assert_eq!(table.count(FakeOp::Query), queries + 5);
    drop((first, second));
    tokio::time::sleep(POLL).await;
    let stopped = table.count(FakeOp::Query);
    tokio::time::sleep(POLL * 5).await;
    assert_eq!(table.count(FakeOp::Query), stopped);
}

/// A poll that fails fatally hands the error to every stream of the
/// prefix, and each stream then ends.
#[tokio::test(start_paused = true)]
async fn a_fatal_poll_ends_every_stream_with_its_error() {
    let table = FakeTable::new();
    let a = handle(&table, &TestClock::frozen());
    let mut streams = [a.watch(E, "").await.unwrap(), a.watch(E, "").await.unwrap()];
    for watch in &mut streams {
        snapshot(watch).await;
    }
    table.fail_fatally(FakeOp::Query);
    for watch in &mut streams {
        let event = tokio::time::timeout(TTL * 20, watch.next()).await;
        assert!(
            matches!(event, Ok(Some(Err(StoreError::Fatal(_))))),
            "{event:?}"
        );
        let after = tokio::time::timeout(TTL * 20, watch.next()).await;
        assert!(matches!(after, Ok(None)), "{after:?}");
    }
}

/// A handle whose poller stopped with its runtime starts another on the
/// runtime of its next watch, which then delivers events.
#[test]
fn a_poller_whose_runtime_stopped_is_replaced() {
    let runtime = || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap()
    };
    let table = FakeTable::new();
    let a = handle(&table, &TestClock::frozen());
    let first = runtime();
    let held = first.block_on(async {
        let mut watch = a.watch(E, "").await.unwrap();
        snapshot(&mut watch).await;
        watch
    });
    drop(first);
    runtime().block_on(async {
        let mut watch = a.watch(E, "").await.expect("a watch on the second runtime");
        snapshot(&mut watch).await;
        let rev = won(a.create(E, "k", b"v".to_vec()).await.unwrap());
        assert_eq!(put_without_delete(&mut watch, "k").await, rev);
    });
    drop(held);
}

/// Each poll records one `op="poll"` store operation.
#[test]
fn polls_are_metered() {
    let rendered = spate_test::render_metrics(|| {
        let labels = spate_core::metrics::ComponentLabels::new(
            "dynamodb-poll",
            spate_test::unique_name("dynamodb-poll"),
            "s3",
        );
        let metrics = CoordinationMetrics::new(&labels);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        rt.block_on(async {
            let table = FakeTable::new();
            let a = handle(&table, &TestClock::frozen());
            a.attach_metrics(&metrics);
            let mut watch = a.watch(E, "").await.unwrap();
            snapshot(&mut watch).await;
            tokio::time::sleep(POLL * 3 + POLL / 2).await;
        });
    });
    let polls = spate_test::metric_sum(
        &rendered,
        "spate_coordination_store_op_duration_seconds_count",
        &[("op", "poll")],
    );
    assert_eq!(polls, Some(3.0), "{rendered}");
}

/// A watch or listing of the empty prefix reads the partition with no
/// sort-key condition.
#[tokio::test(start_paused = true)]
async fn an_empty_prefix_has_no_sort_key_condition() {
    let table = FakeTable::new();
    let a = handle(&table, &TestClock::frozen());
    won(a.create(D, "d", b"d".to_vec()).await.unwrap());
    assert_eq!(a.list(D, "").await.unwrap().len(), 1);
    let mut watch = a.watch(E, "").await.unwrap();
    snapshot(&mut watch).await;
    tokio::time::sleep(POLL * 2).await;
    assert!(table.queries().iter().all(|q| q.prefix.is_none()));
}

struct EmptyPlanner;

impl crate::SplitPlanner for EmptyPlanner {
    fn fingerprint(&self) -> String {
        "empty".into()
    }

    fn plan(
        &mut self,
        _: crate::PlanContext<'_>,
    ) -> Result<crate::SplitPlan, crate::CoordinationError> {
        Ok(crate::SplitPlan::new(Vec::new(), crate::PlanFinality::Open))
    }
}

/// The coordinator refuses a store whose `op_timeout` differs from its own
/// and hands the store its metrics at start; `Metered` forwards both.
#[test]
fn op_timeout_is_checked_and_forwarded() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let mut coordination = crate::CoordinationConfig {
        lease_duration: TTL,
        op_timeout: OP_TIMEOUT * 2,
        ..Default::default()
    };
    let err = crate::StoreCoordinator::new(
        handle(&table, &clock),
        coordination.clone(),
        rt.handle().clone(),
        None,
    )
    .expect_err("a differing op_timeout was accepted");
    assert!(err.to_string().contains("op_timeout"), "{err}");
    coordination.op_timeout = OP_TIMEOUT / 2;
    let err = crate::StoreCoordinator::new(
        handle(&table, &clock),
        coordination.clone(),
        rt.handle().clone(),
        None,
    )
    .expect_err("a longer store op_timeout was accepted");
    assert!(err.to_string().contains("op_timeout"), "{err}");
    coordination.op_timeout = OP_TIMEOUT;
    let started = handle(&table, &clock);
    let labels = spate_core::metrics::ComponentLabels::new(
        "dynamodb-start",
        spate_test::unique_name("start"),
        "s3",
    );
    let mut accepted = crate::StoreCoordinator::new(
        started.clone(),
        coordination,
        rt.handle().clone(),
        Some(CoordinationMetrics::new(&labels)),
    )
    .expect("a matching op_timeout");
    crate::SplitCoordinator::start(&mut accepted, Box::new(EmptyPlanner)).unwrap();
    assert!(
        started.inner.metrics.get().is_some(),
        "start attached no metrics"
    );
    drop(accepted);

    let store = handle(&table, &clock);
    let metered = crate::store::metered::Metered::new(store.clone(), OP_TIMEOUT, None);
    assert_eq!(metered.op_timeout(), Some(OP_TIMEOUT));
    let labels = spate_core::metrics::ComponentLabels::new(
        "dynamodb-forward",
        spate_test::unique_name("forward"),
        "s3",
    );
    metered.attach_metrics(&CoordinationMetrics::new(&labels));
    assert!(store.inner.metrics.get().is_some());
}

/// Construction refuses a malformed table or job name, an empty region, an
/// endpoint that is not an http or https URL with a host, and a lease below
/// one second or shorter than five poll intervals.
#[test]
fn invalid_configs_are_fatal() {
    let clock = TestClock::frozen();
    let table = FakeTable::new();
    type Edit = fn(&mut DynamoDbConfig);
    let build = |edit: Edit, lease| {
        let mut config = config();
        edit(&mut config);
        DynamoDbStore::over_fake_table(config, lease, OP_TIMEOUT, clock.clone(), &table)
    };
    assert!(build(|_| {}, TTL).is_ok());
    assert!(
        build(
            |c| c.table = "arn:aws:dynamodb:eu-west-1:1:table/t".into(),
            TTL
        )
        .is_ok()
    );
    assert!(build(|c| c.endpoint = Some("http://127.0.0.1:8000".into()), TTL).is_ok());
    let invalid: [(Edit, Duration); 11] = [
        (|c| c.table = "ab".into(), TTL),
        (|c| c.table = "a b c".into(), TTL),
        (|c| c.job = String::new(), TTL),
        (|c| c.job = "a#b".into(), TTL),
        (|c| c.region = Some(String::new()), TTL),
        (|c| c.endpoint = Some("127.0.0.1:8000".into()), TTL),
        (|c| c.endpoint = Some("ftp://host".into()), TTL),
        (|c| c.endpoint = Some("https:///path".into()), TTL),
        (|c| c.poll_interval = Duration::ZERO, TTL),
        (|c| c.poll_interval = TTL / 4, TTL),
        (|_| {}, Duration::from_millis(999)),
    ];
    for (edit, lease) in invalid {
        assert!(matches!(build(edit, lease), Err(StoreError::Fatal(_))));
    }
}

/// Every documented key parses, the optional ones default, an unknown key
/// is refused, and `Debug` redacts the endpoint's userinfo.
#[test]
fn the_config_parses_and_redacts_its_endpoint() {
    let full: DynamoDbConfig = serde_yaml::from_str(
        "{ table: spate-coordination, job: backfill, region: eu-west-1, \
         endpoint: \"http://user:hunter2@127.0.0.1:8000\", create_table: true, \
         poll_interval: 3s }",
    )
    .unwrap();
    assert_eq!(full.region.as_deref(), Some("eu-west-1"));
    assert!(full.create_table);
    assert_eq!(full.poll_interval, Duration::from_secs(3));
    let debug = format!("{full:?}");
    assert!(!debug.contains("hunter2"), "{debug}");
    assert!(debug.contains("127.0.0.1:8000"), "{debug}");

    let minimal: DynamoDbConfig = serde_yaml::from_str("{ table: t12, job: j }").unwrap();
    assert_eq!(
        (minimal.region, minimal.endpoint, minimal.create_table),
        (None, None, false)
    );
    assert_eq!(minimal.poll_interval, Duration::from_secs(2));
    assert!(serde_yaml::from_str::<DynamoDbConfig>("{ table: t12, job: j, secret: s }").is_err());
}
