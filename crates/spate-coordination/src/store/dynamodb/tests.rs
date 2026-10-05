use super::fake::good_shape;
use super::table::{
    BoxFuture as TableFuture, Cond, KeyAttr, Meta, Page, Shape, Status, Ttl, Write,
};
use super::*;
use crate::store::WatchEvent;
use futures_util::{FutureExt as _, StreamExt as _};
use spate_core::clock::tokio::TestClock;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

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

/// A key re-created at a lower revision between two polls, after native TTL
/// collected its floor, reaches the watch as a delete above what it held,
/// then the new put.
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
    table.collect("job#f", "k");
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

/// A key renewed by another handle after its expiry delete is in a subscribing read's snapshot, above that delete.
#[tokio::test(start_paused = true)]
async fn a_subscribing_read_lists_a_renewal_above_the_expiry_delete() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let (a, b) = (handle(&table, &clock), handle(&table, &clock));
    let r = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    let mut first = a.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut first).await.len(), 1);
    clock.advance(TTL + Duration::from_millis(1));
    let deleted = match next(&mut first).await {
        WatchEvent::Delete { revision, .. } => revision,
        o => panic!("{o:?}"),
    };
    let renewed = won(b.update(E, "k", b"b".to_vec(), r).await.unwrap());
    assert!(
        renewed > deleted,
        "renewed at {renewed:?}, deleted at {deleted:?}"
    );
    let mut gate = table.hold_next_query();
    let watcher = a.clone();
    let watching = tokio::spawn(async move { watcher.watch(E, "").await });
    gate.reached().await;
    won(a.update(E, "k", b"a".to_vec(), renewed).await.unwrap());
    gate.release();
    let mut second = watching.await.unwrap().unwrap();
    let snap = snapshot(&mut second).await;
    assert_eq!(snap.len(), 1, "k live throughout read: {snap:?}");
}

/// A subscribing read does not list a lease that another poller of the handle deleted for expiry while the read ran.
#[tokio::test(start_paused = true)]
async fn a_subscribing_read_skips_a_lease_another_poller_expired() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let a = handle(&table, &clock);
    won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    let mut first = a.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut first).await.len(), 1);
    clock.advance(TTL + Duration::from_millis(1));
    let mut gate = table.hold_next_query();
    let watcher = a.clone();
    let watching = tokio::spawn(async move { watcher.watch(E, "k").await });
    gate.reached().await;
    let deleted = match next(&mut first).await {
        WatchEvent::Delete { revision, .. } => revision,
        o => panic!("{o:?}"),
    };
    gate.release();
    let mut second = watching.await.unwrap().unwrap();
    let snap = snapshot(&mut second).await;
    assert!(
        snap.is_empty(),
        "expired lease listed after delete {deleted:?}: {snap:?}"
    );
}

/// A put at a higher revision clears the expiry floor, so a later re-create below it is listed.
#[tokio::test(start_paused = true)]
async fn an_expiry_floor_is_cleared_by_a_later_put() {
    let table = FakeTable::new();
    table.freeze_wall(1_000);
    let clock = TestClock::frozen();
    let (a, b, c) = (
        handle(&table, &clock),
        handle(&table, &clock),
        handle(&table, &clock),
    );
    let r = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    let mut first = a.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut first).await.len(), 1);
    clock.advance(TTL + Duration::from_millis(1));
    match next(&mut first).await {
        WatchEvent::Delete { .. } => {}
        o => panic!("{o:?}"),
    }
    won(b.delete(E, "k", Some(r)).await.unwrap());
    table.collect("job#f", "k");
    table.freeze_wall(500);
    let low = won(c.create(E, "k", b"c".to_vec()).await.unwrap());
    match next(&mut first).await {
        WatchEvent::Put(e) => assert_eq!(e.revision, low),
        o => panic!("{o:?}"),
    }
    won(c.update(E, "k", b"c".to_vec(), low).await.unwrap());
    let mut gate = table.hold_next_query();
    let watcher = a.clone();
    let watching = tokio::spawn(async move { watcher.watch(E, "").await });
    gate.reached().await;
    a.get(E, "k").await.unwrap();
    gate.release();
    let mut second = watching.await.unwrap().unwrap();
    assert_eq!(snapshot(&mut second).await.len(), 1);
}

/// An expiry floor does not outlive its key: a re-create below it is listed once the key has left the table.
#[tokio::test(start_paused = true)]
async fn an_expiry_floor_is_dropped_once_its_key_leaves_the_listing() {
    let table = FakeTable::new();
    table.freeze_wall(1_000);
    let clock = TestClock::frozen();
    let (a, b, c) = (
        handle(&table, &clock),
        handle(&table, &clock),
        handle(&table, &clock),
    );
    let r = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    let mut first = a.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut first).await.len(), 1);
    clock.advance(TTL + Duration::from_millis(1));
    match next(&mut first).await {
        WatchEvent::Delete { .. } => {}
        o => panic!("{o:?}"),
    }
    won(b.delete(E, "k", Some(r)).await.unwrap());
    tokio::time::sleep(Duration::from_secs(3600) + Duration::from_millis(1)).await;
    table.collect("job#f", "k");
    table.freeze_wall(500);
    won(c.create(E, "k", b"c".to_vec()).await.unwrap());
    let mut gate = table.hold_next_query();
    let watcher = a.clone();
    let watching = tokio::spawn(async move { watcher.watch(E, "").await });
    gate.reached().await;
    a.get(E, "k").await.unwrap();
    gate.release();
    let mut second = watching.await.unwrap().unwrap();
    assert_eq!(snapshot(&mut second).await.len(), 1);
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

/// A concurrent writer's renewal that lands between an observer's expiry
/// judgment and its takeover stands, and the observer loses.
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

/// A [`FakeTable`] that records the partition of every write, and can fail
/// the next floor write, renew a key before each removal, hold the next
/// removal's reply past the op timeout once it has landed, fail the create
/// that follows a floor answer, hold the reply of the next create that
/// lands, hold the next get or query before it reads, or fail every query.
#[derive(Debug)]
struct Wrapped {
    table: FakeTable,
    pks: Mutex<Vec<String>>,
    fail_floor: AtomicBool,
    /// How many removals still get a renewal of their key first.
    renewals: Mutex<usize>,
    stall_after_remove: AtomicBool,
    fail_after_floor: AtomicBool,
    fail_next_create: AtomicBool,
    hold_created: AtomicBool,
    created: Notify,
    release_created: Notify,
    hold_read: AtomicBool,
    fail_query: AtomicBool,
    read_held: Notify,
    release_read: Notify,
}

impl Wrapped {
    fn new() -> Arc<Wrapped> {
        Arc::new(Wrapped {
            table: FakeTable::new(),
            pks: Mutex::default(),
            fail_floor: AtomicBool::new(false),
            renewals: Mutex::new(0),
            stall_after_remove: AtomicBool::new(false),
            fail_after_floor: AtomicBool::new(false),
            fail_next_create: AtomicBool::new(false),
            hold_created: AtomicBool::new(false),
            created: Notify::new(),
            release_created: Notify::new(),
            hold_read: AtomicBool::new(false),
            fail_query: AtomicBool::new(false),
            read_held: Notify::new(),
            release_read: Notify::new(),
        })
    }

    fn writes(&self) -> Vec<String> {
        self.pks.lock().unwrap().clone()
    }

    fn take_renewal(&self) -> bool {
        let mut left = self.renewals.lock().unwrap();
        let due = *left > 0;
        *left = left.saturating_sub(1);
        due
    }

    async fn renew(&self, pk: &str, sk: &str) {
        let Some(old) = self.table.item(pk, sk) else {
            return;
        };
        let renewal = Write::Put {
            v: old.v + 1,
            b: old.b.unwrap_or_default(),
            w: write_id(),
            x: old.x,
            cond: Cond::VersionIs(old.v),
        };
        self.table.write(pk, sk, renewal).await.unwrap();
    }
}

impl Table for Wrapped {
    fn write<'a>(
        &'a self,
        pk: &'a str,
        sk: &'a str,
        write: Write,
    ) -> TableFuture<'a, Result<Written, StoreError>> {
        Box::pin(async move {
            self.pks.lock().unwrap().push(pk.to_string());
            if pk.ends_with("#f") && self.fail_floor.swap(false, Ordering::SeqCst) {
                return Err(StoreError::Retryable("injected floor failure".into()));
            }
            let stall = matches!(write, Write::Remove { .. })
                && self.stall_after_remove.swap(false, Ordering::SeqCst);
            if matches!(write, Write::Remove { .. }) && self.take_renewal() {
                self.renew(pk, sk).await;
            }
            let out = self.table.write(pk, sk, write).await;
            if stall {
                tokio::time::sleep(OP_TIMEOUT * 10).await;
            }
            out
        })
    }

    fn create_above<'a>(
        &'a self,
        pk: &'a str,
        floor_pk: &'a str,
        sk: &'a str,
        put: Write,
    ) -> TableFuture<'a, Result<Created, StoreError>> {
        Box::pin(async move {
            self.pks.lock().unwrap().push(pk.to_string());
            if self.fail_next_create.swap(false, Ordering::SeqCst) {
                return Err(StoreError::Retryable("injected create failure".into()));
            }
            let out = self.table.create_above(pk, floor_pk, sk, put).await;
            match out {
                Ok(Created::Floor(_)) if self.fail_after_floor.load(Ordering::SeqCst) => {
                    self.fail_next_create.store(true, Ordering::SeqCst);
                }
                Ok(Created::Ok) if self.hold_created.swap(false, Ordering::SeqCst) => {
                    self.created.notify_one();
                    self.release_created.notified().await;
                }
                _ => {}
            }
            out
        })
    }

    fn get<'a>(
        &'a self,
        pk: &'a str,
        sk: &'a str,
    ) -> TableFuture<'a, Result<Option<Item>, StoreError>> {
        Box::pin(async move {
            if self.hold_read.swap(false, Ordering::SeqCst) {
                self.read_held.notify_one();
                self.release_read.notified().await;
            }
            self.table.get(pk, sk).await
        })
    }

    fn query(&self, query: Query) -> TableFuture<'_, Result<Page, StoreError>> {
        Box::pin(async move {
            if self.fail_query.load(Ordering::SeqCst) {
                return Err(StoreError::Retryable("injected query failure".into()));
            }
            if self.hold_read.swap(false, Ordering::SeqCst) {
                self.read_held.notify_one();
                self.release_read.notified().await;
            }
            self.table.query(query).await
        })
    }

    fn put_meta<'a>(
        &'a self,
        pk: &'a str,
        meta: Meta,
    ) -> TableFuture<'a, Result<Option<Meta>, StoreError>> {
        self.table.put_meta(pk, meta)
    }

    fn describe(&self) -> TableFuture<'_, Result<Option<Shape>, StoreError>> {
        self.table.describe()
    }

    fn create_table(&self) -> TableFuture<'_, Result<(), StoreError>> {
        self.table.create_table()
    }

    fn describe_ttl(&self) -> TableFuture<'_, Result<Ttl, StoreError>> {
        self.table.describe_ttl()
    }

    fn enable_ttl(&self) -> TableFuture<'_, Result<(), StoreError>> {
        self.table.enable_ttl()
    }
}

fn handle_over(wrapped: &Arc<Wrapped>, clock: &Arc<TestClock>) -> DynamoDbStore {
    let wall = wrapped.table.clone();
    let connector = Arc::clone(wrapped);
    DynamoDbStore::build(
        config(),
        TTL,
        OP_TIMEOUT,
        clock.clone(),
        Box::new(move || wall.now_ms()),
        Box::new(move || {
            let table: Arc<dyn Table> = connector.clone();
            Box::pin(async move { Ok(table) })
        }),
    )
    .expect("store")
}

/// A guarded delete raises the key's floor to one above the revision it
/// removes, collectable a day later, before it removes the key.
#[tokio::test]
async fn a_delete_raises_the_floor_before_removing_the_key() {
    let wrapped = Wrapped::new();
    wrapped.table.freeze_wall(1_000_000);
    let a = handle_over(&wrapped, &TestClock::frozen());
    let rev = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    let before = wrapped.writes().len();
    won(a.delete(E, "k", Some(rev)).await.unwrap());
    assert_eq!(wrapped.writes()[before..], ["job#f", "job#e"]);
    let floor = wrapped.table.item("job#f", "k").expect("a floor item");
    assert_eq!((floor.v, floor.x), (rev.0 + 1, Some(1_000 + 86_400)));
    assert!(wrapped.table.item("job#e", "k").is_none());
}

/// A delete whose floor raise fails leaves the key in place: a guarded
/// delete returns the error, and a delete at a version judged expired wins.
#[tokio::test(start_paused = true)]
async fn a_failed_floor_raise_keeps_the_key() {
    let wrapped = Wrapped::new();
    let clock = TestClock::frozen();
    let (a, b) = (
        handle(&wrapped.table, &clock),
        handle_over(&wrapped, &clock),
    );
    let rev = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    wrapped.fail_floor.store(true, Ordering::SeqCst);
    let guarded = b.delete(E, "k", Some(rev)).await;
    assert!(
        matches!(guarded, Err(StoreError::Retryable(_))),
        "{guarded:?}"
    );
    assert!(wrapped.table.item("job#e", "k").is_some(), "guarded");

    let mut watch = b.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut watch).await.len(), 1);
    clock.advance(TTL);
    assert!(matches!(next(&mut watch).await, WatchEvent::Delete { .. }));
    wrapped.fail_floor.store(true, Ordering::SeqCst);
    assert_eq!(
        b.delete(E, "k", Some(rev)).await.unwrap(),
        CasOutcome::Won(Revision(0))
    );
    assert!(wrapped.table.item("job#e", "k").is_some(), "expired");
}

/// A delete at the version this handle judged expired raises the floor to
/// one above it.
#[tokio::test(start_paused = true)]
async fn deleting_an_expired_key_raises_its_floor() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let (a, b) = (handle(&table, &clock), handle(&table, &clock));
    let rev = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    let mut watch = b.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut watch).await.len(), 1);
    clock.advance(TTL);
    assert!(matches!(next(&mut watch).await, WatchEvent::Delete { .. }));
    won(b.delete(E, "k", Some(rev)).await.unwrap());
    assert_eq!(table.item("job#f", "k").map(|i| i.v), Some(rev.0 + 1));
    assert!(table.item("job#e", "k").is_none());
}

/// An unguarded delete raises the floor to one above the revision it
/// removes.
#[tokio::test]
async fn an_unguarded_delete_raises_the_floor_to_the_revision_it_removes() {
    let table = FakeTable::new();
    let a = handle(&table, &TestClock::frozen());
    let mut rev = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    for _ in 0..2 {
        rev = won(a.update(E, "k", b"a".to_vec(), rev).await.unwrap());
    }
    won(a.delete(E, "k", None).await.unwrap());
    assert_eq!(table.item("job#f", "k").map(|i| i.v), Some(rev.0 + 1));
    assert!(table.item("job#e", "k").is_none());
}

/// An unguarded delete that loses its removal to a renewal reads the key
/// again and removes the renewed revision; after three lost rounds it
/// returns a retryable error and the key stays.
#[tokio::test]
async fn an_unguarded_delete_retries_a_lost_round_then_gives_up() {
    let wrapped = Wrapped::new();
    let table = &wrapped.table;
    let a = handle_over(&wrapped, &TestClock::frozen());
    let rev = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    let renewal = Write::Put {
        v: rev.0 + 1,
        b: b"a".to_vec(),
        w: write_id(),
        x: None,
        cond: Cond::VersionIs(rev.0),
    };
    table.interpose(1, "job#e", "k", renewal);
    won(a.delete(E, "k", None).await.unwrap());
    assert_eq!(table.item("job#f", "k").map(|i| i.v), Some(rev.0 + 2));
    assert!(table.item("job#e", "k").is_none());

    won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    *wrapped.renewals.lock().unwrap() = 100;
    let reads = table.count(FakeOp::Get);
    let given_up = a.delete(E, "k", None).await;
    assert!(
        matches!(given_up, Err(StoreError::Retryable(_))),
        "{given_up:?}"
    );
    assert_eq!(table.count(FakeOp::Get) - reads, 3, "rounds");
    assert!(table.item("job#e", "k").is_some());
}

/// A delete at a revision below the key's floor leaves the floor where it
/// stands.
#[tokio::test]
async fn a_floor_never_falls() {
    let table = FakeTable::new();
    table.freeze_wall(2_000);
    let clock = TestClock::frozen();
    let (a, b) = (handle(&table, &clock), handle(&table, &clock));
    let rev = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    won(a.delete(E, "k", Some(rev)).await.unwrap());
    won(b.delete(E, "k", Some(Revision(1_000))).await.unwrap());
    assert_eq!(table.item("job#f", "k").map(|i| i.v), Some(2_001));
}

/// An own re-create on a lagging wall lands above the floor its own delete
/// left, in one write call.
#[tokio::test(start_paused = true)]
async fn an_own_recreate_clears_the_floor_in_one_call() {
    let table = FakeTable::new();
    let w = handle(&table, &TestClock::frozen());
    table.freeze_wall(10_000);
    let r = won(w.create(E, "k", b"w".to_vec()).await.unwrap());
    won(w.delete(E, "k", Some(r)).await.unwrap());
    table.freeze_wall(5_000);
    let before = table.count(FakeOp::Write);
    let again = won(w.create(E, "k", b"w".to_vec()).await.unwrap());
    assert_eq!(table.count(FakeOp::Write) - before, 1);
    assert!(again.0 > r.0 + 1, "{again:?} not above floor {}", r.0 + 1);
}

/// An own re-create on a lagging wall lands above the floor an own
/// unguarded delete left.
#[tokio::test(start_paused = true)]
async fn an_own_recreate_clears_an_unguarded_delete_floor() {
    let table = FakeTable::new();
    let w = handle(&table, &TestClock::frozen());
    table.freeze_wall(10_000);
    won(w.create(E, "k", b"w".to_vec()).await.unwrap());
    won(w.delete(E, "k", None).await.unwrap());
    let floor = table.item("job#f", "k").expect("floor").v;
    table.freeze_wall(5_000);
    let again = won(w.create(E, "k", b"w".to_vec()).await.unwrap());
    assert!(again.0 > floor, "{again:?} not above floor {floor}");
}

/// A later own delete that finds the key absent leaves the in-memory floor
/// where an earlier own delete put it.
#[tokio::test(start_paused = true)]
async fn an_absent_unguarded_delete_keeps_the_own_floor() {
    let table = FakeTable::new();
    let w = handle(&table, &TestClock::frozen());
    table.freeze_wall(10_000);
    let r = won(w.create(E, "k", b"w".to_vec()).await.unwrap());
    won(w.delete(E, "k", Some(r)).await.unwrap());
    won(w.delete(E, "k", None).await.unwrap());
    let floor = table.item("job#f", "k").expect("floor").v;
    table.freeze_wall(5_000);
    let again = won(w.create(E, "k", b"w".to_vec()).await.unwrap());
    assert!(again.0 > floor, "{again:?} not above floor {floor}");
}

/// Creates `k` at 10_000, then runs a delete whose removal lands but whose
/// reply outlives `OP_TIMEOUT`; returns the handle and the created revision.
async fn delete_cut_after_its_removal(
    wrapped: &Arc<Wrapped>,
    guarded: bool,
) -> (DynamoDbStore, Revision) {
    let a = handle_over(wrapped, &TestClock::frozen());
    wrapped.table.freeze_wall(10_000);
    let r = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    wrapped.stall_after_remove.store(true, Ordering::SeqCst);
    let expected = guarded.then_some(r);
    let cut = tokio::time::timeout(OP_TIMEOUT, a.delete(E, "k", expected)).await;
    assert!(
        cut.is_err(),
        "the delete was not cut by the timeout: {cut:?}"
    );
    assert!(
        wrapped.table.item("job#e", "k").is_none(),
        "the removal did not land"
    );
    let floor = wrapped.table.item("job#f", "k").expect("a floor item").v;
    assert_eq!(floor, r.0 + 1);
    (a, r)
}

/// An unguarded delete cut short after its removal landed, then retried:
/// the own re-create lands above the floor.
#[tokio::test(start_paused = true)]
async fn a_cut_unguarded_delete_then_a_retry_keeps_the_own_floor() {
    let wrapped = Wrapped::new();
    let (a, r) = delete_cut_after_its_removal(&wrapped, false).await;
    won(a.delete(E, "k", None).await.unwrap());
    wrapped.table.freeze_wall(5_000);
    let again = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    assert!(again.0 > r.0 + 1, "{again:?} not above floor {}", r.0 + 1);
}

/// An unguarded delete cut short after its removal landed, with no retry:
/// the own re-create lands above the floor.
#[tokio::test(start_paused = true)]
async fn a_cut_unguarded_delete_keeps_the_own_floor() {
    let wrapped = Wrapped::new();
    let (a, r) = delete_cut_after_its_removal(&wrapped, false).await;
    wrapped.table.freeze_wall(5_000);
    let again = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    assert!(again.0 > r.0 + 1, "{again:?} not above floor {}", r.0 + 1);
}

/// A guarded delete cut short after its removal landed, with no retry: the
/// own re-create lands above the floor.
#[tokio::test(start_paused = true)]
async fn a_cut_guarded_delete_keeps_the_own_floor() {
    let wrapped = Wrapped::new();
    let (a, r) = delete_cut_after_its_removal(&wrapped, true).await;
    wrapped.table.freeze_wall(5_000);
    let again = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    assert!(again.0 > r.0 + 1, "{again:?} not above floor {}", r.0 + 1);
}

/// A guarded delete cut short after its removal landed, then retried
/// guarded: the own re-create lands above the floor.
#[tokio::test(start_paused = true)]
async fn a_cut_guarded_delete_then_a_retry_keeps_the_own_floor() {
    let wrapped = Wrapped::new();
    let (a, r) = delete_cut_after_its_removal(&wrapped, true).await;
    won(a.delete(E, "k", Some(r)).await.unwrap());
    wrapped.table.freeze_wall(5_000);
    let again = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    assert!(again.0 > r.0 + 1, "{again:?} not above floor {}", r.0 + 1);
}

/// A handle that deletes a key it watches reports the delete at or below
/// the key's floor, so a re-create at the floor plus one reaches its watch
/// as a later put.
#[tokio::test(start_paused = true)]
async fn the_deleters_own_watch_reports_its_delete_at_the_floor() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let (w, c) = (handle(&table, &clock), handle(&table, &clock));
    table.freeze_wall(10_000);
    let r = won(w.create(E, "k", b"w".to_vec()).await.unwrap());
    let mut watch = w.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut watch).await.len(), 1);
    won(w.delete(E, "k", Some(r)).await.unwrap());
    let floor = Revision(r.0 + 1);
    let deleted = match next(&mut watch).await {
        WatchEvent::Delete { revision, .. } => revision,
        other => panic!("expected the vanish delete, got {other:?}"),
    };
    table.freeze_wall(floor.0 + 1);
    let recreated = won(c.create(E, "k", b"c".to_vec()).await.unwrap());
    let put = put_without_delete(&mut watch, "k").await;
    assert!(
        deleted <= floor && put > deleted,
        "own watch delete {deleted:?} vs floor {floor:?}; re-create {recreated:?} reached the \
         watch at {put:?}"
    );
}

/// A stale holder's CAS at its old revision loses to a lease another handle
/// re-created on a lagging clock and renewed. Regression for #832.
#[tokio::test]
async fn a_stale_cas_loses_to_a_key_recreated_on_a_lagging_clock() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let (a, c, d) = (
        handle(&table, &clock),
        handle(&table, &clock),
        handle(&table, &clock),
    );
    table.freeze_wall(10_000);
    let held = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    assert!(c.get(E, "k").await.unwrap().is_some());
    clock.advance(TTL);
    assert!(c.get(E, "k").await.unwrap().is_none());
    let taken = won(c.create(E, "k", b"c".to_vec()).await.unwrap());
    won(c.delete(E, "k", Some(taken)).await.unwrap());
    table.freeze_wall(9_999);
    let low = won(d.create(E, "k", b"d".to_vec()).await.unwrap());
    let renewed = won(d.update(E, "k", b"d".to_vec(), low).await.unwrap());
    let stale = a.update(E, "k", b"a".to_vec(), held).await.unwrap();
    assert!(
        renewed > held && stale == CasOutcome::Lost,
        "d's lease reached {renewed:?} over a's old {held:?}; a's stale CAS returned {stale:?}"
    );
}

/// A create on a clock at exactly the key's floor lands above it.
#[tokio::test]
async fn a_create_at_exactly_the_floor_lands_above_it() {
    let table = FakeTable::new();
    table.freeze_wall(1_000);
    let clock = TestClock::frozen();
    let (a, b) = (handle(&table, &clock), handle(&table, &clock));
    let r = won(a.create(E, "k", b"a".to_vec()).await.unwrap());
    won(a.delete(E, "k", Some(r)).await.unwrap());
    let floor = r.0 + 1;
    table.freeze_wall(floor);
    let again = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    assert!(again.0 > floor, "{again:?} not above floor {floor}");
}

/// A re-create on a lagging clock, by a handle that neither watched nor
/// deleted the key, reaches a watch as a put above the delete it reported.
#[tokio::test(start_paused = true)]
async fn a_recreate_on_a_lagging_clock_lands_above_the_watch_delete() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let (w, b, c) = (
        handle(&table, &clock),
        handle(&table, &clock),
        handle(&table, &clock),
    );
    table.freeze_wall(10_000);
    let r = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    let mut watch = w.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut watch).await[0].revision, r);
    won(b.delete(E, "k", Some(r)).await.unwrap());
    let deleted = match next(&mut watch).await {
        WatchEvent::Delete { revision, .. } => revision,
        other => panic!("expected the vanish delete, got {other:?}"),
    };
    table.freeze_wall(5_000);
    let again = won(c.create(E, "k", b"c".to_vec()).await.unwrap());
    let put = put_without_delete(&mut watch, "k").await;
    assert!(
        again > deleted && put == again,
        "re-created at {again:?}, reached the watch at {put:?}, after a delete at {deleted:?}"
    );
}

/// A lease renewed after a watch sent its expiry delete reaches that watch as
/// a put above the delete. Regression for #949.
#[tokio::test(start_paused = true)]
async fn a_renewal_after_an_expiry_delete_lands_above_it() {
    let table = FakeTable::new();
    table.freeze_wall(10_000);
    let clock = TestClock::frozen();
    let (a, b) = (handle(&table, &clock), handle(&table, &clock));
    let r = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    let mut watch = a.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut watch).await.len(), 1);
    clock.advance(TTL + Duration::from_millis(1));
    let deleted = match next(&mut watch).await {
        WatchEvent::Delete { revision, .. } => revision,
        other => panic!("expected the expiry delete, got {other:?}"),
    };
    let renewed = won(b.update(E, "k", b"b".to_vec(), r).await.unwrap());
    match next(&mut watch).await {
        WatchEvent::Put(e) => assert!(
            e.revision > deleted,
            "put at {:?} follows the delete at {deleted:?}; the renewal wrote {renewed:?}",
            e.revision
        ),
        other => panic!("expected the put, got {other:?}"),
    }
}

/// A takeover on a lagging clock of a lease a watch already reported expired
/// reaches that watch as a put above the expiry delete. Regression for #949.
#[tokio::test(start_paused = true)]
async fn a_takeover_on_a_lagging_clock_lands_above_the_expiry_delete() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let (w, b, c) = (
        handle(&table, &clock),
        handle(&table, &clock),
        handle(&table, &clock),
    );
    table.freeze_wall(10_000);
    let r = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    let mut watch = w.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut watch).await[0].revision, r);
    assert!(c.get(E, "k").await.unwrap().is_some());
    clock.advance(TTL + Duration::from_millis(1));
    let deleted = match next(&mut watch).await {
        WatchEvent::Delete { revision, .. } => revision,
        other => panic!("expected the expiry delete, got {other:?}"),
    };
    // c's wall clock trails b's.
    table.freeze_wall(5_000);
    assert!(c.get(E, "k").await.unwrap().is_none());
    let taken = won(c.create(E, "k", b"c".to_vec()).await.unwrap());
    let put = put_without_delete(&mut watch, "k").await;
    assert!(
        taken > deleted && put == taken,
        "taken over at {taken:?}, reached the watch at {put:?}, after an expiry delete at \
         {deleted:?}"
    );
}

/// Two watches of one handle report a removed key below a re-create on a
/// lagging clock, and both deliver the re-create as a put. Regression for #949.
#[tokio::test(start_paused = true)]
async fn two_watches_of_one_handle_report_a_removal_below_a_lagging_recreate() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let (w, b, c) = (
        handle(&table, &clock),
        handle(&table, &clock),
        handle(&table, &clock),
    );
    table.freeze_wall(10_000);
    let r = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    let mut all = w.watch(E, "").await.unwrap();
    let mut ks = w.watch(E, "k").await.unwrap();
    assert_eq!(snapshot(&mut all).await[0].revision, r);
    assert_eq!(snapshot(&mut ks).await[0].revision, r);
    won(b.delete(E, "k", Some(r)).await.unwrap());
    let mut deletes = Vec::new();
    for watch in [&mut all, &mut ks] {
        match next(watch).await {
            WatchEvent::Delete { revision, .. } => deletes.push(revision),
            other => panic!("expected the vanish delete, got {other:?}"),
        }
    }
    table.freeze_wall(5_000);
    let again = won(c.create(E, "k", b"c".to_vec()).await.unwrap());
    let puts = [
        put_without_delete(&mut all, "k").await,
        put_without_delete(&mut ks, "k").await,
    ];
    assert!(
        deletes.iter().all(|d| again > *d) && puts == [again, again],
        "removed at {r:?}; watches reported deletes at {deletes:?}; re-created at {again:?}, \
         reached the watches at {puts:?}"
    );
}

/// An own create over a key native TTL collected sits above the vanish delete
/// its watch reported.
#[tokio::test(start_paused = true)]
async fn an_own_create_sits_above_a_vanish_delete_of_a_collected_key() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let (w, b) = (handle(&table, &clock), handle(&table, &clock));
    table.freeze_wall(10_000);
    let r = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    let mut watch = w.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut watch).await[0].revision, r);
    table.collect("job#e", "k");
    let deleted = match next(&mut watch).await {
        WatchEvent::Delete { revision, .. } => revision,
        other => panic!("expected the vanish delete, got {other:?}"),
    };
    assert_eq!(deleted, Revision(r.0 + 1));
    table.freeze_wall(5_000);
    let again = won(w.create(E, "k", b"w".to_vec()).await.unwrap());
    let put = put_without_delete(&mut watch, "k").await;
    assert!(
        again > deleted && put == again,
        "created at {r:?}; vanish delete at {deleted:?}; own create at {again:?}, reached the \
         watch at {put:?}"
    );
}

/// A takeover of a key re-created lower after native TTL collected it lands
/// above the expiry delete its own watch reported.
#[tokio::test(start_paused = true)]
async fn own_takeover_sits_above_its_expiry_delete_after_a_collected_key() {
    let table = FakeTable::new();
    let clock = TestClock::frozen();
    let (w, b, c) = (
        handle(&table, &clock),
        handle(&table, &clock),
        handle(&table, &clock),
    );
    table.freeze_wall(10_000);
    let r = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    let mut watch = w.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut watch).await[0].revision, r);
    table.collect("job#e", "k");
    let vanished = match next(&mut watch).await {
        WatchEvent::Delete { revision, .. } => revision,
        other => panic!("expected the vanish delete, got {other:?}"),
    };
    table.freeze_wall(5_000);
    let low = won(c.create(E, "k", b"c".to_vec()).await.unwrap());
    assert!(low < vanished);
    assert_eq!(put_without_delete(&mut watch, "k").await, low);
    clock.advance(TTL + Duration::from_millis(1));
    let expired = match next(&mut watch).await {
        WatchEvent::Delete { revision, .. } => revision,
        other => panic!("expected the expiry delete, got {other:?}"),
    };
    let taken = won(w.create(E, "k", b"w".to_vec()).await.unwrap());
    let put = put_without_delete(&mut watch, "k").await;
    assert!(
        taken > expired && put == taken,
        "expired {expired:?} taken {taken:?} put {put:?}"
    );
}

/// A job whose meta holds layout 1, whose creates do not check the floor, is
/// refused at startup.
#[tokio::test]
async fn a_job_started_at_layout_1_is_refused() {
    let table = FakeTable::new();
    let before = Meta {
        lease_ms: u64::try_from(TTL.as_millis()).unwrap(),
        layout: 1,
    };
    table.put_meta("job#m", before).await.unwrap();
    let err = handle(&table, &TestClock::frozen()).get(D, "k").await;
    assert!(
        matches!(&err, Err(StoreError::Fatal(m)) if m.contains("layout 1")),
        "{err:?}"
    );
}

/// A watch that delivered a key before another handle deleted it sends this
/// handle's re-create on a lagging clock as a put above that revision, with
/// no delete first, even when the poll that delivered it returns while the
/// create's reply is in flight. Regression for #831.
#[tokio::test(start_paused = true)]
async fn an_own_create_sits_above_a_deleted_key_its_watch_delivered() {
    let wrapped = Wrapped::new();
    wrapped.table.freeze_wall(10_000);
    let clock = TestClock::frozen();
    let (own, peer) = (
        handle_over(&wrapped, &clock),
        handle(&wrapped.table, &clock),
    );
    let mut watch = own.watch(E, "").await.unwrap();
    assert!(snapshot(&mut watch).await.is_empty());
    peer.get(D, "warm").await.unwrap();
    let mut gate = wrapped.table.hold_next_query();
    let u = won(peer.create(E, "k", b"peer".to_vec()).await.unwrap());
    gate.reached().await;
    won(peer.delete(E, "k", Some(u)).await.unwrap());
    wrapped.table.freeze_wall(5_000);
    wrapped.hold_created.store(true, Ordering::SeqCst);
    let creating = own.clone();
    let mut create = tokio::spawn(async move { creating.create(E, "k", b"own".to_vec()).await });
    tokio::select! {
        () = wrapped.created.notified() => {}
        done = &mut create => panic!("the create returned before one landed: {done:?}"),
    }
    gate.release();
    assert!(matches!(next(&mut watch).await, WatchEvent::Put(e) if e.revision == u));
    wrapped.release_created.notify_one();
    let mine = won(create.await.unwrap().unwrap());
    let put = put_without_delete(&mut watch, "k").await;
    assert!(
        put == mine && mine > u,
        "own create {mine:?} reached the watch at {put:?}, after {u:?}"
    );
}

/// A create that meets a floor and then fails leaves the key's floor out of
/// what its watch reports, so the vanish delete sits at the floor and a
/// re-create one above it reaches the watch as a later put.
#[tokio::test(start_paused = true)]
async fn a_floor_answer_leaves_the_watch_delete_at_the_floor() {
    let wrapped = Wrapped::new();
    wrapped.table.freeze_wall(10_000);
    let clock = TestClock::frozen();
    let (b, c, d) = (
        handle(&wrapped.table, &clock),
        handle_over(&wrapped, &clock),
        handle(&wrapped.table, &clock),
    );
    let r = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    let mut watch = c.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut watch).await[0].revision, r);
    won(b.delete(E, "k", Some(r)).await.unwrap());
    let floor = Revision(r.0 + 1);
    wrapped.fail_after_floor.store(true, Ordering::SeqCst);
    wrapped.table.freeze_wall(5_000);
    let failed = c.create(E, "k", b"c".to_vec()).await;
    assert!(
        matches!(failed, Err(StoreError::Retryable(_))),
        "{failed:?}"
    );
    let deleted = match next(&mut watch).await {
        WatchEvent::Delete { revision, .. } => revision,
        other => panic!("expected the vanish delete, got {other:?}"),
    };
    wrapped.table.freeze_wall(floor.0 + 1);
    let recreated = won(d.create(E, "k", b"d".to_vec()).await.unwrap());
    let put = put_without_delete(&mut watch, "k").await;
    assert!(
        deleted <= floor && put > deleted,
        "watch delete {deleted:?} vs floor {floor:?}; re-create {recreated:?} reached the \
         watch at {put:?}"
    );
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
        started.inner.poll_recorder.get().is_some(),
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
    assert!(store.inner.poll_recorder.get().is_some());
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

/// The revision of the next delete of `key`, skipping puts of it at or below
/// `below`.
async fn delete_of(watch: &mut WatchStream, key: &str, below: Revision) -> Revision {
    loop {
        match next(watch).await {
            WatchEvent::Delete { key: k, revision } if k == key => return revision,
            WatchEvent::Put(entry) if entry.key == key => {
                assert!(entry.revision <= below, "{entry:?}")
            }
            other => panic!("unexpected event {other:?}"),
        }
    }
}

/// Two handles over one table at a frozen wall of 10 000, and a watch of
/// the first on every ephemeral key once `k` exists, written by the second.
async fn watching_k(
    wrapped: &Arc<Wrapped>,
) -> (
    Arc<TestClock>,
    DynamoDbStore,
    DynamoDbStore,
    WatchStream,
    Revision,
) {
    wrapped.table.freeze_wall(10_000);
    let clock = TestClock::frozen();
    let (a, b) = (handle_over(wrapped, &clock), handle_over(wrapped, &clock));
    let r = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    let mut watch = a.watch(E, "").await.unwrap();
    assert_eq!(snapshot(&mut watch).await.len(), 1);
    (clock, a, b, watch, r)
}

/// A vanish delete sits above the revision its watch delivered when another
/// handle renewed and removed the key between two polls.
#[tokio::test(start_paused = true)]
async fn a_vanish_delete_sits_above_what_its_watch_delivered() {
    let (_clock, _a, b, mut watch, r) = watching_k(&Wrapped::new()).await;
    let renewed = won(b.update(E, "k", b"b".to_vec(), r).await.unwrap());
    won(b.delete(E, "k", Some(renewed)).await.unwrap());
    let deleted = delete_of(&mut watch, "k", r).await;
    assert!(deleted > r, "delete {deleted:?}; the watch delivered {r:?}");
}

/// A vanish delete sits above a revision of the key its handle's `get`
/// returned and its watch never delivered.
#[tokio::test(start_paused = true)]
async fn a_vanish_delete_sits_above_a_revision_its_handle_read() {
    let (_clock, a, b, mut watch, r) = watching_k(&Wrapped::new()).await;
    let renewed = won(b.update(E, "k", b"b".to_vec(), r).await.unwrap());
    assert_eq!(a.get(E, "k").await.unwrap().unwrap().revision, renewed);
    won(b.delete(E, "k", Some(renewed)).await.unwrap());
    let deleted = delete_of(&mut watch, "k", r).await;
    assert!(
        deleted > renewed,
        "delete {deleted:?}; handle read {renewed:?}"
    );
}

/// A vanish delete sits above a revision of the key its handle's `list`
/// returned and its watch never delivered.
#[tokio::test(start_paused = true)]
async fn a_vanish_delete_sits_above_a_revision_its_handle_listed() {
    let (_clock, a, b, mut watch, r) = watching_k(&Wrapped::new()).await;
    let renewed = won(b.update(E, "k", b"b".to_vec(), r).await.unwrap());
    assert_eq!(a.list(E, "").await.unwrap()[0].revision, renewed);
    won(b.delete(E, "k", Some(renewed)).await.unwrap());
    let deleted = delete_of(&mut watch, "k", r).await;
    assert!(
        deleted > renewed,
        "delete {deleted:?}; handle listed {renewed:?}"
    );
}

/// A vanish delete sits above the handle's own renewal when another handle
/// removed the key before the watch polled it.
#[tokio::test(start_paused = true)]
async fn a_vanish_delete_sits_above_an_own_write_its_watch_missed() {
    let (_clock, a, b, mut watch, r) = watching_k(&Wrapped::new()).await;
    let own = won(a.update(E, "k", b"a".to_vec(), r).await.unwrap());
    won(b.delete(E, "k", Some(own)).await.unwrap());
    let deleted = delete_of(&mut watch, "k", r).await;
    assert!(deleted > own, "delete {deleted:?}; own write {own:?}");
}

/// A `get` that an own write overtook still orders a later vanish delete
/// above the revision it returned. Regression for #959.
#[tokio::test(start_paused = true)]
async fn a_read_overtaken_by_an_own_write_still_orders_the_delete() {
    let wrapped = Wrapped::new();
    let (_clock, a, b, mut watch, r) = watching_k(&wrapped).await;
    wrapped.hold_read.store(true, Ordering::SeqCst);
    let reader = a.clone();
    let reading = tokio::spawn(async move { reader.get(E, "k").await });
    wrapped.read_held.notified().await;
    let own = won(a.update(E, "k", b"a".to_vec(), r).await.unwrap());
    let theirs = won(b.update(E, "k", b"b".to_vec(), own).await.unwrap());
    wrapped.release_read.notify_one();
    let read = reading.await.unwrap().unwrap().expect("k").revision;
    assert_eq!(read, theirs);
    won(b.delete(E, "k", Some(theirs)).await.unwrap());
    let deleted = delete_of(&mut watch, "k", r).await;
    assert!(deleted > read, "delete {deleted:?}; handle read {read:?}");
}

/// A `list` that an own write overtook still orders a later vanish delete
/// above the revision it returned. Regression for #959.
#[tokio::test(start_paused = true)]
async fn a_listing_overtaken_by_an_own_write_still_orders_the_delete() {
    let wrapped = Wrapped::new();
    let (_clock, a, b, mut watch, r) = watching_k(&wrapped).await;
    wrapped.hold_read.store(true, Ordering::SeqCst);
    let lister = a.clone();
    let listing = tokio::spawn(async move { lister.list(E, "").await });
    wrapped.read_held.notified().await;
    let own = won(a.update(E, "k", b"a".to_vec(), r).await.unwrap());
    let theirs = won(b.update(E, "k", b"b".to_vec(), own).await.unwrap());
    wrapped.release_read.notify_one();
    let listed = listing.await.unwrap().unwrap()[0].revision;
    assert_eq!(listed, theirs);
    won(b.delete(E, "k", Some(theirs)).await.unwrap());
    let deleted = delete_of(&mut watch, "k", r).await;
    assert!(
        deleted > listed,
        "delete {deleted:?}; handle listed {listed:?}"
    );
}

/// A new watch's first read that an own write overtook still orders another
/// watch's vanish delete above the revision that read delivered. Regression
/// for #959.
#[tokio::test(start_paused = true)]
async fn a_snapshot_overtaken_by_an_own_write_still_orders_another_watchs_delete() {
    let wrapped = Wrapped::new();
    let (_clock, a, b, mut first, r) = watching_k(&wrapped).await;
    wrapped.hold_read.store(true, Ordering::SeqCst);
    let watcher = a.clone();
    let watching = tokio::spawn(async move { watcher.watch(E, "k").await });
    wrapped.read_held.notified().await;
    let own = won(a.update(E, "k", b"a".to_vec(), r).await.unwrap());
    let theirs = won(b.update(E, "k", b"b".to_vec(), own).await.unwrap());
    wrapped.release_read.notify_one();
    let mut second = watching.await.unwrap().unwrap();
    let delivered = snapshot(&mut second).await[0].revision;
    assert_eq!(delivered, theirs);
    won(b.delete(E, "k", Some(theirs)).await.unwrap());
    let deleted = delete_of(&mut first, "k", theirs).await;
    assert!(
        deleted > delivered,
        "delete {deleted:?}; the handle delivered {delivered:?}"
    );
}

/// A key a watch still holds keeps what its handle wrote and read through
/// two TTLs of failed polls, so the vanish delete sits above both.
/// Regression for #959.
#[tokio::test(start_paused = true)]
async fn a_key_a_watch_holds_outlives_failed_polls_with_what_its_handle_saw() {
    let wrapped = Wrapped::new();
    let (clock, a, b, mut watch, r) = watching_k(&wrapped).await;
    wrapped.fail_query.store(true, Ordering::SeqCst);
    let own = won(a.update(E, "k", b"a".to_vec(), r).await.unwrap());
    let theirs = won(b.update(E, "k", b"b".to_vec(), own).await.unwrap());
    assert_eq!(a.get(E, "k").await.unwrap().unwrap().revision, theirs);
    won(b.delete(E, "k", Some(theirs)).await.unwrap());
    assert!(a.get(E, "k").await.unwrap().is_none());
    clock.advance(TTL * 2 + Duration::from_millis(1));
    tokio::time::sleep(POLL * 3).await;
    wrapped.fail_query.store(false, Ordering::SeqCst);
    let deleted = delete_of(&mut watch, "k", r).await;
    assert!(
        deleted > theirs,
        "delete {deleted:?}; handle read {theirs:?} before the removal"
    );
}

/// A gone key is evicted two TTLs after a watch reported its delete, or
/// after the watch that held it dropped.
#[tokio::test(start_paused = true)]
async fn a_gone_key_no_watch_holds_is_evicted() {
    let table = FakeTable::new();
    table.freeze_wall(10_000);
    let clock = TestClock::frozen();
    let (a, b) = (handle(&table, &clock), handle(&table, &clock));
    let mut evictor = a.watch(E, "x").await.unwrap();
    assert!(snapshot(&mut evictor).await.is_empty());
    let j = won(b.create(E, "j", b"b".to_vec()).await.unwrap());
    let k = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    let mut on_j = a.watch(E, "j").await.unwrap();
    let mut on_k = a.watch(E, "k").await.unwrap();
    assert_eq!(snapshot(&mut on_j).await.len(), 1);
    assert_eq!(snapshot(&mut on_k).await.len(), 1);
    won(b.delete(E, "k", Some(k)).await.unwrap());
    delete_of(&mut on_k, "k", k).await;
    drop(on_j);
    won(b.delete(E, "j", Some(j)).await.unwrap());
    assert!(a.get(E, "j").await.unwrap().is_none());
    clock.advance(TTL * 2 + Duration::from_millis(1));
    tokio::time::sleep(POLL * 3).await;
    let observed = a.inner.observed();
    assert!(
        !observed.tracks("j") && !observed.tracks("k"),
        "{observed:?}"
    );
}

/// A poll read in flight while another poller evicts keeps the keys it may
/// still judge, so the watch it starts reports a vanish delete above a
/// revision its handle read before the removal. Regression for #959.
#[tokio::test(start_paused = true)]
async fn a_poll_read_spanning_two_ttls_still_orders_the_delete() {
    let table = FakeTable::new();
    table.freeze_wall(10_000);
    let clock = TestClock::frozen();
    let (a, b) = (handle(&table, &clock), handle(&table, &clock));
    let mut evictor = a.watch(E, "x").await.unwrap();
    assert!(snapshot(&mut evictor).await.is_empty());
    let r = won(b.create(E, "k", b"b".to_vec()).await.unwrap());
    let mut gate = table.hold_next_query();
    let watcher = a.clone();
    let watching = tokio::spawn(async move { watcher.watch(E, "").await });
    gate.reached().await;
    let theirs = won(b.update(E, "k", b"b".to_vec(), r).await.unwrap());
    assert_eq!(a.get(E, "k").await.unwrap().unwrap().revision, theirs);
    won(b.delete(E, "k", Some(theirs)).await.unwrap());
    assert!(a.get(E, "k").await.unwrap().is_none());
    clock.advance(TTL * 2 + Duration::from_millis(1));
    tokio::time::sleep(POLL * 3).await;
    gate.release();
    let mut watch = watching.await.unwrap().unwrap();
    snapshot(&mut watch).await;
    let deleted = delete_of(&mut watch, "k", theirs).await;
    assert!(
        deleted > theirs,
        "delete {deleted:?}; handle read {theirs:?} before the removal"
    );
}

/// A poll read that failed no longer keeps keys from eviction.
#[tokio::test(start_paused = true)]
async fn a_failed_poll_read_stops_holding_back_eviction() {
    let wrapped = Wrapped::new();
    wrapped.table.freeze_wall(10_000);
    let clock = TestClock::frozen();
    let (a, b) = (handle_over(&wrapped, &clock), handle_over(&wrapped, &clock));
    let mut evictor = a.watch(E, "x").await.unwrap();
    assert!(snapshot(&mut evictor).await.is_empty());
    wrapped.fail_query.store(true, Ordering::SeqCst);
    tokio::time::sleep(POLL * 2).await;
    wrapped.fail_query.store(false, Ordering::SeqCst);
    let j = won(b.create(E, "j", b"b".to_vec()).await.unwrap());
    assert_eq!(a.get(E, "j").await.unwrap().unwrap().revision, j);
    won(b.delete(E, "j", Some(j)).await.unwrap());
    assert!(a.get(E, "j").await.unwrap().is_none());
    clock.advance(TTL * 2 + Duration::from_millis(1));
    tokio::time::sleep(POLL * 3).await;
    let observed = a.inner.observed();
    assert!(!observed.tracks("j"), "{observed:?}");
}
