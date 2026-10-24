//! A feed reader driven through `Arc<dyn Feed>` against the scripted feed, as
//! the extending guide renders it.

use spate_core::feed::memory::MemoryFeed;
use spate_core::feed::{Feed, FeedError, FeedErrorKind, Receipt, Unit, wake_channel};
use spate_test::scripted_feed;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// Pins the reader's side of a redelivery: the unit is held again under a
/// new receipt, the lapsed receipt answers Gone to a renewal and is let go,
/// and only the live receipt is acknowledged.
#[test]
fn a_reader_holds_a_redelivered_unit_by_its_new_receipt() -> Result<(), FeedError> {
    // ANCHOR: reader
    let (feed, script) = scripted_feed();
    let feed: Arc<dyn Feed> = Arc::new(feed);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);

    // Units the reader holds, keyed by receipt: one unit id can be held twice.
    let mut held: BTreeMap<Receipt, Unit> = BTreeMap::new();

    let first = script.deliver(Unit::new("day-1", b"2026-10-01".to_vec()));
    assert!(park.park(Duration::from_secs(5)));
    for delivery in feed.receive(8)? {
        held.insert(delivery.receipt, delivery.unit);
    }

    // The feed gives the unit out again, as a queue does when a lease lapses.
    // The reader reads the new delivery from its start.
    let second = script.redeliver(first);
    assert!(park.park(Duration::from_secs(5)));
    for delivery in feed.receive(8)? {
        assert_eq!(delivery.deliveries, 2);
        held.insert(delivery.receipt, delivery.unit);
    }

    // Each control-plane pass renews what the reader holds. A receipt that
    // answers Gone names a delivery the feed has replaced, so the reader
    // lets go of it. Any other error keeps the delivery for the next pass.
    held.retain(|receipt, _| match feed.renew(*receipt) {
        Ok(()) => true,
        Err(e) => e.kind != FeedErrorKind::Gone,
    });
    assert_eq!(held.keys().copied().collect::<Vec<_>>(), vec![second]);

    // Read to its end, with every record acknowledged by the sink:
    feed.acknowledge(second)?;
    assert_eq!(script.acknowledged(), vec![second]);
    assert_eq!(script.gone(), vec![first]);
    // ANCHOR_END: reader
    Ok(())
}

/// Pins publish refusal through `Arc<dyn Feed>`: the publisher reads the
/// refusal without waiting, and the refused unit is never delivered.
#[test]
fn a_refused_publish_is_reported_and_never_delivered() {
    let (feed, script) = scripted_feed();
    let feed: Arc<dyn Feed> = Arc::new(feed);
    script.refuse_next_publish(FeedErrorKind::Refused);
    let mut confirmation = feed.publish(Unit::new("too-big", vec![0; 16]));
    let refused = confirmation.try_outcome().unwrap().unwrap_err();
    assert_eq!(refused.kind, FeedErrorKind::Refused);
    assert!(feed.receive(8).unwrap().is_empty());
    assert_eq!(script.published().len(), 1);
}

fn counts_for_a_unit_published_twice(feed: Arc<dyn Feed>) -> Vec<u32> {
    drop(feed.publish(Unit::new("twice", b"x".to_vec())));
    drop(feed.publish(Unit::new("twice", b"x".to_vec())));
    feed.receive(8)
        .unwrap()
        .into_iter()
        .map(|d| d.deliveries)
        .collect()
}

/// Pins that the scripted feed counts deliveries per published unit, as the
/// in-process feed does: two units sharing an id are each on their first
/// delivery.
#[test]
fn both_feeds_count_a_unit_published_twice_alike() {
    let memory = counts_for_a_unit_published_twice(Arc::new(MemoryFeed::new()));
    let (scripted, _script) = scripted_feed();
    let scripted = counts_for_a_unit_published_twice(Arc::new(scripted));
    assert_eq!(memory, vec![1, 1]);
    assert_eq!(scripted, memory, "scripted feed counts by unit id");
}

/// Pins that a unit published again after the first copy was acknowledged
/// arrives on its first delivery.
#[test]
fn a_unit_published_again_after_its_ack_is_a_first_delivery() {
    let (feed, _script) = scripted_feed();
    drop(feed.publish(Unit::new("k", b"x".to_vec())));
    let first = feed.receive(8).unwrap().remove(0);
    feed.acknowledge(first.receipt).unwrap();
    drop(feed.publish(Unit::new("k", b"x".to_vec())));
    assert_eq!(feed.receive(8).unwrap().remove(0).deliveries, 1);
}

/// Pins that a scripted duplicate after a redelivery is the unit's third
/// delivery.
#[test]
fn a_duplicate_after_a_redelivery_counts_three() {
    let (feed, script) = scripted_feed();
    let first = script.deliver(Unit::new("a", b"x".to_vec()));
    assert_eq!(feed.receive(8).unwrap()[0].deliveries, 1);
    script.redeliver(first);
    assert_eq!(feed.receive(8).unwrap()[0].deliveries, 2);
    script.deliver(Unit::new("a", b"x".to_vec()));
    assert_eq!(feed.receive(8).unwrap()[0].deliveries, 3);
}

/// Pins that a redelivery after a scripted duplicate is the unit's third
/// delivery.
#[test]
fn a_redelivery_after_a_duplicate_counts_three() {
    let (feed, script) = scripted_feed();
    let first = script.deliver(Unit::new("a", b"x".to_vec()));
    script.deliver(Unit::new("a", b"x".to_vec()));
    let got: Vec<u32> = feed
        .receive(8)
        .unwrap()
        .iter()
        .map(|d| d.deliveries)
        .collect();
    assert_eq!(got, vec![1, 2]);
    script.redeliver(first);
    assert_eq!(feed.receive(8).unwrap()[0].deliveries, 3);
}

/// Pins that redelivering a redelivery is the unit's third delivery.
#[test]
fn a_second_redelivery_counts_three() {
    let (feed, script) = scripted_feed();
    let first = script.deliver(Unit::new("a", b"x".to_vec()));
    let _ = feed.receive(8).unwrap();
    let second = script.redeliver(first);
    let _ = feed.receive(8).unwrap();
    script.redeliver(second);
    assert_eq!(feed.receive(8).unwrap()[0].deliveries, 3);
}

/// Pins that a published unit keeps its own count beside a scripted unit
/// of the same id.
#[test]
fn a_published_unit_keeps_its_count_beside_a_scripted_id() {
    let (feed, script) = scripted_feed();
    let scripted = script.deliver(Unit::new("a", b"x".to_vec()));
    drop(feed.publish(Unit::new("a", b"y".to_vec())));
    let published = feed.receive(8).unwrap()[1].receipt;
    script.redeliver(scripted);
    assert_eq!(feed.receive(8).unwrap()[0].deliveries, 2);
    script.redeliver(published);
    assert_eq!(feed.receive(8).unwrap()[0].deliveries, 2);
}

/// Pins that a published unit redelivered twice is on its third delivery.
#[test]
fn a_published_unit_redelivered_twice_counts_three() {
    let (feed, script) = scripted_feed();
    drop(feed.publish(Unit::new("a", b"x".to_vec())));
    let first = feed.receive(8).unwrap()[0].receipt;
    let second = script.redeliver(first);
    assert_eq!(feed.receive(8).unwrap()[0].deliveries, 2);
    script.redeliver(second);
    assert_eq!(feed.receive(8).unwrap()[0].deliveries, 3);
}

/// Pins the scripted feed's stop calls: it hands back only once told to,
/// records each stop call in order, and lets go of a handed-back delivery.
#[test]
fn a_scripted_feed_records_hand_backs_and_closes() {
    let (feed, script) = scripted_feed();
    let feed: Arc<dyn Feed> = Arc::new(feed);
    assert!(!feed.hands_back());
    script.hand_back_on_stop();
    assert!(feed.hands_back());

    let first = script.deliver(Unit::new("a", b"a".to_vec()));
    let second = script.deliver(Unit::new("b", b"b".to_vec()));
    let kept = script.deliver(Unit::new("c", b"c".to_vec()));
    assert_eq!(feed.receive(8).unwrap().len(), 3);
    feed.stop_receiving();
    feed.stop_receiving();
    feed.hand_back(first).unwrap();
    feed.hand_back(second).unwrap();
    assert_eq!(feed.hand_back(first).unwrap_err().kind, FeedErrorKind::Gone);
    let mut closed = feed.close(Duration::from_secs(2)).expect("the feed closes");
    assert!(matches!(closed.try_outcome(), Some(Ok(()))));

    assert_eq!(script.stopped_receiving(), 2);
    assert_eq!(script.handed_back(), vec![first, second]);
    assert_eq!(script.held(), vec![kept]);
    assert_eq!(script.closes(), vec![Duration::from_secs(2)]);
    assert_eq!(script.gone(), vec![first]);
}
