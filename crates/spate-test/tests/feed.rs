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
