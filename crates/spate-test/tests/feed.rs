//! A feed reader driven through `Arc<dyn Feed>` against the scripted feed, as
//! the extending guide renders it.

use spate_core::feed::{Feed, FeedErrorKind, Receipt, Unit, wake_channel};
use spate_test::scripted_feed;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// Pins the reader's side of a redelivery: the unit is held again under a
/// new receipt, the lapsed receipt answers Gone to a renewal and is let go,
/// and only the live receipt is acknowledged.
#[test]
fn a_reader_holds_a_redelivered_unit_by_its_new_receipt() {
    // ANCHOR: reader
    let (feed, script) = scripted_feed();
    let feed: Arc<dyn Feed> = Arc::new(feed);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);

    // Units the reader holds, keyed by receipt: one unit id can be held twice.
    let mut held: BTreeMap<Receipt, Unit> = BTreeMap::new();

    let first = script.deliver(Unit::new("day-1", b"2026-10-01".to_vec()));
    assert!(park.park(Duration::from_secs(5)));
    for delivery in feed.receive(8).unwrap() {
        held.insert(delivery.receipt, delivery.unit);
    }

    // The feed gives the unit out again, as a queue does when a lease lapses.
    // The reader reads the new delivery from its start.
    let second = script.redeliver(first);
    assert!(park.park(Duration::from_secs(5)));
    for delivery in feed.receive(8).unwrap() {
        assert_eq!(delivery.deliveries, 2);
        held.insert(delivery.receipt, delivery.unit);
    }

    // Each control-plane pass renews what the reader holds. A receipt that
    // answers Gone names a delivery the feed has replaced, so the reader
    // lets go of it.
    held.retain(|receipt, _| match feed.renew(*receipt) {
        Ok(()) => true,
        Err(e) if e.kind == FeedErrorKind::Gone => false,
        Err(e) => panic!("renew failed: {e}"),
    });
    assert_eq!(held.keys().copied().collect::<Vec<_>>(), vec![second]);

    // Read to its end, with every record acknowledged by the sink:
    feed.acknowledge(second).unwrap();
    assert_eq!(script.acknowledged(), vec![second]);
    assert_eq!(script.gone(), vec![first]);
    // ANCHOR_END: reader
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
