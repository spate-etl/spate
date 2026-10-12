//! The scripted feed's delivery count across mixed script sequences.
//!
//! `Delivery::deliveries` counts per published unit, and two units may share
//! an id. The scripted feed treats every `deliver` of one id as one unit, so
//! its duplicates and their redeliveries share a count. A published unit keeps
//! a count of its own, whatever its id. Each test runs one sequence and
//! asserts the count of every delivery it receives.

use spate_core::feed::{Feed, Receipt, Unit};
use spate_test::{FeedScript, ScriptedFeed, scripted_feed};

fn unit(id: &str) -> Unit {
    Unit::new(id, id.as_bytes().to_vec())
}

/// Receive everything queued and return each delivery's receipt and count.
fn receive(feed: &ScriptedFeed) -> Vec<(Receipt, u32)> {
    feed.receive(64)
        .unwrap()
        .into_iter()
        .map(|d| (d.receipt, d.deliveries))
        .collect()
}

/// Receive everything queued and return each delivery's count.
fn counts(feed: &ScriptedFeed) -> Vec<u32> {
    receive(feed).into_iter().map(|(_, n)| n).collect()
}

/// Publish `id` unscripted and return the receipt of its first delivery.
fn publish(feed: &ScriptedFeed, id: &str) -> Receipt {
    drop(feed.publish(unit(id)));
    let got = receive(feed);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].1, 1, "a publish starts at 1");
    got[0].0
}

fn deliver(feed: &ScriptedFeed, script: &FeedScript, id: &str) -> (Receipt, u32) {
    script.deliver(unit(id));
    let got = receive(feed);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].1, 1, "a first scripted delivery is 1");
    got[0]
}

/// Pins that one scripted delivery is the first.
#[test]
fn one_delivery_counts_one() {
    let (feed, script) = scripted_feed();
    script.deliver(unit("a"));
    assert_eq!(counts(&feed), vec![1]);
}

/// Pins that a scripted duplicate is the second delivery.
#[test]
fn a_duplicate_counts_two() {
    let (feed, script) = scripted_feed();
    script.deliver(unit("a"));
    script.deliver(unit("a"));
    assert_eq!(counts(&feed), vec![1, 2]);
}

/// Pins that a redelivery of a scripted unit is the second delivery.
#[test]
fn a_redelivery_counts_two() {
    let (feed, script) = scripted_feed();
    let (first, _) = deliver(&feed, &script, "a");
    script.redeliver(first);
    assert_eq!(counts(&feed), vec![2]);
}

/// Pins that two redeliveries of a scripted unit count two, then three.
#[test]
fn two_redeliveries_count_two_then_three() {
    let (feed, script) = scripted_feed();
    let (first, _) = deliver(&feed, &script, "a");
    let second = script.redeliver(first);
    assert_eq!(counts(&feed), vec![2]);
    script.redeliver(second);
    assert_eq!(counts(&feed), vec![3]);
}

/// Pins that a duplicate after a redelivery is the third delivery.
#[test]
fn a_duplicate_after_a_redelivery_counts_three() {
    let (feed, script) = scripted_feed();
    let (first, _) = deliver(&feed, &script, "a");
    script.redeliver(first);
    assert_eq!(counts(&feed), vec![2]);
    script.deliver(unit("a"));
    assert_eq!(counts(&feed), vec![3]);
}

/// Pins that a redelivery of a scripted redelivery carries on the count its
/// id shares with a later duplicate: four, then five for the next duplicate.
#[test]
fn a_redelivered_redelivery_carries_on_the_shared_count() {
    let (feed, script) = scripted_feed();
    let (first, _) = deliver(&feed, &script, "a");
    let second = script.redeliver(first);
    assert_eq!(counts(&feed), vec![2]);
    script.deliver(unit("a"));
    assert_eq!(counts(&feed), vec![3]);
    script.redeliver(second);
    assert_eq!(counts(&feed), vec![4]);
    script.deliver(unit("a"));
    assert_eq!(counts(&feed), vec![5]);
}

/// Pins that redelivering the first of two duplicates, then the second,
/// counts three, then four.
#[test]
fn redelivering_the_first_duplicate_then_the_second_counts_three_then_four() {
    let (feed, script) = scripted_feed();
    script.deliver(unit("a"));
    script.deliver(unit("a"));
    let got = receive(&feed);
    assert_eq!(got.iter().map(|d| d.1).collect::<Vec<_>>(), vec![1, 2]);
    script.redeliver(got[0].0);
    assert_eq!(counts(&feed), vec![3]);
    script.redeliver(got[1].0);
    assert_eq!(counts(&feed), vec![4]);
}

/// Pins that redelivering the second of two duplicates, then the first,
/// counts three, then four.
#[test]
fn redelivering_the_second_duplicate_then_the_first_counts_three_then_four() {
    let (feed, script) = scripted_feed();
    script.deliver(unit("a"));
    script.deliver(unit("a"));
    let got = receive(&feed);
    assert_eq!(got.iter().map(|d| d.1).collect::<Vec<_>>(), vec![1, 2]);
    script.redeliver(got[1].0);
    assert_eq!(counts(&feed), vec![3]);
    script.redeliver(got[0].0);
    assert_eq!(counts(&feed), vec![4]);
}

/// Pins that redelivering a scripted delivery still queued replaces it with
/// the second delivery.
#[test]
fn a_redelivery_of_a_queued_delivery_counts_two() {
    let (feed, script) = scripted_feed();
    let queued = script.deliver(unit("a"));
    script.redeliver(queued);
    assert_eq!(counts(&feed), vec![2]);
}

/// Pins that a duplicate after the unit was dead-lettered is the second
/// delivery.
#[test]
fn a_duplicate_after_a_dead_letter_counts_two() {
    let (feed, script) = scripted_feed();
    let (first, _) = deliver(&feed, &script, "a");
    script.dead_letter(first);
    script.deliver(unit("a"));
    assert_eq!(counts(&feed), vec![2]);
}

/// Pins that a redelivery of a published unit is its second delivery.
#[test]
fn a_redelivery_of_a_published_unit_counts_two() {
    let (feed, script) = scripted_feed();
    let first = publish(&feed, "a");
    script.redeliver(first);
    assert_eq!(counts(&feed), vec![2]);
}

/// Pins that two redeliveries of a published unit count two, then three.
#[test]
fn two_redeliveries_of_a_published_unit_count_two_then_three() {
    let (feed, script) = scripted_feed();
    let first = publish(&feed, "a");
    let second = script.redeliver(first);
    assert_eq!(counts(&feed), vec![2]);
    script.redeliver(second);
    assert_eq!(counts(&feed), vec![3]);
}

/// Pins that two publishes of one id, each redelivered once, are each on
/// their second delivery.
#[test]
fn two_publishes_of_one_id_redelivered_once_each_count_two() {
    let (feed, script) = scripted_feed();
    let first = publish(&feed, "a");
    let second = publish(&feed, "a");
    script.redeliver(first);
    assert_eq!(counts(&feed), vec![2]);
    script.redeliver(second);
    assert_eq!(counts(&feed), vec![2]);
}

/// Pins that a unit published after a scripted delivery of its id counts
/// its own redeliveries: two, then three.
#[test]
fn a_publish_after_a_delivery_redelivered_twice_counts_two_then_three() {
    let (feed, script) = scripted_feed();
    deliver(&feed, &script, "a");
    let first = publish(&feed, "a");
    let second = script.redeliver(first);
    assert_eq!(counts(&feed), vec![2]);
    script.redeliver(second);
    assert_eq!(counts(&feed), vec![3]);
}

/// Pins that a scripted delivery and a publish of one id, each redelivered
/// once, are each on their second delivery.
#[test]
fn a_delivery_and_a_publish_of_one_id_redelivered_once_each_count_two() {
    let (feed, script) = scripted_feed();
    let (scripted, _) = deliver(&feed, &script, "a");
    let published = publish(&feed, "a");
    script.redeliver(scripted);
    assert_eq!(counts(&feed), vec![2]);
    script.redeliver(published);
    assert_eq!(counts(&feed), vec![2]);
}

/// Pins that after two duplicates and a publish of one id, the published
/// redelivery counts two and the scripted redeliveries carry on from the
/// duplicates: three, then four.
#[test]
fn two_duplicates_and_a_publish_redelivered_count_apart() {
    let (feed, script) = scripted_feed();
    script.deliver(unit("a"));
    script.deliver(unit("a"));
    let scripted = receive(&feed);
    assert_eq!(scripted.iter().map(|d| d.1).collect::<Vec<_>>(), vec![1, 2]);
    let published = publish(&feed, "a");
    script.redeliver(published);
    assert_eq!(counts(&feed), vec![2]);
    script.redeliver(scripted[0].0);
    assert_eq!(counts(&feed), vec![3]);
    script.redeliver(scripted[1].0);
    assert_eq!(counts(&feed), vec![4]);
}

/// Pins that interleaved scripted and published redeliveries of one id
/// count two and two, and a later duplicate counts three.
#[test]
fn interleaved_redeliveries_then_a_duplicate_count_two_two_three() {
    let (feed, script) = scripted_feed();
    let (scripted, _) = deliver(&feed, &script, "a");
    let published = publish(&feed, "a");
    script.redeliver(scripted);
    assert_eq!(counts(&feed), vec![2]);
    script.redeliver(published);
    assert_eq!(counts(&feed), vec![2]);
    script.deliver(unit("a"));
    assert_eq!(counts(&feed), vec![3]);
}

/// Pins that a publish before the first scripted delivery of its id leaves
/// that delivery at one, and each redelivers to two.
#[test]
fn a_publish_before_the_first_delivery_counts_apart() {
    let (feed, script) = scripted_feed();
    let published = publish(&feed, "a");
    let (scripted, _) = deliver(&feed, &script, "a");
    script.redeliver(scripted);
    assert_eq!(counts(&feed), vec![2]);
    script.redeliver(published);
    assert_eq!(counts(&feed), vec![2]);
}

/// Pins that a published redelivery leaves a later scripted delivery of the
/// same id at one.
#[test]
fn a_delivery_after_a_published_redelivery_counts_one() {
    let (feed, script) = scripted_feed();
    let published = publish(&feed, "a");
    script.redeliver(published);
    assert_eq!(counts(&feed), vec![2]);
    script.deliver(unit("a"));
    assert_eq!(counts(&feed), vec![1]);
}

/// Pins that a confirmed held publish counts its own redeliveries, two then
/// three, and a later duplicate of a scripted delivery of its id counts two.
#[test]
fn a_confirmed_held_publish_counts_apart_from_a_scripted_id() {
    let (feed, script) = scripted_feed();
    deliver(&feed, &script, "a");
    script.hold_publishes();
    drop(feed.publish(unit("a")));
    script.confirm_held_publishes();
    let got = receive(&feed);
    assert_eq!(got.iter().map(|d| d.1).collect::<Vec<_>>(), vec![1]);
    let second = script.redeliver(got[0].0);
    assert_eq!(counts(&feed), vec![2]);
    script.redeliver(second);
    assert_eq!(counts(&feed), vec![3]);
    script.deliver(unit("a"));
    assert_eq!(counts(&feed), vec![2]);
}

/// Pins that units with different ids are counted apart.
#[test]
fn different_ids_count_apart() {
    let (feed, script) = scripted_feed();
    script.deliver(unit("a"));
    script.deliver(unit("b"));
    let got = receive(&feed);
    assert_eq!(got.iter().map(|d| d.1).collect::<Vec<_>>(), vec![1, 1]);
    script.redeliver(got[0].0);
    assert_eq!(counts(&feed), vec![2]);
    script.deliver(unit("b"));
    assert_eq!(counts(&feed), vec![2]);
}
