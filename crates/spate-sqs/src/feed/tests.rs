use super::*;
use crate::scripted::{Failure, Message, ScriptedQueue};
use spate_core::feed::{FeedPark, wake_channel};

const MINUTE: Duration = Duration::from_secs(60);

fn config(visibility: Duration) -> SqsFeedConfig {
    SqsFeedConfig {
        queue_url: "https://sqs.eu-west-2.amazonaws.com/111122223333/jobs".to_owned(),
        visibility,
        region: None,
        endpoint: None,
    }
}

fn start(visibility: Duration) -> (SqsFeed, Arc<ScriptedQueue>) {
    let queue = Arc::new(ScriptedQueue::default());
    let feed = SqsFeed::start(
        Arc::clone(&queue) as Arc<dyn Queue>,
        &config(visibility),
        &tokio::runtime::Handle::current(),
    );
    (feed, queue)
}

/// Yield to the feed's tasks until `cond` holds. Never moves tokio time.
async fn until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if cond() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("never happened: {what}");
}

/// Let the feed's tasks run every step they can without time moving.
async fn settle_tasks() {
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
}

/// Ask for `n` units, wait until they are buffered, and take them.
async fn receive_n(feed: &SqsFeed, park: &FeedPark, n: usize) -> Vec<Delivery> {
    assert!(feed.receive(n).unwrap().is_empty(), "nothing buffered yet");
    until("the units are buffered", || feed.buffered() == n).await;
    assert!(park.park(Duration::ZERO), "buffering woke the reader");
    let got = feed.receive(n).unwrap();
    assert_eq!(got.len(), n);
    got
}

fn failure(index: usize, code: &'static str) -> Failure {
    Failure {
        index,
        code,
        sender_fault: true,
    }
}

/// Pins the clamp to the 12-hour hold, counted from the first receive: as a
/// pure rule, as a hold computed from the first-receive time, and through a
/// renewal tick for a unit first received 12 h − 70 s ago.
#[tokio::test(start_paused = true)]
async fn an_extension_is_clamped_to_the_time_left() {
    let now = Instant::now();
    let secs = Duration::from_secs;
    for (left, ext) in [
        (secs(61), Some(secs(60))),
        (secs(59), Some(secs(59))),
        (secs(1), Some(secs(1))),
        (Duration::from_millis(999), None),
        (Duration::ZERO, None),
    ] {
        assert_eq!(extension(MINUTE, now + left, now), ext, "left {left:?}");
    }

    let wall = UNIX_EPOCH + secs(2_000_000_000);
    let first_ms = |ago: Duration| {
        u64::try_from((wall - ago).duration_since(UNIX_EPOCH).unwrap().as_millis()).unwrap()
    };
    assert_eq!(
        hold_end(now, wall, Some(first_ms(HOLD - secs(31)))),
        now + secs(1)
    );
    assert_eq!(hold_end(now, wall, Some(first_ms(HOLD - secs(29)))), now);

    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u").redelivered(2, HOLD - secs(70)));
    let [delivery] = <[Delivery; 1]>::try_from(receive_n(&feed, &park, 1).await).unwrap();
    assert_eq!(delivery.deliveries, 2);
    let receipt = delivery.receipt;

    feed.renew(receipt).unwrap();
    tokio::time::advance(secs(20)).await;
    until("the tick extends the unit", || {
        queue.visibility().len() == 1
    })
    .await;
    let sent: Vec<i32> = queue.visibility()[0].iter().map(|e| e.seconds).collect();
    assert!(
        sent == [19] || sent == [20],
        "the extension sent is {sent:?}, not the 20 s left of the hold"
    );

    feed.renew(receipt).unwrap();
    tokio::time::advance(secs(20)).await;
    settle_tasks().await;
    assert_eq!(queue.visibility().len(), 1, "no extension past the hold");
    assert_eq!(feed.renew(receipt).unwrap_err().kind, FeedErrorKind::Gone);
}

/// Pins that a unit whose hold leaves less than one renewal period is
/// dropped at every receive and never handed out, while each receive spends
/// one of the queue's `maxReceiveCount`: at the default visibility for a unit
/// first received 12 h − 10 s ago, and at a 12 h visibility for one first
/// received 8 h ago.
#[tokio::test(start_paused = true)]
async fn a_unit_first_received_almost_12_hours_ago_is_never_handed_out() {
    for (visibility, ago) in [
        (MINUTE, HOLD - Duration::from_secs(10)),
        (HOLD, Duration::from_secs(8 * 60 * 60)),
    ] {
        let (feed, queue) = start(visibility);
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        queue.repeat(Message::new("u").redelivered(2, ago));
        assert!(feed.receive(1).unwrap().is_empty());
        for round in 1..=3 {
            until("the unit is received", || queue.receives().len() == round).await;
            until("the unit is buffered", || park.park(Duration::ZERO)).await;
            assert!(
                feed.receive(1).unwrap().is_empty(),
                "a unit {ago:?} past its first receive was handed out at visibility {visibility:?}"
            );
        }
        until("a fourth receive", || queue.receives().len() == 4).await;
        assert!(
            queue.visibility().is_empty(),
            "no extension for a dropped unit"
        );
    }
}

/// Pins the long-poll wait at a third of the visibility, at most 20 s, so a
/// unit that arrives at the end of a poll is still handed out.
#[tokio::test(start_paused = true)]
async fn a_unit_received_at_the_end_of_the_long_poll_is_handed_out() {
    let secs = Duration::from_secs;
    for (visibility, wait) in [(secs(3), 1), (secs(15), 5), (MINUTE, 20), (HOLD, 20)] {
        assert_eq!(wait_seconds(visibility), wait, "{visibility:?}");
    }

    let (feed, queue) = start(secs(15));
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.answer_at_end_of_poll();
    queue.ready(Message::new("late"));
    assert!(feed.receive(1).unwrap().is_empty());
    until("the poll is sent", || queue.receives().len() == 1).await;
    let wait = queue.receives()[0].wait;
    assert_eq!(wait, 5);
    tokio::time::advance(secs(u64::try_from(wait).unwrap())).await;
    until("the unit is buffered", || park.park(Duration::ZERO)).await;
    let got = feed.receive(1).unwrap();
    assert_eq!(got.len(), 1, "the unit that arrived at the end of the poll");
}

/// Pins that each publish is confirmed or rejected by its own entry: a
/// failed entry rejects only its unit, as Refused when the sender is at fault
/// and Retryable otherwise.
#[tokio::test(start_paused = true)]
async fn a_failed_publish_entry_settles_only_its_own_confirmation() {
    for (index, sender_fault) in [(1, true), (2, false), (2, true), (1, false)] {
        let (feed, queue) = start(MINUTE);
        queue.fail_next_send(vec![Failure {
            index,
            code: "InvalidMessageContents",
            sender_fault,
        }]);
        let mut confirmations: Vec<Confirmation> = (0..3)
            .map(|i| feed.publish(Unit::new(format!("u{i}"), format!("body {i}"))))
            .collect();
        until("every publish settles", || {
            confirmations.iter_mut().all(|c| c.try_outcome().is_some())
        })
        .await;
        for (i, c) in confirmations.iter_mut().enumerate() {
            let outcome = c.try_outcome().unwrap();
            if i == index {
                let expected = if sender_fault {
                    FeedErrorKind::Refused
                } else {
                    FeedErrorKind::Retryable
                };
                assert_eq!(outcome.unwrap_err().kind, expected, "entry {i}");
            } else {
                assert!(
                    outcome.is_ok(),
                    "entry {i} of a batch that failed entry {index}"
                );
            }
        }
        assert_eq!(queue.sends().len(), 1, "one call, not retried");
        assert_eq!(queue.sends()[0].len(), 3);
    }
}

/// Pins that an extension SQS refuses for one receipt, with either code in
/// either spelling, makes only that receipt `Gone`, and that the others are
/// extended from the time the call was sent.
#[tokio::test(start_paused = true)]
async fn a_renewal_entry_the_queue_refuses_makes_only_its_receipt_gone() {
    for (index, code) in [
        (1, "ReceiptHandleIsInvalid"),
        (2, "ReceiptHandleIsInvalid"),
        (1, "AWS.SimpleQueueService.MessageNotInflight"),
    ] {
        let (feed, queue) = start(MINUTE);
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        for i in 0..3 {
            queue.ready(Message::new(&format!("u{i}")));
        }
        let receipts: Vec<Receipt> = receive_n(&feed, &park, 3)
            .await
            .iter()
            .map(|d| d.receipt)
            .collect();
        for r in &receipts {
            feed.renew(*r).unwrap();
        }
        queue.fail_next_visibility(vec![failure(index, code)]);
        tokio::time::advance(Duration::from_secs(20)).await;
        let ticked = Instant::now();
        until("the tick extends the units", || {
            queue.visibility().len() == 1
        })
        .await;
        settle_tasks().await;
        assert_eq!(queue.visibility()[0].len(), 3);
        for (i, r) in receipts.iter().enumerate() {
            if i == index {
                assert_eq!(
                    feed.renew(*r).unwrap_err().kind,
                    FeedErrorKind::Gone,
                    "{code}"
                );
            } else {
                assert_eq!(
                    feed.deadline_of(*r),
                    Some(ticked + MINUTE),
                    "{code}: entry {i}"
                );
                feed.renew(*r).unwrap();
            }
        }
    }
}

/// Pins that a delete SQS fails for one entry is dropped, never sent again.
#[tokio::test(start_paused = true)]
async fn a_failed_delete_entry_is_dropped() {
    for index in [1, 2] {
        let (feed, queue) = start(MINUTE);
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        for i in 0..3 {
            queue.ready(Message::new(&format!("u{i}")));
        }
        let deliveries = receive_n(&feed, &park, 3).await;
        queue.fail_next_delete(vec![failure(index, "ReceiptHandleIsInvalid")]);
        for d in &deliveries {
            feed.acknowledge(d.receipt).unwrap();
        }
        until("the deletes are sent", || !queue.deletes().is_empty()).await;
        let failed = queue.deletes()[0][index].handle.clone();
        assert_eq!(queue.deletes()[0].len(), 3);
        tokio::time::advance(2 * MINUTE).await;
        settle_tasks().await;
        assert_eq!(
            queue.deletes().len(),
            1,
            "the failed entry {index} was sent again"
        );
        assert!(
            queue
                .deletes()
                .iter()
                .flatten()
                .filter(|e| e.handle == failed)
                .count()
                == 1,
            "the failed handle was sent once"
        );
    }
}

/// Pins that publishes are cut at ten entries or 1 MiB, whichever comes
/// first, and that a unit SQS cannot store is refused unsent.
#[tokio::test(start_paused = true)]
async fn publishes_are_cut_at_ten_entries_or_one_mib() {
    let (feed, queue) = start(MINUTE);
    let small: Vec<Confirmation> = (0..25)
        .map(|i| feed.publish(Unit::new(format!("s{i}"), "x")))
        .collect();
    drop(small);
    until("the small units are sent", || queue.sends().len() == 3).await;
    let sizes: Vec<usize> = queue.sends().iter().map(Vec::len).collect();
    assert_eq!(sizes, [10, 10, 5]);

    let (feed, queue) = start(MINUTE);
    let big = "b".repeat(400 * 1024);
    let mut big: Vec<Confirmation> = (0..3)
        .map(|i| feed.publish(Unit::new(format!("b{i}"), big.clone())))
        .collect();
    until("the large units are sent", || {
        big.iter_mut().all(|c| c.try_outcome().is_some())
    })
    .await;
    let sizes: Vec<usize> = queue.sends().iter().map(Vec::len).collect();
    assert_eq!(sizes, [2, 1], "three 400 KiB units fit two to 1 MiB");

    let (feed, queue) = start(MINUTE);
    let mut over = feed.publish(Unit::new("over", "o".repeat(MAX_MESSAGE_BYTES)));
    let mut binary = feed.publish(Unit::new("binary", vec![0xff, 0xfe]));
    assert_eq!(
        over.try_outcome().unwrap().unwrap_err().kind,
        FeedErrorKind::Refused
    );
    assert_eq!(
        binary.try_outcome().unwrap().unwrap_err().kind,
        FeedErrorKind::Refused
    );
    settle_tasks().await;
    assert!(queue.sends().is_empty(), "a refused unit is never sent");
}

/// Pins that no receive is sent before the reader first asks for units, and
/// that the first ask sets how many.
#[tokio::test(start_paused = true)]
async fn nothing_is_received_before_the_first_receive_call() {
    let (feed, queue) = start(MINUTE);
    tokio::time::advance(MINUTE).await;
    settle_tasks().await;
    assert!(queue.receives().is_empty(), "received before any demand");
    assert!(feed.receive(3).unwrap().is_empty());
    until("a receive is sent", || queue.receives().len() == 1).await;
    assert_eq!(queue.receives()[0].max, 3);
}

/// Pins that a waker set while units are buffered is woken at once.
#[tokio::test(start_paused = true)]
async fn set_waker_wakes_at_once_when_units_are_already_buffered() {
    let (feed, queue) = start(MINUTE);
    let (first, first_park) = wake_channel();
    feed.set_waker(first);
    queue.ready(Message::new("u"));
    assert!(feed.receive(2).unwrap().is_empty());
    until("the unit is buffered", || feed.buffered() == 1).await;
    assert!(first_park.park(Duration::ZERO));

    let (second, second_park) = wake_channel();
    feed.set_waker(second);
    assert!(
        second_park.park(Duration::ZERO),
        "the new waker was not woken"
    );
}

/// Pins that a tick extends only receipts renewed since the last one, in
/// batches of ten.
#[tokio::test(start_paused = true)]
async fn only_marked_receipts_are_extended() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    for i in 0..25 {
        queue.ready(Message::new(&format!("u{i}")));
    }
    let deliveries = receive_n(&feed, &park, 25).await;
    for d in &deliveries {
        feed.renew(d.receipt).unwrap();
    }
    tokio::time::advance(Duration::from_secs(20)).await;
    until("the marked units are extended", || {
        queue.visibility().len() == 3
    })
    .await;
    let sizes: Vec<usize> = queue.visibility().iter().map(Vec::len).collect();
    assert_eq!(sizes, [10, 10, 5]);

    tokio::time::advance(Duration::from_secs(20)).await;
    settle_tasks().await;
    assert_eq!(
        queue.visibility().len(),
        3,
        "an unmarked receipt was extended"
    );
}

/// Pins that a failed unit is made visible again with a zero visibility.
#[tokio::test(start_paused = true)]
async fn a_failed_unit_is_set_visible_with_a_zero_visibility() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let delivery = receive_n(&feed, &park, 1).await.remove(0);
    feed.fail(delivery.receipt, "poison").unwrap();
    until("the fail is sent", || queue.visibility().len() == 1).await;
    let sent: Vec<i32> = queue.visibility()[0].iter().map(|e| e.seconds).collect();
    assert_eq!(sent, [0]);
    assert_eq!(
        feed.renew(delivery.receipt).unwrap_err().kind,
        FeedErrorKind::Gone
    );
}

/// Pins that a Fatal receive is returned by every later call and wakes the
/// reader, and that a Retryable one is returned once.
#[tokio::test(start_paused = true)]
async fn a_fatal_receive_is_sticky_and_a_retryable_one_is_returned_once() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.fail_next_receive(CallError::retryable("throttled"));
    assert!(feed.receive(1).unwrap().is_empty());
    until("the failure wakes the reader", || park.park(Duration::ZERO)).await;
    assert_eq!(feed.receive(1).unwrap_err().kind, FeedErrorKind::Retryable);
    assert!(feed.receive(1).unwrap().is_empty());

    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.fail_next_receive(CallError::fatal("queue missing"));
    assert!(feed.receive(1).unwrap().is_empty());
    until("the failure wakes the reader", || park.park(Duration::ZERO)).await;
    for _ in 0..2 {
        assert_eq!(feed.receive(1).unwrap_err().kind, FeedErrorKind::Fatal);
    }
    assert_eq!(
        feed.renew(Receipt(0)).unwrap_err().kind,
        FeedErrorKind::Fatal
    );
    let mut published = feed.publish(Unit::new("u", "x"));
    assert_eq!(
        published.try_outcome().unwrap().unwrap_err().kind,
        FeedErrorKind::Fatal
    );
}
