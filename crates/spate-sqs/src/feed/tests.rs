use super::*;
use crate::queue::BoxFuture;
use crate::scripted::{Failure, Message, ScriptedQueue};
use spate_core::feed::{FeedPark, wake_channel};
use std::sync::atomic::{AtomicBool, Ordering};

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

/// Pins the clamp to the 12-hour hold, counted from each receive: as a pure
/// rule, and through renewal ticks for a unit received again 12 h − 70 s after
/// its first receive, at a 12 h visibility.
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
    assert_eq!(hold_end(now), now + HOLD - secs(30));

    let (feed, queue) = start(HOLD);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let first = receive_n(&feed, &park, 1).await.remove(0);
    feed.fail(first.receipt, "handed back").unwrap();
    until("the fail is sent", || queue.visibility().len() == 1).await;
    tokio::time::advance(HOLD - secs(70)).await;

    queue.ready(Message::new("u").redelivered(2));
    let second = receive_n(&feed, &park, 1).await.remove(0);
    assert_eq!(second.deliveries, 2);
    let received = Instant::now();
    assert_eq!(
        feed.deadline_of(second.receipt),
        Some(received + HOLD - secs(30)),
        "the first visibility is clamped to this receive's hold"
    );

    // A renewal period is 4 h at a 12 h visibility.
    let period = HOLD / 3;
    for (tick, left) in [(1, 8), (2, 4)] {
        feed.renew(second.receipt).unwrap();
        tokio::time::advance(period).await;
        until("the tick extends the unit", || {
            queue.visibility().len() == 1 + tick
        })
        .await;
        let sent: Vec<i32> = queue.visibility()[tick].iter().map(|e| e.seconds).collect();
        let left = left * 60 * 60 - 30;
        assert_eq!(
            sent,
            [left],
            "tick {tick}: the time left of this receive's hold"
        );
    }

    feed.renew(second.receipt).unwrap();
    tokio::time::advance(period).await;
    settle_tasks().await;
    assert_eq!(queue.visibility().len(), 3, "no extension past the hold");
    assert_eq!(
        feed.renew(second.receipt).unwrap_err().kind,
        FeedErrorKind::Gone
    );
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

/// A [`ScriptedQueue`] whose whole calls can be failed until a flag is
/// cleared, and which records when each fail and delete was sent.
#[derive(Debug, Default)]
struct Gated {
    inner: ScriptedQueue,
    /// Fail every `SendMessageBatch` as Retryable.
    send_down: AtomicBool,
    /// Fail every `DeleteMessageBatch` as Retryable.
    delete_down: AtomicBool,
    /// Fail every `DeleteMessageBatch` as Fatal.
    delete_fatal: AtomicBool,
    visibility_at: Mutex<Vec<Instant>>,
    deletes_at: Mutex<Vec<Instant>>,
}

impl Queue for Gated {
    fn receive(&self, r: ReceiveRequest) -> BoxFuture<'_, Result<Vec<Received>, CallError>> {
        self.inner.receive(r)
    }

    fn send_batch(&self, e: Vec<SendEntry>) -> BoxFuture<'_, Result<BatchOutcome, CallError>> {
        if self.send_down.load(Ordering::SeqCst) {
            return Box::pin(async { Err(CallError::retryable("KMS.ThrottlingException")) });
        }
        self.inner.send_batch(e)
    }

    fn change_visibility_batch(
        &self,
        e: Vec<VisibilityEntry>,
    ) -> BoxFuture<'_, Result<BatchOutcome, CallError>> {
        self.visibility_at.lock().unwrap().push(Instant::now());
        self.inner.change_visibility_batch(e)
    }

    fn delete_batch(&self, e: Vec<DeleteEntry>) -> BoxFuture<'_, Result<BatchOutcome, CallError>> {
        self.deletes_at.lock().unwrap().push(Instant::now());
        if self.delete_fatal.load(Ordering::SeqCst) {
            return Box::pin(async { Err(CallError::fatal("AccessDenied")) });
        }
        if self.delete_down.load(Ordering::SeqCst) {
            return Box::pin(async { Err(CallError::retryable("InternalError")) });
        }
        self.inner.delete_batch(e)
    }
}

fn start_gated(visibility: Duration) -> (SqsFeed, Arc<Gated>) {
    let queue = Arc::new(Gated::default());
    let feed = SqsFeed::start(
        Arc::clone(&queue) as Arc<dyn Queue>,
        &config(visibility),
        &tokio::runtime::Handle::current(),
    );
    (feed, queue)
}

/// Pins that a publish SQS keeps failing holds back no delete or fail: both
/// are sent before the receipts' deadline while every send fails.
#[tokio::test(start_paused = true)]
async fn a_publish_outage_does_not_hold_back_deletes_and_fails() {
    let (feed, queue) = start_gated(MINUTE);
    queue.send_down.store(true, Ordering::SeqCst);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.inner.ready(Message::new("a"));
    queue.inner.ready(Message::new("b"));
    let got = receive_n(&feed, &park, 2).await;
    let deadline = feed.deadline_of(got[0].receipt).unwrap();
    let _publish = feed.publish(Unit::new("p", b"body".to_vec()));
    settle_tasks().await;
    feed.acknowledge(got[0].receipt).unwrap();
    feed.fail(got[1].receipt, "unreadable").unwrap();
    for _ in 0..30 {
        tokio::time::advance(Duration::from_secs(5)).await;
        settle_tasks().await;
    }
    assert!(Instant::now() > deadline);
    let deletes = queue.deletes_at.lock().unwrap().clone();
    let fails = queue.visibility_at.lock().unwrap().clone();
    assert_eq!(deletes.len(), 1, "the delete was sent once");
    assert_eq!(fails.len(), 1, "the fail was sent once");
    assert!(
        deletes.iter().chain(&fails).all(|t| *t < deadline),
        "a delete or fail was sent after the receipts' deadline"
    );
}

/// Pins that a fail whose unit's deadline passed while earlier deletes were
/// retried is dropped unsent.
#[tokio::test(start_paused = true)]
async fn a_fail_past_its_deadline_is_dropped_unsent() {
    let (feed, queue) = start_gated(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.inner.ready(Message::new("failed"));
    let failed = receive_n(&feed, &park, 1).await.remove(0);
    tokio::time::advance(Duration::from_secs(30)).await;
    queue.inner.ready(Message::new("acknowledged"));
    let acknowledged = receive_n(&feed, &park, 1).await.remove(0);
    let deadline = feed.deadline_of(failed.receipt).unwrap();

    queue.delete_down.store(true, Ordering::SeqCst);
    feed.acknowledge(acknowledged.receipt).unwrap();
    feed.fail(failed.receipt, "unreadable").unwrap();
    for _ in 0..30 {
        tokio::time::advance(Duration::from_secs(5)).await;
        settle_tasks().await;
    }
    let deletes = queue.deletes_at.lock().unwrap().clone();
    assert!(
        deletes.iter().any(|t| *t > deadline),
        "the delete was retried past the failed unit's deadline"
    );
    let fails = queue.visibility_at.lock().unwrap().clone();
    assert!(
        fails.is_empty(),
        "a fail was sent at {fails:?}, past {deadline:?}"
    );
}

/// Pins that a Fatal rejects with Fatal every publish the feed holds: the
/// batch the publish task is sending and the publishes still queued behind it.
#[tokio::test(start_paused = true)]
async fn a_fatal_delete_rejects_every_held_publish() {
    let (feed, queue) = start_gated(MINUTE);
    queue.send_down.store(true, Ordering::SeqCst);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.inner.ready(Message::new("a"));
    let got = receive_n(&feed, &park, 1).await;
    let mut publishes: Vec<Confirmation> = (0..11)
        .map(|i| feed.publish(Unit::new(format!("p{i}"), "body")))
        .collect();
    settle_tasks().await;
    assert_eq!(
        feed.lock().sending.len(),
        10,
        "the first batch is being sent"
    );
    assert_eq!(feed.lock().publishes.len(), 1, "one publish is queued");

    queue.delete_fatal.store(true, Ordering::SeqCst);
    feed.acknowledge(got[0].receipt).unwrap();
    until("every publish settles", || {
        publishes.iter_mut().all(|c| c.try_outcome().is_some())
    })
    .await;
    for (i, c) in publishes.iter_mut().enumerate() {
        let kind = c.try_outcome().unwrap().unwrap_err().kind;
        assert_eq!(kind, FeedErrorKind::Fatal, "publish {i} read {kind:?}");
    }
}
