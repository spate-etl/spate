use super::*;
use crate::queue::BoxFuture;
use crate::scripted::{Failure, Message, ScriptedQueue};
use spate_core::feed::{FeedPark, wake_channel};
use std::sync::atomic::{AtomicBool, Ordering};

const MINUTE: Duration = Duration::from_secs(60);
const QUEUE_URL: &str = "https://sqs.eu-west-2.amazonaws.com/111122223333/jobs";
const DEAD_URL: &str = "https://sqs.eu-west-2.amazonaws.com/111122223333/jobs-dlq";

fn config(visibility: Duration) -> SqsFeedConfig {
    SqsFeedConfig {
        queue_url: QUEUE_URL.to_owned(),
        visibility,
        region: None,
        endpoint: None,
    }
}

fn start(visibility: Duration) -> (SqsFeed, Arc<ScriptedQueue>) {
    let queue = Arc::new(ScriptedQueue::default());
    let feed = SqsFeed::start(
        Arc::clone(&queue) as Arc<dyn Queue>,
        Arc::clone(&queue) as Arc<dyn startup::QueueAttributes>,
        DEAD_URL.to_owned(),
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

    queue.ready(Message::new("u").redelivered(2).same_message_as("m0"));
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

/// Pins a delivery's unit id: the `spate.unit` attribute when the message
/// carries one, and its `MessageId` when it carries none, as a storage
/// notification does.
#[tokio::test(start_paused = true)]
async fn a_message_without_the_unit_attribute_takes_its_message_id() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("published"));
    queue.ready(
        Message::new("notified")
            .without_unit_attribute()
            .same_message_as("msg-1"),
    );
    let got = receive_n(&feed, &park, 2).await;
    let ids: Vec<&str> = got.iter().map(|d| d.unit.id.as_str()).collect();
    assert_eq!(ids, ["u0", "msg-1"]);
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

/// Pins that a delete SQS fails for one entry is dropped, never sent again,
/// and that an acknowledged receipt is `Gone`.
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
        for d in &deliveries {
            assert_eq!(feed.renew(d.receipt).unwrap_err().kind, FeedErrorKind::Gone);
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
        within: Option<Duration>,
    ) -> BoxFuture<'_, Result<BatchOutcome, CallError>> {
        self.visibility_at.lock().unwrap().push(Instant::now());
        self.inner.change_visibility_batch(e, within)
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
        Arc::new(ScriptedQueue::default()),
        DEAD_URL.to_owned(),
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
    // The tick due at 20 s extends the failed unit, handed out unrenewed, to
    // 90 s. The acknowledged unit, received at 40 s, holds until 100 s, so
    // its delete is retried past the failed unit's deadline.
    tokio::time::advance(Duration::from_secs(30)).await;
    until("the tick extends the failed unit", || {
        queue.visibility_at.lock().unwrap().len() == 1
    })
    .await;
    tokio::time::advance(Duration::from_secs(10)).await;
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
    let fails = queue.inner.visibility();
    assert_eq!(
        fails.len(),
        1,
        "a fail was sent past {deadline:?}: {fails:?}"
    );
}

/// Pins how a Fatal settles the publishes the feed holds: the batch the
/// publish task is sending reads Retryable, since SQS may have stored it, and
/// the publishes still queued behind it read Fatal.
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
        let expected = if i < 10 {
            FeedErrorKind::Retryable
        } else {
            FeedErrorKind::Fatal
        };
        assert_eq!(kind, expected, "publish {i} read {kind:?}");
    }
}

/// Pins that a delete call the queue fails whole is retried with backoff until
/// the receipt's deadline, then dropped.
#[tokio::test(start_paused = true)]
async fn a_failed_delete_call_is_retried_until_the_deadline_then_dropped() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let delivery = receive_n(&feed, &park, 1).await.remove(0);
    for _ in 0..100 {
        queue.fail_next_delete_call(CallError::retryable("InternalError"));
    }
    feed.acknowledge(delivery.receipt).unwrap();
    for _ in 0..12 {
        tokio::time::advance(Duration::from_secs(5)).await;
        settle_tasks().await;
    }
    let at_deadline = queue.deletes().len();
    assert!(at_deadline > 1, "the delete was not retried");
    tokio::time::advance(MINUTE).await;
    settle_tasks().await;
    assert_eq!(
        queue.deletes().len(),
        at_deadline,
        "the delete was retried past its deadline"
    );
}

/// Pins that an extension call the queue fails whole is sent again at the
/// next tick without a new renewal.
#[tokio::test(start_paused = true)]
async fn a_failed_extension_call_is_sent_again_at_the_next_tick() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let delivery = receive_n(&feed, &park, 1).await.remove(0);
    feed.renew(delivery.receipt).unwrap();
    queue.fail_next_visibility_call(CallError::retryable("InternalError"));
    tokio::time::advance(Duration::from_secs(20)).await;
    until("the first extension is sent", || {
        queue.visibility().len() == 1
    })
    .await;
    tokio::time::advance(Duration::from_secs(20)).await;
    let ticked = Instant::now();
    until("the extension is sent again", || {
        queue.visibility().len() == 2
    })
    .await;
    settle_tasks().await;
    assert_eq!(feed.deadline_of(delivery.receipt), Some(ticked + MINUTE));
}

/// Pins that a publish call the queue fails whole and transiently is sent
/// again and confirmed.
#[tokio::test(start_paused = true)]
async fn a_failed_publish_call_is_sent_again() {
    let (feed, queue) = start(MINUTE);
    queue.fail_next_send_call(CallError::retryable("InternalError"));
    let mut published = feed.publish(Unit::new("u", "body"));
    for _ in 0..5 {
        tokio::time::advance(Duration::from_secs(1)).await;
        settle_tasks().await;
    }
    assert!(published.try_outcome().unwrap().is_ok());
    assert_eq!(queue.sends().len(), 2);
}

/// Pins that a publish call failed on a batch-shape code refuses its units
/// and stops the feed with a Fatal.
#[tokio::test(start_paused = true)]
async fn a_batch_shape_failure_refuses_its_publishes_and_stops_the_feed() {
    let (feed, queue) = start(MINUTE);
    queue.fail_next_send_call(CallError::batch_shape("BatchRequestTooLong"));
    let mut published = feed.publish(Unit::new("u", "body"));
    until("the publish settles", || published.try_outcome().is_some()).await;
    assert_eq!(
        published.try_outcome().unwrap().unwrap_err().kind,
        FeedErrorKind::Refused
    );
    assert_eq!(feed.receive(1).unwrap_err().kind, FeedErrorKind::Fatal);
}

/// Pins the receipt rules past a deadline: a fail after the visibility lapsed
/// sends nothing and returns `Gone`, while an acknowledge still queues the
/// delete.
#[tokio::test(start_paused = true)]
async fn past_the_deadline_a_fail_is_gone_and_an_acknowledge_still_deletes() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("a"));
    queue.ready(Message::new("b"));
    let got = receive_n(&feed, &park, 2).await;
    tokio::time::advance(MINUTE + Duration::from_secs(1)).await;
    assert_eq!(
        feed.fail(got[0].receipt, "late").unwrap_err().kind,
        FeedErrorKind::Gone
    );
    feed.acknowledge(got[1].receipt).unwrap();
    until("the delete is sent", || queue.deletes().len() == 1).await;
    settle_tasks().await;
    assert!(
        queue.visibility().is_empty(),
        "a fail past the deadline was sent"
    );
}

/// Pins the 30 s bound on connecting, and that its error names the step
/// under way.
#[tokio::test(start_paused = true)]
async fn connecting_is_bounded_at_30_seconds_and_names_its_step() {
    let step = Mutex::new("reading the queue's ARN");
    let started = Instant::now();
    let err = bounded::<()>(&step, std::future::pending())
        .await
        .unwrap_err();
    assert_eq!(started.elapsed(), Duration::from_secs(30));
    let SqsError::Unavailable(message) = err else {
        panic!("{err:?}");
    };
    assert!(message.contains("reading the queue's ARN"), "{message}");
}

/// Moves tokio time in 100 ms steps, so each renewal tick runs at its own
/// instant.
async fn step(d: Duration) {
    let n = d.as_millis() / 100;
    for _ in 0..n {
        tokio::time::advance(Duration::from_millis(100)).await;
        settle_tasks().await;
    }
}

/// Pins that a unit handed out with one renewal period left is kept when a
/// tick falls between the hand-out and the reader's first renew.
#[tokio::test(start_paused = true)]
async fn a_unit_handed_out_before_a_tick_is_kept_by_its_first_renew() {
    let (feed, queue) = start(MINUTE); // ticks near 0, 20, 40, 60, 80
    let (waker, _park) = wake_channel();
    feed.set_waker(waker);
    settle_tasks().await;
    step(Duration::from_millis(19_500)).await;
    queue.ready(Message::new("u"));
    assert!(feed.receive(1).unwrap().is_empty());
    until("buffered", || feed.buffered() == 1).await; // deadline near 79.5 s
    step(Duration::from_secs(40)).await; // t near 59.5 s, 20 s left
    let got = feed.receive(1).unwrap();
    assert_eq!(
        got.len(),
        1,
        "a unit one period from its deadline is handed out"
    );
    let d = &got[0];
    let deadline = feed.deadline_of(d.receipt).unwrap();
    step(Duration::from_millis(600)).await; // the tick near 60 s runs first
    let renewed_at = Instant::now();
    feed.renew(d.receipt).unwrap();
    step(Duration::from_secs(21)).await;
    let renew = feed.renew(d.receipt).map_err(|e| e.kind);
    assert_eq!(
        renew,
        Ok(()),
        "renewed {:?} before its deadline, never extended: {:?}",
        deadline - renewed_at,
        queue.visibility()
    );
}

/// Pins the drop rule at its boundary: a buffered unit exactly one renewal
/// period from its deadline is handed out, and one a millisecond closer is
/// dropped unread.
#[tokio::test(start_paused = true)]
async fn a_buffered_unit_under_one_period_from_its_deadline_is_dropped_unread() {
    let period = MINUTE / 3;
    for (left, handed_out) in [(period, true), (period - Duration::from_millis(1), false)] {
        let (feed, queue) = start(MINUTE);
        let (waker, _park) = wake_channel();
        feed.set_waker(waker);
        queue.ready(Message::new("u"));
        assert!(feed.receive(1).unwrap().is_empty());
        until("the unit is buffered", || feed.buffered() == 1).await;
        let deadline = feed.lock().held.values().next().unwrap().deadline;
        tokio::time::advance(deadline - left - Instant::now()).await;
        let got = feed.receive(1).unwrap();
        assert_eq!(got.len() == 1, handed_out, "{left:?} from its deadline");
        assert_eq!(feed.buffered(), 0);
    }
}

/// Pins the first deadline to when the receive was sent, so a unit that
/// arrives at the end of a long poll is not held locally past SQS's lapse.
#[tokio::test(start_paused = true)]
async fn the_first_deadline_counts_from_when_the_receive_was_sent() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.answer_at_end_of_poll();
    queue.ready(Message::new("late"));
    // The poll ends on the first tick, which would extend the buffered unit.
    queue.fail_next_visibility_call(CallError::retryable("InternalError"));
    let sent = Instant::now();
    assert!(feed.receive(1).unwrap().is_empty());
    until("the poll is sent", || queue.receives().len() == 1).await;
    tokio::time::advance(Duration::from_secs(20)).await;
    until("the unit is buffered", || park.park(Duration::ZERO)).await;
    let got = feed.receive(1).unwrap();
    assert_eq!(feed.deadline_of(got[0].receipt), Some(sent + MINUTE));
}

/// Pins the `set_waker` contract: a receive that takes some units and
/// leaves others wakes the reader.
#[tokio::test(start_paused = true)]
async fn a_receive_that_leaves_units_buffered_wakes_the_reader() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("a"));
    queue.ready(Message::new("b"));
    assert!(feed.receive(2).unwrap().is_empty());
    until("both units are buffered", || feed.buffered() == 2).await;
    assert!(park.park(Duration::ZERO));
    assert_eq!(feed.receive(1).unwrap().len(), 1);
    assert!(
        park.park(Duration::ZERO),
        "a receive that left a unit buffered did not wake the reader"
    );
}

/// Pins that an extension SQS refuses with any other code leaves the
/// receipt's deadline where it was, so the unit lapses at it.
#[tokio::test(start_paused = true)]
async fn a_renewal_entry_refused_with_another_code_keeps_its_deadline() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let delivery = receive_n(&feed, &park, 1).await.remove(0);
    let deadline = feed.deadline_of(delivery.receipt).unwrap();
    feed.renew(delivery.receipt).unwrap();
    queue.fail_next_visibility(vec![failure(0, "InvalidParameterValue")]);
    tokio::time::advance(Duration::from_secs(20)).await;
    until("the tick sends the extension", || {
        queue.visibility().len() == 1
    })
    .await;
    settle_tasks().await;
    assert_eq!(feed.deadline_of(delivery.receipt), Some(deadline));
    feed.renew(delivery.receipt).unwrap();
    tokio::time::advance(MINUTE).await;
    assert_eq!(
        feed.renew(delivery.receipt).unwrap_err().kind,
        FeedErrorKind::Gone
    );
}

/// Pins that a receipt marked before its deadline, whose next tick runs only
/// after the deadline, is not extended and reads as Gone.
#[tokio::test(start_paused = true)]
async fn a_marked_receipt_past_its_deadline_is_not_extended() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let d = receive_n(&feed, &park, 1).await.remove(0);
    settle_tasks().await;
    feed.renew(d.receipt).unwrap();
    // One jump past the deadline: the next tick runs after it.
    tokio::time::advance(MINUTE).await;
    settle_tasks().await;
    assert!(
        queue.visibility().is_empty(),
        "a lapsed receipt was extended: {:?}",
        queue.visibility()
    );
    assert_eq!(feed.renew(d.receipt).unwrap_err().kind, FeedErrorKind::Gone);
}

/// Pins that a publish retrying a failed send stops once another task stops
/// the feed on a Fatal.
#[tokio::test(start_paused = true)]
async fn a_publish_retry_stops_once_the_feed_stops() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let d = receive_n(&feed, &park, 1).await.remove(0);
    queue.fail_next_send_call(CallError::retryable("throttled"));
    let mut published = feed.publish(Unit::new("p", "body"));
    until("the first send fails", || queue.sends().len() == 1).await;
    queue.fail_next_delete_call(CallError::fatal("AccessDenied"));
    feed.acknowledge(d.receipt).unwrap();
    until("the publish is rejected", || {
        published.try_outcome().is_some()
    })
    .await;
    tokio::time::advance(Duration::from_secs(10)).await;
    settle_tasks().await;
    assert_eq!(
        queue.sends().len(),
        1,
        "the publish task sent again after the feed stopped"
    );
}

/// Pins that a fail call is bounded by the time left to the earliest
/// deadline in its batch, and that an extension call is not bounded.
#[tokio::test(start_paused = true)]
async fn a_fail_call_is_bounded_by_the_earliest_deadline_in_its_batch() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("a"));
    queue.ready(Message::new("b"));
    let got = receive_n(&feed, &park, 2).await;
    // The first tick extends both units, as handed out. Only `a` is renewed
    // for the second, so `b` holds until an earlier deadline.
    tokio::time::advance(Duration::from_secs(20)).await;
    until("the tick extends both units", || {
        queue.visibility().len() == 1
    })
    .await;
    feed.renew(got[0].receipt).unwrap();
    tokio::time::advance(Duration::from_secs(20)).await;
    until("the tick extends `a`", || queue.visibility().len() == 2).await;
    settle_tasks().await;
    let a = feed.deadline_of(got[0].receipt).unwrap();
    let b = feed.deadline_of(got[1].receipt).unwrap();
    assert!(b < a, "`b` holds until {b:?}, `a` until {a:?}");
    tokio::time::advance(Duration::from_secs(5)).await;
    let failed_at = Instant::now();
    feed.fail(got[0].receipt, "unreadable").unwrap();
    feed.fail(got[1].receipt, "unreadable").unwrap();
    until("the fails are sent", || queue.visibility().len() == 3).await;
    assert_eq!(queue.visibility()[2].len(), 2, "one batch");
    assert_eq!(queue.visibility_within(), [None, None, Some(b - failed_at)]);
}

/// Pins that a send failed Fatal after the same batch failed Retryable
/// rejects the batch as Retryable, since SQS may have stored the earlier
/// attempt.
#[tokio::test(start_paused = true)]
async fn a_send_retried_into_a_fatal_reads_retryable() {
    let (feed, queue) = start(MINUTE);
    queue.fail_next_send_call(CallError::retryable("read timeout"));
    queue.fail_next_send_call(CallError::fatal("AccessDenied"));
    let mut c = feed.publish(Unit::new("p", "body"));
    until("the first send", || queue.sends().len() == 1).await;
    tokio::time::advance(Duration::from_millis(200)).await;
    until("the publish settles", || c.try_outcome().is_some()).await;
    let kind = c.try_outcome().unwrap().unwrap_err().kind;
    assert_eq!(queue.sends().len(), 2);
    assert_eq!(
        kind,
        FeedErrorKind::Retryable,
        "a batch SQS may have stored"
    );
}

/// Pins that a batch whose first send fails Fatal is rejected as Fatal.
#[tokio::test(start_paused = true)]
async fn a_send_fatal_on_its_first_attempt_reads_fatal() {
    let (feed, queue) = start(MINUTE);
    queue.fail_next_send_call(CallError::fatal("AccessDenied"));
    let mut c = feed.publish(Unit::new("p", "body"));
    until("the publish settles", || c.try_outcome().is_some()).await;
    assert_eq!(queue.sends().len(), 1);
    assert_eq!(
        c.try_outcome().unwrap().unwrap_err().kind,
        FeedErrorKind::Fatal
    );
}

/// Pins that a retried fail call is bounded by the time left when the retry
/// is sent.
#[tokio::test(start_paused = true)]
async fn a_retried_fail_call_is_bounded_by_the_time_left_at_the_retry() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let d = receive_n(&feed, &park, 1).await.remove(0);
    let deadline = feed.deadline_of(d.receipt).unwrap();
    queue.fail_next_visibility_call(CallError::retryable("throttled"));
    feed.fail(d.receipt, "unreadable").unwrap();
    until("the first fail call", || queue.visibility().len() == 1).await;
    tokio::time::advance(Duration::from_millis(200)).await;
    until("the retry", || queue.visibility().len() == 2).await;
    let retried_at = Instant::now();
    assert_eq!(
        queue.visibility_within(),
        [
            Some(deadline - (retried_at - Duration::from_millis(200))),
            Some(deadline - retried_at)
        ]
    );
}

/// Pins that a send attempt that times out, then one denied with a 403,
/// rejects the batch as Retryable, since SQS may have stored the first.
#[tokio::test]
async fn a_send_timed_out_then_denied_reads_retryable() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let conns = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&conns);
    tokio::spawn(async move {
        while let Ok((mut tcp, _)) = listener.accept().await {
            let n = seen.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                if n == 0 {
                    let _ = tokio::io::copy(&mut tcp, &mut tokio::io::sink()).await;
                    return;
                }
                let mut buf = vec![0u8; 65536];
                let _ = tcp.read(&mut buf).await;
                let body = r#"{"__type":"com.amazon.coral.service#AccessDeniedException","message":"denied"}"#;
                let reply = format!(
                    "HTTP/1.1 403 Forbidden\r\nContent-Type: application/x-amz-json-1.0\r\n\
                     x-amzn-query-error: AccessDenied;Sender\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                );
                let _ = tcp.write_all(reply.as_bytes()).await;
                let _ = tokio::io::copy(&mut tcp, &mut tokio::io::sink()).await;
            });
        }
    });
    let endpoint = format!("http://{addr}");
    let cfg = SqsFeedConfig {
        queue_url: format!("{endpoint}/000000000000/jobs"),
        visibility: MINUTE,
        region: Some("eu-west-2".to_owned()),
        endpoint: Some(endpoint),
    };
    let credentials = aws_sdk_sqs::config::Credentials::new("x", "x", None, None, "test");
    let step = std::sync::Mutex::new("starting");
    let queue = crate::client::sdk_queue(
        &cfg,
        Some(aws_sdk_sqs::config::SharedCredentialsProvider::new(
            credentials,
        )),
        &step,
    )
    .await
    .unwrap();
    // Paused only now: building the client reads the trust store on a
    // blocking thread.
    tokio::time::pause();
    let queue = Arc::new(queue);
    let feed = SqsFeed::start(
        Arc::clone(&queue) as Arc<dyn Queue>,
        queue,
        DEAD_URL.to_owned(),
        &cfg,
        &tokio::runtime::Handle::current(),
    );
    let c = feed.publish(Unit::new("p", "body"));
    let e = tokio::time::timeout(MINUTE, c).await.unwrap().unwrap_err();
    assert_eq!(conns.load(Ordering::SeqCst), 2, "{e}");
    assert_eq!(e.kind, FeedErrorKind::Retryable, "{e}");
}

/// Pins that a batch-shape failure after a retried send keeps Refused.
#[tokio::test(start_paused = true)]
async fn a_batch_shape_failure_after_a_retried_send_reads_refused() {
    let (feed, queue) = start(MINUTE);
    queue.fail_next_send_call(CallError::retryable("read timeout"));
    queue.fail_next_send_call(CallError::batch_shape("BatchRequestTooLong"));
    let mut c = feed.publish(Unit::new("p", "body"));
    until("the first send", || queue.sends().len() == 1).await;
    tokio::time::advance(Duration::from_millis(200)).await;
    until("the publish settles", || c.try_outcome().is_some()).await;
    assert_eq!(queue.sends().len(), 2);
    assert_eq!(
        c.try_outcome().unwrap().unwrap_err().kind,
        FeedErrorKind::Refused
    );
}

/// Answers every read of a queue with a redrive policy, except a read of the
/// queue at `url`, which never answers.
#[derive(Debug)]
struct Hangs {
    url: &'static str,
}

impl startup::QueueAttributes for Hangs {
    fn attributes<'a>(
        &'a self,
        queue_url: &'a str,
        _names: &'static [&'static str],
    ) -> BoxFuture<'a, Result<HashMap<String, String>, CallError>> {
        let hang = queue_url == self.url;
        Box::pin(async move {
            if hang {
                std::future::pending::<()>().await;
            }
            Ok(HashMap::from([
                (
                    "QueueArn".to_owned(),
                    "arn:aws:sqs:x:000000000000:jobs".to_owned(),
                ),
                (
                    "RedrivePolicy".to_owned(),
                    r#"{"deadLetterTargetArn":"arn:aws:sqs:x:000000000000:jobs-dlq","maxReceiveCount":5}"#
                        .to_owned(),
                ),
            ]))
        })
    }
}

/// Pins that the connect bound names the read it was on, for the queue and
/// for its dead-letter queue.
#[tokio::test(start_paused = true)]
async fn the_connect_bound_names_the_read_it_was_on() {
    for (url, step) in [
        (
            "http://h/000000000000/jobs",
            "while reading the queue's attributes",
        ),
        (
            "http://h/000000000000/jobs-dlq",
            "while reading the dead-letter queue's attributes",
        ),
    ] {
        let current = Mutex::new("starting");
        let queue = Hangs { url };
        let checks = startup::check(&queue, "http://h/000000000000/jobs", &current);
        match bounded(&current, checks).await {
            Err(SqsError::Unavailable(message)) => assert!(message.ends_with(step), "{message}"),
            other => panic!("expected unavailable, got {other:?}"),
        }
    }
}

/// Buffer `taken` and `waiting`, and hand out `taken` (handle `h0`), leaving
/// `waiting` (handle `h1`) buffered.
async fn one_taken_one_buffered(feed: &SqsFeed, queue: &ScriptedQueue) -> Delivery {
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("taken"));
    queue.ready(Message::new("waiting"));
    assert!(feed.receive(2).unwrap().is_empty());
    until("both units are buffered", || feed.buffered() == 2).await;
    assert!(park.park(Duration::ZERO));
    feed.receive(1).unwrap().remove(0)
}

fn extended_handles(calls: &[Vec<VisibilityEntry>]) -> Vec<String> {
    calls.iter().flatten().map(|e| e.handle.clone()).collect()
}

/// Pins that a unit received for the reader but not yet handed out is
/// extended on every tick while the reader is active, so it outlives its
/// first visibility. Regression for #1276.
#[tokio::test(start_paused = true)]
async fn a_buffered_unit_is_extended_until_the_reader_takes_it() {
    let (feed, queue) = start(MINUTE);
    let taken = one_taken_one_buffered(&feed, &queue).await;
    for _ in 0..4 {
        feed.renew(taken.receipt).unwrap();
        step(Duration::from_secs(20)).await;
    }
    let extended = extended_handles(&queue.visibility());
    assert!(
        extended.iter().any(|h| h == "h1"),
        "the buffered unit was never extended: {extended:?}"
    );
    let got = feed.receive(1).unwrap();
    assert_eq!(
        got.len(),
        1,
        "the buffered unit lapsed before the reader took it"
    );
}

/// Pins that a buffered unit is not extended once the reader stops calling
/// the feed, so a stalled worker loses it one visibility period after its
/// last extension.
#[tokio::test(start_paused = true)]
async fn a_buffered_unit_is_not_extended_once_the_reader_stops_calling() {
    let (feed, queue) = start(MINUTE);
    one_taken_one_buffered(&feed, &queue).await;
    step(Duration::from_secs(20)).await;
    until("the tick after the last call runs", || {
        !queue.visibility().is_empty()
    })
    .await;
    let quiet_from = queue.visibility().len();
    step(Duration::from_secs(80)).await;
    let later = extended_handles(&queue.visibility()[quiet_from..]);
    assert!(
        !later.iter().any(|h| h == "h1"),
        "extended for a reader that stopped calling: {later:?}"
    );
    assert!(
        feed.receive(1).unwrap().is_empty(),
        "the buffered unit outlived its deadline"
    );
}

/// Pins that a buffered unit whose extension SQS refuses as no longer held
/// is not extended again and is dropped unread at the next receive.
#[tokio::test(start_paused = true)]
async fn a_buffered_unit_the_queue_refuses_to_extend_is_dropped_unread() {
    let (feed, queue) = start(MINUTE);
    let taken = one_taken_one_buffered(&feed, &queue).await;
    feed.renew(taken.receipt).unwrap();
    // Entries go out sorted by receipt, so the buffered unit is entry 1.
    queue.fail_next_visibility(vec![failure(1, "ReceiptHandleIsInvalid")]);
    step(Duration::from_secs(20)).await;
    until("the tick extends the units", || {
        queue.visibility().len() == 1
    })
    .await;
    feed.renew(taken.receipt).unwrap();
    step(Duration::from_secs(20)).await;
    let later = extended_handles(&queue.visibility()[1..]);
    assert!(
        !later.iter().any(|h| h == "h1"),
        "a refused unit was extended again: {later:?}"
    );
    assert_eq!(
        feed.receive(1).unwrap().len(),
        0,
        "a gone unit was handed out"
    );
    assert_eq!(feed.buffered(), 0);
}

/// Pins that a reader calling only `receive`, with no unit to renew, keeps
/// its buffered units extended.
#[tokio::test(start_paused = true)]
async fn a_reader_that_only_receives_keeps_its_buffered_units() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    for i in 0..4 {
        queue.ready(Message::new(&format!("u{i}")));
    }
    assert!(feed.receive(4).unwrap().is_empty());
    until("the units are buffered", || feed.buffered() == 4).await;
    assert!(park.park(Duration::ZERO));
    for at in [0, 30, 60] {
        let got = feed.receive(1).unwrap();
        assert_eq!(got.len(), 1, "nothing handed out at {at} s");
        step(Duration::from_secs(30)).await;
    }
    assert_eq!(
        feed.receive(1).unwrap().len(),
        1,
        "the last buffered unit lapsed"
    );
}

/// Pins that a buffered unit whose extension call failed whole is extended
/// at the next tick, after the reader stopped calling the feed.
#[tokio::test(start_paused = true)]
async fn a_failed_extension_of_a_buffered_unit_is_sent_again_at_the_next_tick() {
    let (feed, queue) = start(MINUTE);
    let taken = one_taken_one_buffered(&feed, &queue).await;
    feed.renew(taken.receipt).unwrap();
    queue.fail_next_visibility_call(CallError::retryable("InternalError"));
    step(Duration::from_secs(20)).await;
    until("the first extension is sent", || {
        queue.visibility().len() == 1
    })
    .await;
    step(Duration::from_secs(20)).await;
    let retried = extended_handles(&queue.visibility()[1..]);
    assert!(
        retried.iter().any(|h| h == "h1"),
        "the failed extension was not sent again: {retried:?}"
    );
}

/// Pins that a `receive` answered with a transient receive failure counts as
/// the reader polling, so its buffered unit is still extended.
#[tokio::test(start_paused = true)]
async fn a_receive_answered_with_a_receive_failure_keeps_buffered_units() {
    let (feed, queue) = start(MINUTE); // ticks near 20, 40, 60, 80
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    assert!(feed.receive(2).unwrap().is_empty());
    until("the unit is buffered", || feed.buffered() == 1).await;
    assert!(park.park(Duration::ZERO));
    for _ in 0..2 {
        // The receive task's next poll fails, and the reader's next
        // `receive` answers with it, handing out nothing.
        queue.fail_next_receive(CallError::retryable("InternalError"));
        step(Duration::from_secs(25)).await;
        assert_eq!(feed.receive(1).unwrap_err().kind, FeedErrorKind::Retryable);
    }
    step(Duration::from_secs(35)).await; // t near 85 s
    assert_eq!(
        feed.receive(1).unwrap().len(),
        1,
        "the buffered unit lapsed while the reader polled"
    );
}

/// Read `of` through the feed and wait for the answer.
async fn depth(feed: &SqsFeed, of: DepthOf) -> Result<Depth, FeedError> {
    let mut reading = feed.read_depth(of).expect("the SQS feed reports depth");
    let mut outcome = None;
    until("the depth read settles", || {
        outcome = reading.try_outcome();
        outcome.is_some()
    })
    .await;
    outcome.unwrap()
}

/// Pins how each count maps: the queue's ready, delayed and not-visible
/// messages report as visible, delayed and in flight, each alone making the
/// depth non-empty, approximate, and named after the queue read.
#[tokio::test(start_paused = true)]
async fn each_count_alone_reads_as_not_empty() {
    let (feed, queue) = start(MINUTE);
    for (url, of) in [(QUEUE_URL, DepthOf::Feed), (DEAD_URL, DepthOf::DeadLetters)] {
        for (counts, field) in [
            ([3, 0, 0], "visible"),
            ([0, 4, 0], "delayed"),
            ([0, 0, 5], "in_flight"),
        ] {
            queue.set_counts(url, counts[0], counts[1], counts[2]);
            let d = depth(&feed, of).await.unwrap();
            assert!(!d.is_empty(), "{of:?} {field}");
            assert_eq!(
                (d.visible, d.delayed, d.in_flight),
                (counts[0], counts[1], counts[2])
            );
            assert!(d.approximate);
            assert!(d.what.contains(url), "{}", d.what);
        }
        queue.set_counts(url, 0, 0, 0);
    }
}

/// Pins that zero counts on both queues read as empty, and that the
/// dead-letter read asks the dead-letter queue's URL the feed was given at
/// connect, and the feed read the queue's own.
#[tokio::test(start_paused = true)]
async fn zero_counts_read_as_empty_from_each_queue() {
    let (feed, queue) = start(MINUTE);
    assert!(depth(&feed, DepthOf::Feed).await.unwrap().is_empty());
    assert!(depth(&feed, DepthOf::DeadLetters).await.unwrap().is_empty());
    assert_eq!(queue.attribute_reads(), vec![QUEUE_URL, DEAD_URL]);
}

/// Pins that a settled depth reading wakes the reader, so its park ends on
/// the answer.
#[tokio::test(start_paused = true)]
async fn a_settled_depth_read_wakes_the_reader() {
    let (feed, _queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    assert!(depth(&feed, DepthOf::Feed).await.unwrap().is_empty());
    assert!(
        park.park(Duration::ZERO),
        "the answer did not wake the reader"
    );
}

/// Pins that a depth reading and its error name the queues with the URL's
/// userinfo redacted.
#[tokio::test(start_paused = true)]
async fn depth_text_carries_no_queue_url_userinfo() {
    const SECRET_URL: &str = "https://ops:hunter2@sqs.eu-west-2.amazonaws.com/111122223333/jobs";
    let section: FeedSection =
        serde_yaml::from_str(&format!("sqs: {{ queue_url: \"{SECRET_URL}\" }}")).unwrap();
    let config = SqsFeedConfig::from_section(&section).expect("the section is accepted");
    let source = url::Url::parse(&config.queue_url).unwrap();
    let dead = startup::dead_letter_url(&source, "arn:aws:sqs:eu-west-2:111122223333:jobs-dlq")
        .unwrap()
        .to_string();
    let queue = Arc::new(ScriptedQueue::default());
    let feed = SqsFeed::start(
        Arc::clone(&queue) as Arc<dyn Queue>,
        Arc::clone(&queue) as Arc<dyn startup::QueueAttributes>,
        dead.clone(),
        &config,
        &tokio::runtime::Handle::current(),
    );
    let feed_depth = depth(&feed, DepthOf::Feed).await.unwrap();
    let dead_depth = depth(&feed, DepthOf::DeadLetters).await.unwrap();
    queue.fail_attributes(&dead, CallError::fatal("AccessDenied"));
    let e = depth(&feed, DepthOf::DeadLetters).await.unwrap_err();
    for text in [
        feed_depth.to_string(),
        dead_depth.to_string(),
        e.to_string(),
    ] {
        assert!(
            !text.contains("hunter2"),
            "credential in depth text: {text}"
        );
        assert!(text.contains("sqs.eu-west-2.amazonaws.com"), "{text}");
    }
}

/// Pins how a failed read of the counts maps: a fatal class reads as Fatal,
/// a retryable one as Retryable, each naming the queue.
#[tokio::test(start_paused = true)]
async fn a_failed_count_read_keeps_its_class() {
    let (feed, queue) = start(MINUTE);
    queue.fail_attributes(DEAD_URL, CallError::fatal("AccessDenied"));
    let e = depth(&feed, DepthOf::DeadLetters).await.unwrap_err();
    assert_eq!(e.kind, FeedErrorKind::Fatal);
    assert!(
        e.reason.contains("AccessDenied") && e.reason.contains(DEAD_URL),
        "{e}"
    );
    queue.fail_attributes(QUEUE_URL, CallError::retryable("throttled"));
    let e = depth(&feed, DepthOf::Feed).await.unwrap_err();
    assert_eq!(e.kind, FeedErrorKind::Retryable);
    assert!(
        e.reason.contains("throttled") && e.reason.contains(QUEUE_URL),
        "{e}"
    );
}

/// Pins that an acknowledge and a fail the feed accepted and has not yet
/// sent count in `unsent`, queued and while the delete is retried, and leave
/// it once sent; the dead-letter read counts none.
#[tokio::test(start_paused = true)]
async fn an_unsent_delete_reports_in_unsent() {
    let (feed, queue) = start_gated(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.inner.ready(Message::new("a"));
    queue.inner.ready(Message::new("b"));
    let got = receive_n(&feed, &park, 2).await;
    queue.delete_down.store(true, Ordering::SeqCst);
    feed.acknowledge(got[0].receipt).unwrap();
    feed.fail(got[1].receipt, "unreadable").unwrap();
    assert_eq!(
        depth(&feed, DepthOf::Feed).await.unwrap().unsent,
        2,
        "queued"
    );
    settle_tasks().await;
    assert_eq!(
        depth(&feed, DepthOf::Feed).await.unwrap().unsent,
        2,
        "being sent"
    );
    assert_eq!(depth(&feed, DepthOf::DeadLetters).await.unwrap().unsent, 0);
    queue.delete_down.store(false, Ordering::SeqCst);
    for _ in 0..10 {
        tokio::time::advance(Duration::from_secs(1)).await;
        settle_tasks().await;
    }
    assert_eq!(
        queue.inner.deletes().len(),
        1,
        "the delete reached the queue"
    );
    assert_eq!(depth(&feed, DepthOf::Feed).await.unwrap().unsent, 0);
}

/// Pins that the settle task's batch counts in `unsent` while a fail in it
/// is retried, after the delete taken beside it has been sent.
#[tokio::test(start_paused = true)]
async fn a_fail_being_retried_reports_in_unsent() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("a"));
    queue.ready(Message::new("b"));
    let got = receive_n(&feed, &park, 2).await;
    queue.fail_next_visibility_call(CallError::retryable("InternalError"));
    feed.acknowledge(got[0].receipt).unwrap();
    feed.fail(got[1].receipt, "unreadable").unwrap();
    settle_tasks().await;
    assert_eq!(queue.deletes().len(), 1, "the delete was sent");
    assert_eq!(queue.visibility().len(), 1, "the fail was tried once");
    assert_eq!(
        depth(&feed, DepthOf::Feed).await.unwrap().unsent,
        2,
        "the batch holding the fail"
    );
    for _ in 0..10 {
        tokio::time::advance(Duration::from_secs(1)).await;
        settle_tasks().await;
    }
    assert_eq!(queue.visibility().len(), 2, "the fail was sent again");
    assert_eq!(depth(&feed, DepthOf::Feed).await.unwrap().unsent, 0);
}

/// Answers every attribute read without the message counts.
#[derive(Debug)]
struct Uncounted;

impl startup::QueueAttributes for Uncounted {
    fn attributes<'a>(
        &'a self,
        _queue_url: &'a str,
        _names: &'static [&'static str],
    ) -> BoxFuture<'a, Result<HashMap<String, String>, CallError>> {
        Box::pin(async { Ok(HashMap::new()) })
    }
}

/// Pins that a depth read whose reply lacks a count reads as Fatal, naming
/// the count and the queue.
#[tokio::test(start_paused = true)]
async fn a_reply_without_the_counts_reads_as_fatal() {
    let feed = SqsFeed::start(
        Arc::new(ScriptedQueue::default()) as Arc<dyn Queue>,
        Arc::new(Uncounted),
        DEAD_URL.to_owned(),
        &config(MINUTE),
        &tokio::runtime::Handle::current(),
    );
    for (of, url) in [(DepthOf::Feed, QUEUE_URL), (DepthOf::DeadLetters, DEAD_URL)] {
        let e = depth(&feed, of).await.unwrap_err();
        assert_eq!(e.kind, FeedErrorKind::Fatal, "{of:?}");
        assert!(
            e.reason
                .contains("reported no `ApproximateNumberOfMessages`")
                && e.reason.contains(url),
            "{e}"
        );
    }
}
