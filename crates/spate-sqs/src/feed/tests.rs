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
/// cleared, whose visibility calls can be held unanswered, and which records
/// when each fail and delete was sent.
#[derive(Debug, Default)]
struct Gated {
    inner: ScriptedQueue,
    /// Fail every `SendMessageBatch` as Retryable.
    send_down: AtomicBool,
    /// Fail every `DeleteMessageBatch` as Retryable.
    delete_down: AtomicBool,
    /// Fail every `DeleteMessageBatch` as Fatal.
    delete_fatal: AtomicBool,
    /// Hold each `ChangeMessageVisibilityBatch`, once recorded, until
    /// [`release_visibility`](Gated::release_visibility), or until its
    /// `within` passes, when it fails as Retryable as an SDK timeout does.
    visibility_held: AtomicBool,
    release: Notify,
    visibility_at: Mutex<Vec<Instant>>,
    deletes_at: Mutex<Vec<Instant>>,
}

impl Gated {
    fn hold_visibility(&self) {
        self.visibility_held.store(true, Ordering::SeqCst);
    }

    /// Answer every held call, and hold no later one.
    fn release_visibility(&self) {
        self.visibility_held.store(false, Ordering::SeqCst);
        self.release.notify_waiters();
    }
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
        let held = self.visibility_held.load(Ordering::SeqCst);
        let call = self.inner.change_visibility_batch(e, within);
        Box::pin(async move {
            let outcome = call.await;
            if held {
                let release = self.release.notified();
                match within {
                    Some(within) => {
                        if tokio::time::timeout(within, release).await.is_err() {
                            return Err(CallError::retryable("operation timeout"));
                        }
                    }
                    None => release.await,
                }
            }
            outcome
        })
    }

    fn delete_batch(
        &self,
        e: Vec<DeleteEntry>,
        within: Option<Duration>,
    ) -> BoxFuture<'_, Result<BatchOutcome, CallError>> {
        self.deletes_at.lock().unwrap().push(Instant::now());
        if self.delete_fatal.load(Ordering::SeqCst) {
            return Box::pin(async { Err(CallError::fatal("AccessDenied")) });
        }
        if self.delete_down.load(Ordering::SeqCst) {
            return Box::pin(async { Err(CallError::retryable("InternalError")) });
        }
        self.inner.delete_batch(e, within)
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

/// Renew each of `receipts` every 100 ms for `d`, asserting every renew is
/// accepted.
async fn renew_through(feed: &SqsFeed, receipts: &[Receipt], d: Duration, what: &str) {
    let start = Instant::now();
    for _ in 0..d.as_millis() / 100 {
        for &receipt in receipts {
            assert_eq!(
                feed.renew(receipt).map_err(|e| e.kind),
                Ok(()),
                "{what}: the unit lapsed {:?} in, renewed every 100 ms",
                Instant::now() - start
            );
        }
        step(Duration::from_millis(100)).await;
    }
}

/// Pins that a held unit the reader keeps renewing survives one extension
/// call SQS does not answer, at visibilities either side of the 45 s edge:
/// the call is given up after one renewal period and sent once more at once,
/// with half the time left, and the next tick, with three periods left, is
/// bounded by one period. Regression for #1446.
#[tokio::test(start_paused = true)]
async fn a_renewed_unit_survives_one_slow_extension_call() {
    let ms = Duration::from_millis;
    for (visibility, first, second) in [
        (3, ms(1_000), ms(500)),
        (30, ms(10_000), ms(5_000)),
        (45, ms(15_000), ms(7_500)),
        (60, ms(20_000), ms(10_000)),
    ] {
        let visibility = Duration::from_secs(visibility);
        let (feed, queue) = start(visibility);
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        queue.ready(Message::new("u"));
        let held = receive_n(&feed, &park, 1).await.remove(0);
        queue.delay_next_visibility_call(MINUTE);
        renew_through(
            &feed,
            &[held.receipt],
            2 * visibility,
            &format!("visibility {visibility:?}"),
        )
        .await;
        assert_eq!(
            queue.visibility_within()[..3],
            [Some(first), Some(second), Some(first)],
            "visibility {visibility:?}"
        );
    }
}

/// Pins the running bound of an extension call: one renewal period, or half
/// the time left to the batch's earliest deadline when that is shorter.
/// Regression for #1446.
#[tokio::test(start_paused = true)]
async fn a_running_extension_is_bounded_by_the_period_and_the_time_left() {
    let secs = Duration::from_secs;
    // The 12 h hold ends 12 h − 30 s after the receive, so 8 h − 30 s is left
    // at the first tick.
    for (visibility, within) in [
        (secs(3), secs(1)),
        (MINUTE, secs(20)),
        (HOLD, HOLD / 3 - secs(15)),
    ] {
        let (feed, queue) = start(visibility);
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        queue.ready(Message::new("u"));
        let held = receive_n(&feed, &park, 1).await.remove(0);
        feed.renew(held.receipt).unwrap();
        tokio::time::advance(visibility / 3).await;
        until("the tick extends the unit", || {
            queue.visibility().len() == 1
        })
        .await;
        assert_eq!(
            queue.visibility_within(),
            [Some(within)],
            "visibility {visibility:?}"
        );
    }

    let (feed, queue) = start(MINUTE);
    let started = Instant::now();
    let held = hand_out_one_period_from_its_deadline(&feed, &queue).await;
    let deadline = feed.deadline_of(held.receipt).unwrap();
    let calls = queue.visibility_within().len();
    step(Duration::from_millis(600)).await;
    let tick = started + MINUTE;
    assert_eq!(
        queue.visibility_within()[calls..],
        [Some((deadline - tick) / 2)],
        "the tick near 60 s, {:?} before the deadline",
        deadline - tick
    );
}

/// Buffer one unit about 19.5 s into a feed at a one-minute visibility, and
/// hand it out at about 59.5 s, one renewal period from its deadline and
/// just before the tick near 60 s.
async fn hand_out_one_period_from_its_deadline(feed: &SqsFeed, queue: &ScriptedQueue) -> Delivery {
    let (waker, _park) = wake_channel();
    feed.set_waker(waker);
    settle_tasks().await;
    step(Duration::from_millis(19_500)).await;
    queue.ready(Message::new("u"));
    assert!(feed.receive(1).unwrap().is_empty());
    until("buffered", || feed.buffered() == 1).await;
    step(Duration::from_secs(40)).await;
    let mut got = feed.receive(1).unwrap();
    assert_eq!(
        got.len(),
        1,
        "a unit one period from its deadline is handed out"
    );
    got.remove(0)
}

/// Pins that a unit handed out with one renewal period left survives one
/// extension call SQS does not answer at the tick that follows.
/// Regression for #1446.
#[tokio::test(start_paused = true)]
async fn a_unit_handed_out_one_period_from_its_deadline_survives_one_slow_extension_call() {
    let (feed, queue) = start(MINUTE);
    let held = hand_out_one_period_from_its_deadline(&feed, &queue).await;
    queue.delay_next_visibility_call(MINUTE);
    renew_through(&feed, &[held.receipt], 2 * MINUTE, "handed out").await;
}

/// Pins that a batch is sent at most twice in a tick, so a call SQS keeps
/// failing does not loop on ever shorter bounds until its unit lapses.
#[tokio::test(start_paused = true)]
async fn a_batch_is_sent_at_most_twice_in_a_tick() {
    let (feed, queue) = start(MINUTE);
    let held = hand_out_one_period_from_its_deadline(&feed, &queue).await;
    feed.renew(held.receipt).unwrap();
    let calls = queue.visibility_within().len();
    for _ in 0..5 {
        queue.delay_next_visibility_call(MINUTE);
    }
    step(Duration::from_secs(19)).await;
    assert_eq!(
        queue.visibility_within()[calls..].len(),
        2,
        "{:?}",
        queue.visibility_within()
    );
}

/// Pins that the units of a later chunk survive a slow call for an earlier
/// chunk in the same tick, and a slow call of their own.
/// Regression for #1446.
#[tokio::test(start_paused = true)]
async fn units_after_a_slow_chunk_survive_it() {
    let visibility = Duration::from_secs(30);
    let (feed, queue) = start(visibility);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    for i in 0..21 {
        queue.ready(Message::new(&format!("u{i}")));
    }
    let got = receive_n(&feed, &park, 21).await;
    let receipts: Vec<Receipt> = got.iter().map(|d| d.receipt).collect();
    queue.delay_next_visibility_call(MINUTE);
    queue.delay_next_visibility_call(Duration::ZERO);
    queue.delay_next_visibility_call(MINUTE);
    renew_through(&feed, &receipts, 2 * visibility, "21 units").await;
}

/// Pins that a unit one renewal period from its deadline at a tick survives
/// one extension call SQS does not answer when another batch of the same
/// tick takes most of a period to answer, since a batch is sent once more as
/// soon as its own call is given up.
#[tokio::test(start_paused = true)]
async fn a_unit_survives_one_unanswered_call_beside_a_slow_batch() {
    let (feed, queue) = start(MINUTE); // ticks near 20, 40, 60, 80
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    settle_tasks().await;
    step(Duration::from_millis(19_500)).await;
    queue.ready(Message::new("u"));
    assert!(feed.receive(1).unwrap().is_empty());
    until("buffered", || feed.buffered() == 1).await; // deadline near 79.5 s
    // SQS refuses the extension of `u` at the tick near 20 s, so its
    // deadline stays near 79.5 s.
    queue.fail_next_visibility(vec![failure(0, "InvalidParameterValue")]);
    step(Duration::from_secs(40)).await; // t near 59.5 s, 20 s left
    let mut held: Vec<Receipt> = feed.receive(1).unwrap().iter().map(|d| d.receipt).collect();
    assert_eq!(held.len(), 1);
    for i in 0..10 {
        queue.ready(Message::new(&format!("f{i}")));
    }
    held.extend(receive_n(&feed, &park, 10).await.iter().map(|d| d.receipt));
    // At the tick near 60 s, SQS does not answer the call for the batch
    // holding `u`, and answers the other batch's call after 19.9 s.
    queue.delay_next_visibility_call(MINUTE);
    queue.delay_next_visibility_call(Duration::from_millis(19_900));
    renew_through(&feed, &held, Duration::from_secs(40), "beside a slow batch").await;
}

/// Pins that every chunk of a tick survives extension calls that each take
/// most of a renewal period, since no chunk waits behind another.
#[tokio::test(start_paused = true)]
async fn every_chunk_survives_uniformly_slow_extension_calls() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    for i in 0..11 {
        queue.ready(Message::new(&format!("u{i}")));
    }
    let got = receive_n(&feed, &park, 11).await;
    let receipts: Vec<Receipt> = got.iter().map(|d| d.receipt).collect();
    for _ in 0..64 {
        queue.delay_next_visibility_call(Duration::from_secs(15));
    }
    renew_through(&feed, &receipts, 3 * MINUTE, "11 units, 15 s calls").await;
}

/// Pins that a buffered unit survives one extension call SQS does not answer
/// and is still handed out, since the call sent once more keeps it one
/// renewal period from its deadline. Regression for #1446.
#[tokio::test(start_paused = true)]
async fn a_buffered_unit_survives_one_slow_extension_call() {
    let (feed, queue) = start(Duration::from_secs(30));
    let taken = one_taken_one_buffered(&feed, &queue).await;
    queue.delay_next_visibility_call(MINUTE);
    renew_through(&feed, &[taken.receipt], Duration::from_secs(25), "taken").await;
    let got = feed.receive(1).unwrap();
    assert_eq!(
        got.iter().map(|d| d.unit.id.as_str()).collect::<Vec<_>>(),
        ["u1"],
        "the buffered unit is handed out at 25 s: {:?}",
        queue.visibility_within()
    );
}

/// Pins that an extension answered just before its bound counts from when
/// it was sent, as SQS counts it.
#[tokio::test(start_paused = true)]
async fn an_extension_answered_just_before_its_bound_counts_from_when_it_was_sent() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let started = Instant::now();
    let held = receive_n(&feed, &park, 1).await.remove(0);
    queue.delay_next_visibility_call(Duration::from_millis(19_900));
    step(Duration::from_millis(39_900)).await;
    assert_eq!(queue.visibility().len(), 1, "one extension, answered");
    assert_eq!(
        feed.deadline_of(held.receipt),
        Some(started + Duration::from_secs(20) + MINUTE)
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
/// deadline in its batch, and that an extension call carries the running
/// bound.
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
    assert_eq!(
        queue.visibility_within(),
        [
            Some(Duration::from_secs(20)),
            Some(Duration::from_secs(20)),
            Some(b - failed_at)
        ]
    );
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

/// The handles of the entries set visible at once, in call order.
fn handed_back(calls: &[Vec<VisibilityEntry>]) -> Vec<String> {
    calls
        .iter()
        .flatten()
        .filter(|e| e.seconds == 0)
        .map(|e| e.handle.clone())
        .collect()
}

/// The handles of the entries extended, in call order.
fn extended(calls: &[Vec<VisibilityEntry>]) -> Vec<String> {
    calls
        .iter()
        .flatten()
        .filter(|e| e.seconds > 0)
        .map(|e| e.handle.clone())
        .collect()
}

/// Pins that the feed reports it hands back, which turns on the reserve
/// check at startup and the read to the reserve at a stop.
#[tokio::test(start_paused = true)]
async fn an_sqs_feed_hands_back() {
    let (feed, _queue) = start(MINUTE);
    assert!(feed.hands_back());
}

/// Pins that a handed-back unit is set visible at once, with no tick, bounded
/// by one renewal period or its deadline, and that its receipt is gone
/// afterwards.
#[tokio::test(start_paused = true)]
async fn a_handed_back_unit_is_set_visible_at_once() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let d = receive_n(&feed, &park, 1).await.remove(0);
    feed.hand_back(d.receipt).unwrap();
    let at = Instant::now();
    settle_tasks().await;
    assert_eq!(Instant::now(), at, "no time moved");
    assert_eq!(handed_back(&queue.visibility()), ["h0"]);
    assert_eq!(queue.visibility().len(), 1, "one call");
    assert_eq!(queue.visibility_within(), [Some(MINUTE / 3)]);
    assert_eq!(feed.renew(d.receipt).unwrap_err().kind, FeedErrorKind::Gone);
    assert_eq!(
        feed.acknowledge(d.receipt).unwrap_err().kind,
        FeedErrorKind::Gone
    );
    assert_eq!(
        feed.hand_back(d.receipt).unwrap_err().kind,
        FeedErrorKind::Gone
    );
}

/// Pins the hand-back's deadline rule at its boundary: at the deadline the
/// receipt is gone and nothing is sent, and a second before it the unit is
/// set visible.
#[tokio::test(start_paused = true)]
async fn a_hand_back_at_its_deadline_is_gone_and_unsent() {
    for (before, sent) in [(Duration::ZERO, false), (Duration::from_secs(1), true)] {
        let (feed, queue) = start(MINUTE);
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        queue.ready(Message::new("u"));
        let d = receive_n(&feed, &park, 1).await.remove(0);
        // The tick at 20 s extends the unit, as handed out; no later tick
        // does.
        tokio::time::advance(Duration::from_secs(21)).await;
        until("the tick extends the unit", || {
            queue.visibility().len() == 1
        })
        .await;
        settle_tasks().await;
        let deadline = feed.deadline_of(d.receipt).unwrap();
        tokio::time::advance(deadline - before - Instant::now()).await;
        settle_tasks().await;
        let answer = feed.hand_back(d.receipt);
        settle_tasks().await;
        if sent {
            answer.unwrap();
            assert_eq!(handed_back(&queue.visibility()), ["h0"]);
        } else {
            assert_eq!(answer.unwrap_err().kind, FeedErrorKind::Gone);
            assert!(handed_back(&queue.visibility()).is_empty());
        }
    }
}

/// Pins that a hand-back of a receipt gone after a refused extension, of an
/// unknown receipt, and of a receipt still buffered is `Gone` and sends
/// nothing.
#[tokio::test(start_paused = true)]
async fn a_gone_unknown_or_buffered_receipt_handed_back_is_gone() {
    let (feed, queue) = start(MINUTE);
    let taken = one_taken_one_buffered(&feed, &queue).await;
    let buffered = Receipt(feed.lock().buffered[0]);
    queue.fail_next_visibility(vec![failure(0, "ReceiptHandleIsInvalid")]);
    step(Duration::from_secs(20)).await;
    until("the tick extends the units", || {
        queue.visibility().len() == 1
    })
    .await;
    for receipt in [taken.receipt, Receipt(999), buffered] {
        assert_eq!(
            feed.hand_back(receipt).unwrap_err().kind,
            FeedErrorKind::Gone,
            "{receipt}"
        );
    }
    settle_tasks().await;
    assert!(handed_back(&queue.visibility()).is_empty());
}

/// Pins that a hand-back whose unit's deadline comes while it waits behind
/// an extension call is dropped unsent, and one still before its deadline is
/// sent.
#[tokio::test(start_paused = true)]
async fn a_hand_back_whose_deadline_passes_while_queued_is_dropped_unsent() {
    let (feed, queue) = start_gated(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.inner.ready(Message::new("b"));
    let b = receive_n(&feed, &park, 1).await.remove(0);
    // SQS refuses the tick's extension of `b`, so `b` lapses at 60 s, before
    // the held extension of `a` and `c` below gives up at 61 s.
    queue
        .inner
        .fail_next_visibility(vec![failure(0, "InvalidParameterValue")]);
    tokio::time::advance(Duration::from_secs(21)).await;
    until("the tick tries to extend `b`", || {
        queue.inner.visibility().len() == 1
    })
    .await;
    tokio::time::advance(Duration::from_secs(5)).await;
    queue.inner.ready(Message::new("a"));
    queue.inner.ready(Message::new("c"));
    let got = receive_n(&feed, &park, 2).await;
    let c = got[1].receipt;
    queue.hold_visibility();
    step(Duration::from_secs(20)).await;
    until("the tick's extension of `a` is held", || {
        queue.inner.visibility().len() == 2
    })
    .await;
    let b_deadline = feed.deadline_of(b.receipt).unwrap();
    let c_deadline = feed.deadline_of(c).unwrap();
    assert!(b_deadline < c_deadline);
    feed.hand_back(b.receipt).unwrap();
    feed.hand_back(c).unwrap();
    step(b_deadline - Instant::now() + Duration::from_secs(2)).await;
    assert_eq!(handed_back(&queue.inner.visibility()), ["h2"]);
}

/// Pins that hand-backs queued together go out in one batch, in the order
/// they were queued.
#[tokio::test(start_paused = true)]
async fn hand_backs_queued_together_go_out_in_one_batch() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("a"));
    let a = receive_n(&feed, &park, 1).await.remove(0);
    tokio::time::advance(Duration::from_secs(10)).await;
    queue.ready(Message::new("b"));
    let b = receive_n(&feed, &park, 1).await.remove(0);
    feed.hand_back(b.receipt).unwrap();
    feed.hand_back(a.receipt).unwrap();
    settle_tasks().await;
    assert_eq!(queue.visibility().len(), 1, "one batch");
    assert_eq!(handed_back(&queue.visibility()), ["h1", "h0"]);
    assert_eq!(queue.visibility_within(), [Some(MINUTE / 3)]);
}

/// Pins that a hand-back entry SQS refuses is dropped, never sent again, and
/// leaves the feed working.
#[tokio::test(start_paused = true)]
async fn a_refused_hand_back_entry_is_dropped() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let d = receive_n(&feed, &park, 1).await.remove(0);
    queue.fail_next_visibility(vec![failure(
        0,
        "AWS.SimpleQueueService.MessageNotInflight",
    )]);
    feed.hand_back(d.receipt).unwrap();
    settle_tasks().await;
    step(Duration::from_secs(60)).await;
    assert_eq!(handed_back(&queue.visibility()), ["h0"], "sent once");
    assert!(feed.receive(1).is_ok());
}

/// Pins that a hand-back call the queue fails whole is retried with backoff,
/// and not past its unit's deadline.
#[tokio::test(start_paused = true)]
async fn a_failed_hand_back_call_is_retried_within_its_bound() {
    let (feed, queue) = start_gated(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.inner.ready(Message::new("u"));
    let d = receive_n(&feed, &park, 1).await.remove(0);
    let deadline = feed.deadline_of(d.receipt).unwrap();
    for _ in 0..100 {
        queue
            .inner
            .fail_next_visibility_call(CallError::retryable("InternalError"));
    }
    feed.hand_back(d.receipt).unwrap();
    step(Duration::from_secs(90)).await;
    // Each call is recorded by both, in the same order.
    let calls = queue.inner.visibility();
    let at = queue.visibility_at.lock().unwrap().clone();
    let sent: Vec<Instant> = calls
        .iter()
        .zip(&at)
        .filter(|(call, _)| call.iter().any(|e| e.seconds == 0))
        .map(|(_, t)| *t)
        .collect();
    assert!(sent.len() > 1, "the hand-back was not retried: {calls:?}");
    assert!(
        sent.iter().all(|t| *t < deadline),
        "a hand-back was sent at or past {deadline:?}: {sent:?}"
    );
}

/// Pins the stop's log lines: one line for a stop asked twice, counting the
/// buffered units handed back and no hand-back queued before it, a hand-back
/// logged as one, and no line that reads as a failure.
#[test]
fn the_stop_logs_buffered_units_and_no_failure() {
    let lines = spate_test::capture_logs(tracing::Level::DEBUG, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        rt.block_on(async {
            let (feed, queue) = start(MINUTE);
            let taken = one_taken_one_buffered(&feed, &queue).await;
            // The handed-out unit is entry 0, the buffered one entry 1.
            queue.fail_next_visibility(vec![failure(0, "ReceiptHandleIsInvalid")]);
            feed.hand_back(taken.receipt).unwrap();
            feed.stop_receiving();
            feed.stop_receiving();
            settle_tasks().await;
            assert_eq!(handed_back(&queue.visibility()), ["h0", "h1"]);
        });
    });
    let stops: Vec<&String> = lines.iter().filter(|l| l.contains("buffered=")).collect();
    assert_eq!(stops.len(), 1, "{lines:#?}");
    assert!(stops[0].contains("buffered=1"), "{lines:#?}");
    assert!(
        lines.iter().any(|l| l.contains("handing back")),
        "{lines:#?}"
    );
    assert!(
        !lines.iter().any(|l| l.to_lowercase().contains("fail")),
        "{lines:#?}"
    );
}

/// Pins that a stop sets each unit received and not yet handed out visible
/// at once, and hands out nothing more.
#[tokio::test(start_paused = true)]
async fn buffered_units_are_handed_back_when_the_feed_stops_receiving() {
    let (feed, queue) = start(MINUTE);
    one_taken_one_buffered(&feed, &queue).await;
    feed.stop_receiving();
    settle_tasks().await;
    assert_eq!(handed_back(&queue.visibility()), ["h1"]);
    assert_eq!(feed.buffered(), 0);
    assert!(feed.receive(1).unwrap().is_empty());
}

/// Pins that a stopped feed sends no further receive, though the reader
/// asked for more units than it got.
#[tokio::test(start_paused = true)]
async fn a_stopped_feed_sends_no_receive() {
    let (feed, queue) = start(MINUTE);
    assert!(feed.receive(3).unwrap().is_empty());
    until("a receive is sent", || queue.receives().len() == 1).await;
    feed.stop_receiving();
    step(Duration::from_secs(40)).await;
    assert_eq!(queue.receives().len(), 1);
}

/// Pins that a receive waiting at the stop runs to its reply, whose unit is
/// set visible at once and never buffered, and that no receive follows.
#[tokio::test(start_paused = true)]
async fn a_receive_answered_after_the_stop_is_handed_back() {
    let (feed, queue) = start(MINUTE);
    queue.answer_at_end_of_poll();
    assert!(feed.receive(2).unwrap().is_empty());
    until("the poll is sent", || queue.receives().len() == 1).await;
    queue.ready(Message::new("late"));
    feed.stop_receiving();
    step(Duration::from_secs(20)).await;
    assert_eq!(handed_back(&queue.visibility()), ["h0"]);
    assert_eq!(feed.buffered(), 0);
    step(Duration::from_secs(20)).await;
    assert_eq!(queue.receives().len(), 1);
}

/// Pins that from the stop every unit handed out is extended on each tick,
/// renewed or not, until it is acknowledged or handed back, each call
/// bounded by the stop's bound.
#[tokio::test(start_paused = true)]
async fn held_units_are_extended_from_the_stop_until_close() {
    let visibility = Duration::from_secs(3);
    let (feed, queue) = start(visibility);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("a"));
    queue.ready(Message::new("b"));
    let got = receive_n(&feed, &park, 2).await;
    feed.renew(got[0].receipt).unwrap();
    feed.stop_receiving();
    for tick in 1..=4 {
        let before: Vec<Instant> = got
            .iter()
            .map(|d| feed.deadline_of(d.receipt).unwrap())
            .collect();
        step(Duration::from_secs(1)).await;
        assert_eq!(queue.visibility().len(), tick, "tick {tick}");
        assert_eq!(extended(&queue.visibility()[tick - 1..]), ["h0", "h1"]);
        for (d, before) in got.iter().zip(before) {
            assert!(feed.deadline_of(d.receipt).unwrap() > before, "tick {tick}");
        }
    }
    assert!(
        queue
            .visibility_within()
            .iter()
            .all(|w| *w == Some(STOP_EXTENSION_BOUND))
    );
    feed.acknowledge(got[0].receipt).unwrap();
    feed.hand_back(got[1].receipt).unwrap();
    settle_tasks().await;
    let after = queue.visibility().len();
    step(Duration::from_secs(3)).await;
    assert_eq!(handed_back(&queue.visibility()), ["h1"]);
    assert!(extended(&queue.visibility()[after..]).is_empty());
    assert_eq!(queue.deletes().len(), 1);
    assert_eq!(queue.deletes()[0][0].handle, "h0");
}

/// Pins that a hand-back waits for an extension of the same unit already in
/// flight, and that no extension of it follows: within one chunk, and when
/// the unit is in the last of three chunks of the same tick, all in flight
/// together, where the hand-back goes out once every chunk is answered.
#[tokio::test(start_paused = true)]
async fn a_hand_back_is_sent_after_an_extension_of_the_same_receipt_in_flight() {
    for units in [1, 21] {
        let (feed, queue) = start_gated(MINUTE);
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        for i in 0..units {
            queue.inner.ready(Message::new(&format!("u{i}")));
        }
        let got = receive_n(&feed, &park, units).await;
        let r = got.last().unwrap().receipt;
        let handle = format!("h{}", units - 1);
        let chunks = units.div_ceil(BATCH_ENTRIES);
        queue.hold_visibility();
        step(Duration::from_secs(20)).await;
        until("every chunk is held", || {
            queue.inner.visibility().len() == chunks
        })
        .await;
        feed.hand_back(r).unwrap();
        settle_tasks().await;
        assert_eq!(
            queue.inner.visibility().len(),
            chunks,
            "sent behind the chunks"
        );
        queue.release_visibility();
        settle_tasks().await;
        step(Duration::from_secs(40)).await;
        let calls = queue.inner.visibility();
        let back = calls
            .iter()
            .position(|c| c.iter().any(|e| e.handle == handle && e.seconds == 0))
            .expect("the hand-back was sent");
        assert_eq!(back, chunks, "{units} units: {calls:?}");
        assert!(
            !extended(&calls[back..]).contains(&handle),
            "{units} units: extended after its hand-back: {calls:?}"
        );
        if units == 1 {
            assert_eq!(extended(&calls[..back]), [handle]);
        }
    }
}

/// Pins that during a stop an extension call is bounded, so a hand-back
/// queued behind one that SQS does not answer is sent once the bound passes.
#[tokio::test(start_paused = true)]
async fn a_hand_back_waits_at_most_the_stop_bound_behind_an_extension() {
    let (feed, queue) = start_gated(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.inner.ready(Message::new("a"));
    queue.inner.ready(Message::new("b"));
    let got = receive_n(&feed, &park, 2).await;
    feed.stop_receiving();
    queue.hold_visibility();
    step(Duration::from_secs(20)).await;
    until("the extension is held", || {
        queue.inner.visibility().len() == 1
    })
    .await;
    queue.visibility_held.store(false, Ordering::SeqCst);
    feed.hand_back(got[1].receipt).unwrap();
    settle_tasks().await;
    assert_eq!(
        queue.inner.visibility().len(),
        1,
        "sent behind the extension"
    );
    tokio::time::advance(STOP_EXTENSION_BOUND).await;
    settle_tasks().await;
    assert_eq!(handed_back(&queue.inner.visibility()), ["h1"]);
    assert_eq!(
        queue.inner.visibility_within()[0],
        Some(STOP_EXTENSION_BOUND)
    );
}

/// Pins that two receipts for one message settle apart: handing back the
/// second leaves the first held, extended and deleted on its own.
#[tokio::test(start_paused = true)]
async fn a_second_receipt_handed_back_leaves_the_first_held() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    queue.ready(Message::new("u").same_message_as("m0"));
    let got = receive_n(&feed, &park, 2).await;
    feed.stop_receiving();
    feed.hand_back(got[1].receipt).unwrap();
    settle_tasks().await;
    step(Duration::from_secs(20)).await;
    feed.acknowledge(got[0].receipt).unwrap();
    settle_tasks().await;
    let calls = queue.visibility();
    assert_eq!(handed_back(&calls), ["h1"]);
    assert_eq!(extended(&calls[1..]), ["h0"], "{calls:?}");
    assert_eq!(queue.deletes().len(), 1);
    assert_eq!(queue.deletes()[0][0].handle, "h0");
}

/// Pins that `close` sends the deletes still queued, each call bounded by
/// the close, and settles only once they are sent.
#[tokio::test(start_paused = true)]
async fn close_sends_queued_deletes_within_its_bound() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let d = receive_n(&feed, &park, 1).await.remove(0);
    let deadline = feed.deadline_of(d.receipt).unwrap();
    feed.acknowledge(d.receipt).unwrap();
    let within = Duration::from_secs(10);
    let mut closed = feed.close(within).unwrap();
    assert!(closed.try_outcome().is_none(), "settled before the delete");
    settle_tasks().await;
    assert_eq!(queue.deletes().len(), 1);
    assert_eq!(queue.deletes()[0][0].handle, "h0");
    assert_eq!(queue.delete_within(), [Some(within)]);
    assert!(within <= deadline - Instant::now());
    assert!(closed.try_outcome().unwrap().is_ok());
}

/// Pins that `close` waits for the delete batch the settle task is already
/// sending.
#[tokio::test(start_paused = true)]
async fn close_settles_after_the_batch_in_flight() {
    let (feed, queue) = start_gated(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.inner.ready(Message::new("a"));
    queue.inner.ready(Message::new("b"));
    let got = receive_n(&feed, &park, 2).await;
    queue.delete_down.store(true, Ordering::SeqCst);
    for d in &got {
        feed.acknowledge(d.receipt).unwrap();
    }
    settle_tasks().await;
    assert_eq!(queue.deletes_at.lock().unwrap().len(), 1, "in flight");
    let mut closed = feed.close(Duration::from_secs(10)).unwrap();
    settle_tasks().await;
    assert!(closed.try_outcome().is_none());
    queue.delete_down.store(false, Ordering::SeqCst);
    tokio::time::advance(BACKOFF_START).await;
    settle_tasks().await;
    assert_eq!(queue.inner.deletes().len(), 1);
    assert_eq!(queue.inner.deletes()[0].len(), 2);
    assert!(closed.try_outcome().unwrap().is_ok());
}

/// Pins that `close` sets every unit still held visible at once, bounded by
/// the close, and that no extension follows.
#[tokio::test(start_paused = true)]
async fn close_gives_up_every_unit_still_held() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("a"));
    queue.ready(Message::new("b"));
    let got = receive_n(&feed, &park, 2).await;
    feed.acknowledge(got[0].receipt).unwrap();
    let within = Duration::from_secs(10);
    let mut closed = feed.close(within).unwrap();
    settle_tasks().await;
    assert_eq!(queue.deletes().len(), 1);
    assert_eq!(queue.deletes()[0][0].handle, "h0");
    assert_eq!(handed_back(&queue.visibility()), ["h1"]);
    assert_eq!(queue.visibility_within(), [Some(within)]);
    assert!(closed.try_outcome().unwrap().is_ok());
    step(Duration::from_secs(40)).await;
    assert_eq!(queue.visibility().len(), 1, "extended after the close");
}

/// Pins that a close with nothing to send settles at once, with no call,
/// though a long poll is waiting.
#[tokio::test(start_paused = true)]
async fn an_idle_close_settles_at_once() {
    let (feed, queue) = start(MINUTE);
    assert!(feed.receive(1).unwrap().is_empty());
    until("a receive is sent", || queue.receives().len() == 1).await;
    let mut closed = feed.close(Duration::from_secs(10)).unwrap();
    assert!(closed.try_outcome().unwrap().is_ok());
    assert!(queue.visibility().is_empty());
    assert!(queue.deletes().is_empty());
}

/// Pins that a close given no time sends nothing and settles rejected.
#[tokio::test(start_paused = true)]
async fn a_close_with_no_time_left_sends_nothing() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let d = receive_n(&feed, &park, 1).await.remove(0);
    feed.acknowledge(d.receipt).unwrap();
    let mut closed = feed.close(Duration::ZERO).unwrap();
    settle_tasks().await;
    assert!(queue.deletes().is_empty());
    assert!(closed.try_outcome().unwrap().is_err());
}

/// Pins that a delete failing at close is retried only until the close's
/// bound, and that the close has settled by then.
#[tokio::test(start_paused = true)]
async fn a_failing_delete_at_close_is_retried_only_until_within() {
    let (feed, queue) = start_gated(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.inner.ready(Message::new("u"));
    let d = receive_n(&feed, &park, 1).await.remove(0);
    queue.delete_down.store(true, Ordering::SeqCst);
    feed.acknowledge(d.receipt).unwrap();
    let within = Duration::from_secs(2);
    let bound = Instant::now() + within;
    let mut closed = feed.close(within).unwrap();
    step(within).await;
    assert!(closed.try_outcome().unwrap().is_err());
    step(Duration::from_secs(3)).await;
    let sent = queue.deletes_at.lock().unwrap().clone();
    assert!(sent.len() > 1, "the delete was not retried");
    assert!(
        sent.iter().all(|t| *t < bound),
        "a delete was sent at or past {bound:?}: {sent:?}"
    );
}

/// Pins that `close` called off the runtime, as the pipeline calls it,
/// bounds its calls by the runtime's clock.
#[test]
fn close_called_off_the_runtime_bounds_by_tokio_time() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    let start_on = |queue: &Arc<ScriptedQueue>| {
        SqsFeed::start(
            Arc::clone(queue) as Arc<dyn Queue>,
            Arc::clone(queue) as Arc<dyn startup::QueueAttributes>,
            DEAD_URL.to_owned(),
            &config(MINUTE),
            rt.handle(),
        )
    };
    let queue = Arc::new(ScriptedQueue::default());
    let feed = start_on(&queue);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let d = rt.block_on(receive_n(&feed, &park, 1)).remove(0);
    feed.acknowledge(d.receipt).unwrap();
    let within = Duration::from_secs(5);
    let closed = feed.close(within).unwrap();
    rt.block_on(closed).unwrap();
    assert_eq!(queue.deletes().len(), 1);
    assert_eq!(queue.delete_within(), [Some(within)]);

    let idle = start_on(&Arc::new(ScriptedQueue::default()));
    rt.block_on(idle.close(Duration::ZERO).unwrap()).unwrap();
}

/// Pins that `close` without a stop first stops receiving and sets every
/// buffered unit visible at once, and that it waits for the receive in
/// flight for a second after that hand-back, and no longer.
#[tokio::test(start_paused = true)]
async fn close_without_a_stop_stops_receiving_and_gives_up_buffered_units() {
    let (feed, queue) = start(MINUTE);
    queue.ready(Message::new("a"));
    queue.ready(Message::new("b"));
    assert!(feed.receive(3).unwrap().is_empty());
    until("both units are buffered", || feed.buffered() == 2).await;
    until("the next poll is sent", || queue.receives().len() == 2).await;
    let mut closed = feed.close(Duration::from_secs(10)).unwrap();
    settle_tasks().await;
    assert_eq!(handed_back(&queue.visibility()), ["h0", "h1"]);
    step(RECEIVE_GRACE - Duration::from_millis(100)).await;
    assert!(closed.try_outcome().is_none(), "settled within the grace");
    step(Duration::from_millis(100)).await;
    assert!(closed.try_outcome().unwrap().is_ok());
    step(Duration::from_secs(40)).await;
    assert_eq!(queue.receives().len(), 2, "received after the close");
}

/// Pins that a close waits for a receive in flight that takes a unit just
/// handed back, and hands that unit back again before it settles.
#[tokio::test(start_paused = true)]
async fn a_unit_received_again_at_close_is_handed_back_again() {
    // Each poll waits 1 s.
    let (feed, queue) = start(Duration::from_secs(3));
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.answer_at_end_of_poll();
    queue.ready(Message::new("u"));
    assert!(feed.receive(2).unwrap().is_empty());
    until("the first poll is sent", || queue.receives().len() == 1).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    until("the unit is buffered", || park.park(Duration::ZERO)).await;
    until("the next poll is sent", || queue.receives().len() == 2).await;
    let d = feed.receive(2).unwrap().remove(0);
    feed.stop_receiving();
    step(Duration::from_millis(500)).await;
    feed.hand_back(d.receipt).unwrap();
    settle_tasks().await;
    assert_eq!(handed_back(&queue.visibility()), ["h0"]);
    // The poll in flight takes the unit as SQS makes it visible again.
    queue.ready(Message::new("u").redelivered(2).same_message_as("m0"));
    let mut closed = feed.close(Duration::from_secs(10)).unwrap();
    settle_tasks().await;
    assert!(
        closed.try_outcome().is_none(),
        "settled with a receive in flight"
    );
    step(Duration::from_millis(500)).await;
    assert_eq!(handed_back(&queue.visibility()), ["h0", "h1"]);
    assert!(closed.try_outcome().unwrap().is_ok());
}

/// Pins that once the feed has stopped on a Fatal, a stop sends nothing, a
/// hand-back answers the Fatal, and a close settles with it at once.
#[tokio::test(start_paused = true)]
async fn the_stop_methods_on_a_fatal_feed_answer_at_once() {
    let (feed, queue) = start_gated(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.inner.ready(Message::new("a"));
    queue.inner.ready(Message::new("b"));
    queue.inner.ready(Message::new("buffered"));
    let got = receive_n(&feed, &park, 2).await;
    assert!(feed.receive(1).unwrap().is_empty());
    until("the third unit is buffered", || feed.buffered() == 1).await;
    queue.delete_fatal.store(true, Ordering::SeqCst);
    feed.acknowledge(got[0].receipt).unwrap();
    until("the feed stops", || feed.renew(got[1].receipt).is_err()).await;
    feed.stop_receiving();
    assert_eq!(feed.buffered(), 1, "the stop gave back a buffered unit");
    assert_eq!(
        feed.hand_back(got[1].receipt).unwrap_err().kind,
        FeedErrorKind::Fatal
    );
    let mut closed = feed.close(Duration::from_secs(10)).unwrap();
    assert_eq!(
        closed.try_outcome().unwrap().unwrap_err().kind,
        FeedErrorKind::Fatal
    );
    settle_tasks().await;
    assert!(queue.inner.visibility().is_empty());
}

/// Pins that a Fatal while a close waits rejects the close with it before
/// the bound.
#[tokio::test(start_paused = true)]
async fn a_fatal_during_close_rejects_it() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let d = receive_n(&feed, &park, 1).await.remove(0);
    queue.fail_next_delete_call(CallError::retryable("InternalError"));
    queue.fail_next_delete_call(CallError::fatal("AccessDenied"));
    feed.acknowledge(d.receipt).unwrap();
    let within = Duration::from_secs(10);
    let bound = Instant::now() + within;
    let mut closed = feed.close(within).unwrap();
    settle_tasks().await;
    assert!(closed.try_outcome().is_none());
    tokio::time::advance(BACKOFF_START).await;
    settle_tasks().await;
    assert!(Instant::now() < bound);
    assert_eq!(
        closed.try_outcome().unwrap().unwrap_err().kind,
        FeedErrorKind::Fatal
    );
}

/// Pins that an abandoned feed sends nothing for a stop, a hand-back or a
/// close: a hand-back answers `Ok` and a close `None`.
#[tokio::test(start_paused = true)]
async fn the_stop_methods_on_an_abandoned_feed_send_nothing() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("a"));
    queue.ready(Message::new("b"));
    let got = receive_n(&feed, &park, 2).await;
    feed.abandon();
    feed.stop_receiving();
    feed.hand_back(got[0].receipt).unwrap();
    assert!(feed.close(Duration::from_secs(10)).is_none());
    step(Duration::from_secs(40)).await;
    assert!(queue.visibility().is_empty());
    assert!(queue.deletes().is_empty());
}

/// Pins that a close whose delete SQS refuses settles rejected, naming how
/// many entries it gave up.
#[tokio::test(start_paused = true)]
async fn a_close_that_drops_a_delete_settles_err() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let d = receive_n(&feed, &park, 1).await.remove(0);
    queue.fail_next_delete(vec![failure(0, "ReceiptHandleIsInvalid")]);
    feed.acknowledge(d.receipt).unwrap();
    let mut closed = feed.close(Duration::from_secs(10)).unwrap();
    settle_tasks().await;
    let e = closed.try_outcome().unwrap().unwrap_err().reason;
    assert!(
        e.starts_with("the close gave up 1 of its deletes, fails and hand-backs unapplied"),
        "{e}"
    );
}

/// Pins that `close` sends the fails still queued, bounded by the close.
#[tokio::test(start_paused = true)]
async fn close_sends_queued_fails_within_its_bound() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let d = receive_n(&feed, &park, 1).await.remove(0);
    feed.fail(d.receipt, "unreadable").unwrap();
    let within = Duration::from_secs(10);
    let mut closed = feed.close(within).unwrap();
    settle_tasks().await;
    assert_eq!(handed_back(&queue.visibility()), ["h0"]);
    assert_eq!(queue.visibility_within(), [Some(within)]);
    assert!(closed.try_outcome().unwrap().is_ok());
}

/// Pins that `close` waits for a hand-back call already in flight.
#[tokio::test(start_paused = true)]
async fn close_waits_for_a_hand_back_in_flight() {
    let (feed, queue) = start_gated(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.inner.ready(Message::new("u"));
    let d = receive_n(&feed, &park, 1).await.remove(0);
    queue.hold_visibility();
    feed.hand_back(d.receipt).unwrap();
    until("the hand-back is held", || {
        queue.inner.visibility().len() == 1
    })
    .await;
    let mut closed = feed.close(Duration::from_secs(10)).unwrap();
    settle_tasks().await;
    assert!(
        closed.try_outcome().is_none(),
        "settled with a hand-back in flight"
    );
    queue.release_visibility();
    settle_tasks().await;
    assert!(closed.try_outcome().unwrap().is_ok());
}

/// Pins that a close whose entries are still unsent at its bound settles
/// rejected, naming them unsent: the hand-backs it queues wait behind an
/// extension SQS does not answer.
#[tokio::test(start_paused = true)]
async fn a_close_reaching_its_bound_with_entries_unsent_settles_err() {
    let (feed, queue) = start_gated(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.inner.ready(Message::new("u"));
    receive_n(&feed, &park, 1).await;
    queue.hold_visibility();
    step(Duration::from_secs(20)).await;
    until("the extension is held", || {
        queue.inner.visibility().len() == 1
    })
    .await;
    let within = Duration::from_secs(2);
    let mut closed = feed.close(within).unwrap();
    step(within - Duration::from_millis(100)).await;
    assert!(closed.try_outcome().is_none(), "settled before the bound");
    // tokio rounds a timer up to the next millisecond.
    step(Duration::from_millis(200)).await;
    let e = closed.try_outcome().unwrap().unwrap_err().reason;
    assert!(
        e.starts_with("the close reached its bound with 1 deletes, fails or hand-backs unsent;"),
        "{e}"
    );
}

/// Pins that a receive answered with nothing does not wake the reader.
#[tokio::test(start_paused = true)]
async fn an_empty_receive_does_not_wake_the_reader() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    assert!(feed.receive(1).unwrap().is_empty());
    until("a receive is sent", || queue.receives().len() == 1).await;
    step(Duration::from_secs(21)).await;
    assert!(queue.receives().len() > 1, "the first receive answered");
    assert!(!park.park(Duration::ZERO), "woken with nothing to take");
}

/// Pins that a hand-back queued as the feed stops on a Fatal is not sent.
#[tokio::test(start_paused = true)]
async fn a_hand_back_queued_as_the_feed_stops_is_not_sent() {
    let (feed, queue) = start_gated(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.inner.ready(Message::new("a"));
    queue.inner.ready(Message::new("b"));
    let got = receive_n(&feed, &park, 2).await;
    queue.delete_fatal.store(true, Ordering::SeqCst);
    // The settle task runs first and stops the feed.
    feed.acknowledge(got[0].receipt).unwrap();
    feed.hand_back(got[1].receipt).unwrap();
    settle_tasks().await;
    assert!(feed.receive(1).is_err(), "the feed stopped");
    assert!(queue.inner.visibility().is_empty());
}

/// Pins that a batch sent once more after a stop, in a tick that began
/// before it, is bounded by the stop bound, while the tick's own calls keep
/// the running bound.
#[tokio::test(start_paused = true)]
async fn an_extension_sent_after_a_stop_in_a_tick_begun_before_it_is_bounded() {
    let (feed, queue) = start_gated(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    for i in 0..21 {
        queue.inner.ready(Message::new(&format!("u{i}")));
    }
    receive_n(&feed, &park, 21).await;
    queue.hold_visibility();
    step(Duration::from_secs(20)).await;
    until("every chunk is held", || {
        queue.inner.visibility().len() == 3
    })
    .await;
    feed.stop_receiving();
    // The tick's calls are given up at 40 s, with 20 s left, and sent once
    // more under the stop.
    step(Duration::from_secs(20)).await;
    until("every chunk is sent once more", || {
        queue.inner.visibility().len() == 6
    })
    .await;
    let within = queue.inner.visibility_within();
    assert_eq!(
        within[..3],
        [Some(Duration::from_secs(20)); 3],
        "the extensions sent before the stop do not take the running bound"
    );
    assert_eq!(
        within[3..],
        [Some(STOP_EXTENSION_BOUND); 3],
        "the extensions sent after the stop do not take the stop bound"
    );
}

/// Pins that a close whose only entries are hand-backs past their deadline
/// settles as soon as the renewal task drops them, before its bound: for a
/// buffered unit that lapsed while the reader stalled, closed without a
/// stop, and for a handed-out unit that lapsed before the stop.
#[tokio::test(start_paused = true)]
async fn a_close_with_only_lapsed_hand_backs_settles_at_once() {
    let within = Duration::from_secs(10);
    let (feed, queue) = start(MINUTE);
    queue.ready(Message::new("a"));
    assert!(feed.receive(1).unwrap().is_empty());
    until("the unit is buffered", || feed.buffered() == 1).await;
    step(Duration::from_secs(90)).await;
    let mut closed = feed.close(within).unwrap();
    settle_tasks().await;
    assert!(handed_back(&queue.visibility()).is_empty());
    assert!(
        closed.try_outcome().is_some(),
        "close without a stop: nothing left to send, and the close waits for its bound"
    );

    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("b"));
    let _held = receive_n(&feed, &park, 1).await;
    step(Duration::from_secs(90)).await;
    feed.stop_receiving();
    settle_tasks().await;
    let mut closed = feed.close(within).unwrap();
    settle_tasks().await;
    assert!(handed_back(&queue.visibility()).is_empty());
    assert!(
        closed.try_outcome().is_some(),
        "close after a stop: nothing left to send, and the close waits for its bound"
    );
}

/// Pins that a close whose bound ends while it waits for a receive in
/// flight that takes a unit just handed back settles rejected: that unit
/// stays hidden for a visibility period.
#[tokio::test(start_paused = true)]
async fn a_close_cut_short_with_a_receive_in_flight_is_rejected() {
    // Each poll waits 1 s.
    let (feed, queue) = start(Duration::from_secs(3));
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.answer_at_end_of_poll();
    queue.ready(Message::new("u"));
    assert!(feed.receive(2).unwrap().is_empty());
    until("the first poll is sent", || queue.receives().len() == 1).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    until("the unit is buffered", || park.park(Duration::ZERO)).await;
    until("the next poll is sent", || queue.receives().len() == 2).await;
    let d = feed.receive(2).unwrap().remove(0);
    feed.stop_receiving();
    step(Duration::from_millis(500)).await;
    feed.hand_back(d.receipt).unwrap();
    settle_tasks().await;
    assert_eq!(handed_back(&queue.visibility()), ["h0"]);
    // The poll in flight takes the unit as SQS makes it visible again, and
    // answers after the close's bound.
    queue.ready(Message::new("u").redelivered(2).same_message_as("m0"));
    let mut closed = feed.close(Duration::from_millis(200)).unwrap();
    settle_tasks().await;
    step(Duration::from_millis(300)).await;
    let outcome = closed.try_outcome().expect("settled at the bound");
    assert!(feed.lock().receiving, "the receive is still in flight");
    assert!(outcome.is_err(), "settled Ok with a receive in flight");
}

/// Pins that a close reaching its bound with entries unsent also counts the
/// entries it gave up before the bound: both sets of units return through
/// redelivery.
#[tokio::test(start_paused = true)]
async fn a_close_at_its_bound_names_both_unsent_and_given_up_entries() {
    let (feed, queue) = start_gated(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.inner.ready(Message::new("kept"));
    queue.inner.ready(Message::new("done"));
    let got = receive_n(&feed, &park, 2).await;
    queue.hold_visibility();
    step(Duration::from_secs(20)).await;
    until("the extension is held", || {
        queue.inner.visibility().len() == 1
    })
    .await;
    // SQS refuses the delete of the finished unit: it is read again.
    queue
        .inner
        .fail_next_delete(vec![failure(0, "ReceiptHandleIsInvalid")]);
    feed.acknowledge(got[1].receipt).unwrap();
    let within = Duration::from_secs(2);
    let mut closed = feed.close(within).unwrap();
    step(within + Duration::from_millis(200)).await;
    assert_eq!(queue.inner.deletes().len(), 1, "the delete was sent");
    let e = closed.try_outcome().unwrap().unwrap_err();
    assert!(
        e.to_string()
            .contains("1 deletes, fails or hand-backs unsent"),
        "{e}"
    );
    assert!(e.to_string().contains("gave up 1"), "{e}");
}

/// Pins `SqsFeed::abandon`'s rustdoc on `hand_back`: `Ok` until a unit's
/// deadline, then `Gone`, and nothing sent either way.
#[tokio::test(start_paused = true)]
async fn hand_back_after_abandon_answers_ok_until_the_deadline() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("a"));
    queue.ready(Message::new("b"));
    let got = receive_n(&feed, &park, 2).await;
    feed.abandon();
    feed.hand_back(got[0].receipt).unwrap();
    tokio::time::advance(MINUTE + Duration::from_secs(1)).await;
    assert_eq!(
        feed.hand_back(got[1].receipt).unwrap_err().kind,
        FeedErrorKind::Gone
    );
    assert!(queue.visibility().is_empty());
}

/// Pins that `close` sets no held unit visible whose extension SQS refused
/// as gone, and still hands back the live one.
#[tokio::test(start_paused = true)]
async fn close_hands_back_no_gone_unit() {
    let (feed, queue) = start(MINUTE);
    one_taken_one_buffered(&feed, &queue).await;
    // The handed-out unit is entry 0, the buffered one entry 1.
    queue.fail_next_visibility(vec![failure(0, "ReceiptHandleIsInvalid")]);
    step(Duration::from_secs(20)).await;
    until("the tick extends the units", || {
        queue.visibility().len() == 1
    })
    .await;
    let mut closed = feed.close(Duration::from_secs(10)).unwrap();
    settle_tasks().await;
    assert_eq!(handed_back(&queue.visibility()), ["h1"]);
    assert!(closed.try_outcome().unwrap().is_ok());
}

/// Pins that a stop sets no buffered unit visible whose extension SQS
/// refused as gone.
#[tokio::test(start_paused = true)]
async fn a_stop_hands_back_no_gone_buffered_unit() {
    let (feed, queue) = start(MINUTE);
    one_taken_one_buffered(&feed, &queue).await;
    // The handed-out unit is entry 0, the buffered one entry 1.
    queue.fail_next_visibility(vec![failure(1, "ReceiptHandleIsInvalid")]);
    step(Duration::from_secs(20)).await;
    until("the tick extends the units", || {
        queue.visibility().len() == 1
    })
    .await;
    feed.stop_receiving();
    settle_tasks().await;
    assert!(handed_back(&queue.visibility()).is_empty());
}

/// Pins that a reader asking for units after the stop sends no receive.
#[tokio::test(start_paused = true)]
async fn a_receive_asked_after_the_stop_sends_no_receive() {
    let (feed, queue) = start(MINUTE);
    feed.stop_receiving();
    assert!(feed.receive(3).unwrap().is_empty());
    step(Duration::from_secs(40)).await;
    assert!(queue.receives().is_empty());
}

/// Pins that a reply landing after the stop is set visible as it lands, with
/// no renewal tick in between.
#[tokio::test(start_paused = true)]
async fn a_reply_after_the_stop_is_handed_back_before_the_next_tick() {
    let (feed, queue) = start(MINUTE);
    queue.answer_at_end_of_poll();
    // The 20 s poll runs from 5 s to 25 s, between the ticks at 20 s and
    // 40 s.
    tokio::time::advance(Duration::from_secs(5)).await;
    assert!(feed.receive(1).unwrap().is_empty());
    until("the poll is sent", || queue.receives().len() == 1).await;
    queue.ready(Message::new("late"));
    feed.stop_receiving();
    step(Duration::from_secs(21)).await;
    assert_eq!(handed_back(&queue.visibility()), ["h0"]);
}

/// Pins that a reply landing after the stop does not wake the reader: its
/// units are handed back, so nothing is buffered to take.
#[tokio::test(start_paused = true)]
async fn a_reply_after_the_stop_does_not_wake_the_reader() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.answer_at_end_of_poll();
    assert!(feed.receive(1).unwrap().is_empty());
    until("the poll is sent", || queue.receives().len() == 1).await;
    queue.ready(Message::new("late"));
    feed.stop_receiving();
    step(Duration::from_secs(21)).await;
    assert_eq!(handed_back(&queue.visibility()), ["h0"]);
    assert!(!park.park(Duration::ZERO), "woken with nothing to take");
}

/// Pins that a close whose hand-back SQS refuses settles rejected, naming
/// how many entries it gave up.
#[tokio::test(start_paused = true)]
async fn a_close_whose_hand_back_is_refused_settles_err() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    receive_n(&feed, &park, 1).await;
    queue.fail_next_visibility(vec![failure(
        0,
        "AWS.SimpleQueueService.MessageNotInflight",
    )]);
    let mut closed = feed.close(Duration::from_secs(10)).unwrap();
    settle_tasks().await;
    assert_eq!(handed_back(&queue.visibility()), ["h0"]);
    let e = closed.try_outcome().unwrap().unwrap_err();
    assert!(e.to_string().contains(" 1 "), "{e}");
}

/// Pins that a close given no time sends no queued fail and settles
/// rejected, naming the fail it gave up.
#[tokio::test(start_paused = true)]
async fn a_close_with_no_time_left_gives_up_its_fail() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let d = receive_n(&feed, &park, 1).await.remove(0);
    feed.fail(d.receipt, "unreadable").unwrap();
    let mut closed = feed.close(Duration::ZERO).unwrap();
    settle_tasks().await;
    assert!(queue.visibility().is_empty());
    let e = closed.try_outcome().unwrap().unwrap_err();
    assert!(e.to_string().contains(" 1 "), "{e}");
}

/// Pins the close's wait for a receive in flight at one second after the
/// last hand-back, the figure the SQS feed page states.
#[tokio::test(start_paused = true)]
async fn a_close_waits_one_second_for_a_receive_in_flight() {
    let (feed, queue) = start(MINUTE);
    queue.ready(Message::new("a"));
    assert!(feed.receive(2).unwrap().is_empty());
    until("the unit is buffered", || feed.buffered() == 1).await;
    until("the next poll is sent", || queue.receives().len() == 2).await;
    let mut closed = feed.close(Duration::from_secs(10)).unwrap();
    settle_tasks().await;
    assert_eq!(handed_back(&queue.visibility()), ["h0"]);
    step(Duration::from_millis(900)).await;
    assert!(closed.try_outcome().is_none(), "settled within the second");
    step(Duration::from_millis(100)).await;
    assert!(closed.try_outcome().unwrap().is_ok());
}

/// Pins the bound on an extension sent during a stop at one second.
#[tokio::test(start_paused = true)]
async fn an_extension_during_a_stop_is_bounded_at_one_second() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("a"));
    receive_n(&feed, &park, 1).await;
    feed.stop_receiving();
    step(Duration::from_secs(20)).await;
    assert_eq!(queue.visibility_within(), [Some(Duration::from_secs(1))]);
}

/// Pins that a Fatal visibility extension is returned by every later call.
#[tokio::test(start_paused = true)]
async fn a_fatal_renewal_is_sticky() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let delivery = receive_n(&feed, &park, 1).await.remove(0);
    feed.renew(delivery.receipt).unwrap();
    queue.fail_next_visibility_call(CallError::fatal("InvalidAction"));
    tokio::time::advance(Duration::from_secs(20)).await;
    until("the extension is sent", || queue.visibility().len() == 1).await;
    until("the failure wakes the reader", || park.park(Duration::ZERO)).await;
    for _ in 0..2 {
        assert_eq!(feed.receive(1).unwrap_err().kind, FeedErrorKind::Fatal);
    }
}

/// Pins that `connect` refuses a queue whose attribute read SQS answers
/// with a coded 400, in AWS's wire shape and ElasticMQ's. Regression for
/// #1188.
#[test]
fn an_unlisted_sender_error_refuses_connect() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let shapes = [
        (
            "x-amzn-query-error: InvalidAction;Sender",
            r#"{"__type":"com.amazon.coral.service#InvalidAction","message":"bad"}"#,
        ),
        (
            "X-Amzn-Query-Error: com.amazonaws.sqs#InvalidAction",
            r#"{"__type":"com.amazonaws.sqs#InvalidAction","message":"bad"}"#,
        ),
    ];
    for (header, body) in shapes {
        let listener = io
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let addr = listener.local_addr().unwrap();
        io.spawn(async move {
            while let Ok((mut tcp, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 65536];
                    let _ = tcp.read(&mut buf).await;
                    let reply = format!(
                        "HTTP/1.1 400 Bad Request\r\nContent-Type: application/x-amz-json-1.0\r\n\
                         {header}\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = tcp.write_all(reply.as_bytes()).await;
                    let _ = tokio::io::copy(&mut tcp, &mut tokio::io::sink()).await;
                });
            }
        });
        let section: FeedSection = serde_yaml::from_str(&format!(
            "sqs: {{ queue_url: \"http://{addr}/000000000000/jobs\", \
             region: eu-west-2, endpoint: \"http://{addr}\" }}"
        ))
        .unwrap();
        match SqsFeed::connect_with_static_credentials(&section, io.handle(), "x", "x") {
            Err(SqsError::Refused(m)) => assert!(m.contains("InvalidAction"), "{header}: {m}"),
            Err(e) => panic!("{header}: {e}"),
            Ok(_) => panic!("{header}: connected"),
        }
    }
}

/// The bound each hand-back call was given, in call order.
fn hand_back_within(queue: &ScriptedQueue) -> Vec<Option<Duration>> {
    queue
        .visibility()
        .iter()
        .zip(queue.visibility_within())
        .filter(|(call, _)| call.iter().all(|e| e.seconds == 0))
        .map(|(_, within)| within)
        .collect()
}

/// Receive a kept unit and one to hand back, and renew the kept one for
/// nine tenths of a renewal period, just short of the first tick.
async fn kept_and_other(
    feed: &SqsFeed,
    queue: &ScriptedQueue,
    visibility: Duration,
) -> [Receipt; 2] {
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("kept"));
    queue.ready(Message::new("handed back"));
    let got = receive_n(feed, &park, 2).await;
    renew_through(feed, &[got[0].receipt], visibility * 3 / 10, "before").await;
    [got[0].receipt, got[1].receipt]
}

/// Pins that a held unit the reader keeps renewing survives one hand-back
/// call SQS is slow to answer, sent from the renewal task just before a
/// tick. Regression for #1452.
#[tokio::test(start_paused = true)]
async fn a_renewed_unit_survives_one_slow_hand_back_call() {
    for visibility in [3, 30, 60] {
        let visibility = Duration::from_secs(visibility);
        let (feed, queue) = start(visibility);
        let [kept, other] = kept_and_other(&feed, &queue, visibility).await;
        queue.delay_next_visibility_call(MINUTE);
        feed.hand_back(other).unwrap();
        let what = format!("visibility {visibility:?}, after the hand-back");
        renew_through(&feed, &[kept], 2 * visibility, &what).await;
    }
}

/// Pins that a held unit the reader keeps renewing survives hand-back calls
/// SQS never answers, each followed by a retry that is slow again.
/// Regression for #1452.
#[tokio::test(start_paused = true)]
async fn a_renewed_unit_survives_hand_back_calls_sqs_keeps_slow() {
    for visibility in [3, 30] {
        let visibility = Duration::from_secs(visibility);
        let (feed, queue) = start(visibility);
        let [kept, other] = kept_and_other(&feed, &queue, visibility).await;
        queue.slow_hand_backs(Some(MINUTE));
        feed.hand_back(other).unwrap();
        let what = format!("visibility {visibility:?}, after the hand-back");
        renew_through(&feed, &[kept], 2 * visibility, &what).await;
    }
}

/// Pins that a held unit the reader keeps renewing survives a hand-back
/// whose every call SQS fails at once, retried with backoff until a
/// deadline later than the held unit's. Regression for #1452.
#[tokio::test(start_paused = true)]
async fn a_renewed_unit_survives_a_hand_back_sqs_keeps_failing() {
    let visibility = Duration::from_secs(30);
    let (feed, queue) = start(visibility);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("kept"));
    let kept = receive_n(&feed, &park, 1).await.remove(0).receipt;
    renew_through(&feed, &[kept], Duration::from_secs(4), "before").await;
    queue.ready(Message::new("handed back"));
    let other = receive_n(&feed, &park, 1).await.remove(0).receipt;
    renew_through(&feed, &[kept], Duration::from_secs(5), "before").await;
    queue.fail_hand_backs(Some(CallError::retryable("InternalError")));
    feed.hand_back(other).unwrap();
    renew_through(&feed, &[kept], 2 * visibility, "after the hand-back").await;
    assert!(handed_back(&queue.visibility()).len() > 2, "retried");
}

/// Pins that a held unit survives more hand-backs than one batch carries,
/// queued over a renewal period while SQS answers none of them, and that a
/// batch given up is sent again first. Regression for #1452.
#[tokio::test(start_paused = true)]
async fn held_units_survive_many_slow_hand_backs() {
    let visibility = Duration::from_secs(30);
    let (feed, queue) = start(visibility);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    for i in 0..22 {
        queue.ready(Message::new(&format!("u{i}")));
    }
    let got = receive_n(&feed, &park, 22).await;
    let kept = got[0].receipt;
    renew_through(&feed, &[kept], Duration::from_secs(9), "before").await;
    queue.slow_hand_backs(Some(MINUTE));
    for d in &got[1..12] {
        feed.hand_back(d.receipt).unwrap();
    }
    for d in &got[12..] {
        renew_through(&feed, &[kept], Duration::from_secs(1), "handing back").await;
        feed.hand_back(d.receipt).unwrap();
    }
    renew_through(&feed, &[kept], 2 * visibility, "after the hand-backs").await;
    let calls: Vec<Vec<String>> = queue
        .visibility()
        .iter()
        .filter(|call| call.iter().all(|e| e.seconds == 0))
        .map(|call| call.iter().map(|e| e.handle.clone()).collect())
        .collect();
    assert!(calls.len() > 1, "{calls:?}");
    assert_eq!(calls[1], calls[0], "the batch given up goes first");
}

/// Pins that a unit the worker holds through a stop keeps its lease while
/// SQS answers none of the hand-backs of the units still buffered, and that
/// those hand-backs carry only the buffered units, each before its deadline.
/// Regression for #1452.
#[tokio::test(start_paused = true)]
async fn a_held_unit_survives_a_slow_hand_back_at_the_stop() {
    let visibility = Duration::from_secs(30);
    let (feed, queue) = start_gated(visibility);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    for i in 0..11 {
        queue.inner.ready(Message::new(&format!("u{i}")));
    }
    assert!(feed.receive(11).unwrap().is_empty());
    until("the units are buffered", || feed.buffered() == 11).await;
    assert!(park.park(Duration::ZERO));
    let taken = feed.receive(1).unwrap().remove(0);
    let start = Instant::now();
    queue.inner.slow_hand_backs(Some(MINUTE));
    step(Duration::from_secs(9)).await;
    feed.stop_receiving();
    let mut deadline = feed.deadline_of(taken.receipt).unwrap();
    let mut moved = 0;
    for _ in 0..(2 * visibility).as_millis() / 100 {
        step(Duration::from_millis(100)).await;
        let now = feed.deadline_of(taken.receipt).unwrap();
        assert!(
            now > Instant::now(),
            "lapsed {:?} in",
            Instant::now() - start
        );
        moved += usize::from(now != deadline);
        deadline = now;
    }
    assert!(moved >= 5, "extended on {moved} ticks");
    feed.acknowledge(taken.receipt).unwrap();
    let at = queue.visibility_at.lock().unwrap().clone();
    let buffered: Vec<String> = (1..11).map(|i| format!("h{i}")).collect();
    let hand_backs: Vec<_> = queue
        .inner
        .visibility()
        .into_iter()
        .zip(at)
        .filter(|(call, _)| call.iter().all(|e| e.seconds == 0))
        .collect();
    assert!(!hand_backs.is_empty());
    for (call, sent) in hand_backs {
        assert!(sent < start + visibility, "sent {:?} in", sent - start);
        for e in call {
            assert!(buffered.contains(&e.handle), "{}", e.handle);
        }
    }
}

/// Pins the bound of a hand-back call: one renewal period from the start of
/// its pass, or the time left to its batch's earliest deadline, or a
/// close's bound, if that is shorter. A batch left when a pass ends waits
/// for the next one. Regression for #1452.
#[tokio::test(start_paused = true)]
async fn a_hand_back_call_is_bounded_by_the_period_and_the_time_left() {
    let s = Duration::from_secs;
    // Hand back one unit `at` into its visibility, after SQS refuses the
    // tick's extension so the deadline stays at 60 s.
    for (at, within) in [(0, s(20)), (40, s(20)), (45, s(15))] {
        let (feed, queue) = start(MINUTE);
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        queue.ready(Message::new("u"));
        let d = receive_n(&feed, &park, 1).await.remove(0);
        queue.fail_next_visibility(vec![failure(0, "InvalidParameterValue")]);
        step(s(at)).await;
        feed.hand_back(d.receipt).unwrap();
        settle_tasks().await;
        assert_eq!(hand_back_within(&queue), [Some(within)], "at {at} s");
    }
    // A second unit, later in the batch, does not lengthen the bound.
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("a"));
    let a = receive_n(&feed, &park, 1).await.remove(0);
    queue.fail_next_visibility(vec![failure(0, "InvalidParameterValue")]);
    step(s(26)).await;
    queue.ready(Message::new("b"));
    let b = receive_n(&feed, &park, 1).await.remove(0);
    step(s(19)).await;
    feed.hand_back(b.receipt).unwrap();
    feed.hand_back(a.receipt).unwrap();
    settle_tasks().await;
    assert_eq!(handed_back(&queue.visibility()), ["h1", "h0"]);
    assert_eq!(hand_back_within(&queue), [Some(s(15))], "earliest deadline");
    // A close's bound past the period does not lengthen it.
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    receive_n(&feed, &park, 1).await;
    let _closed = feed.close(s(30)).unwrap();
    settle_tasks().await;
    assert_eq!(hand_back_within(&queue), [Some(s(20))], "close");
    // Eleven hand-backs: the second batch gets what is left of the pass,
    // and none once the first takes all of it.
    for (first, second) in [(s(5), s(15)), (s(20), s(20))] {
        let (feed, queue) = start(MINUTE);
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        for i in 0..11 {
            queue.ready(Message::new(&format!("u{i}")));
        }
        let got = receive_n(&feed, &park, 11).await;
        queue.delay_next_visibility_call(first);
        for d in &got {
            feed.hand_back(d.receipt).unwrap();
        }
        step(s(21)).await;
        assert_eq!(
            hand_back_within(&queue),
            [Some(s(20)), Some(second)],
            "first call {first:?}"
        );
    }
}

/// Pins that a hand-back call given up at the end of its pass is sent again
/// at once, after the tick that is due by then.
#[tokio::test(start_paused = true)]
async fn a_hand_back_given_up_at_the_period_is_sent_again_at_once() {
    let (feed, queue) = start_gated(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.inner.ready(Message::new("u"));
    let d = receive_n(&feed, &park, 1).await.remove(0);
    let start = Instant::now();
    queue.inner.delay_next_visibility_call(MINUTE);
    feed.hand_back(d.receipt).unwrap();
    settle_tasks().await;
    step(Duration::from_secs(21)).await;
    assert_eq!(handed_back(&queue.inner.visibility()), ["h0", "h0"]);
    let at: Vec<Duration> = queue
        .visibility_at
        .lock()
        .unwrap()
        .iter()
        .map(|t| *t - start)
        .collect();
    assert_eq!(at, [Duration::ZERO, Duration::from_secs(20)]);
}

/// Pins that a close waits for a hand-back given up at the end of its pass
/// and sent again, and counts it once when it is never answered.
#[tokio::test(start_paused = true)]
async fn a_close_waits_for_a_hand_back_given_up_at_the_period() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let d = receive_n(&feed, &park, 1).await.remove(0);
    queue.delay_next_visibility_call(MINUTE);
    feed.hand_back(d.receipt).unwrap();
    step(Duration::from_secs(10)).await;
    let mut closed = feed.close(Duration::from_secs(30)).unwrap();
    step(Duration::from_millis(9_900)).await;
    assert!(closed.try_outcome().is_none(), "settled before the resend");
    step(Duration::from_millis(200)).await;
    assert!(closed.try_outcome().unwrap().is_ok());

    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("u"));
    let d = receive_n(&feed, &park, 1).await.remove(0);
    queue.slow_hand_backs(Some(MINUTE));
    feed.hand_back(d.receipt).unwrap();
    step(Duration::from_secs(10)).await;
    let mut closed = feed.close(Duration::from_secs(30)).unwrap();
    step(Duration::from_secs(31)).await;
    let e = closed.try_outcome().unwrap().unwrap_err().reason;
    assert!(
        e.starts_with("the close reached its bound with 1 deletes, fails or hand-backs unsent")
            || e.starts_with("the close gave up 1 of"),
        "{e}"
    );
}

/// Pins that a hand-back given up at the end of its pass and queued again
/// is not sent once its unit's deadline passes behind a held extension.
#[tokio::test(start_paused = true)]
async fn a_requeued_hand_back_is_not_sent_past_its_deadline() {
    let s = Duration::from_secs;
    let (feed, queue) = start_gated(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.inner.ready(Message::new("kept"));
    queue.inner.ready(Message::new("other"));
    let got = receive_n(&feed, &park, 2).await;
    let kept = got[0].receipt;
    // SQS refuses the 20 s tick's extension of the other unit, so its
    // deadline stays at 60 s.
    queue
        .inner
        .fail_next_visibility(vec![failure(1, "InvalidParameterValue")]);
    renew_through(&feed, &[kept], s(38), "before").await;
    let other = feed.deadline_of(got[1].receipt).unwrap();
    queue.inner.slow_hand_backs(Some(MINUTE));
    feed.hand_back(got[1].receipt).unwrap();
    renew_through(&feed, &[kept], s(12), "during the pass").await;
    // The tick due at 40 s runs once the pass ends at 58 s, and its
    // extension is held past the other unit's deadline.
    queue.hold_visibility();
    renew_through(&feed, &[kept], s(8), "the pass ends").await;
    assert!(Instant::now() < other);
    renew_through(&feed, &[kept], other - Instant::now() + s(2), "held").await;
    queue.release_visibility();
    renew_through(&feed, &[kept], s(10), "after").await;
    assert_eq!(handed_back(&queue.inner.visibility()), ["h1"]);
}

/// Pins that a held unit the reader keeps renewing survives one slow
/// hand-back and, at the tick it delays, one slow extension. Regression for
/// #1452.
#[tokio::test(start_paused = true)]
async fn a_renewed_unit_survives_a_slow_hand_back_and_a_slow_extension() {
    let visibility = Duration::from_secs(30);
    let (feed, queue) = start(visibility);
    let [kept, other] = kept_and_other(&feed, &queue, visibility).await;
    queue.slow_hand_backs(Some(MINUTE));
    queue.delay_next_visibility_call(MINUTE);
    feed.hand_back(other).unwrap();
    renew_through(&feed, &[kept], 2 * visibility, "after the hand-back").await;
}

/// Pins that a hand-back given up at the end of its pass and queued again
/// is not sent once a close's bound has passed behind a slow extension.
#[tokio::test(start_paused = true)]
async fn a_requeued_hand_back_is_not_sent_past_a_close_bound() {
    let s = Duration::from_secs;
    let (feed, queue) = start_gated(MINUTE);
    let start = Instant::now();
    let [_kept, other] = kept_and_other(&feed, &queue.inner, MINUTE).await;
    queue.inner.slow_hand_backs(Some(MINUTE));
    queue.inner.delay_next_visibility_call(MINUTE);
    feed.hand_back(other).unwrap();
    step(s(22)).await;
    let closed_at = Instant::now();
    let _closed = feed.close(s(5)).unwrap();
    step(s(60)).await;
    let at = queue.visibility_at.lock().unwrap().clone();
    let calls: Vec<(bool, Duration)> = queue
        .inner
        .visibility()
        .into_iter()
        .zip(at)
        .map(|(call, t)| (call.iter().all(|e| e.seconds == 0), t - start))
        .collect();
    let bound = closed_at + s(5) - start;
    assert!(
        calls.iter().all(|&(zero, t)| !zero || t < bound),
        "bound {bound:?}, calls (hand-back, at) {calls:?}"
    );
}

/// Pins that a hand-back SQS fails with a transient error on every call,
/// given up at the end of its pass and queued again, is logged with the
/// error SQS returned.
#[test]
fn a_hand_back_given_up_at_the_period_is_logged_with_its_error() {
    let lines = spate_test::capture_logs(tracing::Level::DEBUG, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        rt.block_on(async {
            let (feed, queue) = start(MINUTE);
            let (waker, park) = wake_channel();
            feed.set_waker(waker);
            queue.ready(Message::new("u"));
            let d = receive_n(&feed, &park, 1).await.remove(0);
            queue.fail_hand_backs(Some(CallError::retryable("InternalError")));
            feed.hand_back(d.receipt).unwrap();
            step(Duration::from_secs(25)).await;
            assert!(handed_back(&queue.visibility()).len() > 2, "retried");
        });
    });
    assert!(
        lines
            .iter()
            .any(|l| l.contains("sending it again") && l.contains("InternalError")),
        "{lines:#?}"
    );
}
