//! Scripted feed for testing feed-reading sources.
//!
//! [`scripted_feed`] pairs a [`Feed`] implementation with a [`FeedScript`]
//! handle. The test scripts deliveries, redeliveries, duplicates,
//! dead-lettering, publish outcomes and depth reads, the source under test
//! reads through the trait, and the script observes every acknowledgement,
//! failure, renewal, publish and depth read. There is no queue and no clock,
//! and behavior is deterministic.

use spate_core::feed::{
    Confirmation, Confirmer, Delivery, Depth, DepthOf, DepthReading, Feed, FeedError,
    FeedErrorKind, FeedWaker, Receipt, Unit, confirmation, depth_reading,
};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Default)]
struct State {
    /// Scripted deliveries not yet received.
    queued: VecDeque<Delivery>,
    /// Deliveries received and not yet acknowledged, failed or lapsed.
    held: BTreeMap<Receipt, Delivery>,
    /// Deliveries so far per unit id that `deliver` scripted. `deliver`
    /// raises it, and so does `redeliver` of a receipt in `scripted`, so
    /// duplicates and their redeliveries share a count.
    counts: HashMap<String, u32>,
    /// Receipts of deliveries `deliver` scripted, and of their redeliveries.
    scripted: HashSet<Receipt>,
    next_receipt: u64,
    delivered: Vec<Delivery>,
    acknowledged: Vec<Receipt>,
    failed: Vec<(Receipt, String)>,
    renewed: Vec<Receipt>,
    gone: Vec<Receipt>,
    published: Vec<Unit>,
    dead_lettered: Vec<Unit>,
    publish_outcomes: VecDeque<FeedErrorKind>,
    hold_publishes: bool,
    held_publishes: Vec<(Unit, Confirmer)>,
    /// Scripted answers to depth reads of the feed, served first.
    feed_depths: VecDeque<Result<Depth, FeedError>>,
    /// Scripted answers to depth reads of the dead-letter destination.
    dead_letter_depths: VecDeque<Result<Depth, FeedError>>,
    feed_depth_reads: usize,
    dead_letter_depth_reads: usize,
    waker: Option<FeedWaker>,
}

impl State {
    fn queue(&mut self, unit: Unit, deliveries: u32) -> Receipt {
        self.next_receipt += 1;
        let receipt = Receipt(self.next_receipt);
        let delivery = Delivery::new(unit, receipt, deliveries);
        self.queued.push_back(delivery);
        receipt
    }

    /// Take the delivery `receipt` names out of the queue or the held set.
    fn lapse(&mut self, receipt: Receipt) -> Option<Delivery> {
        if let Some(delivery) = self.held.remove(&receipt) {
            return Some(delivery);
        }
        let at = self.queued.iter().position(|d| d.receipt == receipt)?;
        self.queued.remove(at)
    }

    /// Answer a call on `receipt`: run `f` on the held delivery, or record
    /// and return Gone.
    fn on_held(
        &mut self,
        receipt: Receipt,
        f: impl FnOnce(&mut State) -> Result<(), FeedError>,
    ) -> Result<(), FeedError> {
        if self.held.contains_key(&receipt) {
            f(self)
        } else {
            self.gone.push(receipt);
            Err(FeedError::new(
                FeedErrorKind::Gone,
                format!("{receipt} is not held by the scripted feed"),
            ))
        }
    }

    fn wake(&self) {
        if let Some(waker) = &self.waker {
            waker.wake();
        }
    }
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("queued", &self.queued.len())
            .field("held", &self.held.len())
            .field("acknowledged", &self.acknowledged.len())
            .finish_non_exhaustive()
    }
}

/// A [`Feed`] whose deliveries and publish outcomes are scripted by the
/// paired [`FeedScript`]. Build both with [`scripted_feed`].
///
/// `receive` hands out scripted deliveries in the order they were scripted.
/// `fail` records the report and lets go of the delivery, and delivers
/// nothing again on its own: the script decides what the feed does next. A
/// call with a receipt the feed no longer holds returns
/// [`Gone`](FeedErrorKind::Gone) and is recorded by
/// [`FeedScript::gone`]. Its [`backend`](Feed::backend) key is `scripted`,
/// so a pipeline it is passed to names `scripted` in its `feed:` section.
#[derive(Debug)]
pub struct ScriptedFeed {
    state: Arc<Mutex<State>>,
}

/// Scripting and observation handle for a [`ScriptedFeed`].
///
/// Every scripting call wakes the reader, so a source parked on the feed
/// picks the change up at once.
#[derive(Clone, Debug)]
pub struct FeedScript {
    state: Arc<Mutex<State>>,
}

/// A scripted feed and its handle.
///
/// # Examples
///
/// ```
/// use spate_core::feed::{Feed, FeedErrorKind, Unit};
///
/// let (feed, script) = spate_test::scripted_feed();
/// let first = script.deliver(Unit::new("day-1", b"2026-10-01".to_vec()));
/// let held = feed.receive(8).unwrap();
/// assert_eq!(held[0].receipt, first);
///
/// // The queue gives the unit to the reader again under a new receipt.
/// let second = script.redeliver(first);
/// let again = feed.receive(8).unwrap();
/// assert_eq!((again[0].receipt, again[0].deliveries), (second, 2));
///
/// // The first receipt has lapsed; the second acknowledges the unit.
/// let lapsed = feed.acknowledge(first).unwrap_err();
/// assert_eq!(lapsed.kind, FeedErrorKind::Gone);
/// feed.acknowledge(second).unwrap();
/// assert_eq!(script.acknowledged(), vec![second]);
/// assert_eq!(script.gone(), vec![first]);
/// ```
#[must_use]
pub fn scripted_feed() -> (ScriptedFeed, FeedScript) {
    let state = Arc::new(Mutex::new(State::default()));
    (
        ScriptedFeed {
            state: Arc::clone(&state),
        },
        FeedScript { state },
    )
}

impl FeedScript {
    /// Queue a delivery of `unit` for the next `receive`, and return its
    /// receipt. Delivering a unit whose id this method delivered before is a
    /// duplicate. [`Delivery::deliveries`] counts the deliveries this method
    /// scripted for an id and their redeliveries together; a unit published
    /// through the feed keeps its own count, whatever its id.
    pub fn deliver(&self, unit: Unit) -> Receipt {
        let mut state = self.lock();
        let count = state.counts.entry(unit.id.clone()).or_default();
        *count += 1;
        let deliveries = *count;
        let receipt = state.queue(unit, deliveries);
        state.scripted.insert(receipt);
        state.wake();
        receipt
    }

    /// Deliver the unit `receipt` names again under a new receipt, as a queue
    /// does when a lease lapses. The old receipt lapses: every later call
    /// with it returns [`Gone`](FeedErrorKind::Gone).
    ///
    /// # Panics
    ///
    /// If `receipt` names no delivery that is queued or held.
    pub fn redeliver(&self, receipt: Receipt) -> Receipt {
        let mut state = self.lock();
        let Some(delivery) = state.lapse(receipt) else {
            panic!("redeliver: {receipt} is neither queued nor held");
        };
        // A unit `deliver` scripted shares one count with its duplicates. A
        // published unit keeps its own count, whatever its id.
        let scripted = state.scripted.remove(&receipt);
        let deliveries = match state.counts.get_mut(&delivery.unit.id) {
            Some(count) if scripted => {
                *count += 1;
                *count
            }
            _ => delivery.deliveries + 1,
        };
        let receipt = state.queue(delivery.unit, deliveries);
        if scripted {
            state.scripted.insert(receipt);
        }
        state.wake();
        receipt
    }

    /// Move the unit `receipt` names to the dead-letter destination. The
    /// receipt lapses as for [`redeliver`](Self::redeliver).
    ///
    /// # Panics
    ///
    /// If `receipt` names no delivery that is queued or held.
    pub fn dead_letter(&self, receipt: Receipt) {
        let mut state = self.lock();
        let Some(delivery) = state.lapse(receipt) else {
            panic!("dead_letter: {receipt} is neither queued nor held");
        };
        state.dead_lettered.push(delivery.unit);
        state.wake();
    }

    /// Refuse the next unscripted `publish` with `kind` (repeat to script a
    /// sequence). A refused unit is never delivered.
    pub fn refuse_next_publish(&self, kind: FeedErrorKind) {
        let mut state = self.lock();
        state.publish_outcomes.push_back(kind);
        state.wake();
    }

    /// Leave every later `publish` unsettled until
    /// [`confirm_held_publishes`](Self::confirm_held_publishes).
    pub fn hold_publishes(&self) {
        let mut state = self.lock();
        state.hold_publishes = true;
        state.wake();
    }

    /// Confirm every held publish, queue a delivery of each unit in publish
    /// order, and stop holding publishes.
    pub fn confirm_held_publishes(&self) {
        let mut state = self.lock();
        state.hold_publishes = false;
        for (unit, confirmer) in std::mem::take(&mut state.held_publishes) {
            confirmer.confirm();
            state.queue(unit, 1);
        }
        state.wake();
    }

    /// Answer the next unscripted depth read of `of` with `answer`, ahead
    /// of the depth the feed's own state gives (repeat to script a
    /// sequence). A read with no scripted answer left reports the units
    /// queued as visible and the units held as in flight, or, for
    /// [`DeadLetters`](DepthOf::DeadLetters), the units
    /// [`dead_letter`](Self::dead_letter) moved as visible, all exact.
    ///
    /// # Examples
    ///
    /// ```
    /// use spate_core::feed::{Depth, DepthOf, Feed, FeedError, FeedErrorKind, Unit};
    ///
    /// let (feed, script) = spate_test::scripted_feed();
    /// let mut peer_held = Depth::new("the scripted feed", true);
    /// peer_held.in_flight = 1;
    /// script.answer_depth(DepthOf::Feed, Ok(peer_held.clone()));
    /// script.answer_depth(
    ///     DepthOf::Feed,
    ///     Err(FeedError::new(FeedErrorKind::Retryable, "throttled")),
    /// );
    ///
    /// let read = |of| feed.read_depth(of).unwrap().try_outcome().unwrap();
    /// assert_eq!(read(DepthOf::Feed).unwrap(), peer_held);
    /// assert_eq!(read(DepthOf::Feed).unwrap_err().kind, FeedErrorKind::Retryable);
    ///
    /// // With the script spent, the feed reports its own state.
    /// let receipt = script.deliver(Unit::new("a", b"a".to_vec()));
    /// script.dead_letter(receipt);
    /// assert!(read(DepthOf::Feed).unwrap().is_empty());
    /// assert_eq!(read(DepthOf::DeadLetters).unwrap().visible, 1);
    /// assert_eq!(script.depth_reads(DepthOf::Feed), 3);
    /// ```
    pub fn answer_depth(&self, of: DepthOf, answer: Result<Depth, FeedError>) {
        let mut state = self.lock();
        match of {
            DepthOf::DeadLetters => state.dead_letter_depths.push_back(answer),
            _ => state.feed_depths.push_back(answer),
        }
        state.wake();
    }

    /// How many depth reads of `of` the feed has answered.
    #[must_use]
    pub fn depth_reads(&self, of: DepthOf) -> usize {
        let state = self.lock();
        match of {
            DepthOf::DeadLetters => state.dead_letter_depth_reads,
            _ => state.feed_depth_reads,
        }
    }

    /// Every delivery `receive` handed out, in order.
    #[must_use]
    pub fn delivered(&self) -> Vec<Delivery> {
        self.lock().delivered.clone()
    }

    /// The receipts received and not yet acknowledged, failed or lapsed, in
    /// receipt order.
    #[must_use]
    pub fn held(&self) -> Vec<Receipt> {
        self.lock().held.keys().copied().collect()
    }

    /// Every receipt acknowledged, in order.
    #[must_use]
    pub fn acknowledged(&self) -> Vec<Receipt> {
        self.lock().acknowledged.clone()
    }

    /// Every receipt failed, with its reason, in order.
    #[must_use]
    pub fn failed(&self) -> Vec<(Receipt, String)> {
        self.lock().failed.clone()
    }

    /// Every receipt renewed, in order, one entry per call.
    #[must_use]
    pub fn renewed(&self) -> Vec<Receipt> {
        self.lock().renewed.clone()
    }

    /// Every receipt a call was answered [`Gone`](FeedErrorKind::Gone) for,
    /// in order.
    #[must_use]
    pub fn gone(&self) -> Vec<Receipt> {
        self.lock().gone.clone()
    }

    /// Every unit passed to `publish`, in order, whatever its outcome.
    #[must_use]
    pub fn published(&self) -> Vec<Unit> {
        self.lock().published.clone()
    }

    /// Every unit moved to the dead-letter destination by
    /// [`dead_letter`](Self::dead_letter), in order.
    #[must_use]
    pub fn dead_lettered(&self) -> Vec<Unit> {
        self.lock().dead_lettered.clone()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("feed script poisoned")
    }
}

impl ScriptedFeed {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("feed script poisoned")
    }
}

impl Feed for ScriptedFeed {
    fn backend(&self) -> &str {
        "scripted"
    }

    fn set_waker(&self, waker: FeedWaker) {
        let mut state = self.lock();
        if !state.queued.is_empty() {
            waker.wake();
        }
        state.waker = Some(waker);
    }

    fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
        let mut state = self.lock();
        let take = max.min(state.queued.len());
        let out: Vec<Delivery> = state.queued.drain(..take).collect();
        for delivery in &out {
            state.held.insert(delivery.receipt, delivery.clone());
            state.delivered.push(delivery.clone());
        }
        // A receive that took nothing sends no signal, or a reader at
        // capacity calling `receive(0)` would never park.
        if take > 0 && !state.queued.is_empty() {
            state.wake();
        }
        Ok(out)
    }

    fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.lock().on_held(receipt, |state| {
            state.renewed.push(receipt);
            Ok(())
        })
    }

    fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.lock().on_held(receipt, |state| {
            state.held.remove(&receipt);
            state.acknowledged.push(receipt);
            Ok(())
        })
    }

    fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError> {
        self.lock().on_held(receipt, |state| {
            state.held.remove(&receipt);
            state.failed.push((receipt, reason.to_owned()));
            Ok(())
        })
    }

    fn publish(&self, unit: Unit) -> Confirmation {
        let mut state = self.lock();
        state.published.push(unit.clone());
        let (confirmer, confirmation) = confirmation();
        if let Some(kind) = state.publish_outcomes.pop_front() {
            confirmer.reject(FeedError::new(kind, "refused by the feed script"));
        } else if state.hold_publishes {
            state.held_publishes.push((unit, confirmer));
        } else {
            confirmer.confirm();
            state.queue(unit, 1);
            state.wake();
        }
        confirmation
    }

    fn read_depth(&self, of: DepthOf) -> Option<DepthReading> {
        let mut state = self.lock();
        let answer = match of {
            DepthOf::DeadLetters => {
                state.dead_letter_depth_reads += 1;
                state.dead_letter_depths.pop_front().unwrap_or_else(|| {
                    let mut depth = Depth::new("the scripted dead-letter destination", false);
                    depth.visible = state.dead_lettered.len() as u64;
                    Ok(depth)
                })
            }
            _ => {
                state.feed_depth_reads += 1;
                state.feed_depths.pop_front().unwrap_or_else(|| {
                    let mut depth = Depth::new("the scripted feed", false);
                    depth.visible = state.queued.len() as u64;
                    depth.in_flight = state.held.len() as u64;
                    Ok(depth)
                })
            }
        };
        let (reporter, reading) = depth_reading();
        reporter.report(answer);
        Some(reading)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spate_core::feed::wake_channel;
    use std::time::Duration;

    fn unit(id: &str) -> Unit {
        Unit::new(id, id.as_bytes().to_vec())
    }

    fn kind(result: Result<(), FeedError>) -> FeedErrorKind {
        result.unwrap_err().kind
    }

    /// Pins that with no scripted answer the feed reports its own queue, held
    /// set and dead letters, exactly, each count told apart by a different
    /// number.
    #[test]
    fn a_scripted_feed_reports_depth_from_its_own_state() {
        let (feed, script) = scripted_feed();
        let read = |of| {
            feed.read_depth(of)
                .expect("the scripted feed reports depth")
                .try_outcome()
                .expect("settled at once")
                .expect("read")
        };
        assert!(read(DepthOf::Feed).is_empty());
        let held = script.deliver(unit("held"));
        let dead: Vec<Receipt> = ["dead-1", "dead-2", "dead-3"]
            .into_iter()
            .map(|id| script.deliver(unit(id)))
            .collect();
        script.deliver(unit("queued-1"));
        script.deliver(unit("queued-2"));
        let got = feed.receive(1).unwrap();
        assert_eq!(got[0].receipt, held);
        for receipt in dead {
            script.dead_letter(receipt);
        }
        let depth = read(DepthOf::Feed);
        assert_eq!(
            (depth.visible, depth.in_flight, depth.delayed, depth.unsent),
            (2, 1, 0, 0)
        );
        assert!(!depth.approximate);
        let dead = read(DepthOf::DeadLetters);
        assert_eq!((dead.visible, dead.in_flight), (3, 0));
        assert_eq!(script.depth_reads(DepthOf::Feed), 2);
        assert_eq!(script.depth_reads(DepthOf::DeadLetters), 1);
    }

    /// Pins that scripted depth answers are served in order per destination
    /// before the feed's own state, and that scripting one wakes the reader.
    #[test]
    fn a_scripted_depth_answer_sequence_is_served_in_order() {
        let (feed, script) = scripted_feed();
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        let mut first = Depth::new("q", true);
        first.delayed = 3;
        script.answer_depth(DepthOf::Feed, Ok(first.clone()));
        assert!(
            park.park(Duration::ZERO),
            "scripting did not wake the reader"
        );
        script.answer_depth(
            DepthOf::Feed,
            Err(FeedError::new(FeedErrorKind::Fatal, "gone")),
        );
        let mut dead = Depth::new("dlq", true);
        dead.visible = 2;
        script.answer_depth(DepthOf::DeadLetters, Ok(dead.clone()));

        let read = |of| feed.read_depth(of).unwrap().try_outcome().unwrap();
        assert_eq!(read(DepthOf::DeadLetters).unwrap(), dead);
        assert_eq!(read(DepthOf::Feed).unwrap(), first);
        assert_eq!(read(DepthOf::Feed).unwrap_err().kind, FeedErrorKind::Fatal);
        assert!(read(DepthOf::Feed).unwrap().is_empty());
        assert!(read(DepthOf::DeadLetters).unwrap().is_empty());
    }

    /// Pins that a redelivery lapses the old receipt, counts the delivery,
    /// and records every call on the lapsed receipt as Gone.
    #[test]
    fn a_redelivery_lapses_the_old_receipt() {
        let (feed, script) = scripted_feed();
        let first = script.deliver(unit("a"));
        assert_eq!(feed.receive(8).unwrap()[0].receipt, first);
        assert_eq!(script.held(), vec![first]);

        let second = script.redeliver(first);
        assert!(script.held().is_empty(), "the lapsed receipt is still held");
        let again = feed.receive(8).unwrap();
        assert_eq!(again.len(), 1);
        assert_eq!((again[0].receipt, again[0].deliveries), (second, 2));

        assert_eq!(kind(feed.renew(first)), FeedErrorKind::Gone);
        assert_eq!(kind(feed.acknowledge(first)), FeedErrorKind::Gone);
        assert_eq!(kind(feed.fail(first, "late")), FeedErrorKind::Gone);
        assert_eq!(script.gone(), vec![first, first, first]);
        feed.renew(second).unwrap();
        feed.acknowledge(second).unwrap();
        assert_eq!(script.renewed(), vec![second]);
        assert_eq!(script.acknowledged(), vec![second]);
        assert!(script.failed().is_empty());
        assert_eq!(script.delivered().len(), 2);
    }

    /// Pins that a duplicate of a held unit is a second delivery under its
    /// own receipt, and that both receipts stay valid.
    #[test]
    fn a_duplicate_is_held_beside_the_first_delivery() {
        let (feed, script) = scripted_feed();
        let first = script.deliver(unit("a"));
        let _ = feed.receive(8).unwrap();
        let second = script.deliver(unit("a"));
        let dup = feed.receive(8).unwrap();
        assert_eq!((dup[0].receipt, dup[0].deliveries), (second, 2));
        assert_eq!(script.held(), vec![first, second]);
        feed.acknowledge(first).unwrap();
        feed.fail(second, "poison").unwrap();
        assert_eq!(script.acknowledged(), vec![first]);
        assert_eq!(script.failed(), vec![(second, "poison".to_owned())]);
        assert!(script.held().is_empty());
        assert!(feed.receive(8).unwrap().is_empty(), "fail redelivered");
    }

    /// Pins that a dead-lettered unit lapses its receipt, whether queued or
    /// held, and is recorded.
    #[test]
    fn a_dead_lettered_unit_lapses_its_receipt() {
        let (feed, script) = scripted_feed();
        let queued = script.deliver(unit("q"));
        script.dead_letter(queued);
        assert!(feed.receive(8).unwrap().is_empty());

        let held = script.deliver(unit("h"));
        let _ = feed.receive(8).unwrap();
        script.dead_letter(held);
        assert_eq!(kind(feed.acknowledge(held)), FeedErrorKind::Gone);
        assert_eq!(script.dead_lettered(), vec![unit("q"), unit("h")]);
    }

    /// Pins publish outcomes: an unscripted publish is confirmed and
    /// delivered, a refused one is never delivered, and a held one settles
    /// only when confirmed and is delivered then.
    #[test]
    fn publishes_are_confirmed_refused_or_held_as_scripted() {
        let (feed, script) = scripted_feed();
        let mut plain = feed.publish(unit("plain"));
        assert!(matches!(plain.try_outcome(), Some(Ok(()))));

        script.refuse_next_publish(FeedErrorKind::Refused);
        let mut refused = feed.publish(unit("refused"));
        assert_eq!(
            refused.try_outcome().unwrap().unwrap_err().kind,
            FeedErrorKind::Refused
        );

        script.hold_publishes();
        let mut held = feed.publish(unit("held"));
        assert!(held.try_outcome().is_none());
        let ids = |d: Vec<Delivery>| d.into_iter().map(|d| d.unit.id).collect::<Vec<_>>();
        assert_eq!(ids(feed.receive(8).unwrap()), vec!["plain"]);

        script.confirm_held_publishes();
        assert!(matches!(held.try_outcome(), Some(Ok(()))));
        assert_eq!(ids(feed.receive(8).unwrap()), vec!["held"]);
        assert_eq!(
            script.published(),
            vec![unit("plain"), unit("refused"), unit("held")]
        );
    }

    /// Pins that scripting calls wake a parked reader, and that a waker set
    /// after a delivery was queued is woken at once.
    #[test]
    fn scripting_wakes_the_reader() {
        let (feed, script) = scripted_feed();
        let early = script.deliver(unit("early"));
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        assert!(
            park.park(Duration::ZERO),
            "set_waker missed a queued delivery"
        );

        let _ = feed.receive(8).unwrap();
        assert!(!park.park(Duration::ZERO));
        let again = script.redeliver(early);
        assert!(park.park(Duration::ZERO), "redeliver did not wake");
        script.dead_letter(again);
        assert!(park.park(Duration::ZERO), "dead_letter did not wake");
        script.hold_publishes();
        assert!(park.park(Duration::ZERO), "hold_publishes did not wake");
        let _held = feed.publish(unit("p"));
        assert!(!park.park(Duration::ZERO), "a held publish woke the reader");
        script.confirm_held_publishes();
        assert!(
            park.park(Duration::ZERO),
            "confirm_held_publishes did not wake"
        );
    }

    /// Pins that a receive which leaves deliveries queued leaves a wake
    /// pending, and that one which takes nothing sends none.
    #[test]
    fn a_partial_receive_leaves_a_wake_pending() {
        let (feed, script) = scripted_feed();
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        script.deliver(unit("a"));
        script.deliver(unit("b"));
        assert!(park.park(Duration::ZERO));
        assert_eq!(feed.receive(1).unwrap().len(), 1);
        assert!(
            park.park(Duration::ZERO),
            "a delivery is queued but the reader's park was not signalled"
        );
        assert!(feed.receive(0).unwrap().is_empty());
        assert!(
            !park.park(Duration::ZERO),
            "receive(0) at capacity re-signalled"
        );
    }

    /// Pins that a confirmed publish wakes the reader.
    #[test]
    fn a_confirmed_publish_wakes_the_reader() {
        let (feed, _script) = scripted_feed();
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        assert!(!park.park(Duration::ZERO));
        drop(feed.publish(unit("p")));
        assert!(park.park(Duration::ZERO), "publish did not wake the reader");
    }

    /// Pins that scripting a publish refusal wakes the reader, as every
    /// scripting call does.
    #[test]
    fn refusing_the_next_publish_wakes_the_reader() {
        let (feed, script) = scripted_feed();
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        assert!(!park.park(Duration::ZERO));
        script.refuse_next_publish(FeedErrorKind::Refused);
        assert!(
            park.park(Duration::ZERO),
            "refuse_next_publish did not wake the reader"
        );
    }

    /// Pins that a later `set_waker` replaces the waker: a scripting call
    /// signals only the newest one.
    #[test]
    fn a_later_set_waker_replaces_the_waker() {
        let (feed, script) = scripted_feed();
        let (first, first_park) = wake_channel();
        feed.set_waker(first);
        let (second, second_park) = wake_channel();
        feed.set_waker(second);
        script.deliver(unit("a"));
        assert!(
            second_park.park(Duration::ZERO),
            "deliver signalled the replaced waker"
        );
        assert!(!first_park.park(Duration::ZERO));
    }
}
