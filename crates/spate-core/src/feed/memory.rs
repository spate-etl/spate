//! The in-process feed: a [`Feed`] for one pipeline instance.
//!
//! [`MemoryFeed`] keeps its units in memory and nothing across a restart. It
//! has no lease: a received unit stays held until it is acknowledged or
//! failed, so it is delivered again only after a [`fail`](Feed::fail). It
//! counts every delivery of a unit and moves a unit failed at its attempt cap,
//! the tenth delivery unless [`with_attempt_cap`](MemoryFeed::with_attempt_cap)
//! or a section's `attempt_cap` sets another, to an in-memory dead-letter list.
//! Its [`counts`](MemoryFeed::counts) are exact.

use super::{
    Confirmation, Delivery, Feed, FeedError, FeedErrorKind, FeedWaker, Receipt, Unit, confirmation,
};
use crate::config::{ConfigError, FeedSection};
use serde::Deserialize;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, MutexGuard};

/// Deliveries after which a failed unit is dead-lettered, unless set.
const ATTEMPT_CAP: NonZeroU32 = NonZeroU32::new(10).expect("non-zero");

/// The key that names this backend in a `feed:` section.
const BACKEND: &str = "memory";

/// What a [`MemoryFeed`] holds at one moment.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct FeedCounts {
    /// Units waiting to be received.
    pub visible: u64,
    /// Units received and not yet acknowledged or failed.
    pub held: u64,
    /// Units moved to the dead-letter list.
    pub dead_lettered: u64,
    /// Units acknowledged as done.
    pub acknowledged: u64,
}

/// A [`Feed`] held in this process, for a single instance.
///
/// Clones share one feed, so a lister and a reader can each hold one. Units
/// are delivered in the order they were published, and a failed unit goes
/// to the back of the queue.
#[derive(Clone, Debug)]
pub struct MemoryFeed {
    state: Arc<Mutex<State>>,
}

impl Default for MemoryFeed {
    fn default() -> MemoryFeed {
        MemoryFeed::with_attempt_cap(ATTEMPT_CAP)
    }
}

#[derive(Debug)]
struct State {
    /// Units waiting to be received, with the deliveries each has had.
    visible: VecDeque<(Unit, u32)>,
    held: HashMap<Receipt, (Unit, u32)>,
    dead: Vec<Unit>,
    acknowledged: u64,
    next_receipt: u64,
    waker: Option<FeedWaker>,
    /// The delivery on which a failed unit is dead-lettered.
    attempt_cap: u32,
}

impl State {
    fn wake(&self) {
        if let Some(waker) = &self.waker {
            waker.wake();
        }
    }
}

impl MemoryFeed {
    /// An empty feed that dead-letters a unit failed on its tenth delivery.
    #[must_use]
    pub fn new() -> MemoryFeed {
        MemoryFeed::default()
    }

    /// An empty feed that dead-letters a unit failed on delivery `cap`.
    #[must_use]
    pub fn with_attempt_cap(cap: NonZeroU32) -> MemoryFeed {
        MemoryFeed {
            state: Arc::new(Mutex::new(State {
                visible: VecDeque::new(),
                held: HashMap::new(),
                dead: Vec::new(),
                acknowledged: 0,
                next_receipt: 0,
                waker: None,
                attempt_cap: cap.get(),
            })),
        }
    }

    /// An empty feed built from a `feed: { memory: {} }` section, with the
    /// section's `attempt_cap`, else 10.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Component`] when the section names another backend,
    /// when the `memory` body holds a key, or when the section sets
    /// `location`.
    ///
    /// # Examples
    ///
    /// ```
    /// use spate_core::config::{ComponentConfig, FeedSection, YamlValue};
    /// use spate_core::feed::memory::MemoryFeed;
    /// use std::num::NonZeroU32;
    ///
    /// let body = YamlValue::Mapping(Default::default());
    /// let mut section = FeedSection::new(ComponentConfig::new("memory", body));
    /// section.attempt_cap = NonZeroU32::new(3);
    ///
    /// let feed = MemoryFeed::from_section(&section).unwrap();
    /// assert_eq!(feed.counts().visible, 0);
    /// ```
    pub fn from_section(section: &FeedSection) -> Result<MemoryFeed, ConfigError> {
        // An empty body: every key the in-process feed takes is a common one.
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Body {}

        let backend = section.backend();
        if backend.type_tag() != BACKEND {
            return Err(ConfigError::Component {
                context: "feed".into(),
                message: format!(
                    "the in-process feed is built from `feed: {{ {BACKEND}: {{}} }}`, but the \
                     section names `{}`",
                    backend.type_tag()
                ),
            });
        }
        let _: Option<Body> = backend.deserialize_into()?;
        if section.location.is_some() {
            return Err(ConfigError::Component {
                context: "feed".into(),
                message: "the in-process feed keeps no state outside the process, so it takes \
                          no `location`"
                    .into(),
            });
        }
        Ok(MemoryFeed::with_attempt_cap(
            section.attempt_cap.unwrap_or(ATTEMPT_CAP),
        ))
    }

    /// The units this feed holds, by state.
    #[must_use]
    pub fn counts(&self) -> FeedCounts {
        let state = self.lock();
        FeedCounts {
            visible: state.visible.len() as u64,
            held: state.held.len() as u64,
            dead_lettered: state.dead.len() as u64,
            acknowledged: state.acknowledged,
        }
    }

    /// The units moved to the dead-letter list, in the order they moved.
    #[must_use]
    pub fn dead_letters(&self) -> Vec<Unit> {
        self.lock().dead.clone()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // Nothing panics while holding the lock, so a poisoned lock still
        // guards consistent state.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn gone(receipt: Receipt) -> FeedError {
    FeedError::new(
        FeedErrorKind::Gone,
        format!("{receipt} is not held by the in-process feed"),
    )
}

impl Feed for MemoryFeed {
    fn backend(&self) -> &str {
        BACKEND
    }

    fn set_waker(&self, waker: FeedWaker) {
        let mut state = self.lock();
        // A unit published before the waker arrived still needs a wakeup.
        if !state.visible.is_empty() {
            waker.wake();
        }
        state.waker = Some(waker);
    }

    fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
        let mut state = self.lock();
        let take = max.min(state.visible.len());
        let mut out = Vec::with_capacity(take);
        for _ in 0..take {
            let Some((unit, deliveries)) = state.visible.pop_front() else {
                break;
            };
            state.next_receipt += 1;
            let receipt = Receipt(state.next_receipt);
            let deliveries = deliveries + 1;
            state.held.insert(receipt, (unit.clone(), deliveries));
            out.push(Delivery::new(unit, receipt, deliveries));
        }
        // A receive that took nothing sends no signal, or a reader at
        // capacity calling `receive(0)` would never park.
        if take > 0 && !state.visible.is_empty() {
            state.wake();
        }
        Ok(out)
    }

    fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
        if self.lock().held.contains_key(&receipt) {
            Ok(())
        } else {
            Err(gone(receipt))
        }
    }

    fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
        let mut state = self.lock();
        if state.held.remove(&receipt).is_none() {
            return Err(gone(receipt));
        }
        state.acknowledged += 1;
        Ok(())
    }

    fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError> {
        let mut state = self.lock();
        let Some((unit, deliveries)) = state.held.remove(&receipt) else {
            return Err(gone(receipt));
        };
        if deliveries >= state.attempt_cap {
            let id = unit.id.clone();
            state.dead.push(unit);
            // Logged after the guard drops: a subscriber that panics must not
            // unwind through the lock.
            drop(state);
            tracing::warn!(
                unit = %id,
                deliveries,
                reason,
                "in-process feed dead-lettered a unit at its delivery cap"
            );
        } else {
            state.visible.push_back((unit, deliveries));
            state.wake();
        }
        Ok(())
    }

    fn publish(&self, unit: Unit) -> Confirmation {
        let mut state = self.lock();
        state.visible.push_back((unit, 0));
        state.wake();
        let (confirmer, confirmation) = confirmation();
        confirmer.confirm();
        confirmation
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::wake_channel;
    use std::time::Duration;

    fn unit(id: &str) -> Unit {
        Unit::new(id, id.as_bytes().to_vec())
    }

    fn receive_one(feed: &MemoryFeed) -> Delivery {
        let mut got = feed.receive(usize::MAX).unwrap();
        assert_eq!(got.len(), 1, "expected exactly one delivery, got {got:?}");
        got.remove(0)
    }

    /// Pins the attempt cap: a unit failed on deliveries 1 through 9 is
    /// delivered again with its count raised by one, and one failed on its
    /// tenth delivery moves to the dead-letter list and is never delivered
    /// again.
    #[test]
    fn a_unit_at_the_cap_moves_to_the_in_memory_dead_letter_list() {
        let feed = MemoryFeed::new();
        drop(feed.publish(unit("poison")));
        // The cap is written out: a loop bound read from the constant would
        // follow a changed cap.
        for i in 1..=10 {
            let delivery = receive_one(&feed);
            assert_eq!(delivery.deliveries, i, "delivery count on attempt {i}");
            assert_eq!(delivery.unit, unit("poison"));
            feed.fail(delivery.receipt, "unreadable").unwrap();
            if i < 10 {
                assert_eq!(feed.counts().visible, 1, "re-queued after attempt {i}");
                assert_eq!(feed.counts().dead_lettered, 0);
            }
        }
        assert_eq!(
            feed.counts(),
            FeedCounts {
                visible: 0,
                held: 0,
                dead_lettered: 1,
                acknowledged: 0,
            }
        );
        assert_eq!(feed.dead_letters(), vec![unit("poison")]);
        assert!(feed.receive(usize::MAX).unwrap().is_empty());
    }

    /// Pins that the attempt cap a section sets is the delivery that
    /// dead-letters a failed unit: with `attempt_cap: 3`, failures on
    /// deliveries 1 and 2 re-queue the unit and the failure on delivery 3
    /// moves it to the dead-letter list.
    #[test]
    fn the_attempt_cap_sets_the_delivery_that_dead_letters() {
        let section: FeedSection =
            serde_yaml::from_str("memory: {}\nattempt_cap: 3\n").expect("section");
        let feed = MemoryFeed::from_section(&section).unwrap();
        drop(feed.publish(unit("poison")));
        for i in 1..=2 {
            let delivery = receive_one(&feed);
            assert_eq!(delivery.deliveries, i);
            feed.fail(delivery.receipt, "unreadable").unwrap();
            assert_eq!(feed.counts().visible, 1, "re-queued after attempt {i}");
        }
        let third = receive_one(&feed);
        assert_eq!(third.deliveries, 3);
        feed.fail(third.receipt, "unreadable").unwrap();
        assert_eq!(feed.counts().dead_lettered, 1, "dead-lettered on attempt 3");
        assert_eq!(feed.counts().visible, 0);
    }

    /// Pins that `from_section` refuses a `location`, which a caller can set
    /// on a section built in code, as the `feed` component error its
    /// `# Errors` names, without printing the value.
    #[test]
    fn from_section_refuses_a_location() {
        let mut section: FeedSection = serde_yaml::from_str("memory: {}\n").expect("section");
        section.location = Some("s3://u:hunter2@ops/jobs/a/?X-Amz-Signature=hunter3".to_owned());
        let err = MemoryFeed::from_section(&section).unwrap_err();
        assert!(
            matches!(&err, ConfigError::Component { context, message }
                if context == "feed" && message.contains("`location`")),
            "{err:?}"
        );
        for shown in [err.to_string(), format!("{err:?}")] {
            assert!(
                !shown.contains("hunter2") && !shown.contains("ops/jobs/a"),
                "the location must not print: {shown}"
            );
        }
    }

    /// Pins that `from_section` refuses a section naming another backend, and
    /// a key in the `memory` body.
    #[test]
    fn from_section_refuses_another_backend_and_a_body_key() {
        let section: FeedSection = serde_yaml::from_str("other: {}\n").expect("section");
        let err = MemoryFeed::from_section(&section).unwrap_err().to_string();
        assert!(err.contains("names `other`"), "{err}");

        let section: FeedSection = serde_yaml::from_str("memory: { depth: 4 }\n").expect("section");
        let err = MemoryFeed::from_section(&section).unwrap_err().to_string();
        assert!(err.starts_with("feed.memory"), "{err}");
        assert!(err.contains("depth"), "{err}");

        let bare: FeedSection = serde_yaml::from_str("memory:\n").expect("section");
        assert!(MemoryFeed::from_section(&bare).is_ok());
    }

    /// Pins that acknowledging removes the unit for good, and that its
    /// receipt then answers Gone to every call.
    #[test]
    fn an_acknowledged_unit_leaves_and_its_receipt_is_gone() {
        let feed = MemoryFeed::new();
        drop(feed.publish(unit("done")));
        let delivery = receive_one(&feed);
        assert_eq!(feed.counts().held, 1);
        feed.renew(delivery.receipt).unwrap();
        feed.acknowledge(delivery.receipt).unwrap();
        assert_eq!(
            feed.counts(),
            FeedCounts {
                visible: 0,
                held: 0,
                dead_lettered: 0,
                acknowledged: 1,
            }
        );
        let kind = |r: Result<(), FeedError>| r.unwrap_err().kind;
        assert_eq!(
            kind(feed.acknowledge(delivery.receipt)),
            FeedErrorKind::Gone
        );
        assert_eq!(kind(feed.renew(delivery.receipt)), FeedErrorKind::Gone);
        assert_eq!(kind(feed.fail(delivery.receipt, "x")), FeedErrorKind::Gone);
        assert_eq!(feed.counts().acknowledged, 1);
        assert!(feed.receive(usize::MAX).unwrap().is_empty());
    }

    /// Pins that the feed does not deduplicate by id: a unit published twice
    /// is delivered twice, under two receipts, each needing its own
    /// acknowledgement.
    #[test]
    fn a_unit_published_twice_is_delivered_twice() {
        let feed = MemoryFeed::new();
        drop(feed.publish(unit("twice")));
        drop(feed.publish(unit("twice")));
        let got = feed.receive(usize::MAX).unwrap();
        assert_eq!(got.len(), 2);
        assert_ne!(got[0].receipt, got[1].receipt);
        assert!(
            got.iter()
                .all(|d| d.unit == unit("twice") && d.deliveries == 1)
        );
        feed.acknowledge(got[0].receipt).unwrap();
        assert_eq!(feed.counts().held, 1);
        feed.acknowledge(got[1].receipt).unwrap();
        assert_eq!(feed.counts().acknowledged, 2);
    }

    /// Pins that `publish` settles its confirmation before returning and
    /// wakes a reader parked on the feed, including one whose waker was set
    /// after the publish.
    #[test]
    fn a_publish_is_confirmed_at_once_and_wakes_the_reader() {
        let feed = MemoryFeed::new();
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        assert!(!park.park(Duration::ZERO));
        let mut confirmation = feed.publish(unit("one"));
        assert!(matches!(confirmation.try_outcome(), Some(Ok(()))));
        assert!(park.park(Duration::ZERO), "publish did not wake the reader");

        let late = MemoryFeed::new();
        drop(late.publish(unit("early")));
        let (waker, park) = wake_channel();
        late.set_waker(waker);
        assert!(park.park(Duration::ZERO), "set_waker missed a waiting unit");
    }

    /// Pins FIFO order, the `max` bound on one receive, and that a failed
    /// unit below the cap re-queues behind units already waiting and wakes
    /// the reader.
    #[test]
    fn units_arrive_in_publish_order_and_a_failed_unit_goes_to_the_back() {
        let feed = MemoryFeed::new();
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        for id in ["a", "b", "c"] {
            drop(feed.publish(unit(id)));
        }
        let first = feed.receive(1).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].unit.id, "a");
        assert!(park.park(Duration::ZERO));
        feed.fail(first[0].receipt, "retry later").unwrap();
        assert!(park.park(Duration::ZERO), "fail did not wake the reader");
        let ids: Vec<_> = feed
            .receive(usize::MAX)
            .unwrap()
            .into_iter()
            .map(|d| (d.unit.id, d.deliveries))
            .collect();
        assert_eq!(
            ids,
            vec![
                ("b".to_owned(), 1),
                ("c".to_owned(), 1),
                ("a".to_owned(), 2)
            ]
        );
    }

    /// Pins that a receive which leaves units visible leaves a wake pending,
    /// so a reader that took only what it had room for does not wait out
    /// its park.
    #[test]
    fn a_partial_receive_leaves_a_wake_pending() {
        let feed = MemoryFeed::new();
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        for id in ["a", "b"] {
            drop(feed.publish(unit(id)));
        }
        assert!(park.park(Duration::ZERO));
        assert_eq!(feed.receive(1).unwrap().len(), 1);
        assert_eq!(feed.counts().visible, 1);
        assert!(
            park.park(Duration::ZERO),
            "a unit is visible but the reader's park was not signalled"
        );
    }

    /// Pins that a receive which takes nothing sends no signal, so a reader
    /// at capacity calling `receive(0)` parks instead of spinning.
    #[test]
    fn an_empty_receive_at_capacity_does_not_wake() {
        let feed = MemoryFeed::new();
        let (waker, park) = wake_channel();
        feed.set_waker(waker);
        drop(feed.publish(unit("a")));
        assert!(park.park(Duration::ZERO));
        assert!(feed.receive(0).unwrap().is_empty());
        assert!(
            !park.park(Duration::ZERO),
            "receive(0) at capacity re-signalled"
        );
    }

    /// Pins that a unit failed at the cap is on the dead-letter list, and
    /// the lock is not poisoned, when the warning about it unwinds.
    #[test]
    fn a_panicking_subscriber_in_fail_at_the_cap_keeps_the_unit() {
        use tracing_subscriber::layer::SubscriberExt;

        struct PanicOnWarn;
        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for PanicOnWarn {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                if *event.metadata().level() == tracing::Level::WARN {
                    panic!("warning emitted");
                }
            }
        }

        let feed = MemoryFeed::new();
        drop(feed.publish(unit("poison")));
        for _ in 1..10 {
            let delivery = receive_one(&feed);
            feed.fail(delivery.receipt, "x").unwrap();
        }
        let last = receive_one(&feed);
        let subscriber = tracing_subscriber::registry().with(PanicOnWarn);
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tracing::subscriber::with_default(subscriber, || feed.fail(last.receipt, "x"))
        }));
        assert!(unwound.is_err(), "the subscriber did not panic");
        assert_eq!(
            feed.counts(),
            FeedCounts {
                visible: 0,
                held: 0,
                dead_lettered: 1,
                acknowledged: 0,
            }
        );
        assert!(
            !feed.state.is_poisoned(),
            "the warning unwound through the lock"
        );
    }

    /// Pins that a later `set_waker` replaces the waker: a publish signals
    /// only the newest one.
    #[test]
    fn a_later_set_waker_replaces_the_waker() {
        let feed = MemoryFeed::new();
        let (first, first_park) = wake_channel();
        feed.set_waker(first);
        let (second, second_park) = wake_channel();
        feed.set_waker(second);
        drop(feed.publish(unit("a")));
        assert!(
            second_park.park(Duration::ZERO),
            "publish signalled the replaced waker"
        );
        assert!(!first_park.park(Duration::ZERO));
    }
}
