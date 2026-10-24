//! The work feed: the contract a queue-fed source reads its work through.
//!
//! A [`Feed`] carries [`Unit`]s from receipt to acknowledgement. A source
//! [`receive`](Feed::receive)s units, reads each one, and
//! [`acknowledge`](Feed::acknowledge)s it under the ack rule below. A lister
//! or a worker [`publish`](Feed::publish)es units into the same feed and
//! learns from a [`Confirmation`] when the feed has accepted one.
//! [`memory::MemoryFeed`] is the in-process feed for a single instance.
//!
//! # The ack rule
//!
//! Every backend and every source follows it:
//!
//! - A worker acknowledges a unit only after it has read the unit to its end
//!   and the sink has acknowledged every record in it.
//! - A unit read in several parts is acknowledged once, after every part
//!   meets that rule.
//! - A unit whose input was replaced while it was read may be acknowledged
//!   once a unit naming the input's current state is stored in the feed and
//!   the sink has acknowledged every record read from it. Past the source's
//!   re-enqueue cap it is acknowledged without one, and the source counts it.
//! - Otherwise, a unit whose read stops before its end leaves the feed
//!   through redelivery, until the feed moves it to its dead-letter
//!   destination.
//!
//! A feed delivers a unit at least once. A second delivery of a unit the
//! worker already holds, under a new [`Receipt`], is read again from its
//! start. No backend deduplicates by [`Unit::id`], and a source keys the
//! units it holds by receipt.
//!
//! # Threading
//!
//! A feed is driven synchronously from the pipeline's controller thread, so
//! no method waits on I/O. A backend runs its receives, lease renewals and
//! publishes on a task or thread it owns. [`receive`](Feed::receive) returns
//! what that task has already buffered, and the other methods hand their
//! request to it. The backend signals the [`FeedWaker`] it was given when a
//! unit becomes ready to receive, and after a `receive` that takes some units
//! and leaves others, so the source parks on the paired [`FeedPark`] between
//! polls. A `receive` that takes nothing sends no signal. Methods take
//! `&self`, so one `Arc<dyn Feed>` serves a reader and a publisher at once.
//!
//! # Ending a bounded job
//!
//! A backend that can count what it holds answers
//! [`read_depth`](Feed::read_depth) with a [`DepthReading`], checked like a
//! [`Confirmation`]. A source ends a bounded job through an [`EndCheck`]: the
//! feed and the reader must stay empty over a settling window of reads at
//! most [`END_CHECK_INTERVAL`] apart, and the feed's dead-letter destination
//! decides between a completed and a stalled job.
//!
//! # Stopping
//!
//! A backend that [`hands_back`](Feed::hands_back) takes part in a graceful
//! stop, in this order:
//!
//! 1. The source calls [`stop_receiving`](Feed::stop_receiving) when the stop
//!    begins, and reads on the units it holds, renewing them, until each is
//!    read to its end or [`STOP_RESERVE`] before the drain deadline.
//! 2. The source [`hand_back`](Feed::hand_back)s every unit it has not read
//!    to its end, so another reader can take it at once.
//! 3. The pipeline drains its sink and acknowledges what the sink has
//!    acknowledged, under the ack rule.
//! 4. The runtime calls [`close`](Feed::close), which sends what the backend
//!    still has queued and gives up what is held, within the time left before
//!    the deadline.
//!
//! A backend that does not hand back skips the reading, and its held units
//! return through redelivery.

use crate::error::ErrorClass;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

mod end_check;
pub mod memory;

pub use end_check::{END_CHECK_INTERVAL, EndAction, EndCheck, EndOutcome, MarkerRead};

/// The settling window a bounded job waits out when its `feed:` section sets
/// none: five minutes.
pub const DEFAULT_SETTLING_WINDOW: Duration = Duration::from_secs(5 * 60);

/// How long before the drain deadline a stopping worker stops reading the
/// units of a feed that [`hands_back`](Feed::hands_back): five seconds. The
/// hand-back, the sink drain and the feed's close run in this reserve.
pub const STOP_RESERVE: Duration = Duration::from_secs(5);

/// One unit of work: an identity and an opaque body the source decodes.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Unit {
    /// The unit's name, for logs and dead letters. Two units may share it,
    /// and no feed deduplicates by it.
    pub id: String,
    /// What the source reads to find the unit's input.
    pub body: Vec<u8>,
}

impl Unit {
    /// Build a unit.
    pub fn new(id: impl Into<String>, body: impl Into<Vec<u8>>) -> Unit {
        Unit {
            id: id.into(),
            body: body.into(),
        }
    }
}

/// Names one delivery of one unit.
///
/// The feed mints a new receipt for every delivery, so a redelivery or a
/// duplicate of a held unit carries a receipt the holder has not seen. A
/// receipt is valid until the delivery it names is acknowledged, failed or
/// replaced by a redelivery; a call with it after that returns
/// [`Gone`](FeedErrorKind::Gone).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Receipt(pub u64);

impl fmt::Display for Receipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "receipt {}", self.0)
    }
}

/// A unit handed to a reader, with the receipt that names this delivery.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Delivery {
    /// The unit delivered.
    pub unit: Unit,
    /// The receipt for this delivery, unique within the feed.
    pub receipt: Receipt,
    /// How many times the feed has delivered this published unit, this
    /// delivery included, so `1` on the first. Each publish starts its own
    /// count, whatever the unit's id.
    pub deliveries: u32,
}

impl Delivery {
    /// Build a delivery. Backends outside this crate construct it here, as the
    /// struct is `#[non_exhaustive]`.
    #[must_use]
    pub fn new(unit: Unit, receipt: Receipt, deliveries: u32) -> Delivery {
        Delivery {
            unit,
            receipt,
            deliveries,
        }
    }
}

/// How a caller must react to a [`FeedError`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FeedErrorKind {
    /// The receipt no longer names a delivery the caller holds: the unit was
    /// acknowledged, failed, or delivered again under a new receipt. The call
    /// changed nothing. The caller lets go of that delivery and does not
    /// retry.
    Gone,
    /// The feed refused the request, and sending it again fails the same way:
    /// a unit the feed cannot store, such as one over its size limit.
    Refused,
    /// Transient: the backend could not take the request now. Retrying may
    /// succeed.
    Retryable,
    /// Unrecoverable: misconfiguration, missing permissions, a feed that no
    /// longer exists. The pipeline must stop.
    Fatal,
}

/// Error from a feed backend.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("feed error ({kind:?}): {reason}")]
#[non_exhaustive]
pub struct FeedError {
    /// How the caller must react; see [`FeedErrorKind`].
    pub kind: FeedErrorKind,
    /// Human-readable cause.
    pub reason: String,
}

impl FeedError {
    /// Build an error. Backends outside this crate construct it here, as the
    /// struct is `#[non_exhaustive]`.
    pub fn new(kind: FeedErrorKind, reason: impl Into<String>) -> FeedError {
        FeedError {
            kind,
            reason: reason.into(),
        }
    }

    /// Map to the framework error taxonomy.
    ///
    /// [`Gone`](FeedErrorKind::Gone) and [`Refused`](FeedErrorKind::Refused)
    /// map to [`Fatal`](ErrorClass::Fatal) only as a backstop. A caller
    /// handles both before classifying, by letting go of the delivery or the
    /// unit.
    #[must_use]
    pub fn class(&self) -> ErrorClass {
        match self.kind {
            FeedErrorKind::Retryable => ErrorClass::Retryable,
            FeedErrorKind::Gone | FeedErrorKind::Refused | FeedErrorKind::Fatal => {
                ErrorClass::Fatal
            }
        }
    }
}

/// A queue of work units, read and filled from the controller thread.
///
/// The module docs state the ack rule every implementation and caller
/// follows. No method waits on I/O: a backend does its I/O on a task it owns
/// and answers each call from what that task has buffered. `Ok` from
/// [`renew`](Feed::renew), [`acknowledge`](Feed::acknowledge) and
/// [`fail`](Feed::fail) means the backend accepted the request. A backend
/// that cannot apply an accepted request leaves the unit to be delivered
/// again, which the ack rule allows.
///
/// Dyn-compatible; a source holds an `Arc<dyn Feed>`.
///
/// ```
/// use spate_core::feed::{Feed, Unit, memory::MemoryFeed, wake_channel};
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// let feed: Arc<dyn Feed> = Arc::new(MemoryFeed::new());
/// let (waker, park) = wake_channel();
/// feed.set_waker(waker);
///
/// let mut confirmation = feed.publish(Unit::new("day-1", b"2026-10-01".to_vec()));
/// assert!(matches!(confirmation.try_outcome(), Some(Ok(()))));
///
/// // The publish woke the reader, so this park returns at once.
/// assert!(park.park(Duration::from_secs(5)));
/// let delivery = feed.receive(8).unwrap().remove(0);
/// assert_eq!(delivery.unit.id, "day-1");
/// assert_eq!(delivery.deliveries, 1);
///
/// // Read the unit to its end; once the sink has acknowledged every record:
/// feed.acknowledge(delivery.receipt).unwrap();
/// ```
pub trait Feed: Send + Sync {
    /// The key that names this backend in a `feed:` section, such as
    /// `"memory"`. The runtime refuses a feed whose key differs from the
    /// one the pipeline's [`FeedSection`](crate::config::FeedSection) names.
    fn backend(&self) -> &str;

    /// Hand the backend the waker it signals when a unit becomes ready to
    /// receive, including one that arrived before this call, and after a
    /// [`receive`](Feed::receive) that takes some units and leaves others. A
    /// `receive` that takes nothing sends no signal. Called before the first
    /// `receive`; a later call replaces the waker.
    fn set_waker(&self, waker: FeedWaker);

    /// Up to `max` deliveries the backend has buffered, oldest first. Never
    /// waits; returns an empty list when nothing is ready.
    ///
    /// # Errors
    ///
    /// [`Retryable`](FeedErrorKind::Retryable) when the backend's receive
    /// task is failing transiently, [`Fatal`](FeedErrorKind::Fatal) when it
    /// cannot continue.
    fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError>;

    /// Mark a held unit as still in work.
    ///
    /// The backend extends the unit's lease on its own schedule, and only for
    /// units marked since its last extension. A caller renews every unit it
    /// holds from the controller thread. On a backend that leases units, a
    /// controller that stops running stops renewing, and its units return to
    /// the feed for another reader.
    ///
    /// # Errors
    ///
    /// [`Gone`](FeedErrorKind::Gone) when the receipt is no longer held.
    fn renew(&self, receipt: Receipt) -> Result<(), FeedError>;

    /// Remove a unit from the feed as done. Only the ack rule decides when a
    /// caller may send it.
    ///
    /// # Errors
    ///
    /// [`Gone`](FeedErrorKind::Gone) when the receipt is no longer held:
    /// the unit was delivered again, or this receipt was already used.
    /// [`Retryable`](FeedErrorKind::Retryable) when the request may be sent
    /// again.
    fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError>;

    /// Return a held unit that this reader could not process, so the feed
    /// delivers it again. At the feed's delivery cap the unit moves to its
    /// dead-letter destination instead. `reason` is for the feed's logs.
    ///
    /// # Errors
    ///
    /// [`Gone`](FeedErrorKind::Gone) when the receipt is no longer held.
    /// [`Retryable`](FeedErrorKind::Retryable) when the request may be sent
    /// again.
    fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError>;

    /// Add a unit to the feed. The returned [`Confirmation`] resolves once
    /// the feed has stored the unit or refused it. A unit published twice is
    /// delivered twice.
    fn publish(&self, unit: Unit) -> Confirmation;

    /// Count what the feed, or its dead-letter destination, holds. The
    /// returned [`DepthReading`] settles once the backend has read the
    /// counts. Never waits.
    ///
    /// `None` means the backend cannot count, and a source that ends a
    /// bounded job on this feed refuses it. The default returns `None`.
    fn read_depth(&self, _of: DepthOf) -> Option<DepthReading> {
        None
    }

    /// Whether a stopping worker reads this feed's units to the
    /// [`STOP_RESERVE`] and hands back the rest, as the module docs'
    /// "Stopping" order describes. Startup refuses a drain timeout no longer
    /// than the reserve on a feed that answers `true`. The default returns
    /// `false`.
    fn hands_back(&self) -> bool {
        false
    }

    /// Receive nothing more: the worker is stopping. Units already buffered
    /// are the backend's to give back. The default does nothing.
    fn stop_receiving(&self) {}

    /// Return a held unit for another reader to take at once. The
    /// delivery does not count as a failure, and the unit does not move to
    /// the dead-letter destination.
    ///
    /// The default accepts the call and leaves the unit held, so it returns
    /// through redelivery; a backend whose positions move only by commit
    /// keeps it.
    ///
    /// # Errors
    ///
    /// [`Gone`](FeedErrorKind::Gone) when the receipt is no longer held.
    /// [`Retryable`](FeedErrorKind::Retryable) when the backend could not
    /// take the request now; [`close`](Feed::close) gives the unit up.
    fn hand_back(&self, _receipt: Receipt) -> Result<(), FeedError> {
        Ok(())
    }

    /// Send what the backend still has queued and give up every unit still
    /// held, within `within`. Called once, after the pipeline's final
    /// commit. Never waits: the returned [`Confirmation`] settles when the
    /// backend is done, and a caller stops waiting for it after `within`.
    ///
    /// `None` means the backend has nothing to close. The default returns
    /// `None`.
    fn close(&self, _within: Duration) -> Option<Confirmation> {
        None
    }
}

/// Where the feed handed to
/// [`Source::configure_feed`](crate::source::Source::configure_feed) came
/// from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FeedOrigin {
    /// The runtime built it from the `feed:` section, so only the pipeline
    /// holds it.
    Section,
    /// Code passed it to `Pipeline::feed`, and may keep a handle that
    /// publishes into it.
    Given,
}

/// What a [`read_depth`](Feed::read_depth) counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DepthOf {
    /// The units the feed holds for its readers.
    Feed,
    /// The units the feed has moved to its dead-letter destination.
    DeadLetters,
}

/// The counts one [`read_depth`](Feed::read_depth) found.
///
/// Every count is `0` in a feed that holds nothing. A backend whose counts
/// are estimates sets [`approximate`](Depth::approximate).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Depth {
    /// What was read: the queue, topic or list, for logs and errors. Carries
    /// no credential.
    pub what: String,
    /// Units ready to be received.
    pub visible: u64,
    /// Units the feed will make visible later.
    pub delayed: u64,
    /// Units received by a reader and not yet acknowledged or failed.
    pub in_flight: u64,
    /// Acknowledges and fails this instance handed the backend that the
    /// backend has not yet applied.
    pub unsent: u64,
    /// Whether the counts are estimates that can lag the feed.
    pub approximate: bool,
}

impl Depth {
    /// A reading of `what` with every count `0`. Backends outside this crate
    /// build it here and set the counts, as the struct is
    /// `#[non_exhaustive]`.
    #[must_use]
    pub fn new(what: impl Into<String>, approximate: bool) -> Depth {
        Depth {
            what: what.into(),
            visible: 0,
            delayed: 0,
            in_flight: 0,
            unsent: 0,
            approximate,
        }
    }

    /// Whether every count is `0`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        // Compared one by one: a sum could wrap to zero.
        self.visible == 0 && self.delayed == 0 && self.in_flight == 0 && self.unsent == 0
    }
}

impl fmt::Display for Depth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {} visible, {} delayed, {} in flight, {} unsent",
            self.what, self.visible, self.delayed, self.in_flight, self.unsent
        )?;
        if self.approximate {
            f.write_str(" (approximate)")?;
        }
        Ok(())
    }
}

/// The backend's half of a [`DepthReading`]: settles one read.
///
/// Dropping it unsettled resolves the reading as
/// [`Retryable`](FeedErrorKind::Retryable).
#[derive(Debug)]
pub struct DepthReporter(tokio::sync::oneshot::Sender<Result<Depth, FeedError>>);

impl DepthReporter {
    /// Report the counts read, or why they could not be read.
    pub fn report(self, outcome: Result<Depth, FeedError>) {
        // A dropped `DepthReading` means nobody is waiting for the outcome.
        let _ = self.0.send(outcome);
    }
}

/// The outcome of one [`read_depth`](Feed::read_depth), once the backend
/// reports it.
///
/// Checked from a synchronous loop with
/// [`try_outcome`](DepthReading::try_outcome).
#[derive(Debug)]
pub struct DepthReading {
    rx: tokio::sync::oneshot::Receiver<Result<Depth, FeedError>>,
    // The settled outcome, kept so a check after settling repeats it.
    outcome: Option<Result<Depth, FeedError>>,
}

impl DepthReading {
    /// The outcome if the backend has settled the read, `None` while it is
    /// pending. Never waits. Once settled, every later call returns the same
    /// outcome.
    pub fn try_outcome(&mut self) -> Option<Result<Depth, FeedError>> {
        if self.outcome.is_none() {
            use tokio::sync::oneshot::error::TryRecvError;
            self.outcome = match self.rx.try_recv() {
                Ok(outcome) => Some(outcome),
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Closed) => Some(Err(FeedError::new(
                    FeedErrorKind::Retryable,
                    "the feed dropped a depth read before settling it",
                ))),
            };
        }
        self.outcome.clone()
    }
}

/// A depth read's two halves: the backend settles the [`DepthReporter`], and
/// the source checks the [`DepthReading`].
#[must_use]
pub fn depth_reading() -> (DepthReporter, DepthReading) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    (DepthReporter(tx), DepthReading { rx, outcome: None })
}

/// Wakes a reader parked on the paired [`FeedPark`].
///
/// Cheap to clone and safe to signal from any thread, because
/// [`wake`](FeedWaker::wake) never blocks. The channel behind it holds one
/// slot, so a burst of signals collapses into one wakeup, and a signal sent
/// while the reader is between its check and its park makes that park
/// return at once.
#[derive(Clone, Debug)]
pub struct FeedWaker(crossbeam_channel::Sender<()>);

impl FeedWaker {
    /// Wake the reader if it is parked, or make its next park return at once.
    /// Never blocks.
    pub fn wake(&self) {
        // Full slot means a wakeup is already pending; nothing to add.
        let _ = self.0.try_send(());
    }
}

/// The reader's half of [`wake_channel`]: where it waits for its feed.
#[derive(Debug)]
pub struct FeedPark {
    rx: crossbeam_channel::Receiver<()>,
    // Keeps the channel connected after every `FeedWaker` drops, so `park`
    // waits out its timeout instead of returning at once in a loop.
    _tx: crossbeam_channel::Sender<()>,
}

impl FeedPark {
    /// Wait until a [`FeedWaker`] signals or `timeout` passes. Returns
    /// `true` when a signal ended the wait, including one sent before the
    /// call.
    pub fn park(&self, timeout: Duration) -> bool {
        self.rx.recv_timeout(timeout).is_ok()
    }
}

/// A waker for a [`Feed`] and the park its reader waits on.
#[must_use]
pub fn wake_channel() -> (FeedWaker, FeedPark) {
    let (tx, rx) = crossbeam_channel::bounded(1);
    (FeedWaker(tx.clone()), FeedPark { rx, _tx: tx })
}

/// The backend's half of a [`Confirmation`]: settles one publish or close.
///
/// Dropping it unsettled resolves the confirmation as
/// [`Retryable`](FeedErrorKind::Retryable), since the feed may or may not
/// have finished.
#[derive(Debug)]
pub struct Confirmer(tokio::sync::oneshot::Sender<Result<(), FeedError>>);

impl Confirmer {
    /// Report that the feed has stored the unit, or finished closing.
    pub fn confirm(self) {
        // A dropped `Confirmation` means nobody is waiting for the outcome.
        let _ = self.0.send(Ok(()));
    }

    /// Report that the feed did not store the unit or close cleanly, and why.
    pub fn reject(self, error: FeedError) {
        // A dropped `Confirmation` means nobody is waiting for the outcome.
        let _ = self.0.send(Err(error));
    }
}

/// The outcome of one [`publish`](Feed::publish) or
/// [`close`](Feed::close), once the feed reports it.
///
/// Await it, or check it from a synchronous loop with
/// [`try_outcome`](Confirmation::try_outcome). `Ok(())` means the feed has
/// stored the unit, or finished closing.
#[derive(Debug)]
pub struct Confirmation {
    rx: tokio::sync::oneshot::Receiver<Result<(), FeedError>>,
    // The settled outcome, kept so a check after settling repeats it and the
    // receiver is never polled again once it has yielded.
    outcome: Option<Result<(), FeedError>>,
}

impl Confirmation {
    /// The outcome if the feed has settled the call, `None` while it is
    /// pending. Never waits. Once settled, every later call returns the same
    /// outcome.
    pub fn try_outcome(&mut self) -> Option<Result<(), FeedError>> {
        if self.outcome.is_none() {
            use tokio::sync::oneshot::error::TryRecvError;
            self.outcome = match self.rx.try_recv() {
                Ok(outcome) => Some(outcome),
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Closed) => Some(Err(unsettled())),
            };
        }
        self.outcome.clone()
    }
}

impl Future for Confirmation {
    type Output = Result<(), FeedError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(outcome) = &self.outcome {
            return Poll::Ready(outcome.clone());
        }
        let outcome = match Pin::new(&mut self.rx).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Ok(outcome)) => outcome,
            Poll::Ready(Err(_)) => Err(unsettled()),
        };
        self.outcome = Some(outcome.clone());
        Poll::Ready(outcome)
    }
}

/// A publish's or a close's two halves: the backend settles the
/// [`Confirmer`], and the caller reads the [`Confirmation`].
#[must_use]
pub fn confirmation() -> (Confirmer, Confirmation) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    (Confirmer(tx), Confirmation { rx, outcome: None })
}

fn unsettled() -> FeedError {
    FeedError::new(
        FeedErrorKind::Retryable,
        "the feed dropped the confirmation before settling it; the publish or close may or may \
         not have finished",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A feed with nothing in it, which only has to compile behind `dyn`.
    struct EmptyFeed;

    impl Feed for EmptyFeed {
        fn backend(&self) -> &str {
            "empty"
        }

        fn set_waker(&self, _waker: FeedWaker) {}

        fn receive(&self, _max: usize) -> Result<Vec<Delivery>, FeedError> {
            Ok(vec![])
        }

        fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
            Err(FeedError::new(FeedErrorKind::Gone, receipt.to_string()))
        }

        fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
            Err(FeedError::new(FeedErrorKind::Gone, receipt.to_string()))
        }

        fn fail(&self, receipt: Receipt, _reason: &str) -> Result<(), FeedError> {
            Err(FeedError::new(FeedErrorKind::Gone, receipt.to_string()))
        }

        fn publish(&self, _unit: Unit) -> Confirmation {
            let (confirmer, confirmation) = confirmation();
            confirmer.reject(FeedError::new(FeedErrorKind::Refused, "empty feed"));
            confirmation
        }
    }

    /// Pins that `Feed` is dyn-compatible and that one `Arc<dyn Feed>` can be
    /// shared across threads.
    #[test]
    fn feed_is_dyn_compatible() {
        fn shared<T: Send + Sync>(_: &T) {}
        let feed: std::sync::Arc<dyn Feed> = std::sync::Arc::new(EmptyFeed);
        shared(&feed);
        assert!(feed.receive(1).unwrap().is_empty());
        let err = feed.acknowledge(Receipt(7)).unwrap_err();
        assert_eq!(err.kind, FeedErrorKind::Gone);
        let mut confirmation = feed.publish(Unit::new("u", Vec::new()));
        assert_eq!(
            confirmation.try_outcome().unwrap().unwrap_err().kind,
            FeedErrorKind::Refused
        );
        assert!(feed.read_depth(DepthOf::Feed).is_none());
        assert!(!feed.hands_back());
        feed.stop_receiving();
        assert!(feed.hand_back(Receipt(7)).is_ok());
        assert!(feed.close(Duration::from_secs(1)).is_none());
    }

    /// Pins that a backend that does not override the stop methods neither
    /// hands back nor has anything to close, so a stop drains it as before.
    #[test]
    fn a_feed_neither_hands_back_nor_closes_by_default() {
        assert!(!EmptyFeed.hands_back());
        assert!(EmptyFeed.hand_back(Receipt(1)).is_ok());
        assert!(EmptyFeed.close(Duration::ZERO).is_none());
    }

    /// Pins that a backend that does not override `read_depth` reports no
    /// depth, for the feed and its dead letters alike.
    #[test]
    fn a_feed_reports_no_depth_by_default() {
        assert!(EmptyFeed.read_depth(DepthOf::Feed).is_none());
        assert!(EmptyFeed.read_depth(DepthOf::DeadLetters).is_none());
    }

    /// Pins that a depth read whose backend drops the reporter reads as
    /// Retryable, and that a settled read repeats its outcome.
    #[test]
    fn a_depth_reading_settles_once_and_repeats() {
        let (reporter, mut reading) = depth_reading();
        assert!(reading.try_outcome().is_none());
        drop(reporter);
        let kind = reading.try_outcome().unwrap().unwrap_err().kind;
        assert_eq!(kind, FeedErrorKind::Retryable);

        let (reporter, mut reading) = depth_reading();
        let mut depth = Depth::new("q", true);
        depth.delayed = 2;
        reporter.report(Ok(depth.clone()));
        assert_eq!(reading.try_outcome().unwrap().unwrap(), depth);
        assert_eq!(reading.try_outcome().unwrap().unwrap(), depth);
    }

    /// Pins that a depth reads as empty only when every count is zero, with
    /// no sum that could wrap, and how it prints.
    #[test]
    fn a_depth_is_empty_only_with_every_count_zero() {
        let mut depth = Depth::new("q", false);
        assert!(depth.is_empty());
        depth.visible = u64::MAX;
        depth.delayed = 1;
        assert!(!depth.is_empty());
        let mut depth = Depth::new("q", true);
        depth.unsent = 1;
        assert!(!depth.is_empty());
        assert_eq!(
            depth.to_string(),
            "q: 0 visible, 0 delayed, 0 in flight, 1 unsent (approximate)"
        );
        assert_eq!(
            Depth::new("q", false).to_string(),
            "q: 0 visible, 0 delayed, 0 in flight, 0 unsent"
        );
    }

    /// Pins that a publish whose backend drops the confirmer reads as
    /// Retryable, both checked and awaited.
    #[test]
    fn a_dropped_confirmer_reads_as_retryable() {
        let (confirmer, mut checked) = confirmation();
        assert!(checked.try_outcome().is_none());
        drop(confirmer);
        let kind = checked.try_outcome().unwrap().unwrap_err().kind;
        assert_eq!(kind, FeedErrorKind::Retryable);

        let (confirmer, awaited) = confirmation();
        drop(confirmer);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let kind = rt.block_on(awaited).unwrap_err().kind;
        assert_eq!(kind, FeedErrorKind::Retryable);
    }

    /// Pins that a settled outcome is returned by every later check and by an
    /// await after the check.
    #[test]
    fn a_settled_confirmation_repeats_its_outcome() {
        let (confirmer, mut confirmation) = confirmation();
        assert!(confirmation.try_outcome().is_none());
        confirmer.confirm();
        assert!(matches!(confirmation.try_outcome(), Some(Ok(()))));
        assert!(matches!(confirmation.try_outcome(), Some(Ok(()))));
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        assert!(rt.block_on(confirmation).is_ok());
    }

    /// Pins that a wake sent before the park ends it, and that a burst of
    /// wakes collapses into one.
    #[test]
    fn a_wake_before_the_park_ends_it_once() {
        let (waker, park) = wake_channel();
        assert!(!park.park(Duration::ZERO));
        waker.wake();
        waker.clone().wake();
        assert!(park.park(Duration::ZERO));
        assert!(!park.park(Duration::ZERO));
    }

    /// Pins that a park whose wakers have all dropped still waits out its
    /// timeout, so a reader that loses its feed does not spin.
    #[test]
    fn a_park_with_no_waker_left_waits_out_its_timeout() {
        let (waker, park) = wake_channel();
        drop(waker);
        let timeout = Duration::from_millis(20);
        let start = std::time::Instant::now();
        assert!(!park.park(timeout));
        assert!(
            start.elapsed() >= timeout,
            "park returned after {:?}, before its timeout",
            start.elapsed()
        );
    }

    /// Pins the error classes: only Retryable is retryable.
    #[test]
    fn only_a_retryable_feed_error_classes_as_retryable() {
        let class = |kind| FeedError::new(kind, "x").class();
        assert_eq!(class(FeedErrorKind::Retryable), ErrorClass::Retryable);
        assert_eq!(class(FeedErrorKind::Gone), ErrorClass::Fatal);
        assert_eq!(class(FeedErrorKind::Refused), ErrorClass::Fatal);
        assert_eq!(class(FeedErrorKind::Fatal), ErrorClass::Fatal);
    }

    /// Pins that an awaited outcome is repeated to a later check, though the
    /// await spent the receiver.
    #[test]
    fn an_awaited_confirmation_repeats_its_outcome_to_a_check() {
        let (confirmer, mut confirmation) = confirmation();
        confirmer.confirm();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        assert!(rt.block_on(&mut confirmation).is_ok());
        assert!(matches!(confirmation.try_outcome(), Some(Ok(()))));
    }
}
