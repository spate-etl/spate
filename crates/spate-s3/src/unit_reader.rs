//! The feed read path: [`UnitReader`] reads work units from a [`Feed`] and
//! acknowledges each under the feed's ack rule.
//!
//! A unit's body is a [`SplitDescriptor`](crate::split::SplitDescriptor)
//! naming one object, whole or one byte range of it, or several objects read
//! whole in turn on one lane, or a storage
//! notification, which [`decode`](crate::unit::decode) reads into one part per
//! created object. A notification with no object to read is acknowledged, and
//! one the source cannot read is failed back. A whole-object part over an
//! object the lister would cut is read in the ranges
//! [`parts_of`](crate::unit::parts_of) yields, each opened when there is room
//! for it. Every part opens its own lane and partition and resumes from no
//! committed position, so a redelivery and a duplicate are each read in
//! full. The reader keys deliveries by a sequence number and lanes by
//! partition, and never keys anything by unit id.
//!
//! Each lane in progress takes one of `units_held`, and a delivery whose
//! lanes have all retired, or that has no part, takes one until its
//! acknowledge settles. The pending parts of held deliveries open, oldest
//! delivery first, before the reader receives again, so the deliveries held
//! never exceed `units_held`.
//!
//! A lane retires once it has decided end-of-input at the terminal watermark
//! `T` and the acknowledged watermark `W` that [`commit`](UnitReader::commit)
//! receives equals `T`, or at once when the lane emitted nothing. A unit is
//! acknowledged once every part has opened and retired, so a notification
//! naming several objects is acknowledged once after all of them. An
//! object-level failure on any lane fails the unit back to the feed, which
//! delivers it again until it dead-letters it.
//!
//! A notification unit whose key was overwritten while it was read is the
//! exception. The key's remaining parts are dropped, and the reader publishes
//! a notification unit for the key at its current ETag, with a re-enqueue
//! count one higher, under the old unit's id. The old unit is acknowledged
//! once the feed confirms that publish and every part has retired. At the
//! re-enqueue cap the reader publishes nothing and counts the key. A publish
//! the feed refuses, or cannot confirm, fails the unit back.
//!
//! On a feed that [hands back](Feed::hands_back), a stop reads on. The reader
//! calls [`Feed::stop_receiving`] once when the stop begins and receives
//! nothing more. It keeps reading and renewing what it holds, and opens
//! pending ranges into the room left. At [`stop_reading`](UnitReader::stop_reading)
//! it keeps each unit read to its end (every part opened, every lane at
//! end-of-input, no failure, no unconfirmed re-enqueue) for the final commit,
//! and hands back the rest. On any other feed the stop does none of this.
//! [`spate_core::feed`]'s "Stopping" section gives the order.
//!
//! A bounded job's reader holds a [`JobEnd`]. Each poll that finds the reader
//! holding no delivery, lane or unsent request advances its end check, and
//! each poll that finds it holding any sends the check back to its start, so
//! the job ends only on reads taken while this worker had nothing in hand.

use crate::fetch::Overwrite;
use crate::fetch::{FetcherParams, run_fetcher};
use crate::framer::FramerFactory;
use crate::job_end::JobEnd;
use crate::lane::S3Lane;
use crate::lane_ctx::{LANE_HANDOFF_CHUNKS, LaneLabel, PoisonReport, SplitTracker};
use crate::metrics::S3Metrics;
use crate::split::Packing;
use crate::unit::{Cursor, Decoded, Origin, Part, Scope, decode, re_enqueue_body};
use spate_core::checkpoint::AckIssuer;
use spate_core::error::SourceError;
use spate_core::feed::{
    Confirmation, Delivery, Feed, FeedError, FeedErrorKind, FeedPark, FeedWaker, Receipt, Unit,
    wake_channel,
};
use spate_core::record::PartitionId;
use spate_core::source::{DrainBarrier, LaneId, SourceEvent};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Units held at once when neither the `feed:` section's `units_held` nor a
/// pipeline thread count gives one. The reader receives only while it holds
/// fewer.
pub(crate) const UNITS_HELD: NonZeroUsize = NonZeroUsize::new(96).expect("non-zero");

/// What the reader needs to open a lane, fixed at `open`.
pub(crate) struct LaneParts {
    pub(crate) store: Arc<dyn object_store::ObjectStore>,
    pub(crate) handle: tokio::runtime::Handle,
    pub(crate) issuer: AckIssuer,
    pub(crate) make_framer: FramerFactory,
    /// Which whole objects are read in ranges, and which ranged units the
    /// framer and the codec can read.
    pub(crate) packing: Packing,
    pub(crate) chunk_bytes: usize,
    pub(crate) range_bytes: usize,
    pub(crate) retry_base: Duration,
    pub(crate) metrics: Option<S3Metrics>,
    /// The re-enqueue count at which an overwritten notification unit is
    /// acknowledged without a re-enqueue.
    pub(crate) re_enqueue_cap: u32,
}

/// One delivery the reader holds.
struct HeldUnit {
    receipt: Receipt,
    /// The unit's id, for logs.
    unit: String,
    /// Parts not yet opened. A cursor leaves once it is exhausted.
    pending: VecDeque<Cursor>,
    /// The partitions of the delivery's lanes that have not left.
    lanes: BTreeSet<PartitionId>,
    /// A lane hit poison, and this reason waits for a `fail` the feed
    /// accepts.
    failing: Option<String>,
    /// The keys an overwrite cut short, each with its re-enqueue.
    overwritten: BTreeMap<String, ReEnqueue>,
}

/// The re-enqueue of one overwritten key.
enum ReEnqueue {
    /// Published, and the feed has not confirmed it.
    Publishing(Confirmation),
    /// Confirmed, or not published: the object was emptied, or the unit was
    /// at the re-enqueue cap.
    Done,
}

impl HeldUnit {
    /// Every part has opened and retired, and every re-enqueue is
    /// confirmed, so the unit can be acknowledged.
    fn finished(&self) -> bool {
        self.failing.is_none()
            && self.pending.is_empty()
            && self.lanes.is_empty()
            && !self
                .overwritten
                .values()
                .any(|r| matches!(r, ReEnqueue::Publishing(_)))
    }

    /// The delivery holds no lane and has no part to open. It still takes
    /// one of `units_held`.
    fn idle(&self) -> bool {
        self.lanes.is_empty() && self.pending.is_empty()
    }

    /// Every part has opened and every lane still held is `ended`, with no
    /// failure and no unconfirmed re-enqueue. A stop keeps such a unit for
    /// the final commit.
    fn read_to_its_end(&self, ended: impl Fn(PartitionId) -> bool) -> bool {
        self.failing.is_none()
            && self.pending.is_empty()
            && !self
                .overwritten
                .values()
                .any(|r| matches!(r, ReEnqueue::Publishing(_)))
            && self.lanes.iter().all(|&partition| ended(partition))
    }
}

/// One lane the reader holds, reading one part of a delivery.
struct LaneState {
    /// The sequence number of the delivery the part belongs to.
    delivery: u32,
    lane: LaneId,
    pause: Arc<AtomicBool>,
    tracker: Arc<SplitTracker>,
    /// The latest acknowledged watermark `commit` received for the lane.
    last_committed: Option<i64>,
    /// The lane's end-of-input has been reported through `CommitReady`.
    hinted: bool,
    /// The re-enqueue count of the lane's notification unit, 0 for a
    /// lister's.
    re_enqueues: u32,
    /// The objects the lane reads, its count in `objects_remaining`.
    members: u64,
}

/// Reads work units from a feed, one lane per part in progress.
pub(crate) struct UnitReader {
    feed: Arc<dyn Feed>,
    park: FeedPark,
    /// The waker the feed signals, cloned into every lane so a lane that
    /// ends or fails ends the park too.
    waker: FeedWaker,
    parts: LaneParts,
    /// The bucket and prefix a notification's records must name.
    scope: Scope,
    /// Lanes in progress plus idle deliveries: the reader receives only
    /// while it holds fewer.
    units_held: usize,
    started: bool,
    /// Sequence number of the next delivery held. Lower is older.
    next_delivery: u32,
    /// Sequence number of the next lane, which names its partition, lane
    /// and poison reports.
    next_lane: u32,
    deliveries: BTreeMap<u32, HeldUnit>,
    lanes: BTreeMap<PartitionId, LaneState>,
    /// Lanes to stop through a drain barrier: records they emitted may be
    /// in flight.
    revoked: Vec<LaneId>,
    /// A fatal answer to a renewal from [`renew_held`](Self::renew_held),
    /// returned by the next `poll_events` or `stop_reading`.
    renew_error: Option<SourceError>,
    /// Lanes whose records are all committed, leaving with nothing in
    /// flight.
    retired: Vec<LaneId>,
    /// `fail`s for undecodable units that the feed answered `Retryable`.
    unsent_fails: Vec<(Receipt, String)>,
    poison_tx: std_mpsc::Sender<PoisonReport>,
    poison_rx: std_mpsc::Receiver<PoisonReport>,
    /// The end check of a bounded job; `None` on a job that runs until a
    /// shutdown.
    end: Option<JobEnd>,
    /// The feed's [`Feed::hands_back`], read once as the runtime reads it.
    hands_back: bool,
    /// A stop has begun: the reader receives nothing more.
    stopping: bool,
}

impl std::fmt::Debug for UnitReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnitReader")
            .field("deliveries", &self.deliveries.len())
            .field("lanes", &self.lanes.len())
            .finish_non_exhaustive()
    }
}

impl UnitReader {
    /// A reader over `feed` that holds at most `units_held` units, or
    /// [`UNITS_HELD`] when that is `None`, and reads the notifications whose
    /// records fall within `scope`. With `end`, the reader reports `Drained`
    /// once that check passes.
    pub(crate) fn new(
        feed: Arc<dyn Feed>,
        parts: LaneParts,
        scope: Scope,
        units_held: Option<NonZeroUsize>,
        mut end: Option<JobEnd>,
    ) -> UnitReader {
        let (waker, park) = wake_channel();
        feed.set_waker(waker.clone());
        if let Some(end) = &mut end {
            end.set_waker(waker.clone());
        }
        let (poison_tx, poison_rx) = std_mpsc::channel();
        let hands_back = feed.hands_back();
        UnitReader {
            feed,
            park,
            waker,
            parts,
            scope,
            units_held: units_held.unwrap_or(UNITS_HELD).get(),
            started: false,
            next_delivery: 0,
            next_lane: 0,
            deliveries: BTreeMap::new(),
            lanes: BTreeMap::new(),
            revoked: Vec::new(),
            renew_error: None,
            retired: Vec::new(),
            unsent_fails: Vec::new(),
            poison_tx,
            poison_rx,
            end,
            hands_back,
            stopping: false,
        }
    }

    /// Record that the solo run's lister has published the whole prefix.
    pub(crate) fn lister_finished(&mut self) {
        if let Some(end) = &mut self.end {
            end.lister_finished();
        }
    }

    /// Surface at most one controller event, waiting on the feed for at
    /// most `timeout` when there is nothing to report.
    ///
    /// Reports `Drained` once a bounded job's end check passes, holding no
    /// lane; a job with no end check ends at a shutdown.
    pub(crate) fn poll_events(
        &mut self,
        timeout: Duration,
    ) -> Result<SourceEvent<S3Lane>, SourceError> {
        if !self.started {
            self.started = true;
            return Ok(SourceEvent::LanesAssigned(Vec::new()));
        }
        self.settle()?;
        if self.holds_work()
            && let Some(end) = &mut self.end
        {
            end.busy();
        }
        if !self.revoked.is_empty() {
            let lanes = std::mem::take(&mut self.revoked);
            let barrier = DrainBarrier::new(lanes.len());
            return Ok(SourceEvent::LanesRevoked { lanes, barrier });
        }
        if !self.retired.is_empty() {
            let lanes = std::mem::take(&mut self.retired);
            return Ok(SourceEvent::LanesRetired { lanes });
        }
        // Held parts open before a receive, so a delivery received now
        // waits behind the ranges of an older one. Receive before parking:
        // a park consumes the feed's signal, and a receive that takes
        // nothing sends none.
        let mut opened = self.open_pending();
        let room = self.room();
        if room > 0 && !self.stopping {
            // A unit taken and failed straight back leaves nothing held, and
            // the feed still holds it.
            let took = self.receive(room)?;
            if let Some(end) = &mut self.end {
                match took {
                    Some(0) => {}
                    Some(_) => end.busy(),
                    None => end.receive_failed(),
                }
            }
            opened.extend(self.open_pending());
        }
        if !opened.is_empty() {
            return Ok(SourceEvent::LanesAdded(opened));
        }
        let partitions: Vec<PartitionId> = self
            .lanes
            .iter_mut()
            .filter(|(_, lane)| !lane.hinted && lane.tracker.terminal().is_some())
            .map(|(&partition, lane)| {
                lane.hinted = true;
                partition
            })
            .collect();
        if !partitions.is_empty() {
            return Ok(SourceEvent::CommitReady { partitions });
        }
        // A stopping reader runs no end check, whose closing receive would
        // otherwise read the skipped receive as one that took nothing.
        if !self.stopping
            && !self.holds_work()
            && let Some(mut end) = self.end.take()
        {
            let feed = Arc::clone(&self.feed);
            let ended = end.idle(feed.as_ref(), || {
                let room = self.room();
                self.receive(room)
            });
            self.end = Some(end);
            if ended? {
                return Ok(SourceEvent::Drained);
            }
        }
        let timeout = self
            .end
            .as_ref()
            .map_or(timeout, |end| end.park_within(timeout));
        self.park.park(timeout);
        Ok(SourceEvent::Idle)
    }

    /// Whether a stopping reader reads on: a held unit is not read to its
    /// end, or a fail waits for the feed. The first call begins the stop. A
    /// feed that does not hand back reads nothing on.
    ///
    /// The controller calls this only while the stop is set. It reads the
    /// lanes' reports, so an overwrite publishes its re-enqueue here, and
    /// makes only non-waiting feed calls.
    pub(crate) fn reading_on_stop(&mut self) -> bool {
        if !self.hands_back {
            return false;
        }
        self.begin_stop();
        let ended = self.ended_then_reports();
        !self.unsent_fails.is_empty()
            || self
                .deliveries
                .values()
                .any(|held| !held.read_to_its_end(|p| ended.contains(&p)))
    }

    /// End a stop's reading: hand back every held unit not read to its end,
    /// settle the rest, and let go of every failing unit. A unit kept stays
    /// held for the final commit. A feed that does not hand back is left as
    /// it is.
    ///
    /// Makes only non-waiting feed calls.
    ///
    /// # Errors
    ///
    /// A fatal answer from the feed. A fatal hand-back leaves the units not
    /// yet handed back held, for the feed's close.
    pub(crate) fn stop_reading(&mut self) -> Result<(), SourceError> {
        if !self.hands_back {
            return self.renew_error.take().map_or(Ok(()), Err);
        }
        self.begin_stop();
        let ended = self.ended_then_reports();
        let mut first = self.check_re_enqueues().err();
        let unread: Vec<u32> = self
            .deliveries
            .iter()
            .filter(|(_, held)| {
                held.failing.is_none() && !held.read_to_its_end(|p| ended.contains(&p))
            })
            .map(|(&delivery, _)| delivery)
            .collect();
        let handed_back = unread.len();
        for delivery in unread {
            let receipt = self.deliveries[&delivery].receipt;
            match self.feed.hand_back(receipt) {
                Ok(()) => {}
                Err(e) if e.kind == FeedErrorKind::Gone => {}
                Err(e) if e.kind == FeedErrorKind::Retryable => {
                    tracing::warn!(
                        %receipt,
                        error = %e,
                        "handing back a held unit failed; the feed's close gives it up"
                    );
                }
                Err(e) => return Err(feed_error("handing back a held unit", &e)),
            }
            self.release(delivery);
        }
        // After the hand-back, so a fatal answer here leaves nothing unread
        // held.
        if let Err(e) = self.settle() {
            first.get_or_insert(e);
        }
        // Counted after the settle, whose renewal can let a kept unit go.
        let kept = self
            .deliveries
            .values()
            .filter(|held| held.failing.is_none())
            .count();
        tracing::info!(
            kept,
            handed_back,
            "the stop handed back the units not read to their end"
        );
        let failing: Vec<u32> = self
            .deliveries
            .iter()
            .filter(|(_, held)| held.failing.is_some())
            .map(|(&delivery, _)| delivery)
            .collect();
        for delivery in failing {
            self.release(delivery);
        }
        first.map_or(Ok(()), Err)
    }

    /// The lanes that have decided end-of-input, then every report the lanes
    /// sent. A lane sends its overwrite report before it decides, so each
    /// lane in the set has its report read.
    fn ended_then_reports(&mut self) -> BTreeSet<PartitionId> {
        let ended = self
            .lanes
            .iter()
            .filter(|(_, lane)| lane.tracker.terminal().is_some())
            .map(|(&partition, _)| partition)
            .collect();
        self.read_reports();
        ended
    }

    /// Begin the stop once: the feed receives nothing more for this reader.
    fn begin_stop(&mut self) {
        if !self.stopping {
            self.stopping = true;
            self.feed.stop_receiving();
        }
    }

    /// Whether the reader holds a delivery, a lane, a lane on its way out or
    /// a fail the feed has not taken.
    fn holds_work(&self) -> bool {
        !(self.deliveries.is_empty()
            && self.lanes.is_empty()
            && self.revoked.is_empty()
            && self.retired.is_empty()
            && self.unsent_fails.is_empty())
    }

    /// Record acknowledged watermarks, retire each lane that ended at the
    /// watermark given, and acknowledge each unit whose last lane retires.
    /// A watermark for a partition no longer held belongs to a lane that
    /// already left and is skipped.
    pub(crate) fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
        for &(partition, watermark) in watermarks {
            let Some(lane) = self.lanes.get_mut(&partition) else {
                continue;
            };
            lane.last_committed = Some(watermark);
            let delivery = lane.delivery;
            if lane.tracker.terminal() != Some(watermark) {
                continue;
            }
            // A lane sends its overwrite report before it decides
            // end-of-input, so once its terminal is seen the report is in the
            // channel; reading it here keeps the unit from being finished
            // before its re-enqueue is published.
            self.read_reports();
            if self.deliveries[&delivery].failing.is_none() {
                self.retire(partition);
                if self.deliveries[&delivery].finished() {
                    self.acknowledge(delivery)?;
                }
            }
        }
        Ok(())
    }

    /// Flip the pause flag of every held lane named.
    pub(crate) fn set_paused(&self, lanes: &[LaneId], paused: bool) {
        for lane in self.lanes.values() {
            if lanes.contains(&lane.lane) {
                lane.pause.store(paused, Ordering::Relaxed);
            }
        }
    }

    /// Whether the held lane `lane` is paused, or `None` when no held lane has
    /// that id.
    #[cfg(test)]
    pub(crate) fn is_paused(&self, lane: LaneId) -> Option<bool> {
        self.lanes
            .values()
            .find(|l| l.lane == lane)
            .map(|l| l.pause.load(Ordering::Relaxed))
    }

    /// Settle what the lanes and the feed left since the last poll (poison
    /// reports, refused fails, finished lanes and units), then renew every
    /// unit still held. A fatal renewal kept by
    /// [`renew_held`](Self::renew_held) is returned first, before anything
    /// is settled.
    fn settle(&mut self) -> Result<(), SourceError> {
        if let Some(e) = self.renew_error.take() {
            return Err(e);
        }
        self.read_reports();
        self.check_re_enqueues()?;
        let failing: Vec<u32> = self
            .deliveries
            .iter()
            .filter(|(_, held)| held.failing.is_some())
            .map(|(&delivery, _)| delivery)
            .collect();
        for delivery in failing {
            let held = &self.deliveries[&delivery];
            let reason = held.failing.clone().unwrap_or_default();
            match settled(self.feed.fail(held.receipt, &reason))? {
                Settled::Yes => self.release(delivery),
                Settled::Later => {}
            }
        }
        for (receipt, reason) in std::mem::take(&mut self.unsent_fails) {
            self.fail_unread(receipt, reason)?;
        }

        // Two shapes only this sweep retires: the last watermark arrived
        // before the lane decided end-of-input, or the lane emitted
        // nothing, so no watermark will arrive.
        let ended: Vec<PartitionId> = self
            .lanes
            .iter()
            .filter(|(_, lane)| {
                self.deliveries[&lane.delivery].failing.is_none()
                    && lane
                        .tracker
                        .terminal()
                        .is_some_and(|t| t == 0 || lane.last_committed == Some(t))
            })
            .map(|(&partition, _)| partition)
            .collect();
        // As in `commit`: a lane that decided end-of-input since the first
        // read has its report in the channel now.
        self.read_reports();
        for partition in ended {
            let failing = self
                .lanes
                .get(&partition)
                .is_none_or(|lane| self.deliveries[&lane.delivery].failing.is_some());
            if !failing {
                self.retire(partition);
            }
        }
        // A unit is acknowledged here when its last lane retired in this
        // sweep, or when an earlier acknowledge was answered `Retryable`.
        let finished: Vec<u32> = self
            .deliveries
            .iter()
            .filter(|(_, held)| held.finished())
            .map(|(&delivery, _)| delivery)
            .collect();
        for delivery in finished {
            self.acknowledge(delivery)?;
        }
        self.renew_each()
    }

    /// Renew every unit held, from a revocation's wait for its drain.
    ///
    /// Settles and acknowledges nothing, so no position moves before the
    /// revocation's commit. A unit answered `Gone` is let go, and its lanes
    /// leave on the next `poll_events`. A fatal answer is kept, and the next
    /// `poll_events` or `stop_reading` returns it before it acknowledges
    /// anything.
    pub(crate) fn renew_held(&mut self) {
        if self.renew_error.is_none() {
            self.renew_error = self.renew_each().err();
        }
    }

    /// Renew every unit held, letting go of each one answered `Gone`.
    fn renew_each(&mut self) -> Result<(), SourceError> {
        let receipts: Vec<(u32, Receipt)> = self
            .deliveries
            .iter()
            .map(|(&delivery, held)| (delivery, held.receipt))
            .collect();
        for (delivery, receipt) in receipts {
            match self.feed.renew(receipt) {
                Ok(()) => {}
                // The feed delivered the unit again or settled it, so this
                // reading is no longer the unit's.
                Err(e) if e.kind == FeedErrorKind::Gone => {
                    tracing::warn!(
                        %receipt,
                        "a held unit's lease lapsed or the feed delivered it again; letting it go"
                    );
                    self.release(delivery);
                }
                Err(e) if e.kind == FeedErrorKind::Retryable => {
                    tracing::warn!(%receipt, error = %e, "renewing a held unit failed; retrying");
                }
                Err(e) => return Err(feed_error("renewing a held unit", &e)),
            }
        }
        Ok(())
    }

    /// Read every report the lanes sent: an overwrite goes to
    /// [`on_overwrite`](Self::on_overwrite), and any other report marks its
    /// unit failing.
    fn read_reports(&mut self) {
        while let Ok(report) = self.poison_rx.try_recv() {
            if let Some(overwrite) = report.overwrite {
                self.on_overwrite(report.partition, *overwrite);
                continue;
            }
            if let Some(m) = &self.parts.metrics {
                m.objects_failed(report.kind).increment(1);
            }
            let partition = report.partition;
            let Some(lane) = self.lanes.get(&partition) else {
                continue;
            };
            let Some(held) = self.deliveries.get_mut(&lane.delivery) else {
                continue;
            };
            tracing::warn!(
                unit = %held.unit,
                reason = report.kind.reason_label(),
                detail = %report.reason,
                "object-level failure; failing the unit back to the feed"
            );
            held.failing = Some(report.reason);
        }
    }

    /// Handle a lane's report that `overwrite.key` was overwritten while
    /// the lane on `partition` read it: drop the key's unopened parts, and
    /// re-enqueue the key once per delivery unless the unit is at the cap.
    fn on_overwrite(&mut self, partition: PartitionId, overwrite: Overwrite) {
        let Some(lane) = self.lanes.get(&partition) else {
            return;
        };
        let re_enqueues = lane.re_enqueues;
        let Some(held) = self.deliveries.get_mut(&lane.delivery) else {
            return;
        };
        // A key another of its ranges reported is counted and re-enqueued
        // once.
        if held.overwritten.contains_key(&overwrite.key) {
            return;
        }
        held.pending.retain(|cursor| cursor.key() != overwrite.key);
        if let Some(m) = &self.parts.metrics {
            m.objects_overwritten.increment(1);
        }
        let re_enqueue = if held.failing.is_some() {
            // The unit goes back to the feed and is read again from its
            // start, which reads the key's current content.
            ReEnqueue::Done
        } else if overwrite.size == 0 {
            tracing::info!(
                unit = %held.unit,
                key = %overwrite.key,
                "an object was overwritten while it was read and is now empty; nothing to \
                 re-enqueue"
            );
            ReEnqueue::Done
        } else if re_enqueues >= self.parts.re_enqueue_cap {
            if let Some(m) = &self.parts.metrics {
                m.overwrites_dropped.increment(1);
            }
            tracing::warn!(
                unit = %held.unit,
                key = %overwrite.key,
                re_enqueues,
                "an object was overwritten while it was read, and its unit is at the \
                 re-enqueue cap; acknowledging it without a re-enqueue"
            );
            ReEnqueue::Done
        } else {
            let bucket = self
                .scope
                .bucket
                .as_deref()
                .expect("a notification unit names the source's bucket");
            let body = re_enqueue_body(bucket, &overwrite, re_enqueues + 1);
            tracing::info!(
                unit = %held.unit,
                key = %overwrite.key,
                etag = overwrite.etag.as_deref().unwrap_or(""),
                re_enqueues = re_enqueues + 1,
                "an object was overwritten while it was read; re-enqueueing it at its \
                 current ETag"
            );
            ReEnqueue::Publishing(self.feed.publish(Unit::new(held.unit.clone(), body)))
        };
        held.overwritten.insert(overwrite.key, re_enqueue);
    }

    /// Settle every re-enqueue the feed has answered. A refused or
    /// unconfirmed publish fails its unit back, so the unit is read again;
    /// a fatal answer fails the pipeline.
    fn check_re_enqueues(&mut self) -> Result<(), SourceError> {
        for held in self.deliveries.values_mut() {
            for re_enqueue in held.overwritten.values_mut() {
                let ReEnqueue::Publishing(confirmation) = re_enqueue else {
                    continue;
                };
                match confirmation.try_outcome() {
                    None => {}
                    Some(Ok(())) => *re_enqueue = ReEnqueue::Done,
                    Some(Err(e)) if e.kind == FeedErrorKind::Fatal => {
                        return Err(feed_error("re-enqueueing an overwritten object", &e));
                    }
                    Some(Err(e)) if held.failing.is_none() => {
                        tracing::warn!(
                            unit = %held.unit,
                            error = %e,
                            "the feed did not store the re-enqueue of an overwritten object; \
                             failing the unit back to the feed"
                        );
                        held.failing =
                            Some(format!("re-enqueueing an overwritten object failed: {e}"));
                    }
                    Some(Err(_)) => {}
                }
            }
        }
        Ok(())
    }

    /// Acknowledge the finished unit `delivery` names. `Retryable` leaves
    /// it held for the next sweep.
    fn acknowledge(&mut self, delivery: u32) -> Result<(), SourceError> {
        let receipt = self.deliveries[&delivery].receipt;
        if settled(self.feed.acknowledge(receipt))? == Settled::Yes {
            self.deliveries.remove(&delivery);
        }
        Ok(())
    }

    /// Let a lane whose records are all committed leave without a drain
    /// barrier.
    fn retire(&mut self, partition: PartitionId) {
        let Some(lane) = self.lanes.remove(&partition) else {
            return;
        };
        if let Some(held) = self.deliveries.get_mut(&lane.delivery) {
            held.lanes.remove(&partition);
        }
        self.retired.push(lane.lane);
        self.settle_remaining(&lane);
    }

    /// Let go of a delivery that failed, was answered `Gone`, or was handed
    /// back: its lanes leave through a drain barrier, since records they
    /// emitted may be in flight, and its unopened parts are dropped.
    fn release(&mut self, delivery: u32) {
        let Some(held) = self.deliveries.remove(&delivery) else {
            return;
        };
        for partition in held.lanes {
            if let Some(lane) = self.lanes.remove(&partition) {
                self.revoked.push(lane.lane);
                self.settle_remaining(&lane);
            }
        }
    }

    /// Take back the lane's count in `objects_remaining`, less the objects
    /// it finished.
    fn settle_remaining(&self, lane: &LaneState) {
        if let Some(m) = &self.parts.metrics {
            let done = u64::from(lane.tracker.close());
            m.objects_remaining
                .decrement(lane.members.saturating_sub(done) as f64);
        }
    }

    /// Lanes the reader may still open: `units_held` less the lanes in
    /// progress and the idle deliveries.
    fn room(&self) -> usize {
        let idle = self.deliveries.values().filter(|held| held.idle()).count();
        self.units_held
            .saturating_sub(self.lanes.len().saturating_add(idle))
    }

    /// Open pending parts into the room left, every part of an older
    /// delivery before any part of a newer one. A failing delivery opens
    /// nothing.
    fn open_pending(&mut self) -> Vec<S3Lane> {
        let mut opened = Vec::new();
        while self.room() > 0 {
            let Some((&delivery, held)) = self
                .deliveries
                .iter_mut()
                .find(|(_, held)| held.failing.is_none() && !held.pending.is_empty())
            else {
                break;
            };
            let cursor = held.pending.front_mut().expect("pending is not empty");
            let part = cursor.next().expect("an exhausted cursor leaves pending");
            if cursor.is_exhausted() {
                held.pending.pop_front();
            }
            opened.push(self.open(delivery, part));
        }
        opened
    }

    /// Receive up to `room` units and hold or fail each. Returns how many
    /// the feed handed over, or `None` when the receive failed retryably.
    fn receive(&mut self, room: usize) -> Result<Option<usize>, SourceError> {
        let deliveries = match self.feed.receive(room) {
            Ok(deliveries) => deliveries,
            Err(e) if e.kind == FeedErrorKind::Retryable => {
                tracing::warn!(error = %e, "receiving from the feed failed; retrying");
                return Ok(None);
            }
            Err(e) => return Err(feed_error("receiving from the feed", &e)),
        };
        let taken = deliveries.len();
        for delivery in deliveries {
            match decode(&delivery.unit.body, &self.scope, self.parts.packing) {
                Decoded::Read(cursors) => self.hold(delivery, cursors),
                // Held with no part, the delivery is acknowledged by the next
                // sweep, which retries a `Retryable` answer.
                Decoded::Acknowledge(reason) => {
                    tracing::debug!(unit = %delivery.unit.id, reason, "acknowledging a unit with nothing to read");
                    self.hold(delivery, Vec::new());
                }
                Decoded::Fail(reason) => {
                    tracing::warn!(unit = %delivery.unit.id, %reason, "failing an unreadable unit");
                    self.fail_unread(delivery.receipt, reason)?;
                }
            }
        }
        Ok(Some(taken))
    }

    /// Hold `delivery` with `cursors` as its pending parts.
    fn hold(&mut self, delivery: Delivery, cursors: Vec<Cursor>) {
        let n = self.next_delivery;
        self.next_delivery = n.checked_add(1).expect("delivery sequence exhausted (u32)");
        self.deliveries.insert(
            n,
            HeldUnit {
                receipt: delivery.receipt,
                unit: delivery.unit.id,
                pending: VecDeque::from(cursors),
                lanes: BTreeSet::new(),
                failing: None,
                overwritten: BTreeMap::new(),
            },
        );
    }

    /// Fail a unit no lane read. `Retryable` keeps the fail for the next
    /// poll.
    fn fail_unread(&mut self, receipt: Receipt, reason: String) -> Result<(), SourceError> {
        if settled(self.feed.fail(receipt, &reason))? == Settled::Later {
            self.unsent_fails.push((receipt, reason));
        }
        Ok(())
    }

    /// Open a lane that reads `part` of the held `delivery` in full.
    fn open(&mut self, delivery: u32, part: Part) -> S3Lane {
        let n = self.next_lane;
        self.next_lane = n.checked_add(1).expect("lane sequence exhausted (u32)");
        let partition = PartitionId(n);
        let lane = LaneId(n);
        let label: LaneLabel = format!("delivery-{delivery}-lane-{n}").into();
        let parts = &self.parts;
        let tracker = Arc::new(SplitTracker::new());
        let pause = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::channel(LANE_HANDOFF_CHUNKS);
        let re_enqueues = match &part.origin {
            Origin::Notification { re_enqueues, .. } => *re_enqueues,
            Origin::Lister => 0,
        };
        // A range reads one object; a whole part reads every member.
        let members = if part.range.is_some() {
            1
        } else {
            part.object.len() as u64
        };
        // Detached: the fetcher exits when the lane drops its receiver.
        drop(parts.handle.spawn(run_fetcher(FetcherParams {
            split: label.clone(),
            store: Arc::clone(&parts.store),
            slice: part.object,
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: parts.chunk_bytes,
            range_bytes: parts.range_bytes,
            tx,
            pause: Arc::clone(&pause),
            retry_base: parts.retry_base,
            retries: parts.metrics.as_ref().map(|m| m.get_retries.clone()),
            range: part.range,
            bytes_read: parts.metrics.as_ref().map(|m| m.bytes_read.clone()),
            origin: part.origin,
        })));
        if let Some(m) = &parts.metrics {
            m.objects_remaining.increment(members as f64);
        }
        let s3_lane = S3Lane::new(
            lane,
            partition,
            rx,
            parts.handle.clone(),
            parts.issuer.clone(),
            parts.packing.compression,
            Arc::clone(&parts.make_framer),
            None,
            Arc::clone(&tracker),
            self.poison_tx.clone(),
            self.waker.clone(),
            parts.metrics.clone(),
        );
        self.lanes.insert(
            partition,
            LaneState {
                delivery,
                lane,
                pause,
                tracker,
                last_committed: None,
                hinted: false,
                re_enqueues,
                members,
            },
        );
        self.deliveries
            .get_mut(&delivery)
            .expect("a part opens only while its delivery is held")
            .lanes
            .insert(partition);
        s3_lane
    }
}

/// Whether a `fail` or `acknowledge` has settled the delivery.
#[derive(Debug, PartialEq, Eq)]
enum Settled {
    /// The feed took the request, or the receipt had already lapsed.
    Yes,
    /// The feed asked for the request again later.
    Later,
}

fn settled(outcome: Result<(), FeedError>) -> Result<Settled, SourceError> {
    match outcome {
        Ok(()) => Ok(Settled::Yes),
        Err(e) if e.kind == FeedErrorKind::Gone => Ok(Settled::Yes),
        Err(e) if e.kind == FeedErrorKind::Retryable => {
            tracing::warn!(error = %e, "the feed refused a request for now; retrying");
            Ok(Settled::Later)
        }
        Err(e) => Err(feed_error("settling a unit", &e)),
    }
}

fn feed_error(doing: &str, e: &FeedError) -> SourceError {
    SourceError::Client {
        class: e.class(),
        reason: format!("{doing}: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Compression;
    use crate::lane_ctx::PoisonKind;
    use crate::split::{DescriptorObject, SplitDescriptor};
    use crate::testutil::TestLineFramer;
    use spate_core::checkpoint::Checkpointer;
    use spate_core::clock::tokio::{Clock, TestClock};
    use spate_core::feed::{Confirmation, Unit};
    use spate_core::metrics::{Meter, render_local};
    use spate_core::source::SourceLane as _;
    use spate_test::{FeedScript, ScriptedFeed, metric_value, scripted_feed, unique_name};
    use std::sync::Mutex;

    /// The range target of every reader here: an object of `n * TARGET`
    /// bytes with an ETag is read in `n` ranges.
    const TARGET: u64 = 100;

    /// A reader over `feed` holding at most `units_held` lanes and idle
    /// units. The runtime is never driven, so a fetcher spawned for a lane
    /// this reader opens never runs.
    fn reader_over(
        feed: Arc<dyn Feed>,
        units_held: Option<usize>,
        metrics: Option<S3Metrics>,
    ) -> (UnitReader, tokio::runtime::Runtime) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let parts = LaneParts {
            store: Arc::new(object_store::memory::InMemory::new()),
            handle: rt.handle().clone(),
            issuer: Checkpointer::new().handle(),
            make_framer: Arc::new(|| Box::new(TestLineFramer::new(1 << 20))),
            packing: Packing {
                target_bytes: TARGET,
                compression: Compression::Auto,
                delimiter: Some(b'\n'),
            },
            chunk_bytes: 1 << 16,
            range_bytes: 1 << 16,
            retry_base: Duration::from_millis(1),
            metrics,
            re_enqueue_cap: crate::source::DEFAULT_RE_ENQUEUE_CAP,
        };
        let units_held = units_held.map(|n| NonZeroUsize::new(n).unwrap());
        (UnitReader::new(feed, parts, scope(), units_held, None), rt)
    }

    /// The scope of a source over `s3://bucket/data/`.
    fn scope() -> Scope {
        Scope {
            bucket: Some("bucket".to_owned()),
            prefix: object_store::path::Path::from("data"),
        }
    }

    /// A reader over a scripted feed with the default `units_held`.
    fn reader() -> (UnitReader, FeedScript, tokio::runtime::Runtime) {
        let (feed, script) = scripted_feed();
        let (reader, rt) = reader_over(Arc::new(feed), None, None);
        (reader, script, rt)
    }

    /// A reader over a scripted feed holding at most `units_held`.
    fn reader_holding(units_held: usize) -> (UnitReader, FeedScript, tokio::runtime::Runtime) {
        let (feed, script) = scripted_feed();
        let (reader, rt) = reader_over(Arc::new(feed), Some(units_held), None);
        (reader, script, rt)
    }

    /// A reader over a scripted feed that hands back, with the default
    /// `units_held`.
    fn stopping_reader() -> (UnitReader, FeedScript, tokio::runtime::Runtime) {
        let (feed, script) = scripted_feed();
        script.hand_back_on_stop();
        let (reader, rt) = reader_over(Arc::new(feed), None, None);
        (reader, script, rt)
    }

    /// A reader over a scripted feed that hands back, holding at most
    /// `units_held`.
    fn stopping_reader_holding(
        units_held: usize,
    ) -> (UnitReader, FeedScript, tokio::runtime::Runtime) {
        let (feed, script) = scripted_feed();
        script.hand_back_on_stop();
        let (reader, rt) = reader_over(Arc::new(feed), Some(units_held), None);
        (reader, script, rt)
    }

    /// A reader over a [`Refusing`] feed that hands back.
    fn stopping_refusing() -> (
        UnitReader,
        Arc<Refusing>,
        FeedScript,
        tokio::runtime::Runtime,
    ) {
        let (inner, script) = scripted_feed();
        script.hand_back_on_stop();
        let feed = Arc::new(Refusing::new(inner));
        let (reader, rt) = reader_over(Arc::clone(&feed) as Arc<dyn Feed>, None, None);
        (reader, feed, script, rt)
    }

    /// A [`Feed`] over a scripted feed whose `receive`, `acknowledge` and
    /// `fail` answer the error kind set for them, without reaching the
    /// script, and whose `hand_back` answers the kind set for one receipt.
    /// It records the `max` of every `receive`.
    struct Refusing {
        inner: ScriptedFeed,
        receives: Mutex<Vec<usize>>,
        receive: Mutex<Option<FeedErrorKind>>,
        acknowledge: Mutex<Option<FeedErrorKind>>,
        fail: Mutex<Option<FeedErrorKind>>,
        hand_back: Mutex<Option<(Receipt, FeedErrorKind)>>,
    }

    impl Refusing {
        fn new(inner: ScriptedFeed) -> Refusing {
            Refusing {
                inner,
                receives: Mutex::new(Vec::new()),
                receive: Mutex::new(None),
                acknowledge: Mutex::new(None),
                fail: Mutex::new(None),
                hand_back: Mutex::new(None),
            }
        }

        fn answer(kind: &Mutex<Option<FeedErrorKind>>) -> Result<(), FeedError> {
            match *kind.lock().unwrap() {
                Some(kind) => Err(FeedError::new(kind, "refused by the test")),
                None => Ok(()),
            }
        }
    }

    impl Feed for Refusing {
        fn backend(&self) -> &str {
            self.inner.backend()
        }

        fn set_waker(&self, waker: FeedWaker) {
            self.inner.set_waker(waker);
        }

        fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
            self.receives.lock().unwrap().push(max);
            Refusing::answer(&self.receive)?;
            self.inner.receive(max)
        }

        fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
            self.inner.renew(receipt)
        }

        fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
            Refusing::answer(&self.acknowledge)?;
            self.inner.acknowledge(receipt)
        }

        fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError> {
            Refusing::answer(&self.fail)?;
            self.inner.fail(receipt, reason)
        }

        fn publish(&self, unit: Unit) -> Confirmation {
            self.inner.publish(unit)
        }

        fn read_depth(
            &self,
            of: spate_core::feed::DepthOf,
        ) -> Option<spate_core::feed::DepthReading> {
            self.inner.read_depth(of)
        }

        fn hands_back(&self) -> bool {
            self.inner.hands_back()
        }

        fn stop_receiving(&self) {
            self.inner.stop_receiving();
        }

        fn hand_back(&self, receipt: Receipt) -> Result<(), FeedError> {
            if let Some((refused, kind)) = *self.hand_back.lock().unwrap()
                && refused == receipt
            {
                return Err(FeedError::new(kind, "refused by the test"));
            }
            self.inner.hand_back(receipt)
        }

        fn close(&self, within: Duration) -> Option<Confirmation> {
            self.inner.close(within)
        }
    }

    /// Give `reader` the end check of a solo run whose lister has finished:
    /// the job ends on the first poll that finds the reader holding nothing
    /// and the feed empty.
    fn end_solo(reader: &mut UnitReader, rt: &tokio::runtime::Runtime) {
        let mut end = JobEnd::new(
            crate::job_end::Finish::Solo { finished: true },
            TestClock::frozen(),
            rt.handle().clone(),
            spate_core::source::StopSignal::default(),
        );
        end.set_waker(reader.waker.clone());
        reader.end = Some(end);
    }

    /// Poll until the reader reports `Drained`, at most `polls` times, and
    /// whether it did.
    fn drains_within(reader: &mut UnitReader, polls: usize) -> bool {
        (0..polls).any(|_| matches!(poll(reader), SourceEvent::Drained))
    }

    /// Receive one scripted unit and hold it with one lane, as `open` does,
    /// returning the lane's partition, the receipt and the tracker the lane
    /// would report end-of-input on.
    fn hold_one(
        reader: &mut UnitReader,
        script: &FeedScript,
    ) -> (PartitionId, Receipt, Arc<SplitTracker>) {
        let receipt = script.deliver(Unit::new("u", b"unused".to_vec()));
        let delivery = reader.feed.receive(1).unwrap().pop().unwrap();
        let d = reader.next_delivery;
        reader.next_delivery += 1;
        let partition = PartitionId(reader.next_lane);
        reader.next_lane += 1;
        let tracker = Arc::new(SplitTracker::new());
        reader.deliveries.insert(
            d,
            HeldUnit {
                receipt: delivery.receipt,
                unit: delivery.unit.id,
                pending: VecDeque::new(),
                lanes: BTreeSet::from([partition]),
                failing: None,
                overwritten: BTreeMap::new(),
            },
        );
        reader.lanes.insert(
            partition,
            LaneState {
                delivery: d,
                lane: LaneId(partition.0),
                pause: Arc::new(AtomicBool::new(false)),
                tracker: Arc::clone(&tracker),
                last_committed: None,
                hinted: false,
                re_enqueues: 0,
                members: 1,
            },
        );
        (partition, receipt, tracker)
    }

    /// Pins that a reader holding a lane never drains, though the feed reads
    /// empty, and drains once the lane has retired and its unit is
    /// acknowledged.
    #[test]
    fn a_reader_holding_a_lane_does_not_drain_on_an_empty_feed() {
        let (feed, script) = scripted_feed();
        let (mut reader, rt) = reader_over(Arc::new(feed), None, None);
        end_solo(&mut reader, &rt);
        start(&mut reader);
        let (p, receipt, _tracker) = hold_one(&mut reader, &script);
        for _ in 0..8 {
            script.answer_depth(
                spate_core::feed::DepthOf::Feed,
                Ok(spate_core::feed::Depth::new("empty", false)),
            );
        }
        assert!(!drains_within(&mut reader, 8), "drained holding a lane");
        finish(&mut reader, p, 3);
        assert!(drains_within(&mut reader, 8));
        assert_eq!(script.acknowledged(), vec![receipt]);
    }

    /// Pins that a reader whose acknowledge the feed answered `Retryable`
    /// still holds the unit and does not drain, and drains once the
    /// acknowledge goes through.
    #[test]
    fn a_retryable_acknowledge_keeps_the_reader_from_draining() {
        let (inner, script) = scripted_feed();
        let feed = Arc::new(Refusing::new(inner));
        let (mut reader, rt) = reader_over(Arc::clone(&feed) as Arc<dyn Feed>, None, None);
        end_solo(&mut reader, &rt);
        start(&mut reader);
        let (p, receipt, _tracker) = hold_one(&mut reader, &script);
        *feed.acknowledge.lock().unwrap() = Some(FeedErrorKind::Retryable);
        finish(&mut reader, p, 3);
        assert!(
            !drains_within(&mut reader, 8),
            "drained before the acknowledge"
        );
        *feed.acknowledge.lock().unwrap() = None;
        assert!(drains_within(&mut reader, 8));
        assert_eq!(script.acknowledged(), vec![receipt]);
    }

    /// Pins that a reader holding a fail the feed answered `Retryable`, for
    /// a unit it could not read, does not drain until the fail goes
    /// through.
    #[test]
    fn an_unsent_fail_keeps_the_reader_from_draining() {
        let (inner, script) = scripted_feed();
        let feed = Arc::new(Refusing::new(inner));
        let (mut reader, rt) = reader_over(Arc::clone(&feed) as Arc<dyn Feed>, None, None);
        end_solo(&mut reader, &rt);
        start(&mut reader);
        *feed.fail.lock().unwrap() = Some(FeedErrorKind::Retryable);
        let receipt = script.deliver(Unit::new("unreadable", b"not a unit".to_vec()));
        assert!(!drains_within(&mut reader, 8), "drained holding a fail");
        *feed.fail.lock().unwrap() = None;
        assert!(drains_within(&mut reader, 8));
        assert_eq!(script.failed().len(), 1);
        assert_eq!(script.failed()[0].0, receipt);
    }

    /// Give `reader` the end check of a job with a 60 s window over an end
    /// marker that names one run, its reads on a runtime of their own, and
    /// poll until the window has opened with one read of the feed.
    fn end_windowed(
        reader: &mut UnitReader,
        script: &FeedScript,
    ) -> (Arc<TestClock>, tokio::runtime::Runtime) {
        use object_store::ObjectStoreExt as _;
        let io = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let location =
            crate::location::Location::parse("s3://ops/jobs/a/", "s3://data/events/").unwrap();
        io.block_on(store.put(
            &location.end(),
            b"run=0123456789abcdef0123456789abcdef\n".to_vec().into(),
        ))
        .unwrap();
        let clock = TestClock::frozen();
        let mut end = JobEnd::new(
            crate::job_end::Finish::Marker {
                store,
                location,
                window: Duration::from_secs(60),
            },
            Arc::clone(&clock) as Arc<dyn Clock>,
            io.handle().clone(),
            spate_core::source::StopSignal::default(),
        );
        end.set_waker(reader.waker.clone());
        reader.end = Some(end);
        start(reader);
        spate_test::wait_until(Duration::from_secs(30), "the window opens", || {
            poll(reader);
            script.depth_reads(spate_core::feed::DepthOf::Feed) == 1
        });
        (clock, io)
    }

    /// Pins that a poll finding the reader holding only a retired lane
    /// restarts the window: 15 s on, the reader reads the end marker again,
    /// where an unbroken window would read the feed.
    #[test]
    fn a_retired_lane_restarts_the_end_window() {
        let (feed, script) = scripted_feed();
        let (mut reader, _rt) = reader_over(Arc::new(feed), None, None);
        let (clock, _io) = end_windowed(&mut reader, &script);
        let (p, _receipt, _tracker) = hold_one(&mut reader, &script);
        finish(&mut reader, p, 3);
        assert!(reader.deliveries.is_empty() && reader.lanes.is_empty());
        assert_eq!(lanes_retired(poll(&mut reader)).len(), 1);
        clock.advance(Duration::from_secs(15));
        poll(&mut reader);
        assert_eq!(
            script.depth_reads(spate_core::feed::DepthOf::Feed),
            1,
            "the window went on over the retired lane"
        );
    }

    /// Pins that a poll finding the reader holding only a revoked lane, for
    /// a unit it failed back to the feed, restarts the window.
    #[test]
    fn a_revoked_lane_restarts_the_end_window() {
        let (feed, script) = scripted_feed();
        let (mut reader, _rt) = reader_over(Arc::new(feed), None, None);
        let (clock, _io) = end_windowed(&mut reader, &script);
        let (p, _receipt, _tracker) = hold_one(&mut reader, &script);
        let d = delivery_of(&reader, p);
        reader.deliveries.get_mut(&d).unwrap().failing = Some("unreadable".into());
        assert_eq!(lanes_revoked(poll(&mut reader)).len(), 1);
        assert!(reader.deliveries.is_empty() && reader.lanes.is_empty());
        clock.advance(Duration::from_secs(15));
        poll(&mut reader);
        assert_eq!(
            script.depth_reads(spate_core::feed::DepthOf::Feed),
            1,
            "the window went on over the revoked lane"
        );
    }

    /// Holds every GET of a [`gated_get`] store until a test opens it.
    static GATE: (Mutex<bool>, std::sync::Condvar) = (Mutex::new(false), std::sync::Condvar::new());

    fn gated_get(
        _: &object_store::path::Path,
    ) -> Option<object_store::Result<object_store::GetResult>> {
        let (open, opened) = &GATE;
        let mut open = open.lock().unwrap();
        while !*open {
            open = opened.wait(open).unwrap();
        }
        None
    }

    /// Pins that a reader built with an end check hands it the reader's
    /// waker: a marker read that finishes while the reader is parked ends
    /// the park.
    #[test]
    fn a_finished_marker_read_ends_the_readers_park() {
        use object_store::ObjectStoreExt as _;
        *GATE.0.lock().unwrap() = false;
        let io = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let store = crate::testutil::HookStore {
            get: Some(gated_get),
            ..Default::default()
        };
        let location =
            crate::location::Location::parse("s3://ops/jobs/a/", "s3://data/events/").unwrap();
        io.block_on(store.inner.put(
            &location.end(),
            b"run=0123456789abcdef0123456789abcdef\n".to_vec().into(),
        ))
        .unwrap();
        let end = JobEnd::new(
            crate::job_end::Finish::Marker {
                store: Arc::new(store),
                location,
                window: Duration::from_secs(60),
            },
            TestClock::frozen(),
            io.handle().clone(),
            spate_core::source::StopSignal::default(),
        );
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let parts = LaneParts {
            store: Arc::new(object_store::memory::InMemory::new()),
            handle: rt.handle().clone(),
            issuer: Checkpointer::new().handle(),
            make_framer: Arc::new(|| Box::new(TestLineFramer::new(1 << 20))),
            packing: Packing {
                target_bytes: TARGET,
                compression: Compression::Auto,
                delimiter: Some(b'\n'),
            },
            chunk_bytes: 1 << 16,
            range_bytes: 1 << 16,
            retry_base: Duration::from_millis(1),
            metrics: None,
            re_enqueue_cap: crate::source::DEFAULT_RE_ENQUEUE_CAP,
        };
        let (feed, _script) = scripted_feed();
        let mut reader = UnitReader::new(Arc::new(feed), parts, scope(), None, Some(end));
        start(&mut reader);
        // Starts the marker read, held at the gate, and takes any signal
        // already sent.
        poll(&mut reader);
        {
            let (open, opened) = &GATE;
            *open.lock().unwrap() = true;
            opened.notify_all();
        }
        assert!(
            reader.park.park(Duration::from_secs(30)),
            "the marker read did not wake the reader"
        );
    }

    /// Pins INV-1 in `commit`: with end-of-input decided at `T`, a
    /// watermark below `T` acknowledges nothing, in `commit` or in the sweep
    /// after it, and a watermark at `T` acknowledges the unit once.
    #[test]
    fn commit_acknowledges_only_at_the_terminal_watermark() {
        let (mut reader, script, _rt) = reader();
        let (p, receipt, tracker) = hold_one(&mut reader, &script);
        tracker.set_terminal(5);

        reader.commit(&[(p, 4)]).unwrap();
        assert_eq!(
            script.acknowledged(),
            vec![],
            "acknowledged by commit at W < T"
        );
        reader.settle().unwrap();
        assert_eq!(
            script.acknowledged(),
            vec![],
            "acknowledged by the sweep at W < T"
        );

        reader.commit(&[(p, 5)]).unwrap();
        reader.settle().unwrap();
        assert_eq!(script.acknowledged(), vec![receipt]);
    }

    /// Pins INV-1 in the sweep: a watermark that reached the reader before
    /// end-of-input acknowledges the unit only when it equals the terminal
    /// watermark the lane then decides.
    #[test]
    fn the_sweep_acknowledges_only_at_the_terminal_watermark() {
        let (mut reader, script, _rt) = reader();
        let (below, _, below_tracker) = hold_one(&mut reader, &script);
        let (at, at_receipt, at_tracker) = hold_one(&mut reader, &script);
        reader.commit(&[(below, 4), (at, 5)]).unwrap();
        below_tracker.set_terminal(5);
        at_tracker.set_terminal(5);

        reader.settle().unwrap();
        assert_eq!(script.acknowledged(), vec![at_receipt]);
    }

    /// Pins that renewing from a revocation's wait renews every held unit
    /// and acknowledges none, though one has reached its terminal watermark,
    /// and that the next sweep acknowledges it. Regression for #1278.
    #[test]
    fn renewing_during_a_drain_renews_every_held_unit_and_settles_nothing() {
        let (mut reader, script, _rt) = reader();
        start(&mut reader);
        let (_, open, _) = hold_one(&mut reader, &script);
        let (at, at_receipt, at_tracker) = hold_one(&mut reader, &script);
        reader.commit(&[(at, 5)]).unwrap();
        at_tracker.set_terminal(5);

        reader.renew_held();
        assert_eq!(script.renewed(), vec![open, at_receipt]);
        assert_eq!(script.acknowledged(), vec![], "acknowledged by the hook");

        reader.settle().unwrap();
        assert_eq!(script.acknowledged(), vec![at_receipt]);
    }

    /// Pins that a unit answered `Gone` while renewing from a revocation's
    /// wait leaves through a revocation on the next poll. Regression for
    /// #1278.
    #[test]
    fn a_unit_gone_during_a_drain_is_revoked_at_the_next_poll() {
        let (mut reader, script, _rt) = reader();
        start(&mut reader);
        let (p, receipt, _) = hold_one(&mut reader, &script);
        script.redeliver(receipt);

        reader.renew_held();
        assert!(script.gone().contains(&receipt));
        assert_eq!(lanes_revoked(poll(&mut reader)), vec![LaneId(p.0)]);
    }

    /// Pins that a fatal answer to a renewal from a revocation's wait fails
    /// the next poll before it settles anything, and that a later renewal
    /// does not clear it. Regression for #1278.
    #[test]
    fn a_renew_error_during_a_drain_fails_the_next_poll() {
        let (mut reader, script, _rt) = reader();
        start(&mut reader);
        let (at, _, at_tracker) = hold_one(&mut reader, &script);
        reader.commit(&[(at, 5)]).unwrap();
        at_tracker.set_terminal(5);
        script.refuse_next_renew(FeedErrorKind::Fatal);

        reader.renew_held();
        reader.renew_held();
        let Err(SourceError::Client { reason, .. }) = reader.poll_events(Duration::ZERO) else {
            panic!("a fatal renewal did not fail the poll");
        };
        assert!(reason.contains("renewing a held unit"), "{reason}");
        assert_eq!(script.acknowledged(), vec![], "settled before failing");
    }

    /// Pins that a fatal answer to a renewal from a revocation's wait fails a
    /// stop that follows it, before the stop acknowledges anything.
    #[test]
    fn a_renew_error_during_a_drain_fails_the_stop() {
        let (mut reader, script, _rt) = stopping_reader();
        start(&mut reader);
        let (at, _, at_tracker) = hold_one(&mut reader, &script);
        reader.commit(&[(at, 5)]).unwrap();
        at_tracker.set_terminal(5);
        script.refuse_next_renew(FeedErrorKind::Fatal);

        reader.renew_held();
        let Err(SourceError::Client { reason, .. }) = reader.stop_reading() else {
            panic!("a fatal renewal did not fail the stop");
        };
        assert!(reason.contains("renewing a held unit"), "{reason}");
        assert_eq!(script.acknowledged(), vec![], "settled before failing");
    }

    /// Pins that a fatal answer to a renewal from a revocation's wait fails a
    /// stop on a feed that does not hand back.
    #[test]
    fn a_renew_error_during_a_drain_fails_a_stop_that_hands_nothing_back() {
        let (mut reader, script, _rt) = reader();
        start(&mut reader);
        hold_one(&mut reader, &script);
        script.refuse_next_renew(FeedErrorKind::Fatal);

        reader.renew_held();
        let Err(SourceError::Client { reason, .. }) = reader.stop_reading() else {
            panic!("a fatal renewal did not fail the stop");
        };
        assert!(reason.contains("renewing a held unit"), "{reason}");
    }

    /// Pins that a held unit answered `Gone` on renewal logs one warning
    /// naming its receipt.
    #[test]
    fn a_unit_answered_gone_is_logged_once() {
        let (mut reader, script, _rt) = reader();
        start(&mut reader);
        let (_, receipt, _) = hold_one(&mut reader, &script);
        script.redeliver(receipt);

        let logs = spate_test::capture_logs(tracing::Level::WARN, || {
            reader.renew_held();
            poll(&mut reader);
            poll(&mut reader);
        });
        let named: Vec<_> = logs
            .iter()
            .filter(|l| l.contains(&receipt.to_string()))
            .collect();
        assert_eq!(named.len(), 1, "{logs:?}");
    }

    /// A unit whose body is a descriptor naming one object, `object-{i}`.
    fn descriptor_unit(i: usize) -> Unit {
        let object = DescriptorObject {
            key: format!("object-{i}"),
            size: 1,
            etag: None,
            last_modified_ms: 1,
        };
        let body = SplitDescriptor::new(vec![object]).encode().unwrap();
        Unit::new(format!("unit-{i}"), body)
    }

    /// A whole-object unit over `{name}.ndjson`, which has an ETag and is
    /// read in `ranges` ranges.
    fn ranged_unit(name: &str, ranges: u64) -> Unit {
        let object = DescriptorObject {
            key: format!("{name}.ndjson"),
            size: ranges * TARGET,
            etag: Some(format!("\"etag-{name}\"")),
            last_modified_ms: 1,
        };
        let body = SplitDescriptor::new(vec![object]).encode().unwrap();
        Unit::new(name, body)
    }

    fn poll(reader: &mut UnitReader) -> SourceEvent<S3Lane> {
        reader.poll_events(Duration::ZERO).unwrap()
    }

    /// Take the reader past its first poll, which assigns no lane.
    fn start(reader: &mut UnitReader) {
        let first = poll(reader);
        assert!(matches!(first, SourceEvent::LanesAssigned(ref l) if l.is_empty()));
    }

    fn lanes_added(event: SourceEvent<S3Lane>) -> Vec<S3Lane> {
        match event {
            SourceEvent::LanesAdded(lanes) => lanes,
            other => panic!("expected LanesAdded, got {other:?}"),
        }
    }

    fn lanes_retired(event: SourceEvent<S3Lane>) -> Vec<LaneId> {
        match event {
            SourceEvent::LanesRetired { lanes } => lanes,
            other => panic!("expected LanesRetired, got {other:?}"),
        }
    }

    fn lanes_revoked(event: SourceEvent<S3Lane>) -> Vec<LaneId> {
        match event {
            SourceEvent::LanesRevoked { lanes, .. } => lanes,
            other => panic!("expected LanesRevoked, got {other:?}"),
        }
    }

    fn assert_idle(event: SourceEvent<S3Lane>) {
        assert!(
            matches!(event, SourceEvent::Idle),
            "expected Idle, got {event:?}"
        );
    }

    /// Decide end-of-input for the lane on `partition` at `terminal`, and
    /// commit `terminal` for it.
    fn finish(reader: &mut UnitReader, partition: PartitionId, terminal: i64) {
        reader.lanes[&partition].tracker.set_terminal(terminal);
        reader.commit(&[(partition, terminal)]).unwrap();
    }

    fn partitions(lanes: &[S3Lane]) -> Vec<PartitionId> {
        lanes.iter().map(S3Lane::partition).collect()
    }

    /// The delivery the lane on `partition` reads a part of.
    fn delivery_of(reader: &UnitReader, partition: PartitionId) -> u32 {
        reader.lanes[&partition].delivery
    }

    /// Pins the cap on units held when no section value or thread count is
    /// given: the reader holds at most 96 units, leaves the rest queued, and
    /// receives only into the room a settled unit frees.
    #[test]
    fn the_reader_holds_at_most_96_units() {
        let (mut reader, script, _rt) = reader();
        let receipts: Vec<Receipt> = (0..98)
            .map(|i| script.deliver(descriptor_unit(i)))
            .collect();
        start(&mut reader);
        let lanes = lanes_added(poll(&mut reader));
        assert_eq!(lanes.len(), 96, "lanes opened on the first receive");
        assert_eq!(script.held().len(), 96, "units held");

        script.dead_letter(receipts[0]);
        let revoked = lanes_revoked(poll(&mut reader));
        assert_eq!(revoked.len(), 1, "expected one lane revoked");
        let refill = lanes_added(poll(&mut reader));
        assert_eq!(refill.len(), 1, "lanes opened into the freed room");
        assert_eq!(script.held().len(), 96, "units held after the refill");
        assert_eq!(script.delivered().len(), 97, "one unit stays queued");
    }

    /// Pins that a lane the reader opens signals the park the reader waits
    /// on.
    #[test]
    fn every_lane_wakes_the_readers_park() {
        let (mut reader, script, _rt) = reader();
        script.deliver(descriptor_unit(0));
        start(&mut reader);
        let mut lanes = lanes_added(poll(&mut reader));
        assert_eq!(lanes.len(), 1);
        // Drain the signal the delivery left in the slot.
        while reader.park.park(Duration::ZERO) {}

        lanes[0].wake_control();
        assert!(
            reader.park.park(Duration::ZERO),
            "the lane's signal did not reach the reader's park"
        );
    }

    /// Pins that a whole unit over an object that qualifies for ranges opens
    /// one lane per range up to `units_held`, and that every range shares
    /// the cursor's one copy of the object's entry.
    #[test]
    fn a_whole_unit_over_a_qualifying_object_opens_one_lane_per_range() {
        let (mut reader, script, _rt) = reader_holding(3);
        script.deliver(ranged_unit("a", 5));
        start(&mut reader);
        let lanes = lanes_added(poll(&mut reader));
        assert_eq!(lanes.len(), 3, "lanes opened for a unit of five ranges");
        let held = &reader.deliveries[&0];
        assert_eq!(held.lanes.len(), 3);
        let cursor = held.pending.front().expect("two ranges stay pending");
        assert_eq!(
            Arc::strong_count(&cursor.object),
            4,
            "the cursor and three fetchers share one entry"
        );
    }

    /// Pins that a whole unit over a compressed object opens one lane for
    /// the whole object, whether the codec comes from the key or from the
    /// configured compression.
    #[test]
    fn a_whole_unit_over_a_compressed_object_opens_one_lane() {
        for (key, compression) in [
            ("a.ndjson.gz", Compression::Auto),
            ("a.ndjson", Compression::Gzip),
        ] {
            let (mut reader, script, _rt) = reader_holding(3);
            reader.parts.packing.compression = compression;
            let object = DescriptorObject {
                key: key.into(),
                size: 5 * TARGET,
                etag: Some("\"etag\"".into()),
                last_modified_ms: 1,
            };
            script.deliver(Unit::new(
                "a",
                SplitDescriptor::new(vec![object]).encode().unwrap(),
            ));
            start(&mut reader);
            let lanes = lanes_added(poll(&mut reader));
            assert_eq!(lanes.len(), 1, "{key} under {compression:?}");
            assert!(
                reader.deliveries[&0].pending.is_empty(),
                "{key} under {compression:?} left a part pending"
            );
        }
    }

    /// Pins the room rule: each range in progress takes one of
    /// `units_held`, a held unit's ranges open before anything is received,
    /// oldest unit first, and a unit whose lanes have all retired keeps a
    /// slot until its acknowledge settles.
    #[test]
    fn a_unit_read_in_ranges_holds_one_slot_per_range_in_progress() {
        // One range in progress per slot; the fourth range opens into the
        // slot a retired range frees, ahead of a unit not yet received.
        let (mut reader, script, _rt) = reader_holding(3);
        script.deliver(ranged_unit("a", 4));
        start(&mut reader);
        let first = partitions(&lanes_added(poll(&mut reader)));
        assert_eq!(first.len(), 3);
        script.deliver(descriptor_unit(1));
        assert_idle(poll(&mut reader));
        assert_eq!(script.delivered().len(), 1, "received with no room left");
        finish(&mut reader, first[0], 5);
        assert_eq!(lanes_retired(poll(&mut reader)), vec![LaneId(first[0].0)]);
        let fourth = partitions(&lanes_added(poll(&mut reader)));
        assert_eq!(fourth.len(), 1, "the fourth range opens alone");
        assert_eq!(delivery_of(&reader, fourth[0]), 0);
        assert_eq!(
            script.delivered().len(),
            1,
            "received before the held ranges opened"
        );

        // Two units received together: the older one's ranges open first,
        // and the newer one opens only after the last of them.
        let (mut reader, script, _rt) = reader_holding(3);
        script.deliver(ranged_unit("a", 4));
        script.deliver(descriptor_unit(1));
        start(&mut reader);
        let first = partitions(&lanes_added(poll(&mut reader)));
        assert_eq!(first.len(), 3, "A's ranges opened into every slot");
        assert_eq!(script.delivered().len(), 2, "both units received");
        assert!(first.iter().all(|&p| delivery_of(&reader, p) == 0));
        finish(&mut reader, first[0], 5);
        lanes_retired(poll(&mut reader));
        let fourth = partitions(&lanes_added(poll(&mut reader)));
        assert_eq!(fourth.len(), 1);
        assert_eq!(
            delivery_of(&reader, fourth[0]),
            0,
            "B opened before A's last range"
        );
        finish(&mut reader, first[1], 5);
        lanes_retired(poll(&mut reader));
        let next = partitions(&lanes_added(poll(&mut reader)));
        assert_eq!(next.len(), 1);
        assert_eq!(
            delivery_of(&reader, next[0]),
            1,
            "B opens after A's last range"
        );

        // A unit whose lane retired while its acknowledge is answered
        // `Retryable` keeps its slot.
        let (inner, script) = scripted_feed();
        let feed = Arc::new(Refusing::new(inner));
        *feed.acknowledge.lock().unwrap() = Some(FeedErrorKind::Retryable);
        let (mut reader, _rt) = reader_over(feed.clone(), Some(1), None);
        let a = script.deliver(descriptor_unit(0));
        start(&mut reader);
        let lane = partitions(&lanes_added(poll(&mut reader)));
        finish(&mut reader, lane[0], 5);
        assert_eq!(lanes_retired(poll(&mut reader)), vec![LaneId(lane[0].0)]);
        script.deliver(descriptor_unit(1));
        assert_idle(poll(&mut reader));
        assert_eq!(
            script.delivered().len(),
            1,
            "received while a unit waits on its acknowledge"
        );
        *feed.acknowledge.lock().unwrap() = None;
        let b = partitions(&lanes_added(poll(&mut reader)));
        assert_eq!(script.acknowledged(), vec![a], "acknowledged by the sweep");
        assert_eq!(b.len(), 1);
        assert_eq!(delivery_of(&reader, b[0]), 1);
    }

    /// Pins that a lane's end of input is reported through `CommitReady`
    /// once, and that a later poll with nothing else to report parks.
    #[test]
    fn a_lane_at_its_end_of_input_is_reported_once() {
        let (mut reader, script, _rt) = reader_holding(1);
        script.deliver(ranged_unit("a", 2));
        start(&mut reader);
        let lane = partitions(&lanes_added(poll(&mut reader)));
        reader.lanes[&lane[0]].tracker.set_terminal(7);
        reader.commit(&[(lane[0], 6)]).unwrap();
        match poll(&mut reader) {
            SourceEvent::CommitReady { partitions } => assert_eq!(partitions, lane),
            other => panic!("expected CommitReady, got {other:?}"),
        }
        assert_idle(poll(&mut reader));
    }

    /// Pins that a unit with more ranges than `units_held` finishes: each
    /// range retires once its terminal watermark is committed, or at once
    /// when it emitted nothing, and the unit is acknowledged once, after
    /// the last.
    #[test]
    fn a_unit_with_more_ranges_than_units_held_finishes() {
        let (mut reader, script, _rt) = reader_holding(1);
        let receipt = script.deliver(ranged_unit("a", 3));
        start(&mut reader);
        for i in 0..3 {
            let lane = partitions(&lanes_added(poll(&mut reader)));
            assert_eq!(lane.len(), 1, "range {i}");
            reader.lanes[&lane[0]].tracker.set_terminal(7);
            reader.commit(&[(lane[0], 6)]).unwrap();
            assert!(
                matches!(poll(&mut reader), SourceEvent::CommitReady { .. }),
                "range {i} left below its terminal watermark"
            );
            assert_eq!(script.acknowledged(), vec![], "acknowledged at range {i}");
            reader.commit(&[(lane[0], 7)]).unwrap();
            if i < 2 {
                assert_eq!(lanes_retired(poll(&mut reader)), vec![LaneId(lane[0].0)]);
                assert_eq!(script.acknowledged(), vec![], "acknowledged at range {i}");
            }
        }
        assert_eq!(script.acknowledged(), vec![receipt]);

        // Ranges that emit nothing retire in the sweep, with no commit.
        let (mut reader, script, _rt) = reader_holding(1);
        let receipt = script.deliver(ranged_unit("a", 2));
        start(&mut reader);
        for i in 0..2 {
            let lane = partitions(&lanes_added(poll(&mut reader)));
            assert_eq!(lane.len(), 1, "range {i}");
            reader.lanes[&lane[0]].tracker.set_terminal(0);
            assert_eq!(lanes_retired(poll(&mut reader)), vec![LaneId(lane[0].0)]);
        }
        assert_eq!(script.acknowledged(), vec![receipt]);
    }

    /// Pins INV-1 across ranges: a unit is acknowledged only after every
    /// range has opened and reached its terminal watermark, on the commit
    /// path and in the sweep.
    #[test]
    fn a_unit_read_in_ranges_is_acknowledged_only_after_every_range() {
        let (mut reader, script, _rt) = reader_holding(2);
        let receipt = script.deliver(ranged_unit("a", 2));
        start(&mut reader);
        let lanes = partitions(&lanes_added(poll(&mut reader)));
        assert_eq!(lanes.len(), 2);
        for &p in &lanes {
            reader.lanes[&p].tracker.set_terminal(5);
        }
        reader.commit(&[(lanes[0], 5), (lanes[1], 4)]).unwrap();
        assert_eq!(script.acknowledged(), vec![], "acknowledged by commit");
        reader.settle().unwrap();
        assert_eq!(script.acknowledged(), vec![], "acknowledged by the sweep");
        reader.commit(&[(lanes[1], 5)]).unwrap();
        reader.settle().unwrap();
        assert_eq!(script.acknowledged(), vec![receipt]);

        // Every open range done is not enough while a range is pending.
        let (mut reader, script, _rt) = reader_holding(2);
        let receipt = script.deliver(ranged_unit("a", 3));
        start(&mut reader);
        let lanes = partitions(&lanes_added(poll(&mut reader)));
        assert_eq!(lanes.len(), 2);
        finish(&mut reader, lanes[0], 5);
        finish(&mut reader, lanes[1], 5);
        assert_eq!(script.acknowledged(), vec![], "acknowledged by commit");
        assert_eq!(lanes_retired(poll(&mut reader)).len(), 2);
        assert_eq!(script.acknowledged(), vec![], "acknowledged by the sweep");
        let third = partitions(&lanes_added(poll(&mut reader)));
        assert_eq!(script.acknowledged(), vec![], "acknowledged on open");
        finish(&mut reader, third[0], 5);
        reader.settle().unwrap();
        assert_eq!(script.acknowledged(), vec![receipt]);
    }

    /// Reads `spate_s3_objects_remaining` for the pipeline `pipeline` from
    /// `rendered`.
    fn objects_remaining(rendered: &str, pipeline: &str) -> f64 {
        metric_value(
            rendered,
            "spate_s3_objects_remaining",
            &[("pipeline", pipeline)],
        )
        .expect("the gauge is registered")
    }

    /// Pins that a poisoned range fails its whole unit, stops the unit's
    /// other ranges through a drain barrier and opens none of its pending
    /// ones, and that `objects_remaining` counts one per lane opened and
    /// takes each lane's count back when it leaves.
    #[test]
    fn a_poisoned_range_fails_its_unit_and_stops_the_other_ranges() {
        let run = |poison: bool| {
            let pipeline = unique_name("reader");
            let rendered = render_local(|| {
                let metrics =
                    S3Metrics::new(&Meter::with_namespace("s3", pipeline.clone(), "src", "s3"));
                let (feed, script) = scripted_feed();
                let (mut reader, _rt) = reader_over(Arc::new(feed), Some(2), Some(metrics));
                let receipt = script.deliver(ranged_unit("a", 4));
                start(&mut reader);
                let lanes = partitions(&lanes_added(poll(&mut reader)));
                assert_eq!(lanes.len(), 2);
                if poison {
                    reader
                        .poison_tx
                        .send(PoisonReport {
                            partition: PartitionId(lanes[0].0),
                            kind: PoisonKind::NotFound,
                            reason: "gone".into(),
                            overwrite: None,
                        })
                        .unwrap();
                    let revoked = lanes_revoked(poll(&mut reader));
                    assert_eq!(revoked.len(), 2, "every range of the unit stops");
                    assert_eq!(script.failed().len(), 1);
                    assert_eq!(script.failed()[0].0, receipt);
                    assert!(
                        script.gone().is_empty(),
                        "the unit was renewed after its fail settled"
                    );
                    assert_idle(poll(&mut reader));
                }
            });
            objects_remaining(&rendered, &pipeline)
        };
        assert_eq!(run(false), 2.0, "one count per lane opened");
        assert_eq!(run(true), 0.0, "each lane takes its count back");
    }

    /// Pins that a range lane released while it reads, on a poisoned
    /// sibling or on `Gone`, takes its `objects_remaining` count back once
    /// when it finishes its range before its drain barrier trips.
    #[test]
    fn a_released_range_finishing_before_its_barrier_settles_the_gauge_once() {
        use object_store::{ObjectStoreExt as _, PutPayload, path::Path};
        let run = |poison_sibling: bool| {
            let pipeline = unique_name("reader");
            let rendered = render_local(|| {
                let metrics =
                    S3Metrics::new(&Meter::with_namespace("s3", pipeline.clone(), "src", "s3"));
                // The lane's fetcher has to run while the test polls the lane.
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .build()
                    .unwrap();
                let store = object_store::memory::InMemory::new();
                let body = b"line\n".repeat(40);
                let size = body.len() as u64;
                let put = rt
                    .block_on(store.put(&Path::from("a.ndjson"), PutPayload::from(body)))
                    .unwrap();
                let parts = LaneParts {
                    store: Arc::new(store),
                    handle: rt.handle().clone(),
                    issuer: Checkpointer::new().handle(),
                    make_framer: Arc::new(|| Box::new(TestLineFramer::new(1 << 20))),
                    packing: Packing {
                        target_bytes: TARGET,
                        compression: Compression::Auto,
                        delimiter: Some(b'\n'),
                    },
                    chunk_bytes: 1 << 16,
                    range_bytes: 1 << 16,
                    retry_base: Duration::from_millis(1),
                    metrics: Some(metrics),
                    re_enqueue_cap: crate::source::DEFAULT_RE_ENQUEUE_CAP,
                };
                let (feed, script) = scripted_feed();
                let mut reader =
                    UnitReader::new(Arc::new(feed), parts, scope(), NonZeroUsize::new(4), None);
                let object = DescriptorObject {
                    key: "a.ndjson".into(),
                    size,
                    etag: put.e_tag.clone(),
                    last_modified_ms: 1,
                };
                let receipt = script.deliver(Unit::new(
                    "a",
                    SplitDescriptor::new(vec![object]).encode().unwrap(),
                ));
                start(&mut reader);
                let mut lanes = lanes_added(poll(&mut reader));
                assert_eq!(lanes.len(), 2, "two ranges");
                let tracker = Arc::clone(&reader.lanes[&lanes[1].partition()].tracker);
                if poison_sibling {
                    reader
                        .poison_tx
                        .send(PoisonReport {
                            partition: PartitionId(lanes[0].partition().0),
                            kind: PoisonKind::NotFound,
                            reason: "gone".into(),
                            overwrite: None,
                        })
                        .unwrap();
                } else {
                    script.dead_letter(receipt);
                }
                assert_eq!(lanes_revoked(poll(&mut reader)).len(), 2);
                spate_test::wait_until(
                    Duration::from_secs(10),
                    "the released range reaches its end of input",
                    || {
                        let _ = lanes[1].poll(1024, Duration::from_millis(10));
                        tracker.terminal().is_some()
                    },
                );
            });
            objects_remaining(&rendered, &pipeline)
        };
        assert_eq!(run(true), 0.0, "a poisoned sibling");
        assert_eq!(run(false), 0.0, "a unit answered Gone");
    }

    /// Pins that a unit answered `Gone` lets go of every range it opened
    /// and opens none of its pending ones.
    #[test]
    fn a_unit_read_in_ranges_answered_gone_lets_go_of_every_range() {
        let (mut reader, script, _rt) = reader_holding(2);
        let receipt = script.deliver(ranged_unit("a", 4));
        start(&mut reader);
        assert_eq!(lanes_added(poll(&mut reader)).len(), 2);
        script.dead_letter(receipt);
        assert_eq!(lanes_revoked(poll(&mut reader)).len(), 2);
        assert_idle(poll(&mut reader));
        assert!(reader.deliveries.is_empty() && reader.lanes.is_empty());
    }

    /// A unit naming the byte range `[0, TARGET)` of `key`, cut on
    /// `delimiter`.
    fn range_unit(key: &str, delimiter: u8) -> Unit {
        let object = DescriptorObject {
            key: key.to_owned(),
            size: 2 * TARGET,
            etag: Some("\"etag\"".into()),
            last_modified_ms: 1,
        };
        let range = crate::split::SplitRange::new(0, TARGET, delimiter);
        let body = SplitDescriptor::with_range(object, range).encode().unwrap();
        Unit::new(key, body)
    }

    /// The reason the reader gave for failing `unit` back, which it opens no
    /// lane for.
    fn failed_reason(reader: &mut UnitReader, script: &FeedScript, unit: Unit) -> String {
        let receipt = script.deliver(unit);
        assert_idle(poll(reader));
        let failed = script.failed();
        let (got, reason) = failed.last().expect("the unit is failed back");
        assert_eq!(*got, receipt);
        assert!(reader.lanes.is_empty(), "a lane opened for {reason}");
        reason.clone()
    }

    /// Pins that a ranged unit is failed back, naming why, when it is cut on
    /// another byte than the framer's delimiter, when the framer declares
    /// none, or when its object is compressed by its key or by the
    /// configured compression.
    #[test]
    fn a_ranged_unit_the_reader_cannot_read_is_failed_back() {
        let (mut lines, script, _rt) = reader();
        start(&mut lines);
        let reason = failed_reason(&mut lines, &script, range_unit("a.ndjson", b'\r'));
        assert!(reason.contains("on byte 0x0d"), "{reason}");
        assert!(reason.contains("resyncs on byte 0x0a"), "{reason}");
        let reason = failed_reason(&mut lines, &script, range_unit("a.ndjson.gz", b'\n'));
        assert!(reason.contains("decodes as gzip"), "{reason}");

        let (mut none, script, _rt) = reader();
        none.parts.packing.delimiter = None;
        start(&mut none);
        let reason = failed_reason(&mut none, &script, range_unit("a.ndjson", b'\n'));
        assert!(reason.contains("declares no resync delimiter"), "{reason}");

        let (mut gzip, script, _rt) = reader();
        gzip.parts.packing.compression = Compression::Gzip;
        start(&mut gzip);
        let reason = failed_reason(&mut gzip, &script, range_unit("a.ndjson", b'\n'));
        assert!(reason.contains("decodes as gzip"), "{reason}");
    }

    /// Pins that `set_paused` flips the pause flag of the lanes named and of
    /// no other.
    #[test]
    fn set_paused_flips_only_the_lanes_named() {
        let (mut reader, script, _rt) = reader();
        script.deliver(ranged_unit("a", 2));
        start(&mut reader);
        let lanes = partitions(&lanes_added(poll(&mut reader)));
        let paused = |reader: &UnitReader| -> Vec<bool> {
            lanes
                .iter()
                .map(|p| reader.lanes[p].pause.load(Ordering::Relaxed))
                .collect()
        };
        reader.set_paused(&[LaneId(lanes[1].0)], true);
        assert_eq!(paused(&reader), [false, true]);
        reader.set_paused(&[LaneId(lanes[1].0)], false);
        assert_eq!(paused(&reader), [false, false]);
    }

    /// Pins that a `Fatal` answer to `receive`, or to an `acknowledge` sent
    /// from `commit` or from the sweep, stops the reader.
    #[test]
    fn a_fatal_answer_from_the_feed_stops_the_reader() {
        let fatal = |result: Result<(), SourceError>| match result {
            Err(SourceError::Client { class, .. }) => {
                assert_eq!(class, spate_core::error::ErrorClass::Fatal);
            }
            other => panic!("expected a fatal error, got {other:?}"),
        };
        let (inner, script) = scripted_feed();
        let feed = Arc::new(Refusing::new(inner));
        let (mut reader, _rt) = reader_over(feed.clone(), None, None);
        script.deliver(descriptor_unit(0));
        start(&mut reader);
        *feed.receive.lock().unwrap() = Some(FeedErrorKind::Fatal);
        fatal(reader.poll_events(Duration::ZERO).map(drop));

        *feed.receive.lock().unwrap() = None;
        let lane = partitions(&lanes_added(poll(&mut reader)));
        *feed.acknowledge.lock().unwrap() = Some(FeedErrorKind::Fatal);
        reader.lanes[&lane[0]].tracker.set_terminal(5);
        fatal(reader.commit(&[(lane[0], 5)]));

        let (inner, script) = scripted_feed();
        let feed = Arc::new(Refusing::new(inner));
        let (mut reader, _rt) = reader_over(feed.clone(), None, None);
        script.deliver(descriptor_unit(0));
        start(&mut reader);
        let lane = partitions(&lanes_added(poll(&mut reader)));
        *feed.acknowledge.lock().unwrap() = Some(FeedErrorKind::Fatal);
        reader.lanes[&lane[0]].tracker.set_terminal(0);
        fatal(reader.poll_events(Duration::ZERO).map(drop));
    }

    /// Pins that a unit whose fail is answered `Retryable` opens none of its
    /// pending ranges, even when a slot is free.
    #[test]
    fn a_failing_unit_opens_no_pending_range_while_its_fail_is_retried() {
        let (inner, script) = scripted_feed();
        let feed = Arc::new(Refusing::new(inner));
        let (mut reader, _rt) = reader_over(feed.clone(), Some(2), None);
        script.deliver(descriptor_unit(0));
        script.deliver(ranged_unit("a", 3));
        start(&mut reader);
        let lanes = partitions(&lanes_added(poll(&mut reader)));
        assert_eq!(lanes.len(), 2, "the whole unit and one range open");
        let (whole, range) = (lanes[0], lanes[1]);
        assert_eq!(delivery_of(&reader, range), 1);
        *feed.fail.lock().unwrap() = Some(FeedErrorKind::Retryable);
        reader
            .poison_tx
            .send(PoisonReport {
                partition: PartitionId(range.0),
                kind: PoisonKind::NotFound,
                reason: "gone".into(),
                overwrite: None,
            })
            .unwrap();
        finish(&mut reader, whole, 5);
        assert_eq!(lanes_retired(poll(&mut reader)), vec![LaneId(whole.0)]);
        assert_idle(poll(&mut reader));
        assert_eq!(reader.lanes.len(), 1, "a range of the failing unit opened");
    }

    /// Pins that a unit with nothing to read is held with no part: it takes
    /// one of `units_held` until the sweep acknowledges it, and a
    /// `Retryable` answer is tried again.
    #[test]
    fn a_unit_with_nothing_to_read_is_acknowledged_by_the_sweep() {
        let (inner, script) = scripted_feed();
        let feed = Arc::new(Refusing::new(inner));
        *feed.acknowledge.lock().unwrap() = Some(FeedErrorKind::Retryable);
        let (mut reader, _rt) = reader_over(feed.clone(), Some(1), None);
        let test_event = br#"{"Service":"Amazon S3","Event":"s3:TestEvent","Bucket":"bucket"}"#;
        let a = script.deliver(Unit::new("test-event", test_event.to_vec()));
        start(&mut reader);
        assert_idle(poll(&mut reader));
        script.deliver(descriptor_unit(0));
        assert_idle(poll(&mut reader));
        assert!(script.acknowledged().is_empty());
        assert_eq!(
            script.delivered().len(),
            1,
            "received while a unit with nothing to read waits on its acknowledge"
        );
        *feed.acknowledge.lock().unwrap() = None;
        let lanes = lanes_added(poll(&mut reader));
        assert_eq!(script.acknowledged(), vec![a], "acknowledged by the sweep");
        assert_eq!(lanes.len(), 1);
    }

    /// Pins that a `receive` answered `Retryable` is tried again on the next
    /// poll and stops nothing.
    #[test]
    fn a_retryable_receive_is_tried_again_on_the_next_poll() {
        let (inner, script) = scripted_feed();
        let feed = Arc::new(Refusing::new(inner));
        let (mut reader, _rt) = reader_over(feed.clone(), None, None);
        script.deliver(descriptor_unit(0));
        start(&mut reader);
        *feed.receive.lock().unwrap() = Some(FeedErrorKind::Retryable);
        assert_idle(poll(&mut reader));
        *feed.receive.lock().unwrap() = None;
        assert_eq!(lanes_added(poll(&mut reader)).len(), 1);
    }

    /// Pins that a reader with no room left asks the feed for nothing,
    /// and receives again into the room a finished unit frees.
    #[test]
    fn a_full_reader_asks_the_feed_for_no_units() {
        let (inner, script) = scripted_feed();
        let feed = Arc::new(Refusing::new(inner));
        let (mut reader, _rt) = reader_over(Arc::clone(&feed) as Arc<dyn Feed>, Some(1), None);
        script.deliver(descriptor_unit(0));
        script.deliver(descriptor_unit(1));
        start(&mut reader);
        let lane = partitions(&lanes_added(poll(&mut reader)))[0];
        for _ in 0..3 {
            poll(&mut reader);
        }
        assert_eq!(
            *feed.receives.lock().unwrap(),
            vec![1],
            "a receive with no room"
        );
        finish(&mut reader, lane, 3);
        lanes_retired(poll(&mut reader));
        assert_eq!(lanes_added(poll(&mut reader)).len(), 1);
        assert_eq!(*feed.receives.lock().unwrap(), vec![1, 1]);
    }

    // ------------------------------------------------------------ stop --

    /// A poison report of `kind` for the lane on `partition`.
    fn report_poison(reader: &UnitReader, partition: PartitionId, kind: PoisonKind) {
        reader
            .poison_tx
            .send(PoisonReport {
                partition,
                kind,
                reason: "gone".into(),
                overwrite: None,
            })
            .unwrap();
    }

    /// Pins that `stop_reading` hands back each unit with a lane short of
    /// end-of-input or a range not yet opened, keeps a unit whose every lane
    /// has decided end-of-input for the commit that acknowledges it, and
    /// logs the counts.
    #[test]
    fn stop_reading_hands_back_only_units_not_read_to_their_end() {
        let (mut reader, script, _rt) = stopping_reader_holding(4);
        let c = script.deliver(ranged_unit("c", 2));
        let d = script.deliver(ranged_unit("d", 4));
        start(&mut reader);
        let lanes = partitions(&lanes_added(poll(&mut reader)));
        assert_eq!(lanes.len(), 4, "two ranges of c, two of d");
        let (pa, a, ta) = hold_one(&mut reader, &script);
        let (_, b, _) = hold_one(&mut reader, &script);
        ta.set_terminal(5);
        reader.lanes[&lanes[0]].tracker.set_terminal(2);
        reader.lanes[&lanes[2]].tracker.set_terminal(2);
        reader.lanes[&lanes[3]].tracker.set_terminal(2);
        let logs = spate_test::capture_logs(tracing::Level::INFO, || {
            reader.stop_reading().unwrap();
        });
        assert_eq!(script.handed_back(), vec![c, d, b]);
        assert_eq!(script.held(), vec![a]);
        assert!(script.acknowledged().is_empty());
        reader.commit(&[(pa, 5)]).unwrap();
        assert_eq!(script.acknowledged(), vec![a]);
        assert_eq!(
            logs.iter()
                .filter(|l| l.contains("kept=1") && l.contains("handed_back=3"))
                .count(),
            1,
            "{logs:?}"
        );
    }

    /// Pins that `reading_on_stop` reads on while a held unit has a lane
    /// short of end-of-input, an unconfirmed re-enqueue or an unsent fail,
    /// and not once each held lane has decided, though unacknowledged.
    #[test]
    fn reading_on_stop_answers_whether_a_held_unit_is_unread() {
        let (mut reader, script, _rt) = stopping_reader();
        start(&mut reader);
        assert!(!reader.reading_on_stop(), "nothing held");
        let (_, _, tracker) = hold_one(&mut reader, &script);
        assert!(reader.reading_on_stop());
        tracker.set_terminal(3);
        assert!(
            !reader.reading_on_stop(),
            "its lane has decided end-of-input"
        );
        hold_one(&mut reader, &script);
        assert!(reader.reading_on_stop());

        let (mut reader, script, _rt) = stopping_reader();
        script.hold_publishes();
        let (_, lanes) = open_notification(&mut reader, &script, &[("a.ndjson", 10)], 0);
        report_overwrite(&reader, lanes[0], "a.ndjson", 20);
        poll(&mut reader);
        finish(&mut reader, lanes[0], 3);
        assert!(reader.reading_on_stop(), "its re-enqueue is unconfirmed");
        script.confirm_held_publishes();
        poll(&mut reader);
        assert!(!reader.reading_on_stop());

        let (mut reader, feed, script, _rt) = stopping_refusing();
        *feed.fail.lock().unwrap() = Some(FeedErrorKind::Retryable);
        script.deliver(Unit::new("unreadable", b"not a unit".to_vec()));
        start(&mut reader);
        poll(&mut reader);
        assert!(reader.reading_on_stop(), "a fail waits for the feed");
        *feed.fail.lock().unwrap() = None;
        poll(&mut reader);
        assert!(!reader.reading_on_stop());
    }

    /// Pins that a reader over a feed that does not hand back neither reads
    /// on nor hands back at the stop, whether or not `reading_on_stop` came
    /// first.
    #[test]
    fn a_feed_that_does_not_hand_back_reads_nothing_on_at_the_stop() {
        for ask_first in [true, false] {
            let (mut reader, script, _rt) = reader();
            start(&mut reader);
            let (p, receipt, _) = hold_one(&mut reader, &script);
            if ask_first {
                assert!(!reader.reading_on_stop());
            }
            reader.stop_reading().unwrap();
            assert_eq!(script.stopped_receiving(), 0);
            assert!(script.handed_back().is_empty());
            assert_eq!(script.held(), vec![receipt]);
            assert!(reader.lanes.contains_key(&p), "the lane is still held");
        }
    }

    /// Pins that a stopping reader receives nothing, neither into the room
    /// it has left nor through the end check of a solo run, and reports no
    /// `Drained`.
    #[test]
    fn a_stopping_reader_receives_nothing() {
        let (mut reader, script, _rt) = stopping_reader();
        start(&mut reader);
        hold_one(&mut reader, &script);
        assert!(reader.reading_on_stop());
        script.deliver(descriptor_unit(0));
        for _ in 0..3 {
            poll(&mut reader);
        }
        assert_eq!(script.delivered().len(), 1, "received while stopping");

        // The feed's depth reads empty, so the end check would run its
        // closing receive.
        let (mut reader, script, rt) = stopping_reader();
        end_solo(&mut reader, &rt);
        start(&mut reader);
        assert!(!reader.reading_on_stop());
        script.deliver(descriptor_unit(0));
        for _ in 0..3 {
            script.answer_depth(
                spate_core::feed::DepthOf::Feed,
                Ok(spate_core::feed::Depth::new("empty", false)),
            );
            assert!(!matches!(poll(&mut reader), SourceEvent::Drained));
        }
        assert!(script.delivered().is_empty(), "the end check received");
    }

    /// Pins that a stop calls `stop_receiving` once, whether it begins at
    /// `reading_on_stop` or at `stop_reading`, and whether or not anything
    /// is held.
    #[test]
    fn stop_receiving_is_sent_once_per_stop() {
        let (mut reader, script, _rt) = stopping_reader();
        start(&mut reader);
        hold_one(&mut reader, &script);
        for _ in 0..3 {
            reader.reading_on_stop();
            poll(&mut reader);
        }
        reader.stop_reading().unwrap();
        assert_eq!(script.stopped_receiving(), 1);

        let (mut reader, script, _rt) = stopping_reader();
        start(&mut reader);
        let (_, receipt, _) = hold_one(&mut reader, &script);
        reader.stop_reading().unwrap();
        assert_eq!(script.stopped_receiving(), 1);
        assert_eq!(script.handed_back(), vec![receipt]);

        let (mut reader, script, _rt) = stopping_reader();
        start(&mut reader);
        assert!(!reader.reading_on_stop());
        assert_eq!(script.stopped_receiving(), 1);
    }

    /// Pins how `stop_reading` settles each answer to a hand-back: `Gone`
    /// and `Retryable` let the unit go and the hand-back goes on, and any
    /// other kind fails the call and leaves the units not yet handed back
    /// held.
    #[test]
    fn a_hand_back_answer_settles_by_its_kind() {
        let (mut reader, _feed, script, _rt) = stopping_refusing();
        start(&mut reader);
        let (_, first, _) = hold_one(&mut reader, &script);
        let (_, second, _) = hold_one(&mut reader, &script);
        script.redeliver(first);
        reader.stop_reading().unwrap();
        assert!(script.gone().contains(&first));
        assert_eq!(script.handed_back(), vec![second]);
        assert!(reader.deliveries.is_empty());

        let (mut reader, feed, script, _rt) = stopping_refusing();
        start(&mut reader);
        let (_, first, _) = hold_one(&mut reader, &script);
        let (_, second, _) = hold_one(&mut reader, &script);
        *feed.hand_back.lock().unwrap() = Some((first, FeedErrorKind::Retryable));
        let logs = spate_test::capture_logs(tracing::Level::WARN, || {
            reader.stop_reading().unwrap();
        });
        assert_eq!(script.handed_back(), vec![second]);
        assert!(reader.deliveries.is_empty(), "both let go");
        assert!(
            logs.iter().any(|l| l.contains(&first.to_string())),
            "{logs:?}"
        );

        let (mut reader, feed, script, _rt) = stopping_refusing();
        start(&mut reader);
        let (_, first, _) = hold_one(&mut reader, &script);
        let (_, second, _) = hold_one(&mut reader, &script);
        *feed.hand_back.lock().unwrap() = Some((first, FeedErrorKind::Fatal));
        let Err(SourceError::Client { reason, .. }) = reader.stop_reading() else {
            panic!("a fatal hand-back did not fail the call");
        };
        assert!(reason.contains("handing back"), "{reason}");
        assert!(script.handed_back().is_empty());
        assert!(script.held().contains(&second), "left held for the close");
    }

    /// Pins that a unit handed back with its ranges in flight counts no
    /// object failure, is never acknowledged once its lanes end, and takes
    /// its lanes' counts back from `objects_remaining`.
    #[test]
    fn a_handed_back_unit_counts_no_object_failure() {
        let run = |hand_back: bool| {
            let pipeline = unique_name("reader");
            let rendered = render_local(|| {
                let metrics =
                    S3Metrics::new(&Meter::with_namespace("s3", pipeline.clone(), "src", "s3"));
                let (feed, script) = scripted_feed();
                script.hand_back_on_stop();
                let (mut reader, _rt) = reader_over(Arc::new(feed), Some(2), Some(metrics));
                script.deliver(ranged_unit("a", 2));
                start(&mut reader);
                let lanes = partitions(&lanes_added(poll(&mut reader)));
                let trackers: Vec<Arc<SplitTracker>> = lanes
                    .iter()
                    .map(|p| Arc::clone(&reader.lanes[p].tracker))
                    .collect();
                if hand_back {
                    reader.stop_reading().unwrap();
                }
                for (p, tracker) in lanes.iter().zip(&trackers) {
                    tracker.set_terminal(3);
                    reader.commit(&[(*p, 3)]).unwrap();
                }
                assert!(script.failed().is_empty());
                assert!(script.gone().is_empty());
                assert_eq!(
                    script.acknowledged().len(),
                    usize::from(!hand_back),
                    "acknowledged once its lanes end unless handed back"
                );
            });
            if hand_back {
                for kind in [
                    PoisonKind::NotFound,
                    PoisonKind::EtagDrift,
                    PoisonKind::Undecodable,
                    PoisonKind::RetriesExhausted,
                ] {
                    assert_eq!(
                        metric_value(
                            &rendered,
                            "spate_s3_objects_failed_total",
                            &[("pipeline", &pipeline), ("reason", kind.reason_label())],
                        ),
                        Some(0.0),
                        "{kind:?}"
                    );
                }
            }
            objects_remaining(&rendered, &pipeline)
        };
        assert_eq!(run(true), 0.0, "each handed-back lane takes its count back");
        assert_eq!(run(false), 0.0, "each retired lane takes its count back");
    }

    /// Pins that a unit read in two ranges counts one object failure per
    /// range that reports one, and is failed back to the feed once.
    #[test]
    fn a_poisoned_range_counts_once_per_report() {
        let pipeline = unique_name("reader");
        let rendered = render_local(|| {
            let metrics =
                S3Metrics::new(&Meter::with_namespace("s3", pipeline.clone(), "src", "s3"));
            let (feed, script) = scripted_feed();
            let (mut reader, _rt) = reader_over(Arc::new(feed), Some(2), Some(metrics));
            script.deliver(ranged_unit("a", 2));
            start(&mut reader);
            let lanes = partitions(&lanes_added(poll(&mut reader)));
            assert_eq!(lanes.len(), 2);
            for p in &lanes {
                report_poison(&reader, *p, PoisonKind::NotFound);
            }
            poll(&mut reader);
            assert_eq!(script.failed().len(), 1);
        });
        assert_eq!(
            metric_value(
                &rendered,
                "spate_s3_objects_failed_total",
                &[("pipeline", &pipeline), ("reason", "not_found")],
            ),
            Some(2.0),
            "{rendered}"
        );
    }

    /// Pins that each poison kind counts under its own `reason` label. Each
    /// kind sends a different number of reports, so two kinds that swap
    /// handles or labels move a count to the wrong series.
    #[test]
    fn each_poison_kind_counts_under_its_own_reason() {
        let kinds = [
            (PoisonKind::NotFound, "not_found", 1),
            (PoisonKind::EtagDrift, "etag_drift", 2),
            (PoisonKind::Undecodable, "undecodable", 3),
            (PoisonKind::RetriesExhausted, "retries_exhausted", 4),
        ];
        let pipeline = unique_name("reader");
        let rendered = render_local(|| {
            let metrics =
                S3Metrics::new(&Meter::with_namespace("s3", pipeline.clone(), "src", "s3"));
            let (feed, script) = scripted_feed();
            let (mut reader, _rt) = reader_over(Arc::new(feed), Some(1), Some(metrics));
            script.deliver(ranged_unit("a", 1));
            start(&mut reader);
            let lanes = partitions(&lanes_added(poll(&mut reader)));
            assert_eq!(lanes.len(), 1);
            for (kind, _, n) in kinds {
                for _ in 0..n {
                    report_poison(&reader, lanes[0], kind);
                }
            }
            poll(&mut reader);
            assert_eq!(script.failed().len(), 1);
        });
        for (_, reason, n) in kinds {
            assert_eq!(
                metric_value(
                    &rendered,
                    "spate_s3_objects_failed_total",
                    &[("pipeline", &pipeline), ("reason", reason)],
                ),
                Some(f64::from(n)),
                "{reason}: {rendered}"
            );
        }
    }

    /// Pins that a notification naming a key that cannot be read under its
    /// own name fails its message at decode and counts no object failure
    /// under any reason.
    #[test]
    fn an_unaddressable_notification_key_fails_uncounted() {
        let pipeline = unique_name("reader");
        let rendered = render_local(|| {
            let metrics =
                S3Metrics::new(&Meter::with_namespace("s3", pipeline.clone(), "src", "s3"));
            let (feed, script) = scripted_feed();
            let (mut reader, _rt) = reader_over(Arc::new(feed), None, Some(metrics));
            let receipt = script.deliver(Unit::new(
                "unaddressable",
                pinned_notification(&[("/x.ndjson", 10)], 0),
            ));
            start(&mut reader);
            poll(&mut reader);
            assert_eq!(script.failed().len(), 1);
            assert_eq!(script.failed()[0].0, receipt);
        });
        for reason in [
            "not_found",
            "etag_drift",
            "undecodable",
            "retries_exhausted",
        ] {
            assert_eq!(
                metric_value(
                    &rendered,
                    "spate_s3_objects_failed_total",
                    &[("pipeline", &pipeline), ("reason", reason)],
                ),
                Some(0.0),
                "{reason}: {rendered}"
            );
        }
    }

    /// Pins that a hand-back takes its lanes' counts out of
    /// `objects_remaining` at once, and a kept unit's at its commit.
    #[test]
    fn a_hand_back_settles_objects_remaining() {
        let run = |commit: bool| {
            let pipeline = unique_name("reader");
            let rendered = render_local(|| {
                let metrics =
                    S3Metrics::new(&Meter::with_namespace("s3", pipeline.clone(), "src", "s3"));
                let (feed, script) = scripted_feed();
                script.hand_back_on_stop();
                let (mut reader, _rt) = reader_over(Arc::new(feed), Some(2), Some(metrics));
                script.deliver(descriptor_unit(0));
                script.deliver(descriptor_unit(1));
                start(&mut reader);
                let lanes = partitions(&lanes_added(poll(&mut reader)));
                reader.lanes[&lanes[0]].tracker.set_terminal(5);
                reader.stop_reading().unwrap();
                assert_eq!(script.handed_back().len(), 1);
                if commit {
                    reader.commit(&[(lanes[0], 5)]).unwrap();
                }
            });
            objects_remaining(&rendered, &pipeline)
        };
        assert_eq!(run(false), 1.0, "the kept unit's lane");
        assert_eq!(run(true), 0.0);
    }

    /// A unit whose descriptor names `members` objects, `object-0` onwards.
    fn multi_member_unit(members: usize) -> Unit {
        let objects = (0..members)
            .map(|i| DescriptorObject {
                key: format!("object-{i}"),
                size: 1,
                etag: None,
                last_modified_ms: 1,
            })
            .collect();
        let body = SplitDescriptor::new(objects).encode().unwrap();
        Unit::new("multi", body)
    }

    /// Pins that a descriptor of two objects opens one lane over both, in
    /// order, and is acknowledged once when that lane's terminal watermark
    /// is committed.
    #[test]
    fn a_two_member_unit_is_read_in_one_lane() {
        let (mut reader, script, _rt) = reader();
        let receipt = script.deliver(multi_member_unit(2));
        start(&mut reader);
        let lanes = partitions(&lanes_added(poll(&mut reader)));
        assert_eq!(lanes.len(), 1);
        assert!(script.failed().is_empty(), "{:?}", script.failed());
        reader.lanes[&lanes[0]].tracker.set_terminal(5);
        reader.commit(&[(lanes[0], 5)]).unwrap();
        reader.settle().unwrap();
        assert_eq!(script.acknowledged(), vec![receipt]);
    }

    /// Pins that `objects_remaining` counts every object of a lane's unit
    /// when the lane opens, and that a hand-back takes back all but the
    /// objects the lane finished, whose counts the lane takes back itself.
    #[test]
    fn objects_remaining_counts_every_member_of_a_lane() {
        // `done` objects framed, then a hand-back when `stop` is set.
        let run = |done: usize, stop: bool| {
            let pipeline = unique_name("reader");
            let rendered = render_local(|| {
                let metrics =
                    S3Metrics::new(&Meter::with_namespace("s3", pipeline.clone(), "src", "s3"));
                let gauge = metrics.objects_remaining.clone();
                let (feed, script) = scripted_feed();
                script.hand_back_on_stop();
                let (mut reader, _rt) = reader_over(Arc::new(feed), Some(2), Some(metrics));
                script.deliver(multi_member_unit(3));
                start(&mut reader);
                let lanes = partitions(&lanes_added(poll(&mut reader)));
                for _ in 0..done {
                    // As the lane does for each object it frames.
                    if reader.lanes[&lanes[0]].tracker.object_done() {
                        gauge.decrement(1.0);
                    }
                }
                if stop {
                    reader.stop_reading().unwrap();
                    assert_eq!(script.handed_back().len(), 1);
                }
            });
            objects_remaining(&rendered, &pipeline)
        };
        assert_eq!(run(0, false), 3.0, "opened");
        assert_eq!(run(1, false), 2.0, "one object framed");
        assert_eq!(run(0, true), 0.0, "handed back unread");
        assert_eq!(run(1, true), 0.0, "handed back after one object");
    }

    /// Pins how `stop_reading` settles each state a held unit can be in:
    /// the sweep acknowledges what only it can, a failing unit is failed and
    /// never handed back, an unconfirmed re-enqueue is handed back, and a
    /// fatal answer in the sweep comes after the hand-back.
    #[test]
    fn stop_reading_settles_each_held_state() {
        // A lane that emitted nothing.
        let (mut reader, script, _rt) = stopping_reader();
        start(&mut reader);
        let (_, r, t) = hold_one(&mut reader, &script);
        t.set_terminal(0);
        reader.stop_reading().unwrap();
        assert_eq!(script.acknowledged(), vec![r], "terminal 0");
        assert!(script.handed_back().is_empty());

        // The last watermark arrived before end-of-input.
        let (mut reader, script, _rt) = stopping_reader();
        start(&mut reader);
        let (p, r, t) = hold_one(&mut reader, &script);
        reader.commit(&[(p, 5)]).unwrap();
        t.set_terminal(5);
        reader.stop_reading().unwrap();
        assert_eq!(
            script.acknowledged(),
            vec![r],
            "committed before its terminal"
        );

        // A unit with nothing to read whose acknowledge was refused once.
        let (mut reader, feed, script, _rt) = stopping_refusing();
        *feed.acknowledge.lock().unwrap() = Some(FeedErrorKind::Retryable);
        let test_event = br#"{"Service":"Amazon S3","Event":"s3:TestEvent","Bucket":"bucket"}"#;
        let r = script.deliver(Unit::new("test-event", test_event.to_vec()));
        start(&mut reader);
        poll(&mut reader);
        poll(&mut reader);
        assert!(script.acknowledged().is_empty());
        *feed.acknowledge.lock().unwrap() = None;
        reader.stop_reading().unwrap();
        assert_eq!(script.acknowledged(), vec![r], "nothing to read");
        assert!(script.handed_back().is_empty());

        // A poison report no poll has read.
        let (mut reader, script, _rt) = stopping_reader();
        let r = script.deliver(descriptor_unit(0));
        start(&mut reader);
        let lanes = partitions(&lanes_added(poll(&mut reader)));
        report_poison(&reader, lanes[0], PoisonKind::NotFound);
        reader.stop_reading().unwrap();
        assert_eq!(script.failed().len(), 1);
        assert_eq!(script.failed()[0].0, r);
        assert!(
            script.handed_back().is_empty(),
            "a failing unit was handed back"
        );

        // A failing unit whose fail the feed refuses for now.
        let (mut reader, feed, script, _rt) = stopping_refusing();
        script.deliver(descriptor_unit(0));
        start(&mut reader);
        let lanes = partitions(&lanes_added(poll(&mut reader)));
        *feed.fail.lock().unwrap() = Some(FeedErrorKind::Retryable);
        report_poison(&reader, lanes[0], PoisonKind::NotFound);
        reader.stop_reading().unwrap();
        assert!(script.handed_back().is_empty());
        assert!(reader.lanes.is_empty(), "its lane is still held");
        assert!(reader.deliveries.is_empty(), "the unit is still held");

        // An undecodable unit whose fail waits.
        let (mut reader, feed, script, _rt) = stopping_refusing();
        *feed.fail.lock().unwrap() = Some(FeedErrorKind::Retryable);
        script.deliver(Unit::new("unreadable", b"not a unit".to_vec()));
        start(&mut reader);
        poll(&mut reader);
        *feed.fail.lock().unwrap() = None;
        reader.stop_reading().unwrap();
        assert_eq!(script.failed().len(), 1);
        assert!(script.handed_back().is_empty());
        assert!(reader.unsent_fails.is_empty());

        // A re-enqueue the feed has not confirmed.
        let (mut reader, script, _rt) = stopping_reader();
        script.hold_publishes();
        let (r, lanes) = open_notification(&mut reader, &script, &[("a.ndjson", 10)], 0);
        report_overwrite(&reader, lanes[0], "a.ndjson", 20);
        poll(&mut reader);
        finish(&mut reader, lanes[0], 3);
        reader.stop_reading().unwrap();
        assert_eq!(script.handed_back(), vec![r]);
        assert!(script.acknowledged().is_empty());

        // One id delivered twice, read to its end once.
        for lapsed in [false, true] {
            let (mut reader, script, _rt) = stopping_reader();
            start(&mut reader);
            let (_, first, t) = hold_one(&mut reader, &script);
            let (_, second, _) = hold_one(&mut reader, &script);
            t.set_terminal(3);
            if lapsed {
                script.redeliver(first);
            }
            reader.stop_reading().unwrap();
            assert_eq!(script.handed_back(), vec![second]);
            if lapsed {
                assert!(script.gone().contains(&first));
            } else {
                assert_eq!(script.held(), vec![first]);
            }
        }

        // A fatal acknowledge in the sweep, after the hand-back.
        let (mut reader, feed, script, _rt) = stopping_refusing();
        start(&mut reader);
        let (_, _, t) = hold_one(&mut reader, &script);
        let (_, unread, _) = hold_one(&mut reader, &script);
        t.set_terminal(0);
        *feed.acknowledge.lock().unwrap() = Some(FeedErrorKind::Fatal);
        assert!(reader.stop_reading().is_err());
        assert_eq!(script.handed_back(), vec![unread]);
    }

    /// Pins that `stop_reading` renews each kept unit once and no unit it
    /// hands back.
    #[test]
    fn a_kept_unit_is_renewed_once_at_the_hand_back() {
        let (mut reader, script, _rt) = stopping_reader();
        start(&mut reader);
        let (_, kept, t) = hold_one(&mut reader, &script);
        hold_one(&mut reader, &script);
        t.set_terminal(3);
        reader.stop_reading().unwrap();
        assert_eq!(script.renewed(), vec![kept]);
    }

    /// Pins that a stopping reader opens a held unit's pending ranges into
    /// the room its finished ranges leave, receives nothing, and reads on
    /// until the last range ends; and that `stop_reading` hands back a unit
    /// with ranges still pending.
    #[test]
    fn a_unit_cut_into_more_ranges_than_units_held_opens_its_ranges_while_stopping() {
        let (mut reader, script, _rt) = stopping_reader_holding(1);
        let receipt = script.deliver(ranged_unit("a", 3));
        script.deliver(descriptor_unit(0));
        start(&mut reader);
        let mut lane = partitions(&lanes_added(poll(&mut reader)))[0];
        for range in 0..3 {
            assert!(reader.reading_on_stop(), "range {range}");
            finish(&mut reader, lane, 3);
            lanes_retired(poll(&mut reader));
            if range < 2 {
                lane = partitions(&lanes_added(poll(&mut reader)))[0];
            }
        }
        assert_eq!(script.acknowledged(), vec![receipt]);
        assert!(!reader.reading_on_stop());
        assert_eq!(script.delivered().len(), 1, "received while stopping");

        let pipeline = unique_name("reader");
        let rendered = render_local(|| {
            let metrics =
                S3Metrics::new(&Meter::with_namespace("s3", pipeline.clone(), "src", "s3"));
            let (feed, script) = scripted_feed();
            script.hand_back_on_stop();
            let (mut reader, _rt) = reader_over(Arc::new(feed), Some(1), Some(metrics));
            let receipt = script.deliver(ranged_unit("a", 3));
            start(&mut reader);
            lanes_added(poll(&mut reader));
            reader.stop_reading().unwrap();
            assert_eq!(script.handed_back(), vec![receipt]);
            assert_eq!(lanes_revoked(poll(&mut reader)).len(), 1);
            assert_idle(poll(&mut reader));
        });
        assert_eq!(objects_remaining(&rendered, &pipeline), 0.0);
    }

    /// Pins that `reading_on_stop` reads on for a unit whose lane reported an
    /// overwrite and then decided end-of-input since the last poll, so the
    /// stop waits for the re-enqueue to confirm.
    #[test]
    fn reading_on_stop_reads_an_overwrite_sent_before_the_terminal() {
        let (mut reader, script, _rt) = stopping_reader();
        script.hold_publishes();
        let (r, lanes) = open_notification(&mut reader, &script, &[("a.ndjson", 10)], 0);
        report_overwrite(&reader, lanes[0], "a.ndjson", 20);
        reader.lanes[&lanes[0]].tracker.set_terminal(3);
        assert!(reader.reading_on_stop(), "its re-enqueue is unconfirmed");
        assert_eq!(script.published().len(), 1);
        script.confirm_held_publishes();
        reader.commit(&[(lanes[0], 3)]).unwrap();
        poll(&mut reader);
        assert!(!reader.reading_on_stop());
        reader.stop_reading().unwrap();
        assert_eq!(script.acknowledged(), vec![r]);
        assert!(script.handed_back().is_empty());
    }

    /// Pins that a fatal answer to a re-enqueue that the stop reads first
    /// fails `stop_reading`, after the unit is handed back.
    #[test]
    fn a_fatal_re_enqueue_read_at_the_stop_fails_stop_reading() {
        let (mut reader, script, _rt) = stopping_reader();
        script.refuse_next_publish(FeedErrorKind::Fatal);
        let (r, lanes) = open_notification(&mut reader, &script, &[("a.ndjson", 10)], 0);
        report_overwrite(&reader, lanes[0], "a.ndjson", 20);
        let outcome = reader.stop_reading();
        assert_eq!(script.published().len(), 1, "the re-enqueue was published");
        assert_eq!(script.handed_back(), vec![r]);
        let Err(SourceError::Client { reason, .. }) = outcome else {
            panic!("a fatal re-enqueue at the stop did not fail the call: {outcome:?}");
        };
        assert!(reason.contains("re-enqueueing"), "{reason}");
    }

    /// Pins that a unit whose hand-back the feed answers `Gone` is let go in
    /// the hand-back and its lane revoked, though the feed still renews it.
    #[test]
    fn a_hand_back_answered_gone_lets_the_unit_go() {
        let (mut reader, feed, script, _rt) = stopping_refusing();
        start(&mut reader);
        let (p, first, _) = hold_one(&mut reader, &script);
        *feed.hand_back.lock().unwrap() = Some((first, FeedErrorKind::Gone));
        reader.stop_reading().unwrap();
        assert!(reader.deliveries.is_empty(), "the unit is still held");
        assert!(!reader.lanes.contains_key(&p), "its lane is still held");
        assert_eq!(lanes_revoked(poll(&mut reader)).len(), 1);
    }

    /// Pins that the stop's log counts as kept only the units held for the
    /// final commit: no failing unit, and no unit its renewal at the stop
    /// lets go.
    #[test]
    fn the_stop_counts_as_kept_only_units_held_for_the_final_commit() {
        let (mut reader, script, _rt) = stopping_reader_holding(2);
        script.deliver(descriptor_unit(0));
        script.deliver(descriptor_unit(1));
        start(&mut reader);
        let lanes = partitions(&lanes_added(poll(&mut reader)));
        reader.lanes[&lanes[0]].tracker.set_terminal(5);
        report_poison(&reader, lanes[1], PoisonKind::NotFound);
        let logs = spate_test::capture_logs(tracing::Level::INFO, || {
            reader.stop_reading().unwrap();
        });
        assert_eq!(script.failed().len(), 1);
        assert!(script.handed_back().is_empty());
        assert!(
            logs.iter()
                .any(|l| l.contains("kept=1") && l.contains("handed_back=0")),
            "a failing unit counted: {logs:?}"
        );

        let (mut reader, script, _rt) = stopping_reader();
        start(&mut reader);
        let (_, a, ta) = hold_one(&mut reader, &script);
        ta.set_terminal(5);
        script.redeliver(a);
        let logs = spate_test::capture_logs(tracing::Level::INFO, || {
            reader.stop_reading().unwrap();
        });
        assert!(script.gone().contains(&a), "the renewal was answered Gone");
        assert!(reader.deliveries.is_empty(), "the unit was let go");
        assert!(
            logs.iter()
                .any(|l| l.contains("kept=0") && l.contains("handed_back=0")),
            "a unit let go counted: {logs:?}"
        );
    }

    // ------------------------------------------------------ overwrites --

    /// A notification body naming each `(key, size)` under `data/`, pinned
    /// to an ETag and carrying `re_enqueues`.
    fn pinned_notification(objects: &[(&str, u64)], re_enqueues: u32) -> Vec<u8> {
        let records: Vec<serde_json::Value> = objects
            .iter()
            .map(|(key, size)| {
                serde_json::json!({
                    "eventName": "ObjectCreated:Put",
                    "spateReEnqueues": re_enqueues,
                    "s3": {
                        "bucket": { "name": "bucket" },
                        "object": { "key": format!("data/{key}"), "size": size, "eTag": "old" },
                    },
                })
            })
            .collect();
        serde_json::to_vec(&serde_json::json!({ "Records": records })).unwrap()
    }

    /// Report from the lane on `partition` that `data/<key>` was overwritten
    /// to `size` bytes.
    fn report_overwrite(reader: &UnitReader, partition: PartitionId, key: &str, size: u64) {
        reader
            .poison_tx
            .send(PoisonReport {
                partition,
                kind: PoisonKind::EtagDrift,
                reason: "overwritten".into(),
                overwrite: Some(Box::new(Overwrite {
                    key: format!("data/{key}"),
                    etag: Some("\"new\"".into()),
                    size,
                    last_modified_ms: 1_000,
                })),
            })
            .unwrap();
    }

    /// The key, ETag and re-enqueue count a published unit's body names.
    fn re_enqueued(unit: &Unit) -> (String, Option<String>, u32) {
        let Decoded::Read(cursors) = decode(
            &unit.body,
            &scope(),
            crate::split::Packing {
                target_bytes: u64::MAX,
                compression: Compression::None,
                delimiter: None,
            },
        ) else {
            panic!("the published unit is not read");
        };
        let part = cursors.into_iter().flatten().next().unwrap();
        let Origin::Notification { re_enqueues, .. } = part.origin else {
            panic!("the published unit is not a notification");
        };
        (
            part.object[0].key.clone(),
            part.object[0].etag.clone(),
            re_enqueues,
        )
    }

    /// Deliver a pinned notification for `objects` at `re_enqueues`, start
    /// `reader`, and return the receipt and the lanes it opens.
    fn open_notification(
        reader: &mut UnitReader,
        script: &FeedScript,
        objects: &[(&str, u64)],
        re_enqueues: u32,
    ) -> (Receipt, Vec<PartitionId>) {
        let receipt = script.deliver(Unit::new("n", pinned_notification(objects, re_enqueues)));
        start(reader);
        (receipt, partitions(&lanes_added(poll(reader))))
    }

    /// Pins that a unit cut short by an overwrite, whose lane has retired,
    /// waits for the feed to confirm its re-enqueue, renewed and never
    /// failed, and is acknowledged on the poll after the confirmation.
    #[test]
    fn a_412_unit_waits_for_its_publish_after_its_records_commit() {
        let (mut reader, script, _rt) = reader();
        script.hold_publishes();
        let (receipt, lanes) = open_notification(&mut reader, &script, &[("a.ndjson", 10)], 0);
        report_overwrite(&reader, lanes[0], "a.ndjson", 20);
        poll(&mut reader);
        assert_eq!(script.published().len(), 1);
        finish(&mut reader, lanes[0], 3);
        for _ in 0..3 {
            poll(&mut reader);
        }
        assert!(
            script.acknowledged().is_empty(),
            "acknowledged before the publish settled"
        );
        assert!(script.renewed().contains(&receipt));
        script.confirm_held_publishes();
        poll(&mut reader);
        assert_eq!(script.acknowledged(), vec![receipt]);
        assert!(script.failed().is_empty());
        assert_eq!(
            re_enqueued(&script.published()[0]),
            ("data/a.ndjson".to_owned(), Some("\"new\"".to_owned()), 1)
        );
        assert_eq!(
            script.published()[0].id,
            "n",
            "the re-enqueue keeps the unit's id"
        );
    }

    /// Pins that a unit whose re-enqueue the feed confirmed still waits for
    /// its lane to retire, and is acknowledged once it does.
    #[test]
    fn a_412_unit_waits_for_its_records_after_its_publish_is_confirmed() {
        let (mut reader, script, _rt) = reader_holding(1);
        let (receipt, lanes) = open_notification(&mut reader, &script, &[("a.ndjson", 10)], 0);
        report_overwrite(&reader, lanes[0], "a.ndjson", 20);
        for _ in 0..3 {
            poll(&mut reader);
        }
        assert_eq!(script.published().len(), 1);
        assert!(
            script.acknowledged().is_empty(),
            "acknowledged before its lane retired"
        );
        finish(&mut reader, lanes[0], 3);
        assert_eq!(script.acknowledged(), vec![receipt]);
        assert!(script.failed().is_empty());
    }

    /// Pins that two ranges of one key that both report the overwrite
    /// publish one re-enqueue.
    #[test]
    fn two_ranges_of_one_key_that_412_publish_once() {
        let (mut reader, script, _rt) = reader();
        let (receipt, lanes) =
            open_notification(&mut reader, &script, &[("a.ndjson", 2 * TARGET)], 0);
        assert_eq!(lanes.len(), 2);
        report_overwrite(&reader, lanes[0], "a.ndjson", 20);
        report_overwrite(&reader, lanes[1], "a.ndjson", 20);
        poll(&mut reader);
        assert_eq!(script.published().len(), 1);
        finish(&mut reader, lanes[0], 0);
        finish(&mut reader, lanes[1], 2);
        assert_eq!(script.acknowledged(), vec![receipt]);
    }

    /// Pins that an overwrite drops the key's ranges not yet opened.
    #[test]
    fn an_overwrite_drops_the_keys_unopened_ranges() {
        let (mut reader, script, _rt) = reader_holding(1);
        let (receipt, lanes) =
            open_notification(&mut reader, &script, &[("a.ndjson", 3 * TARGET)], 0);
        assert_eq!(lanes.len(), 1);
        report_overwrite(&reader, lanes[0], "a.ndjson", 20);
        poll(&mut reader);
        finish(&mut reader, lanes[0], 2);
        assert_eq!(script.acknowledged(), vec![receipt]);
        assert_eq!(script.published().len(), 1);
    }

    /// Pins that an overwrite of one record's key leaves the unit's other
    /// record read, and that the unit is acknowledged once both settle.
    #[test]
    fn a_412_on_one_record_leaves_the_other_record_read() {
        let (mut reader, script, _rt) = reader();
        let (receipt, lanes) = open_notification(
            &mut reader,
            &script,
            &[("a.ndjson", 10), ("b.ndjson", 10)],
            0,
        );
        assert_eq!(lanes.len(), 2);
        report_overwrite(&reader, lanes[0], "a.ndjson", 20);
        poll(&mut reader);
        finish(&mut reader, lanes[0], 1);
        assert!(
            script.acknowledged().is_empty(),
            "the other record is unread"
        );
        finish(&mut reader, lanes[1], 4);
        assert_eq!(script.acknowledged(), vec![receipt]);
        assert_eq!(script.published().len(), 1);
    }

    /// Pins that a poison on another part fails a unit back after an
    /// overwrite of one of its keys was re-enqueued.
    #[test]
    fn a_poison_on_another_part_fails_an_overwritten_unit_back() {
        let (mut reader, script, _rt) = reader();
        let (receipt, lanes) = open_notification(
            &mut reader,
            &script,
            &[("a.ndjson", 10), ("b.ndjson", 10)],
            0,
        );
        report_overwrite(&reader, lanes[0], "a.ndjson", 20);
        poll(&mut reader);
        reader
            .poison_tx
            .send(PoisonReport {
                partition: PartitionId(lanes[1].0),
                kind: PoisonKind::NotFound,
                reason: "gone".into(),
                overwrite: None,
            })
            .unwrap();
        poll(&mut reader);
        assert_eq!(script.failed().len(), 1);
        assert_eq!(script.failed()[0].0, receipt);
        assert!(script.acknowledged().is_empty());
        assert_eq!(script.published().len(), 1);
    }

    /// Pins that a re-enqueue the feed refuses, or cannot confirm, fails the
    /// unit back with a WARN naming it, never acknowledged.
    #[test]
    fn a_refused_or_retryable_re_enqueue_fails_the_unit_back() {
        for kind in [FeedErrorKind::Refused, FeedErrorKind::Retryable] {
            let (mut reader, script, _rt) = reader();
            script.refuse_next_publish(kind);
            let (receipt, lanes) = open_notification(&mut reader, &script, &[("a.ndjson", 10)], 0);
            report_overwrite(&reader, lanes[0], "a.ndjson", 20);
            let mut revoked = Vec::new();
            let lines = spate_test::capture_logs(tracing::Level::WARN, || {
                revoked = lanes_revoked(poll(&mut reader));
            });
            assert_eq!(revoked, [LaneId(lanes[0].0)], "{kind:?}");
            assert!(
                lines
                    .iter()
                    .any(|l| l.contains("did not store the re-enqueue") && l.contains("unit=n")),
                "{lines:#?}"
            );
            assert_eq!(script.failed().len(), 1, "{kind:?}");
            assert_eq!(script.failed()[0].0, receipt, "{kind:?}");
            assert!(script.acknowledged().is_empty(), "{kind:?}");
        }
    }

    /// Pins that a re-enqueue the feed answers `Fatal` fails the reader.
    #[test]
    fn a_fatal_re_enqueue_stops_the_reader() {
        let (mut reader, script, _rt) = reader();
        script.refuse_next_publish(FeedErrorKind::Fatal);
        let (_, lanes) = open_notification(&mut reader, &script, &[("a.ndjson", 10)], 0);
        report_overwrite(&reader, lanes[0], "a.ndjson", 20);
        let e = reader.poll_events(Duration::ZERO).unwrap_err();
        assert!(
            e.to_string()
                .contains("re-enqueueing an overwritten object"),
            "{e}"
        );
    }

    /// Pins that a re-enqueue confirmed after the feed delivered its unit
    /// again acknowledges nothing, and the reader keeps polling.
    #[test]
    fn a_re_enqueue_confirmed_after_its_unit_lapsed_acknowledges_nothing() {
        let (mut reader, script, _rt) = reader_holding(1);
        script.hold_publishes();
        let (receipt, lanes) = open_notification(&mut reader, &script, &[("a.ndjson", 10)], 0);
        report_overwrite(&reader, lanes[0], "a.ndjson", 20);
        poll(&mut reader);
        script.redeliver(receipt);
        assert_eq!(lanes_revoked(poll(&mut reader)).len(), 1);
        script.confirm_held_publishes();
        for _ in 0..4 {
            reader.poll_events(Duration::ZERO).unwrap();
        }
        assert!(!script.acknowledged().contains(&receipt));
    }

    /// Pins the cap's bound: a unit one below the cap re-enqueues at the cap,
    /// and a unit at 2 re-enqueues at 3.
    #[test]
    fn a_unit_one_below_the_cap_re_enqueues_at_the_cap() {
        let cap = crate::source::DEFAULT_RE_ENQUEUE_CAP;
        for (count, want) in [(cap - 1, cap), (2, 3)] {
            let (mut reader, script, _rt) = reader();
            let (_, lanes) = open_notification(&mut reader, &script, &[("a.ndjson", 10)], count);
            report_overwrite(&reader, lanes[0], "a.ndjson", 20);
            poll(&mut reader);
            assert_eq!(script.published().len(), 1, "{count}");
            assert_eq!(re_enqueued(&script.published()[0]).2, want);
        }
    }

    /// Pins that a unit at the cap, or at `u32::MAX`, publishes nothing on
    /// an overwrite, is acknowledged once its lane retires, is counted in
    /// both counters, and is named in a WARN.
    #[test]
    fn a_unit_at_the_re_enqueue_cap_is_counted() {
        for count in [crate::source::DEFAULT_RE_ENQUEUE_CAP, u32::MAX] {
            let pipeline = unique_name("reader");
            let mut lines = Vec::new();
            let rendered = render_local(|| {
                let metrics =
                    S3Metrics::new(&Meter::with_namespace("s3", pipeline.clone(), "src", "s3"));
                let (feed, script) = scripted_feed();
                let (mut reader, _rt) = reader_over(Arc::new(feed), None, Some(metrics));
                let (receipt, lanes) =
                    open_notification(&mut reader, &script, &[("a.ndjson", 10)], count);
                report_overwrite(&reader, lanes[0], "a.ndjson", 20);
                lines = spate_test::capture_logs(tracing::Level::WARN, || {
                    poll(&mut reader);
                });
                assert!(script.published().is_empty(), "{count}");
                finish(&mut reader, lanes[0], 3);
                assert_eq!(script.acknowledged(), vec![receipt], "{count}");
            });
            let value = |name: &str| {
                metric_value(&rendered, name, &[("pipeline", pipeline.as_str())])
                    .expect("the counter is registered")
            };
            assert_eq!(value("spate_s3_overwrites_dropped_total"), 1.0, "{count}");
            assert_eq!(value("spate_s3_objects_overwritten_total"), 1.0, "{count}");
            assert!(
                lines
                    .iter()
                    .any(|l| l.contains("re-enqueue cap") && l.contains("data/a.ndjson")),
                "{lines:#?}"
            );
        }
    }

    /// Pins that an overwrite reported by both ranges of one key is counted
    /// once, never as an `etag_drift` failure, and that `objects_remaining`
    /// takes back both ranges once the unit is acknowledged, leaving only
    /// the lane of the re-enqueued unit the scripted feed delivers back.
    #[test]
    fn a_412_truncation_is_counted_once_per_key() {
        let pipeline = unique_name("reader");
        let mut held = 0;
        let rendered = render_local(|| {
            let metrics =
                S3Metrics::new(&Meter::with_namespace("s3", pipeline.clone(), "src", "s3"));
            let (feed, script) = scripted_feed();
            let (mut reader, _rt) = reader_over(Arc::new(feed), None, Some(metrics));
            let (receipt, lanes) =
                open_notification(&mut reader, &script, &[("a.ndjson", 2 * TARGET)], 0);
            report_overwrite(&reader, lanes[0], "a.ndjson", 20);
            report_overwrite(&reader, lanes[1], "a.ndjson", 20);
            poll(&mut reader);
            finish(&mut reader, lanes[0], 0);
            finish(&mut reader, lanes[1], 2);
            assert_eq!(script.acknowledged(), vec![receipt]);
            held = reader.lanes.len();
        });
        let labels = [("pipeline", pipeline.as_str())];
        assert_eq!(
            metric_value(&rendered, "spate_s3_objects_overwritten_total", &labels),
            Some(1.0)
        );
        assert_eq!(
            metric_value(&rendered, "spate_s3_overwrites_dropped_total", &labels),
            Some(0.0)
        );
        assert_eq!(
            metric_value(
                &rendered,
                "spate_s3_objects_failed_total",
                &[("pipeline", pipeline.as_str()), ("reason", "etag_drift")]
            ),
            Some(0.0)
        );
        assert_eq!(objects_remaining(&rendered, &pipeline), held as f64);
    }

    /// Pins that an object overwritten to no bytes is acknowledged without a
    /// re-enqueue, since it holds no record, with an INFO line naming the
    /// key, and that one overwritten to a single byte is re-enqueued.
    #[test]
    fn an_overwrite_to_size_0_is_acknowledged_without_a_re_enqueue() {
        let (mut reader, script, _rt) = reader();
        let (receipt, lanes) = open_notification(&mut reader, &script, &[("a.ndjson", 10)], 0);
        report_overwrite(&reader, lanes[0], "a.ndjson", 0);
        let lines = spate_test::capture_logs(tracing::Level::INFO, || {
            poll(&mut reader);
        });
        assert!(
            lines
                .iter()
                .any(|l| l.contains("is now empty") && l.contains("data/a.ndjson")),
            "{lines:#?}"
        );
        finish(&mut reader, lanes[0], 3);
        assert_eq!(script.acknowledged(), vec![receipt]);
        assert!(script.published().is_empty());

        let (mut one_byte, script, _rt) = self::reader();
        let (_, lanes) = open_notification(&mut one_byte, &script, &[("a.ndjson", 10)], 0);
        report_overwrite(&one_byte, lanes[0], "a.ndjson", 1);
        poll(&mut one_byte);
        assert_eq!(
            script.published().len(),
            1,
            "a one-byte object is re-enqueued"
        );
    }

    /// Pins that a commit reaching the reader before a poll has read the
    /// lane's overwrite report does not acknowledge the unit: the commit
    /// reads the report and publishes the re-enqueue, and the unit is
    /// acknowledged once the feed confirms it.
    #[test]
    fn a_commit_before_the_overwrite_report_is_read_waits_for_the_re_enqueue() {
        let (mut reader, script, _rt) = reader_holding(1);
        script.hold_publishes();
        let (receipt, lanes) = open_notification(&mut reader, &script, &[("a.ndjson", 10)], 0);
        report_overwrite(&reader, lanes[0], "a.ndjson", 20);
        finish(&mut reader, lanes[0], 3);
        assert!(
            script.acknowledged().is_empty(),
            "acknowledged before the re-enqueue"
        );
        assert_eq!(script.published().len(), 1);
        script.confirm_held_publishes();
        poll(&mut reader);
        assert_eq!(script.acknowledged(), vec![receipt]);
    }

    /// Pins that an overwrite reported for a unit already failing back is
    /// counted once and not re-enqueued: the redelivered unit reads the key's
    /// current content.
    #[test]
    fn an_overwrite_on_a_failing_unit_is_counted_and_not_re_enqueued() {
        let pipeline = unique_name("reader");
        let rendered = render_local(|| {
            let metrics =
                S3Metrics::new(&Meter::with_namespace("s3", pipeline.clone(), "src", "s3"));
            let (feed, script) = scripted_feed();
            let (mut reader, _rt) = reader_over(Arc::new(feed), None, Some(metrics));
            let (receipt, lanes) = open_notification(
                &mut reader,
                &script,
                &[("a.ndjson", 10), ("b.ndjson", 10)],
                0,
            );
            reader
                .poison_tx
                .send(PoisonReport {
                    partition: PartitionId(lanes[1].0),
                    kind: PoisonKind::NotFound,
                    reason: "gone".into(),
                    overwrite: None,
                })
                .unwrap();
            report_overwrite(&reader, lanes[0], "a.ndjson", 20);
            poll(&mut reader);
            assert!(script.published().is_empty());
            assert_eq!(script.failed().len(), 1);
            assert_eq!(script.failed()[0].0, receipt);
        });
        assert_eq!(
            metric_value(
                &rendered,
                "spate_s3_objects_overwritten_total",
                &[("pipeline", pipeline.as_str())]
            ),
            Some(1.0)
        );
    }

    /// A [`Feed`] over a scripted feed that runs a hook inside its next
    /// `fail`, placing what a lane thread does at that point of the sweep.
    struct FailHook {
        inner: ScriptedFeed,
        hook: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }

    impl Feed for FailHook {
        fn backend(&self) -> &str {
            self.inner.backend()
        }

        fn set_waker(&self, waker: FeedWaker) {
            self.inner.set_waker(waker);
        }

        fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
            self.inner.receive(max)
        }

        fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
            self.inner.renew(receipt)
        }

        fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
            self.inner.acknowledge(receipt)
        }

        fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError> {
            if let Some(hook) = self.hook.lock().unwrap().take() {
                hook();
            }
            self.inner.fail(receipt, reason)
        }

        fn publish(&self, unit: Unit) -> Confirmation {
            self.inner.publish(unit)
        }

        fn read_depth(
            &self,
            of: spate_core::feed::DepthOf,
        ) -> Option<spate_core::feed::DepthReading> {
            self.inner.read_depth(of)
        }
    }

    /// Pins the sweep's second report read: a lane that reports an overwrite
    /// and decides end-of-input with nothing framed after the sweep's first
    /// read is not retired before its report is read, so its unit is
    /// acknowledged only after the re-enqueue is published and confirmed.
    #[test]
    fn an_overwrite_reported_during_the_sweep_is_re_enqueued() {
        let (inner, script) = scripted_feed();
        let feed = Arc::new(FailHook {
            inner,
            hook: Mutex::new(None),
        });
        let (mut reader, _rt) = reader_over(Arc::clone(&feed) as Arc<dyn Feed>, None, None);
        script.hold_publishes();
        let failing = script.deliver(Unit::new("x", pinned_notification(&[("x.ndjson", 10)], 0)));
        let overwritten =
            script.deliver(Unit::new("y", pinned_notification(&[("y.ndjson", 10)], 0)));
        start(&mut reader);
        let lanes = partitions(&lanes_added(poll(&mut reader)));
        assert_eq!(lanes.len(), 2);
        let (px, py) = (lanes[0], lanes[1]);
        assert_ne!(delivery_of(&reader, px), delivery_of(&reader, py));
        reader
            .poison_tx
            .send(PoisonReport {
                partition: PartitionId(px.0),
                kind: PoisonKind::NotFound,
                reason: "gone".into(),
                overwrite: None,
            })
            .unwrap();
        // The lane on `py` sends its overwrite report and then decides
        // end-of-input with nothing framed, while the sweep fails `x` back.
        let tx = reader.poison_tx.clone();
        let tracker = Arc::clone(&reader.lanes[&py].tracker);
        *feed.hook.lock().unwrap() = Some(Box::new(move || {
            tx.send(PoisonReport {
                partition: PartitionId(py.0),
                kind: PoisonKind::EtagDrift,
                reason: "overwritten".into(),
                overwrite: Some(Box::new(Overwrite {
                    key: "data/y.ndjson".into(),
                    etag: Some("\"new\"".into()),
                    size: 20,
                    last_modified_ms: 1_000,
                })),
            })
            .unwrap();
            tracker.set_terminal(0);
        }));
        for _ in 0..4 {
            reader.poll_events(Duration::ZERO).unwrap();
        }
        assert_eq!(script.failed().len(), 1);
        assert_eq!(script.failed()[0].0, failing);
        assert!(
            !script.acknowledged().contains(&overwritten),
            "acknowledged with no re-enqueue confirmed"
        );
        assert_eq!(script.published().len(), 1, "the key is re-enqueued");
        script.confirm_held_publishes();
        for _ in 0..2 {
            reader.poll_events(Duration::ZERO).unwrap();
        }
        assert_eq!(script.acknowledged(), vec![overwritten]);
    }
}
