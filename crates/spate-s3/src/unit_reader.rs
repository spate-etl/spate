//! The feed read path: [`UnitReader`] reads work units from a [`Feed`] and
//! acknowledges each under the feed's ack rule.
//!
//! A unit's body is a [`SplitDescriptor`](crate::split::SplitDescriptor)
//! naming one object, whole or one byte range of it, or a storage
//! notification, which [`decode`](crate::unit::decode) reads into one part per
//! created object. A notification with no object to read is acknowledged, and
//! one the source cannot read is failed back. A whole-object part over an
//! object the planner would cut is read in the ranges
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

use crate::fetch::{FetcherParams, run_fetcher};
use crate::framer::FramerFactory;
use crate::lane::S3Lane;
use crate::metrics::S3Metrics;
use crate::split::Packing;
use crate::split_ctx::{LANE_HANDOFF_CHUNKS, PoisonReport, SplitTracker};
use crate::unit::{Cursor, Decoded, Part, Scope, decode};
use spate_core::checkpoint::AckIssuer;
use spate_core::coordination::{LeaseEpoch, SplitId};
use spate_core::error::SourceError;
use spate_core::feed::{
    Delivery, Feed, FeedError, FeedErrorKind, FeedPark, FeedWaker, Receipt, wake_channel,
};
use spate_core::record::PartitionId;
use spate_core::source::{DrainBarrier, LaneId, SourceEvent};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

/// Units held at once when the `feed:` section sets no `units_held`. The
/// reader receives only while it holds fewer.
const UNITS_HELD: usize = 96;

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
}

impl HeldUnit {
    /// Every part has opened and retired, so the unit can be acknowledged.
    fn finished(&self) -> bool {
        self.failing.is_none() && self.pending.is_empty() && self.lanes.is_empty()
    }

    /// The delivery holds no lane and has no part to open. It still takes
    /// one of `units_held`.
    fn idle(&self) -> bool {
        self.lanes.is_empty() && self.pending.is_empty()
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
    /// Lanes whose records are all committed, leaving with nothing in
    /// flight.
    retired: Vec<LaneId>,
    /// `fail`s for undecodable units that the feed answered `Retryable`.
    unsent_fails: Vec<(Receipt, String)>,
    poison_tx: std::sync::mpsc::Sender<PoisonReport>,
    poison_rx: std::sync::mpsc::Receiver<PoisonReport>,
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
    /// records fall within `scope`.
    pub(crate) fn new(
        feed: Arc<dyn Feed>,
        parts: LaneParts,
        scope: Scope,
        units_held: Option<NonZeroUsize>,
    ) -> UnitReader {
        let (waker, park) = wake_channel();
        feed.set_waker(waker.clone());
        let (poison_tx, poison_rx) = std::sync::mpsc::channel();
        UnitReader {
            feed,
            park,
            waker,
            parts,
            scope,
            units_held: units_held.map_or(UNITS_HELD, NonZeroUsize::get),
            started: false,
            next_delivery: 0,
            next_lane: 0,
            deliveries: BTreeMap::new(),
            lanes: BTreeMap::new(),
            revoked: Vec::new(),
            retired: Vec::new(),
            unsent_fails: Vec::new(),
            poison_tx,
            poison_rx,
        }
    }

    /// Surface at most one controller event, waiting on the feed for at
    /// most `timeout` when there is nothing to report.
    ///
    /// Never reports `Drained`, so the run ends at a shutdown.
    pub(crate) fn poll_events(
        &mut self,
        timeout: Duration,
    ) -> Result<SourceEvent<S3Lane>, SourceError> {
        if !self.started {
            self.started = true;
            return Ok(SourceEvent::LanesAssigned(Vec::new()));
        }
        self.settle()?;
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
        if room > 0 {
            self.receive(room)?;
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
        self.park.park(timeout);
        Ok(SourceEvent::Idle)
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
            if lane.tracker.terminal() == Some(watermark)
                && self.deliveries[&delivery].failing.is_none()
            {
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

    /// Settle what the lanes and the feed left since the last poll (poison
    /// reports, refused fails, finished lanes and units), then renew every
    /// unit still held.
    fn settle(&mut self) -> Result<(), SourceError> {
        while let Ok(report) = self.poison_rx.try_recv() {
            if let Some(m) = &self.parts.metrics {
                m.objects_failed(report.kind).increment(1);
            }
            let partition = PartitionId(report.epoch.0 as u32);
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
        for partition in ended {
            self.retire(partition);
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
                Err(e) if e.kind == FeedErrorKind::Gone => self.release(delivery),
                Err(e) if e.kind == FeedErrorKind::Retryable => {
                    tracing::warn!(%receipt, error = %e, "renewing a held unit failed; retrying");
                }
                Err(e) => return Err(feed_error("renewing a held unit", &e)),
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

    /// Let go of a failed delivery or one answered `Gone`: its lanes leave
    /// through a drain barrier, since records they emitted may be in flight,
    /// and its unopened parts are dropped.
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

    /// Take back the lane's count in `objects_remaining`, less the object
    /// it finished.
    fn settle_remaining(&self, lane: &LaneState) {
        if let Some(m) = &self.parts.metrics {
            let done = u64::from(lane.tracker.close());
            m.objects_remaining
                .decrement(1u64.saturating_sub(done) as f64);
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

    /// Receive up to `room` deliveries and hold each readable one.
    fn receive(&mut self, room: usize) -> Result<(), SourceError> {
        let deliveries = match self.feed.receive(room) {
            Ok(deliveries) => deliveries,
            Err(e) if e.kind == FeedErrorKind::Retryable => {
                tracing::warn!(error = %e, "receiving from the feed failed; retrying");
                return Ok(());
            }
            Err(e) => return Err(feed_error("receiving from the feed", &e)),
        };
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
        Ok(())
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
        let split = SplitId::new(format!("delivery-{delivery}-lane-{n}"))
            .expect("a lane name is a valid id");
        let parts = &self.parts;
        let tracker = Arc::new(SplitTracker::new());
        let pause = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::channel(LANE_HANDOFF_CHUNKS);
        // Detached: the fetcher exits when the lane drops its receiver.
        drop(parts.handle.spawn(run_fetcher(FetcherParams {
            split: split.clone(),
            store: Arc::clone(&parts.store),
            slice: part.object,
            start_ordinal: 0,
            resume_etag: None,
            chunk_bytes: parts.chunk_bytes,
            range_bytes: parts.range_bytes,
            tx,
            pause: Arc::clone(&pause),
            // A delivery is never handed over part-read.
            stop: Arc::new(AtomicBool::new(false)),
            retry_base: parts.retry_base,
            retries: parts.metrics.as_ref().map(|m| m.get_retries.clone()),
            range: part.range,
            bytes_read: parts.metrics.as_ref().map(|m| m.bytes_read.clone()),
        })));
        if let Some(m) = &parts.metrics {
            m.objects_remaining.increment(1.0);
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
            split,
            // Names the lane in poison reports.
            LeaseEpoch(u64::from(n)),
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
    use crate::split::{DescriptorObject, SplitDescriptor};
    use crate::split_ctx::PoisonKind;
    use crate::testutil::TestLineFramer;
    use spate_core::checkpoint::Checkpointer;
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
        };
        let units_held = units_held.map(|n| NonZeroUsize::new(n).unwrap());
        (UnitReader::new(feed, parts, scope(), units_held), rt)
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

    /// A [`Feed`] over a scripted feed whose `receive`, `acknowledge` and
    /// `fail` answer the error kind set for them, without reaching the
    /// script.
    struct Refusing {
        inner: ScriptedFeed,
        receive: Mutex<Option<FeedErrorKind>>,
        acknowledge: Mutex<Option<FeedErrorKind>>,
        fail: Mutex<Option<FeedErrorKind>>,
    }

    impl Refusing {
        fn new(inner: ScriptedFeed) -> Refusing {
            Refusing {
                inner,
                receive: Mutex::new(None),
                acknowledge: Mutex::new(None),
                fail: Mutex::new(None),
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
            },
        );
        (partition, receipt, tracker)
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

    /// Pins the cap on units held: the reader holds at most 96 units,
    /// leaves the rest queued, and receives only into the room a settled
    /// unit frees.
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
                            split: SplitId::new("delivery-0-lane-0").unwrap(),
                            epoch: LeaseEpoch(u64::from(lanes[0].0)),
                            kind: PoisonKind::NotFound,
                            reason: "gone".into(),
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
                };
                let (feed, script) = scripted_feed();
                let mut reader =
                    UnitReader::new(Arc::new(feed), parts, scope(), NonZeroUsize::new(4));
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
                            split: SplitId::new("delivery-0-lane-0").unwrap(),
                            epoch: LeaseEpoch(u64::from(lanes[0].partition().0)),
                            kind: PoisonKind::NotFound,
                            reason: "gone".into(),
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
                split: SplitId::new("delivery-1-lane-1").unwrap(),
                epoch: LeaseEpoch(u64::from(range.0)),
                kind: PoisonKind::NotFound,
                reason: "gone".into(),
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
}
