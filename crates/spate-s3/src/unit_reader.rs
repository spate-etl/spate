//! The feed read path: [`UnitReader`] reads one-object work units from a
//! [`Feed`] and acknowledges each under the feed's ack rule.
//!
//! A unit's body is a [`SplitDescriptor`] naming one whole object. Every
//! delivery opens its own lane and partition and reads its object from the
//! first byte, so a redelivery and a duplicate are each read in full. The
//! reader keys what it holds by partition, one per delivery, and never by
//! unit id.
//!
//! A unit is acknowledged once its lane has decided end-of-input at the
//! terminal watermark `T` and the acknowledged watermark `W` that
//! [`commit`](UnitReader::commit) receives equals `T`, or at once when the
//! lane emitted nothing. An object-level failure fails the unit back to the
//! feed, which delivers it again until it dead-letters it.

use crate::config::Compression;
use crate::fetch::{FetcherParams, ObjectEntry, run_fetcher};
use crate::framer::FramerFactory;
use crate::lane::S3Lane;
use crate::metrics::S3Metrics;
use crate::split::SplitDescriptor;
use crate::split_ctx::{LANE_HANDOFF_CHUNKS, PoisonReport, SplitTracker};
use spate_core::checkpoint::AckIssuer;
use spate_core::coordination::{LeaseEpoch, SplitId};
use spate_core::error::SourceError;
use spate_core::feed::{
    Delivery, Feed, FeedError, FeedErrorKind, FeedPark, FeedWaker, Receipt, wake_channel,
};
use spate_core::record::PartitionId;
use spate_core::source::{DrainBarrier, LaneId, SourceEvent};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

/// Units held at once: the reader receives only while it holds fewer. The
/// read parallelism of the measured runs, until #1090's key replaces it.
const UNITS_HELD: usize = 96;

/// What the reader needs to open a lane, fixed at `open`.
pub(crate) struct LaneParts {
    pub(crate) store: Arc<dyn object_store::ObjectStore>,
    pub(crate) handle: tokio::runtime::Handle,
    pub(crate) issuer: AckIssuer,
    pub(crate) make_framer: FramerFactory,
    pub(crate) compression: Compression,
    pub(crate) chunk_bytes: usize,
    pub(crate) range_bytes: usize,
    pub(crate) retry_base: Duration,
    pub(crate) metrics: Option<S3Metrics>,
}

/// One delivery the reader holds, with the lane reading it.
struct Held {
    receipt: Receipt,
    /// The unit's id, for logs.
    unit: String,
    lane: LaneId,
    pause: Arc<AtomicBool>,
    tracker: Arc<SplitTracker>,
    /// The latest acknowledged watermark `commit` received for the lane.
    last_committed: Option<i64>,
    /// The lane's end-of-input has been reported through `CommitReady`.
    hinted: bool,
    /// The lane hit poison, and this reason waits for a `fail` the feed
    /// accepts.
    failing: Option<String>,
}

/// Reads work units from a feed, one lane per delivery.
pub(crate) struct UnitReader {
    feed: Arc<dyn Feed>,
    park: FeedPark,
    /// The waker the feed signals, cloned into every lane so a lane that
    /// ends or fails ends the park too.
    waker: FeedWaker,
    parts: LaneParts,
    started: bool,
    /// Sequence number of the next delivery, which names its partition,
    /// lane and poison reports.
    next: u32,
    held: BTreeMap<PartitionId, Held>,
    /// Lanes to stop through a drain barrier: records they emitted may be
    /// in flight.
    revoked: Vec<LaneId>,
    /// Lanes whose unit is acknowledged, leaving with nothing in flight.
    retired: Vec<LaneId>,
    /// `fail`s for undecodable units that the feed answered `Retryable`.
    unsent_fails: Vec<(Receipt, String)>,
    poison_tx: std::sync::mpsc::Sender<PoisonReport>,
    poison_rx: std::sync::mpsc::Receiver<PoisonReport>,
}

impl std::fmt::Debug for UnitReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnitReader")
            .field("held", &self.held.len())
            .finish_non_exhaustive()
    }
}

impl UnitReader {
    pub(crate) fn new(feed: Arc<dyn Feed>, parts: LaneParts) -> UnitReader {
        let (waker, park) = wake_channel();
        feed.set_waker(waker.clone());
        let (poison_tx, poison_rx) = std::sync::mpsc::channel();
        UnitReader {
            feed,
            park,
            waker,
            parts,
            started: false,
            next: 0,
            held: BTreeMap::new(),
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
        // Receive before parking: a park consumes the feed's signal, and a
        // receive that takes nothing sends none.
        let room = UNITS_HELD.saturating_sub(self.held.len());
        if room > 0 {
            let lanes = self.receive(room)?;
            if !lanes.is_empty() {
                return Ok(SourceEvent::LanesAdded(lanes));
            }
        }
        let partitions: Vec<PartitionId> = self
            .held
            .iter_mut()
            .filter(|(_, held)| !held.hinted && held.tracker.terminal().is_some())
            .map(|(&partition, held)| {
                held.hinted = true;
                partition
            })
            .collect();
        if !partitions.is_empty() {
            return Ok(SourceEvent::CommitReady { partitions });
        }
        self.park.park(timeout);
        Ok(SourceEvent::Idle)
    }

    /// Record acknowledged watermarks, and acknowledge each unit whose lane
    /// ended at the watermark given. A watermark for a partition no longer
    /// held belongs to a delivery already settled and is skipped.
    pub(crate) fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
        for &(partition, watermark) in watermarks {
            let Some(held) = self.held.get_mut(&partition) else {
                continue;
            };
            held.last_committed = Some(watermark);
            if held.failing.is_none() && held.tracker.terminal() == Some(watermark) {
                self.acknowledge(partition)?;
            }
        }
        Ok(())
    }

    /// Flip the pause flag of every held lane named.
    pub(crate) fn set_paused(&self, lanes: &[LaneId], paused: bool) {
        for held in self.held.values() {
            if lanes.contains(&held.lane) {
                held.pause.store(paused, Ordering::Relaxed);
            }
        }
    }

    /// Settle what the lanes and the feed left since the last poll (poison
    /// reports, refused fails, finished units), then renew every unit still
    /// held.
    fn settle(&mut self) -> Result<(), SourceError> {
        while let Ok(report) = self.poison_rx.try_recv() {
            if let Some(m) = &self.parts.metrics {
                m.objects_failed(report.kind).increment(1);
            }
            let partition = PartitionId(report.epoch.0 as u32);
            let Some(held) = self.held.get_mut(&partition) else {
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
        let failing: Vec<PartitionId> = self
            .held
            .iter()
            .filter(|(_, held)| held.failing.is_some())
            .map(|(&partition, _)| partition)
            .collect();
        for partition in failing {
            let held = &self.held[&partition];
            let reason = held.failing.clone().unwrap_or_default();
            match settled(self.feed.fail(held.receipt, &reason))? {
                Settled::Yes => self.close(partition, false),
                Settled::Later => {}
            }
        }
        for (receipt, reason) in std::mem::take(&mut self.unsent_fails) {
            self.fail_unread(receipt, reason)?;
        }

        // Two shapes only this sweep acknowledges: the last watermark
        // arrived before the lane decided end-of-input, or the lane emitted
        // nothing, so no watermark will arrive.
        let finished: Vec<PartitionId> = self
            .held
            .iter()
            .filter(|(_, held)| {
                held.failing.is_none()
                    && held
                        .tracker
                        .terminal()
                        .is_some_and(|t| t == 0 || held.last_committed == Some(t))
            })
            .map(|(&partition, _)| partition)
            .collect();
        for partition in finished {
            self.acknowledge(partition)?;
        }

        let receipts: Vec<(PartitionId, Receipt)> = self
            .held
            .iter()
            .map(|(&partition, held)| (partition, held.receipt))
            .collect();
        for (partition, receipt) in receipts {
            match self.feed.renew(receipt) {
                Ok(()) => {}
                // The feed delivered the unit again or settled it, so this
                // reading is no longer the unit's.
                Err(e) if e.kind == FeedErrorKind::Gone => self.close(partition, false),
                Err(e) if e.kind == FeedErrorKind::Retryable => {
                    tracing::warn!(%receipt, error = %e, "renewing a held unit failed; retrying");
                }
                Err(e) => return Err(feed_error("renewing a held unit", &e)),
            }
        }
        Ok(())
    }

    /// Acknowledge the unit `partition` reads. `Retryable` leaves it held
    /// for the next sweep.
    fn acknowledge(&mut self, partition: PartitionId) -> Result<(), SourceError> {
        let receipt = self.held[&partition].receipt;
        if settled(self.feed.acknowledge(receipt))? == Settled::Yes {
            self.close(partition, true);
        }
        Ok(())
    }

    /// Let go of a delivery: its lane leaves barrier-less when every record
    /// it emitted is acknowledged, and through a drain barrier otherwise.
    fn close(&mut self, partition: PartitionId, acknowledged: bool) {
        let Some(held) = self.held.remove(&partition) else {
            return;
        };
        if acknowledged {
            self.retired.push(held.lane);
        } else {
            self.revoked.push(held.lane);
        }
        if let Some(m) = &self.parts.metrics {
            let done = u64::from(held.tracker.objects_done());
            m.objects_remaining
                .decrement(1u64.saturating_sub(done) as f64);
        }
    }

    /// Receive up to `room` deliveries and open a lane for each readable
    /// one.
    fn receive(&mut self, room: usize) -> Result<Vec<S3Lane>, SourceError> {
        let deliveries = match self.feed.receive(room) {
            Ok(deliveries) => deliveries,
            Err(e) if e.kind == FeedErrorKind::Retryable => {
                tracing::warn!(error = %e, "receiving from the feed failed; retrying");
                return Ok(Vec::new());
            }
            Err(e) => return Err(feed_error("receiving from the feed", &e)),
        };
        let mut lanes = Vec::with_capacity(deliveries.len());
        for delivery in deliveries {
            match one_object(&delivery) {
                Ok(object) => lanes.push(self.open(delivery, object)),
                Err(reason) => {
                    tracing::warn!(unit = %delivery.unit.id, %reason, "failing an unreadable unit");
                    self.fail_unread(delivery.receipt, reason)?;
                }
            }
        }
        Ok(lanes)
    }

    /// Fail a unit no lane read. `Retryable` keeps the fail for the next
    /// poll.
    fn fail_unread(&mut self, receipt: Receipt, reason: String) -> Result<(), SourceError> {
        if settled(self.feed.fail(receipt, &reason))? == Settled::Later {
            self.unsent_fails.push((receipt, reason));
        }
        Ok(())
    }

    /// Open a lane that reads `object` from its first byte.
    fn open(&mut self, delivery: Delivery, object: ObjectEntry) -> S3Lane {
        let n = self.next;
        self.next = n.checked_add(1).expect("delivery sequence exhausted (u32)");
        let partition = PartitionId(n);
        let lane = LaneId(n);
        let split = SplitId::new(format!("delivery-{n}")).expect("a delivery name is a valid id");
        let parts = &self.parts;
        let slice = Arc::new(vec![object]);
        let tracker = Arc::new(SplitTracker::new());
        let pause = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::channel(LANE_HANDOFF_CHUNKS);
        // Detached: the fetcher exits when the lane drops its receiver.
        drop(parts.handle.spawn(run_fetcher(FetcherParams {
            split: split.clone(),
            store: Arc::clone(&parts.store),
            slice,
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
            range: None,
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
            parts.compression,
            Arc::clone(&parts.make_framer),
            None,
            split,
            // Names the delivery in poison reports.
            LeaseEpoch(u64::from(n)),
            Arc::clone(&tracker),
            self.poison_tx.clone(),
            self.waker.clone(),
            parts.metrics.clone(),
        );
        self.held.insert(
            partition,
            Held {
                receipt: delivery.receipt,
                unit: delivery.unit.id,
                lane,
                pause,
                tracker,
                last_committed: None,
                hinted: false,
                failing: None,
            },
        );
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

/// The one whole object a unit names, or why the unit cannot be read.
fn one_object(delivery: &Delivery) -> Result<ObjectEntry, String> {
    let descriptor = SplitDescriptor::decode(&delivery.unit.body)
        .map_err(|e| format!("the unit's body is not a descriptor: {}", e.reason))?;
    if descriptor.range.is_some() {
        return Err("the unit names a byte range; this source reads whole objects".into());
    }
    match <[ObjectEntry; 1]>::try_from(descriptor.to_entries()) {
        Ok([object]) => Ok(object),
        Err(entries) => Err(format!(
            "the unit names {} objects; this source reads one object per unit",
            entries.len()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::TestLineFramer;
    use spate_core::checkpoint::Checkpointer;
    use spate_core::feed::Unit;
    use spate_test::{FeedScript, scripted_feed};

    /// A reader over a scripted feed. The runtime is never driven, so a
    /// fetcher spawned for a lane this reader opens never runs.
    fn reader() -> (UnitReader, FeedScript, tokio::runtime::Runtime) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (feed, script) = scripted_feed();
        let parts = LaneParts {
            store: Arc::new(object_store::memory::InMemory::new()),
            handle: rt.handle().clone(),
            issuer: Checkpointer::new().handle(),
            make_framer: Arc::new(|| Box::new(TestLineFramer::new(1 << 20))),
            compression: Compression::Auto,
            chunk_bytes: 1 << 16,
            range_bytes: 1 << 16,
            retry_base: Duration::from_millis(1),
            metrics: None,
        };
        (UnitReader::new(Arc::new(feed), parts), script, rt)
    }

    /// Receive one scripted unit and hold it as `open` does, returning its
    /// partition, its receipt and the tracker its lane would report
    /// end-of-input on.
    fn hold_one(
        reader: &mut UnitReader,
        script: &FeedScript,
    ) -> (PartitionId, Receipt, Arc<SplitTracker>) {
        let receipt = script.deliver(Unit::new("u", b"unused".to_vec()));
        let delivery = reader.feed.receive(1).unwrap().pop().unwrap();
        let partition = PartitionId(reader.next);
        reader.next += 1;
        let tracker = Arc::new(SplitTracker::new());
        reader.held.insert(
            partition,
            Held {
                receipt: delivery.receipt,
                unit: delivery.unit.id,
                lane: LaneId(partition.0),
                pause: Arc::new(AtomicBool::new(false)),
                tracker: Arc::clone(&tracker),
                last_committed: None,
                hinted: false,
                failing: None,
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
        let object = crate::split::DescriptorObject {
            key: format!("object-{i}"),
            size: 1,
            etag: None,
            last_modified_ms: 1,
        };
        let body = SplitDescriptor::new(vec![object]).encode().unwrap();
        Unit::new(format!("unit-{i}"), body)
    }

    fn lanes_added(event: SourceEvent<S3Lane>) -> Vec<S3Lane> {
        match event {
            SourceEvent::LanesAdded(lanes) => lanes,
            other => panic!("expected LanesAdded, got {other:?}"),
        }
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
        let first = reader.poll_events(Duration::ZERO).unwrap();
        assert!(matches!(first, SourceEvent::LanesAssigned(ref l) if l.is_empty()));
        let lanes = lanes_added(reader.poll_events(Duration::ZERO).unwrap());
        assert_eq!(lanes.len(), 96, "lanes opened on the first receive");
        assert_eq!(script.held().len(), 96, "units held");

        script.dead_letter(receipts[0]);
        let revoked = reader.poll_events(Duration::ZERO).unwrap();
        assert!(
            matches!(revoked, SourceEvent::LanesRevoked { ref lanes, .. } if lanes.len() == 1),
            "expected one lane revoked, got {revoked:?}"
        );
        let refill = lanes_added(reader.poll_events(Duration::ZERO).unwrap());
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
        let first = reader.poll_events(Duration::ZERO).unwrap();
        assert!(matches!(first, SourceEvent::LanesAssigned(ref l) if l.is_empty()));
        let mut lanes = lanes_added(reader.poll_events(Duration::ZERO).unwrap());
        assert_eq!(lanes.len(), 1);
        // Drain the signal the delivery left in the slot.
        while reader.park.park(Duration::ZERO) {}

        lanes[0].wake_control();
        assert!(
            reader.park.park(Duration::ZERO),
            "the lane's signal did not reach the reader's park"
        );
    }
}
