//! The feed read path: [`UnitReader`] reads work units from a [`Feed`] and
//! acknowledges each under the feed's ack rule.
//!
//! A unit's body is a [`SplitDescriptor`] naming one object, whole or as one
//! byte range. A whole object that qualifies for cutting is read in the byte
//! ranges the coordinated planner would cut it into, so a unit a lister cut
//! and a unit read in ranges here share their boundaries. Every read opens its
//! own lane and partition from the read's first byte, so a redelivery and a
//! duplicate are each read in full. The reader keys what it holds by delivery
//! and by partition, never by unit id.
//!
//! A read is done once its lane has decided end-of-input at the terminal
//! watermark `T` and the acknowledged watermark `W` that
//! [`commit`](UnitReader::commit) receives equals `T`, or at once when the
//! lane emitted nothing. A unit is acknowledged once its last read is done. An
//! object-level failure in any read fails the unit back to the feed, which
//! delivers it again until it dead-letters it.

use crate::fetch::{FetcherParams, ObjectEntry, run_fetcher};
use crate::framer::{Codec, FramerFactory};
use crate::lane::S3Lane;
use crate::metrics::S3Metrics;
use crate::split::{Packing, SplitDescriptor, SplitRange, tile};
use crate::split_ctx::{LANE_HANDOFF_CHUNKS, PoisonReport, SplitTracker};
use spate_core::checkpoint::AckIssuer;
use spate_core::coordination::{LeaseEpoch, SplitId};
use spate_core::error::SourceError;
use spate_core::feed::{
    Delivery, Feed, FeedError, FeedErrorKind, FeedPark, FeedWaker, Receipt, wake_channel,
};
use spate_core::record::PartitionId;
use spate_core::source::{DrainBarrier, LaneId, SourceEvent};
use std::collections::{BTreeMap, BTreeSet};
use std::iter::Peekable;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

/// Reads in progress at once when the `feed:` section sets no `units_held`.
/// The reader opens a read only while it has fewer.
const UNITS_HELD: usize = 96;

/// What the reader needs to open a lane, fixed at `open`.
pub(crate) struct LaneParts {
    pub(crate) store: Arc<dyn object_store::ObjectStore>,
    pub(crate) handle: tokio::runtime::Handle,
    pub(crate) issuer: AckIssuer,
    pub(crate) make_framer: FramerFactory,
    /// Decides which whole objects are read in ranges, and the codec and
    /// delimiter a ranged unit must match.
    pub(crate) packing: Packing,
    pub(crate) chunk_bytes: usize,
    pub(crate) range_bytes: usize,
    pub(crate) retry_base: Duration,
    pub(crate) metrics: Option<S3Metrics>,
}

/// The reads of a unit not yet opened, in order: a byte range, or `None` for
/// the whole object.
type Pending = Peekable<Box<dyn Iterator<Item = Option<SplitRange>> + Send>>;

/// One delivery the reader holds.
struct HeldUnit {
    receipt: Receipt,
    /// The unit's id, for logs.
    unit: String,
    /// The unit's one object.
    object: Arc<Vec<ObjectEntry>>,
    pending: Pending,
    /// The partitions of the unit's reads in progress.
    open: BTreeSet<PartitionId>,
    /// A read hit poison, and this reason waits for a `fail` the feed
    /// accepts.
    failing: Option<String>,
}

impl HeldUnit {
    /// Every read is opened and done.
    fn read(&mut self) -> bool {
        self.open.is_empty() && self.pending.peek().is_none()
    }
}

/// One read in progress, with the lane reading it.
struct Read {
    /// The delivery the read belongs to.
    unit: u32,
    lane: LaneId,
    pause: Arc<AtomicBool>,
    tracker: Arc<SplitTracker>,
    /// The latest acknowledged watermark `commit` received for the lane.
    last_committed: Option<i64>,
    /// The lane's end-of-input has been reported through `CommitReady`.
    hinted: bool,
}

impl Read {
    /// The lane decided end-of-input and every record it emitted is
    /// acknowledged.
    fn done(&self) -> bool {
        self.tracker
            .terminal()
            .is_some_and(|t| t == 0 || self.last_committed == Some(t))
    }
}

/// Reads work units from a feed, one lane per read.
pub(crate) struct UnitReader {
    feed: Arc<dyn Feed>,
    park: FeedPark,
    /// The waker the feed signals, cloned into every lane so a lane that
    /// ends or fails ends the park too.
    waker: FeedWaker,
    parts: LaneParts,
    /// Reads in progress at once: the reader opens one only while it has
    /// fewer.
    units_held: usize,
    started: bool,
    /// Sequence number of the next read, which names its partition, lane
    /// and poison reports.
    next: u32,
    /// Sequence number of the next delivery, which keys `units`.
    next_unit: u32,
    units: BTreeMap<u32, HeldUnit>,
    reads: BTreeMap<PartitionId, Read>,
    /// Lanes to stop through a drain barrier: records they emitted may be
    /// in flight.
    revoked: Vec<LaneId>,
    /// Lanes whose read is done, leaving with nothing in flight.
    retired: Vec<LaneId>,
    /// `fail`s for undecodable units that the feed answered `Retryable`.
    unsent_fails: Vec<(Receipt, String)>,
    poison_tx: std::sync::mpsc::Sender<PoisonReport>,
    poison_rx: std::sync::mpsc::Receiver<PoisonReport>,
}

impl std::fmt::Debug for UnitReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnitReader")
            .field("units", &self.units.len())
            .field("reads", &self.reads.len())
            .finish_non_exhaustive()
    }
}

impl UnitReader {
    /// A reader over `feed` with at most `units_held` reads in progress, or
    /// [`UNITS_HELD`] when that is `None`.
    pub(crate) fn new(
        feed: Arc<dyn Feed>,
        parts: LaneParts,
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
            units_held: units_held.map_or(UNITS_HELD, NonZeroUsize::get),
            started: false,
            next: 0,
            next_unit: 0,
            units: BTreeMap::new(),
            reads: BTreeMap::new(),
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
        // A held unit's remaining ranges go before new deliveries, so a unit
        // read in ranges finishes before the reader takes on more.
        let mut lanes = self.open_pending();
        // Receive before parking: a park consumes the feed's signal, and a
        // receive that takes nothing sends none.
        let room = self.room();
        if room > 0 {
            lanes.extend(self.receive(room)?);
            // A unit just received may have ranges beyond its first.
            lanes.extend(self.open_pending());
        }
        if !lanes.is_empty() {
            return Ok(SourceEvent::LanesAdded(lanes));
        }
        let partitions: Vec<PartitionId> = self
            .reads
            .iter_mut()
            .filter(|(_, read)| !read.hinted && read.tracker.terminal().is_some())
            .map(|(&partition, read)| {
                read.hinted = true;
                partition
            })
            .collect();
        if !partitions.is_empty() {
            return Ok(SourceEvent::CommitReady { partitions });
        }
        self.park.park(timeout);
        Ok(SourceEvent::Idle)
    }

    /// Record acknowledged watermarks, and end each read whose lane ended at
    /// the watermark given. A watermark for a partition no longer read
    /// belongs to a read already settled and is skipped.
    pub(crate) fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
        for &(partition, watermark) in watermarks {
            let Some(read) = self.reads.get_mut(&partition) else {
                continue;
            };
            read.last_committed = Some(watermark);
            if read.tracker.terminal() == Some(watermark) {
                self.finish(partition)?;
            }
        }
        Ok(())
    }

    /// Flip the pause flag of every lane named.
    pub(crate) fn set_paused(&self, lanes: &[LaneId], paused: bool) {
        for read in self.reads.values() {
            if lanes.contains(&read.lane) {
                read.pause.store(paused, Ordering::Relaxed);
            }
        }
    }

    /// Reads the reader may still open.
    fn room(&self) -> usize {
        self.units_held.saturating_sub(self.reads.len())
    }

    /// Settle what the lanes and the feed left since the last poll (poison
    /// reports, refused fails, finished reads and units), then renew every
    /// unit still held.
    fn settle(&mut self) -> Result<(), SourceError> {
        while let Ok(report) = self.poison_rx.try_recv() {
            if let Some(m) = &self.parts.metrics {
                m.objects_failed(report.kind).increment(1);
            }
            let partition = PartitionId(report.epoch.0 as u32);
            let Some(unit) = self
                .reads
                .get(&partition)
                .and_then(|read| self.units.get_mut(&read.unit))
            else {
                continue;
            };
            tracing::warn!(
                unit = %unit.unit,
                reason = report.kind.reason_label(),
                detail = %report.reason,
                "object-level failure; failing the unit back to the feed"
            );
            unit.failing.get_or_insert(report.reason);
        }
        let failing: Vec<u32> = self
            .units
            .iter()
            .filter(|(_, unit)| unit.failing.is_some())
            .map(|(&key, _)| key)
            .collect();
        for key in failing {
            let unit = &self.units[&key];
            let reason = unit.failing.clone().unwrap_or_default();
            match settled(self.feed.fail(unit.receipt, &reason))? {
                Settled::Yes => self.close(key),
                Settled::Later => {}
            }
        }
        for (receipt, reason) in std::mem::take(&mut self.unsent_fails) {
            self.fail_unread(receipt, reason)?;
        }

        // Two shapes only this sweep ends: the last watermark arrived before
        // the lane decided end-of-input, or the lane emitted nothing, so no
        // watermark will arrive.
        let finished: Vec<PartitionId> = self
            .reads
            .iter()
            .filter(|(_, read)| read.done())
            .map(|(&partition, _)| partition)
            .collect();
        for partition in finished {
            self.finish(partition)?;
        }
        // Units whose acknowledgement the feed asked to have sent again.
        let read: Vec<u32> = self
            .units
            .iter_mut()
            .filter_map(|(&key, unit)| (unit.failing.is_none() && unit.read()).then_some(key))
            .collect();
        for key in read {
            self.acknowledge(key)?;
        }

        let receipts: Vec<(u32, Receipt)> = self
            .units
            .iter()
            .map(|(&key, unit)| (key, unit.receipt))
            .collect();
        for (key, receipt) in receipts {
            match self.feed.renew(receipt) {
                Ok(()) => {}
                // The feed delivered the unit again or settled it, so this
                // reading is no longer the unit's.
                Err(e) if e.kind == FeedErrorKind::Gone => self.close(key),
                Err(e) if e.kind == FeedErrorKind::Retryable => {
                    tracing::warn!(%receipt, error = %e, "renewing a held unit failed; retrying");
                }
                Err(e) => return Err(feed_error("renewing a held unit", &e)),
            }
        }
        Ok(())
    }

    /// End the done read `partition`: its lane leaves with nothing in
    /// flight, and its unit is acknowledged when this was its last read.
    ///
    /// A failing unit's reads are left for [`close`](Self::close), which
    /// revokes them.
    fn finish(&mut self, partition: PartitionId) -> Result<(), SourceError> {
        let key = self.reads[&partition].unit;
        let unit = self
            .units
            .get_mut(&key)
            .expect("a read's unit is held while the read is");
        if unit.failing.is_some() {
            return Ok(());
        }
        unit.open.remove(&partition);
        let done = unit.read();
        let read = self
            .reads
            .remove(&partition)
            .expect("the read was found above");
        self.retired.push(read.lane);
        self.settle_gauge(&read);
        if done {
            self.acknowledge(key)?;
        }
        Ok(())
    }

    /// Acknowledge the unit `key`, every read of which is done. `Retryable`
    /// leaves it held for the next sweep.
    fn acknowledge(&mut self, key: u32) -> Result<(), SourceError> {
        let receipt = self.units[&key].receipt;
        if settled(self.feed.acknowledge(receipt))? == Settled::Yes {
            self.units.remove(&key);
        }
        Ok(())
    }

    /// Let go of the unit `key` before its reads are done: each lane still
    /// reading it leaves through a drain barrier, and its unopened reads are
    /// dropped.
    fn close(&mut self, key: u32) {
        let Some(unit) = self.units.remove(&key) else {
            return;
        };
        for partition in unit.open {
            if let Some(read) = self.reads.remove(&partition) {
                self.revoked.push(read.lane);
                self.settle_gauge(&read);
            }
        }
    }

    /// Take a read that leaves before its lane framed its object out of the
    /// `objects_remaining` gauge.
    fn settle_gauge(&self, read: &Read) {
        if let Some(m) = &self.parts.metrics {
            let done = u64::from(read.tracker.objects_done());
            m.objects_remaining
                .decrement(1u64.saturating_sub(done) as f64);
        }
    }

    /// Open held units' unopened reads into the room left, oldest unit
    /// first.
    fn open_pending(&mut self) -> Vec<S3Lane> {
        let mut lanes = Vec::new();
        let keys: Vec<u32> = self
            .units
            .iter_mut()
            .filter(|(_, unit)| unit.failing.is_none())
            .filter_map(|(&key, unit)| unit.pending.peek().is_some().then_some(key))
            .collect();
        for key in keys {
            while self.room() > 0
                && let Some(lane) = self.open(key)
            {
                lanes.push(lane);
            }
        }
        lanes
    }

    /// Receive up to `room` deliveries and open the first read of each
    /// readable one.
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
            match reads_of(&delivery, &self.parts.packing) {
                Ok((object, pending)) => {
                    let key = self.next_unit;
                    self.next_unit = key
                        .checked_add(1)
                        .expect("delivery sequence exhausted (u32)");
                    self.units.insert(
                        key,
                        HeldUnit {
                            receipt: delivery.receipt,
                            unit: delivery.unit.id,
                            object: Arc::new(vec![object]),
                            pending,
                            open: BTreeSet::new(),
                            failing: None,
                        },
                    );
                    lanes.extend(self.open(key));
                }
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

    /// Open a lane for the next unopened read of the unit `key`, from the
    /// read's first byte, or `None` when every read is open.
    fn open(&mut self, key: u32) -> Option<S3Lane> {
        let unit = self.units.get_mut(&key)?;
        let range = unit.pending.next()?;
        let n = self.next;
        self.next = n.checked_add(1).expect("read sequence exhausted (u32)");
        let partition = PartitionId(n);
        let lane = LaneId(n);
        let split = SplitId::new(format!("read-{n}")).expect("a read name is a valid id");
        unit.open.insert(partition);
        let slice = Arc::clone(&unit.object);
        let parts = &self.parts;
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
            // A read is never handed over part-done.
            stop: Arc::new(AtomicBool::new(false)),
            retry_base: parts.retry_base,
            retries: parts.metrics.as_ref().map(|m| m.get_retries.clone()),
            range,
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
            // Names the read in poison reports.
            LeaseEpoch(u64::from(n)),
            Arc::clone(&tracker),
            self.poison_tx.clone(),
            self.waker.clone(),
            parts.metrics.clone(),
        );
        self.reads.insert(
            partition,
            Read {
                unit: key,
                lane,
                pause,
                tracker,
                last_committed: None,
                hinted: false,
            },
        );
        Some(s3_lane)
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

/// The one object a unit names and the reads that cover it, or why the unit
/// cannot be read.
///
/// A unit naming a range is read as that range, and only under the
/// delimiter and codec its range was cut for. A whole object that `packing`
/// cuts is read in the ranges [`tile`] gives it; any other is read whole.
fn reads_of(delivery: &Delivery, packing: &Packing) -> Result<(ObjectEntry, Pending), String> {
    let descriptor = SplitDescriptor::decode(&delivery.unit.body)
        .map_err(|e| format!("the unit's body is not a descriptor: {}", e.reason))?;
    let range = descriptor.range;
    let object = match <[ObjectEntry; 1]>::try_from(descriptor.to_entries()) {
        Ok([object]) => object,
        Err(entries) => {
            return Err(format!(
                "the unit names {} objects; this source reads one object per unit",
                entries.len()
            ));
        }
    };
    let reads: Box<dyn Iterator<Item = Option<SplitRange>> + Send> = match range {
        Some(range) => {
            if packing.delimiter != Some(range.delimiter) {
                return Err(format!(
                    "the unit cuts its object on byte {:#04x}, but the source's framer {}",
                    range.delimiter,
                    match packing.delimiter {
                        Some(d) => format!("resyncs on byte {d:#04x}"),
                        None => "declares no resync delimiter".to_owned(),
                    }
                ));
            }
            let codec = Codec::resolve(packing.compression, &object.key);
            if codec != Codec::Plain {
                return Err(format!(
                    "the unit names a byte range of \"{}\", which the source decodes as {}; \
                     a byte range needs an uncompressed object",
                    object.key,
                    format!("{codec:?}").to_lowercase()
                ));
            }
            Box::new(std::iter::once(Some(range)))
        }
        None => match packing.delimiter_for(&object) {
            Some(delimiter) => {
                Box::new(tile(object.size, packing.target_bytes, delimiter).map(Some))
            }
            None => Box::new(std::iter::once(None)),
        },
    };
    Ok((object, reads.peekable()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Compression;
    use crate::testutil::TestLineFramer;
    use spate_core::checkpoint::Checkpointer;
    use spate_core::feed::Unit;
    use spate_test::{FeedScript, scripted_feed};

    /// A reader over a scripted feed with at most `units_held` reads in
    /// progress, cutting objects above `target` bytes on `\n`. The runtime
    /// is never driven, so a fetcher spawned for a lane this reader opens
    /// never runs.
    fn reader_with(
        units_held: Option<NonZeroUsize>,
        target: u64,
    ) -> (UnitReader, FeedScript, tokio::runtime::Runtime) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (feed, script) = scripted_feed();
        let parts = LaneParts {
            store: Arc::new(object_store::memory::InMemory::new()),
            handle: rt.handle().clone(),
            issuer: Checkpointer::new().handle(),
            make_framer: Arc::new(|| Box::new(TestLineFramer::new(1 << 20))),
            packing: Packing {
                target_bytes: target,
                compression: Compression::Auto,
                delimiter: Some(b'\n'),
            },
            chunk_bytes: 1 << 16,
            range_bytes: 1 << 16,
            retry_base: Duration::from_millis(1),
            metrics: None,
        };
        (
            UnitReader::new(Arc::new(feed), parts, units_held),
            script,
            rt,
        )
    }

    fn reader() -> (UnitReader, FeedScript, tokio::runtime::Runtime) {
        reader_with(None, 64 << 20)
    }

    /// Receive one scripted unit and hold it as one read, as `open` does,
    /// returning the read's partition, the unit's receipt and the tracker
    /// its lane would report end-of-input on.
    fn hold_one(
        reader: &mut UnitReader,
        script: &FeedScript,
    ) -> (PartitionId, Receipt, Arc<SplitTracker>) {
        let receipt = script.deliver(Unit::new("u", b"unused".to_vec()));
        let delivery = reader.feed.receive(1).unwrap().pop().unwrap();
        let key = reader.next_unit;
        reader.next_unit += 1;
        let partition = PartitionId(reader.next);
        reader.next += 1;
        let tracker = Arc::new(SplitTracker::new());
        let none: Box<dyn Iterator<Item = Option<SplitRange>> + Send> =
            Box::new(std::iter::empty());
        reader.units.insert(
            key,
            HeldUnit {
                receipt: delivery.receipt,
                unit: delivery.unit.id,
                object: Arc::new(Vec::new()),
                pending: none.peekable(),
                open: BTreeSet::from([partition]),
                failing: None,
            },
        );
        reader.reads.insert(
            partition,
            Read {
                unit: key,
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

    /// A unit naming the whole of `key`, `size` bytes with an ETag.
    fn object_unit(key: &str, size: u64) -> Unit {
        let object = crate::split::DescriptorObject {
            key: key.to_owned(),
            size,
            etag: Some("\"e\"".to_owned()),
            last_modified_ms: 1,
        };
        let body = SplitDescriptor::new(vec![object]).encode().unwrap();
        Unit::new(key, body)
    }

    /// The partitions of the reads in progress, in the order they opened.
    fn open_reads(reader: &UnitReader) -> Vec<PartitionId> {
        reader.reads.keys().copied().collect()
    }

    /// End the read `partition` with nothing emitted, so the next sweep
    /// finds it done.
    fn end_empty(reader: &UnitReader, partition: PartitionId) {
        reader.reads[&partition].tracker.set_terminal(0);
    }

    /// Pins INV-1 for a unit read in ranges: each done range retires its
    /// lane, and the unit is acknowledged once, after its last range.
    #[test]
    fn a_unit_in_ranges_is_acknowledged_once_after_its_last_range() {
        let (mut reader, script, _rt) = reader_with(None, 10);
        let receipt = script.deliver(object_unit("object.ndjson", 30));
        reader.poll_events(Duration::ZERO).unwrap();
        let lanes = lanes_added(reader.poll_events(Duration::ZERO).unwrap());
        assert_eq!(lanes.len(), 3, "one lane per range");
        let reads = open_reads(&reader);

        for (i, &partition) in reads.iter().enumerate() {
            reader.reads[&partition].tracker.set_terminal(7);
            reader.commit(&[(partition, 7)]).unwrap();
            let expected = if i + 1 == reads.len() {
                vec![receipt]
            } else {
                vec![]
            };
            assert_eq!(script.acknowledged(), expected, "after range {i}");
            let retired = reader.poll_events(Duration::ZERO).unwrap();
            assert!(
                matches!(retired, SourceEvent::LanesRetired { ref lanes } if lanes.len() == 1),
                "expected range {i}'s lane retired, got {retired:?}"
            );
        }
        reader.settle().unwrap();
        assert_eq!(script.acknowledged(), vec![receipt], "acknowledged twice");
    }

    /// Pins that each range in progress counts toward `units_held`: a unit's
    /// remaining ranges open as earlier ones finish, before another unit is
    /// received.
    #[test]
    fn ranges_in_progress_count_toward_units_held() {
        let (mut reader, script, _rt) = reader_with(NonZeroUsize::new(2), 10);
        let first = script.deliver(object_unit("first.ndjson", 30));
        reader.poll_events(Duration::ZERO).unwrap();
        assert_eq!(
            lanes_added(reader.poll_events(Duration::ZERO).unwrap()).len(),
            2
        );
        script.deliver(object_unit("second.ndjson", 5));
        assert!(matches!(
            reader.poll_events(Duration::ZERO).unwrap(),
            SourceEvent::Idle
        ));
        assert_eq!(script.held(), vec![first], "a second unit was received");

        end_empty(&reader, open_reads(&reader)[0]);
        assert!(matches!(
            reader.poll_events(Duration::ZERO).unwrap(),
            SourceEvent::LanesRetired { .. }
        ));
        let third = lanes_added(reader.poll_events(Duration::ZERO).unwrap());
        assert_eq!(third.len(), 1, "the third range opens into the freed room");
        assert_eq!(script.held(), vec![first], "a second unit was received");

        for partition in open_reads(&reader) {
            end_empty(&reader, partition);
        }
        assert!(matches!(
            reader.poll_events(Duration::ZERO).unwrap(),
            SourceEvent::LanesRetired { ref lanes } if lanes.len() == 2
        ));
        assert_eq!(script.acknowledged(), vec![first]);
        let second = lanes_added(reader.poll_events(Duration::ZERO).unwrap());
        assert_eq!(
            second.len(),
            1,
            "the second unit is received once room frees"
        );
    }

    /// Pins that poison in one range fails the whole unit back to the feed
    /// and revokes the unit's other ranges through a drain barrier.
    #[test]
    fn a_poisoned_range_fails_its_unit_and_revokes_its_other_ranges() {
        let (mut reader, script, _rt) = reader_with(None, 10);
        let receipt = script.deliver(object_unit("object.ndjson", 30));
        reader.poll_events(Duration::ZERO).unwrap();
        assert_eq!(
            lanes_added(reader.poll_events(Duration::ZERO).unwrap()).len(),
            3
        );
        let partition = open_reads(&reader)[1];
        reader
            .poison_tx
            .send(PoisonReport {
                split: SplitId::new("read-1").unwrap(),
                epoch: LeaseEpoch(u64::from(partition.0)),
                kind: crate::split_ctx::PoisonKind::EtagDrift,
                reason: "drifted".into(),
            })
            .unwrap();
        // A done range of a failing unit is not acknowledged.
        end_empty(&reader, open_reads(&reader)[0]);

        let revoked = reader.poll_events(Duration::ZERO).unwrap();
        assert!(
            matches!(revoked, SourceEvent::LanesRevoked { ref lanes, .. } if lanes.len() == 3),
            "expected every range revoked, got {revoked:?}"
        );
        assert_eq!(script.failed(), vec![(receipt, "drifted".to_owned())]);
        assert_eq!(script.acknowledged(), vec![]);
        assert!(reader.units.is_empty() && reader.reads.is_empty());
    }

    /// Pins that a unit naming a range is read only under the delimiter and
    /// codec it was cut for, and is otherwise failed back unread.
    #[test]
    fn a_range_unit_for_another_delimiter_or_a_compressed_object_is_failed_back() {
        let (mut reader, script, _rt) = reader_with(None, 10);
        let ranged = |key: &str, delimiter: u8| {
            let object = crate::split::DescriptorObject {
                key: key.to_owned(),
                size: 30,
                etag: Some("\"e\"".to_owned()),
                last_modified_ms: 1,
            };
            let body = SplitDescriptor::with_range(object, SplitRange::new(0, 10, delimiter))
                .encode()
                .unwrap();
            Unit::new(key, body)
        };
        let tab = script.deliver(ranged("object.tsv", b'\t'));
        let gzip = script.deliver(ranged("object.ndjson.gz", b'\n'));
        let plain = script.deliver(ranged("object.ndjson", b'\n'));
        reader.poll_events(Duration::ZERO).unwrap();
        let lanes = lanes_added(reader.poll_events(Duration::ZERO).unwrap());
        assert_eq!(lanes.len(), 1, "only the matching range is read");
        assert_eq!(script.held(), vec![plain]);
        let failed: Vec<Receipt> = script.failed().into_iter().map(|(r, _)| r).collect();
        assert_eq!(failed, vec![tab, gzip]);
    }
}
