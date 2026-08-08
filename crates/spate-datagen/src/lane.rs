//! The data plane: one lane per partition, generating into a reused arena.
//!
//! # Buffers
//!
//! A lane owns one `Vec<u8>` arena and one `Vec<Item>` of spans into it, both
//! cleared and refilled per poll and neither reallocated once they have grown
//! to a batch. Payloads borrow the arena for the batch's lifetime, exactly as
//! a Kafka lane borrows librdkafka's message memory (ADR-0013), so a generated
//! record costs no copy on its way into the chain.
//!
//! Each item's key is the order id in decimal ASCII, written into the same
//! arena immediately before its value. That is what makes
//! [`KeyHashRouter`](spate_core::sink::KeyHashRouter) colocate an order with
//! its payment and its refund: all three carry the same key, so all three hash
//! to the same shard.
//!
//! # The rate gate
//!
//! With a non-zero `tick_interval` a lane releases `events_per_tick` per
//! cadence and parks in between. The cadence is fixed rather than relative:
//! the next deadline is the previous one plus the interval, so a slow poll
//! does not push the whole schedule out. A lane that arrives more than a whole
//! interval late counts an overrun and re-anchors, because catching up on an
//! unbounded backlog of missed ticks would turn a throttled source into an
//! unthrottled one at the worst possible moment.
//!
//! With `tick_interval: 0s` there is no gate at all: the lane fills whatever
//! the caller asked for and lets backpressure set the pace.

use crate::encode::Encoder;
use crate::metrics::LaneCounters;
use crate::plan::EventPlan;
use spate_core::checkpoint::{AckIssuer, AckRef};
use spate_core::error::SourceError;
use spate_core::record::{PartitionId, RawPayload};
use spate_core::source::{LaneId, PayloadBatch, SourceLane};
use std::io::Write;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Per-lane state the control plane reads without touching a lane.
///
/// One `Arc<Shared>` is cloned into every lane. Lanes only ever write their
/// own index and the control plane only ever reads, so this is a publication
/// channel rather than shared mutable state — nothing on the record path
/// contends on it.
#[derive(Debug)]
pub(crate) struct Shared {
    /// Set by a lane that has reached its budget *and been polled again*.
    pub(crate) exhausted: Box<[AtomicBool]>,
    /// Set by the control plane's `pause`/`resume`.
    pub(crate) paused: Box<[AtomicBool]>,
    /// Events left in the lane's budget; meaningless when unbounded.
    pub(crate) remaining: Box<[AtomicU64]>,
    /// Orders the lane has placed and not yet captured.
    pub(crate) open: Box<[AtomicU64]>,
}

impl Shared {
    pub(crate) fn new(partitions: usize) -> Shared {
        let flags = || (0..partitions).map(|_| AtomicBool::new(false)).collect();
        let counts = || (0..partitions).map(|_| AtomicU64::new(0)).collect();
        Shared {
            exhausted: flags(),
            paused: flags(),
            remaining: counts(),
            open: counts(),
        }
    }
}

/// One payload's place in the lane's arena, plus the metadata that travels
/// with it.
#[derive(Clone, Debug)]
struct Item {
    key: Range<usize>,
    value: Range<usize>,
    offset: i64,
    timestamp_ms: i64,
}

/// Everything a lane is built from. A struct rather than nine arguments,
/// because the control plane fills it in one place and clippy is right about
/// the alternative.
pub(crate) struct LaneParts {
    pub(crate) id: LaneId,
    pub(crate) index: usize,
    pub(crate) issuer: AckIssuer,
    pub(crate) plan: EventPlan,
    pub(crate) encoder: Arc<Encoder>,
    pub(crate) counters: Option<LaneCounters>,
    pub(crate) shared: Arc<Shared>,
    pub(crate) budget: u64,
    pub(crate) tick_interval: Duration,
    pub(crate) events_per_tick: usize,
}

/// One partition's data plane.
#[derive(Debug)]
pub struct DatagenLane {
    id: LaneId,
    partition: PartitionId,
    index: usize,
    issuer: AckIssuer,
    plan: EventPlan,
    encoder: Arc<Encoder>,
    counters: Option<LaneCounters>,
    shared: Arc<Shared>,
    /// Events this lane may ever release. `u64::MAX` when unbounded.
    budget: u64,
    emitted: u64,
    next_offset: i64,
    tick_interval: Duration,
    events_per_tick: usize,
    next_tick: Instant,
    arena: Vec<u8>,
    items: Vec<Item>,
}

impl DatagenLane {
    pub(crate) fn new(parts: LaneParts) -> DatagenLane {
        DatagenLane {
            id: parts.id,
            partition: PartitionId(parts.index as u32),
            index: parts.index,
            issuer: parts.issuer,
            plan: parts.plan,
            encoder: parts.encoder,
            counters: parts.counters,
            shared: parts.shared,
            budget: parts.budget,
            emitted: 0,
            next_offset: 0,
            tick_interval: parts.tick_interval,
            events_per_tick: parts.events_per_tick,
            // The first tick is due immediately: a pipeline should produce its
            // first batch on its first poll, not one interval later.
            next_tick: Instant::now(),
            arena: Vec::new(),
            items: Vec::new(),
        }
    }

    /// How many events this poll may release, or `None` when the cadence is
    /// not yet due (the lane has already parked).
    fn rate_gate(&mut self, timeout: Duration) -> Option<usize> {
        if self.tick_interval.is_zero() {
            return Some(usize::MAX);
        }
        let now = Instant::now();
        if now < self.next_tick {
            park(min(self.next_tick - now, timeout));
            return None;
        }
        self.next_tick = advance(self.next_tick, self.tick_interval);
        if self.next_tick <= now {
            // A whole interval was already gone by the time we were polled.
            // Re-anchor instead of firing back-to-back ticks to catch up.
            if let Some(counters) = &self.counters {
                counters.tick_overruns.increment(1);
            }
            self.next_tick = advance(now, self.tick_interval);
        }
        if let Some(counters) = &self.counters {
            counters.ticks.increment(1);
        }
        Some(self.events_per_tick)
    }

    /// Generate `count` events into the arena.
    fn fill(&mut self, count: usize) -> Result<(), SourceError> {
        self.arena.clear();
        self.items.clear();
        for _ in 0..count {
            let (event, timestamp_ms) = self.plan.next();

            let key_start = self.arena.len();
            // Writing to a `Vec<u8>` is infallible; the `io::Write` signature
            // is not, and an `expect` here would be a panic on the record path.
            let _ = write!(self.arena, "{}", event.order_id());
            let key = key_start..self.arena.len();

            let value_start = self.arena.len();
            self.encoder.encode(&event, &mut self.arena)?;
            let value = value_start..self.arena.len();

            if let Some(counters) = &self.counters {
                counters.generated(&event).increment(1);
            }
            self.items.push(Item {
                key,
                value,
                offset: self.next_offset,
                timestamp_ms,
            });
            self.next_offset += 1;
            self.emitted += 1;
        }
        // Publish for the control plane's gauges. `Release` pairs with the
        // controller's `Acquire`, so it never reads a count from before the
        // events it describes.
        self.shared.remaining[self.index]
            .store(self.budget.saturating_sub(self.emitted), Ordering::Release);
        self.shared.open[self.index].store(self.plan.open_orders(), Ordering::Release);
        Ok(())
    }
}

impl SourceLane for DatagenLane {
    type Batch<'a> = DatagenBatch<'a>;

    fn id(&self) -> LaneId {
        self.id
    }

    fn partition(&self) -> PartitionId {
        self.partition
    }

    fn poll(
        &mut self,
        max_records: usize,
        timeout: Duration,
    ) -> Result<Option<DatagenBatch<'_>>, SourceError> {
        if self.emitted >= self.budget {
            // Declaring exhaustion here rather than at emission is what
            // satisfies `SourceEvent::Drained`'s contract: the owning thread
            // runs poll -> push -> poll, so reaching this branch proves the
            // previous batch was consumed and nothing unemitted is left.
            self.shared.exhausted[self.index].store(true, Ordering::Release);
            park(timeout);
            return Ok(None);
        }
        if self.shared.paused[self.index].load(Ordering::Acquire) || max_records == 0 {
            park(timeout);
            return Ok(None);
        }
        // Checked before the gate, so a poll that could not have used its
        // quota does not consume a tick.
        let Some(quota) = self.rate_gate(timeout) else {
            return Ok(None);
        };

        let count = quota
            .min(max_records)
            .min(usize::try_from(self.budget - self.emitted).unwrap_or(usize::MAX));
        self.fill(count)?;

        let last_offset = self.next_offset - 1;
        Ok(Some(DatagenBatch {
            arena: &self.arena,
            items: &self.items,
            next: 0,
            partition: self.partition,
            ack: self.issuer.issue(self.partition, last_offset),
        }))
    }
}

/// One poll's payloads, borrowing the lane's arena.
#[derive(Debug)]
pub struct DatagenBatch<'a> {
    arena: &'a [u8],
    items: &'a [Item],
    next: usize,
    partition: PartitionId,
    ack: AckRef,
}

impl<'a> PayloadBatch<'a> for DatagenBatch<'a> {
    fn next_payload(&mut self) -> Option<RawPayload<'a>> {
        let item = self.items.get(self.next)?;
        self.next += 1;
        Some(RawPayload {
            bytes: &self.arena[item.value.clone()],
            key: Some(&self.arena[item.key.clone()]),
            partition: self.partition,
            offset: item.offset,
            timestamp_ms: item.timestamp_ms,
        })
    }

    fn ack(&self) -> &AckRef {
        &self.ack
    }
}

/// Block the pipeline thread for `how_long`. A lane must never busy-spin when
/// it has nothing to hand over.
fn park(how_long: Duration) {
    if !how_long.is_zero() {
        std::thread::sleep(how_long);
    }
}

fn min(a: Duration, b: Duration) -> Duration {
    if a < b { a } else { b }
}

/// `at + step`, saturating rather than panicking. A `tick_interval` large
/// enough to overflow an `Instant` is legal configuration and must not take
/// the process down.
fn advance(at: Instant, step: Duration) -> Instant {
    at.checked_add(step).unwrap_or(at)
}
