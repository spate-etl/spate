//! The data plane: one lane per partition, generating into a reused arena.
//!
//! # Buffers
//!
//! A lane owns one `Vec<u8>` arena and one `Vec<Item>` of spans into it, both
//! cleared and refilled per poll and neither reallocated once they have grown
//! to a batch. Payloads borrow the arena for the batch's lifetime (ADR-0013),
//! so a generated record costs no copy on its way into the chain.
//!
//! Each item's key is the order id in decimal ASCII, written into the same
//! arena immediately before its value. All three events of an order carry that
//! same key, so [`KeyHashRouter`](spate_core::sink::KeyHashRouter) hashes them
//! to one shard.
//!
//! # The rate gate
//!
//! With a non-zero `tick_interval` a lane releases `events_per_tick` per
//! cadence and parks once the quota is spent. The next deadline is the previous
//! one plus the interval, so a slow poll does not push the whole schedule out.
//!
//! A tick's quota is spread over as many polls as it takes. `max_records`
//! caps a single batch, so an `events_per_tick` above it is released across
//! several polls of the same tick rather than truncated. The release rate is
//! `partitions × events_per_tick ÷ tick_interval` at any `events_per_tick`.
//!
//! A lane polled a whole interval late counts an overrun and re-anchors to the
//! present; missed ticks are not replayed. A `tick_interval` large enough to
//! overflow an `Instant` leaves no next deadline, and the lane parks for good.
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
/// One `Arc<Shared>` is cloned into every lane. A lane writes only its own
/// index and the control plane only reads, so nothing on the record path
/// contends on it. Each value is published on its own; a read across several
/// of them is not a snapshot of one instant.
#[derive(Debug)]
pub(crate) struct Shared {
    /// Set by a lane that has reached its budget *and been polled again*.
    pub(crate) exhausted: Box<[AtomicBool]>,
    /// Set by the control plane's `pause`/`resume`.
    pub(crate) paused: Box<[AtomicBool]>,
    /// Events left in the lane's budget, seeded with the budget itself so it
    /// reads correctly before the lane's first fill. Zero when unbounded.
    pub(crate) remaining: Box<[AtomicU64]>,
    /// Orders the lane has placed and not yet captured.
    pub(crate) open: Box<[AtomicU64]>,
}

impl Shared {
    /// Per-lane state for `partitions` lanes. `budgets` seeds `remaining`; pass
    /// `None` for an unbounded stream, which has no remainder to report.
    pub(crate) fn new(partitions: usize, budgets: Option<&[u64]>) -> Shared {
        let flags = || (0..partitions).map(|_| AtomicBool::new(false)).collect();
        Shared {
            exhausted: flags(),
            paused: flags(),
            remaining: (0..partitions)
                .map(|i| AtomicU64::new(budgets.map_or(0, |b| b[i])))
                .collect(),
            open: (0..partitions).map(|_| AtomicU64::new(0)).collect(),
        }
    }
}

/// One payload's place in the lane's arena, plus the metadata that travels
/// with it.
#[derive(Debug)]
struct Item {
    key: Range<usize>,
    value: Range<usize>,
    offset: i64,
    timestamp_ms: i64,
}

/// Everything a lane is built from.
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
    /// Events left in the current tick's quota, spent across as many polls as
    /// `max_records` takes. Always 0 when unthrottled.
    tick_budget: usize,
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
            // The first tick is due immediately: the first poll releases a
            // batch.
            next_tick: Instant::now(),
            tick_budget: 0,
            arena: Vec::new(),
            items: Vec::new(),
        }
    }

    /// How many events this poll may release, or `None` when the current
    /// tick's quota is spent and the next is not yet due (the lane has already
    /// parked).
    fn rate_gate(&mut self, timeout: Duration) -> Option<usize> {
        if self.tick_interval.is_zero() {
            return Some(usize::MAX);
        }
        // What this tick has left. Spending it over several polls is what
        // keeps the release rate right when `max_records` caps a batch below
        // `events_per_tick`.
        if self.tick_budget > 0 {
            return Some(self.tick_budget);
        }
        let now = Instant::now();
        if now < self.next_tick {
            park(Duration::min(self.next_tick - now, timeout));
            return None;
        }
        let mut next = self.next_tick.checked_add(self.tick_interval);
        if next.is_some_and(|at| at <= now) {
            // A whole interval was already gone by the time we were polled.
            // Re-anchor; missed ticks are not replayed.
            if let Some(counters) = &self.counters {
                counters.tick_overruns.increment(1);
            }
            next = now.checked_add(self.tick_interval);
        }
        // No representable next deadline: the cadence never comes due again.
        // Parking here rather than firing is what keeps an interval past
        // `Instant`'s range throttled instead of unthrottled.
        let Some(next) = next else {
            park(timeout);
            return None;
        };
        self.next_tick = next;
        if let Some(counters) = &self.counters {
            counters.ticks.increment(1);
        }
        self.tick_budget = self.events_per_tick;
        Some(self.tick_budget)
    }

    /// Generate `count` events into the arena.
    fn fill(&mut self, count: usize) -> Result<(), SourceError> {
        self.arena.clear();
        self.items.clear();
        let mut generated = [0u64; 3];
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

            generated[crate::metrics::kind(&event)] += 1;
            self.items.push(Item {
                key,
                value,
                offset: self.next_offset,
                timestamp_ms,
            });
            self.next_offset += 1;
            self.emitted += 1;
        }
        if let Some(counters) = &self.counters {
            counters.add_generated(generated);
        }
        // Publish for the control plane's gauges, at the batch boundary.
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
            // Exhaustion is declared on the poll after the last batch, which
            // is what `SourceEvent::Drained`'s contract asks for: the owning
            // thread runs poll -> push -> poll, so reaching this branch proves
            // the previous batch was consumed and nothing unemitted is left.
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
        self.tick_budget = self.tick_budget.saturating_sub(count);

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

/// Block the calling thread for `how_long`. Neither a lane nor the control
/// plane may busy-spin when it has nothing to hand over.
pub(crate) fn park(how_long: Duration) {
    if !how_long.is_zero() {
        std::thread::sleep(how_long);
    }
}
