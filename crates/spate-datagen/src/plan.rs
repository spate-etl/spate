//! One lane's event plan: what it generates next, and why that is consistent.
//!
//! # The mechanism
//!
//! A lane draws an event type from a fixed categorical mix — 60% place, 35%
//! capture, 5% refund — using its own PRNG, and never touches another lane's
//! state. Referential integrity comes out of two structural properties rather
//! than out of coordination:
//!
//! - **Disjoint id slices.** Lane `i` of `p` mints `order_id = n × p + i`, so
//!   the id spaces cannot overlap and no two lanes can mint the same order.
//! - **Per-lane rings.** A lane remembers the orders it has placed (with their
//!   totals) and the ones it has captured. A capture draws from the first ring,
//!   a refund from the second, and **an empty ring falls through to
//!   `order_placed`** — so a reference is drawn from a set the same lane
//!   populated earlier, or it is not drawn at all.
//!
//! Together those give a referencing event the same partition and a strictly
//! greater offset than the `order_placed` it names, with nothing shared on the
//! record path. A drawn entry is *removed* from its ring, so an order is
//! captured at most once and a capture refunded at most once.
//!
//! The rings are bounded at 256 entries. Past that a push overwrites, so an
//! old order simply never gets paid — the same thing a real storefront does,
//! and the reason the lane's memory is flat rather than proportional to how
//! long it has run.

use crate::config::{Clock, DatagenSourceConfig};
use crate::dims::{CUSTOMERS, REFUND_REASONS, REGIONS, SKUS, UNIT_CENTS};
use crate::events::{OrderLine, OrderPlaced, PaymentCaptured, RefundIssued, StorefrontEvent};
use crate::rng::SplitMix64;
use std::borrow::Cow;
use std::time::{SystemTime, UNIX_EPOCH};

/// How many orders a lane remembers, in each of its two rings. Bounded so a
/// lane's footprint does not grow with its runtime.
const RING_CAPACITY: usize = 256;

/// The categorical mix, as cumulative thresholds over `rng.below(100)`.
const PLACE_THRESHOLD: u32 = 60;
const CAPTURE_THRESHOLD: u32 = 95;

/// An order the lane has placed, and the amount that settles it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Pending {
    order_id: u64,
    amount_cents: u64,
}

/// A bounded, unordered set of pending orders.
///
/// Unordered on purpose: entries are drawn at random and removed with a swap,
/// so both operations are constant-time and neither allocates after the first
/// fill. The `cursor` is only consulted once the ring is full, and rotates so
/// that a saturated ring keeps turning over rather than evicting one slot
/// forever.
#[derive(Clone, Debug)]
struct Ring {
    slots: Vec<Pending>,
    cursor: usize,
}

impl Ring {
    fn new() -> Ring {
        Ring {
            slots: Vec::with_capacity(RING_CAPACITY),
            cursor: 0,
        }
    }

    fn len(&self) -> usize {
        self.slots.len()
    }

    fn push(&mut self, entry: Pending) {
        if self.slots.len() < RING_CAPACITY {
            self.slots.push(entry);
        } else {
            self.slots[self.cursor] = entry;
            self.cursor = (self.cursor + 1) % RING_CAPACITY;
        }
    }

    fn take(&mut self, rng: &mut SplitMix64) -> Option<Pending> {
        if self.slots.is_empty() {
            return None;
        }
        let index = rng.below(self.slots.len() as u32) as usize;
        Some(self.slots.swap_remove(index))
    }
}

/// One lane's generator. Deterministic in its seed: the same seed replays the
/// same events, which is what makes a test able to assert on them.
#[derive(Clone, Debug)]
pub(crate) struct EventPlan {
    rng: SplitMix64,
    /// This lane's residue class in the order-id space.
    lane_index: u64,
    /// The modulus of that space — the configured partition count.
    partitions: u64,
    /// How many orders this lane has minted, i.e. the next `n`.
    minted: u64,
    /// How many events it has produced, which is also the `fixed` clock's
    /// offset from `epoch_ms`.
    produced: u64,
    open: Ring,
    captured: Ring,
    clock: Clock,
    epoch_ms: i64,
}

impl EventPlan {
    /// The plan for lane `lane_index` of `config.partitions`.
    pub(crate) fn new(config: &DatagenSourceConfig, lane_index: u32) -> EventPlan {
        EventPlan {
            rng: SplitMix64::new(config.lane_seed(lane_index)),
            lane_index: u64::from(lane_index),
            partitions: u64::from(config.partitions),
            minted: 0,
            produced: 0,
            open: Ring::new(),
            captured: Ring::new(),
            clock: config.clock,
            epoch_ms: config.epoch_ms,
        }
    }

    /// Orders this lane has placed and not yet captured — the `open_orders`
    /// gauge's per-lane contribution.
    pub(crate) fn open_orders(&self) -> u64 {
        self.open.len() as u64
    }

    /// The next event, and the event time to stamp its payload with.
    pub(crate) fn next(&mut self) -> (StorefrontEvent, i64) {
        let at = self.event_time();
        self.produced += 1;
        let draw = self.rng.below(100);
        let event = if draw < PLACE_THRESHOLD {
            self.place(at)
        } else if draw < CAPTURE_THRESHOLD {
            // An empty ring falls through rather than skipping the event: a
            // lane that has captured everything it placed keeps producing.
            self.capture().unwrap_or_else(|| self.place(at))
        } else {
            self.refund().unwrap_or_else(|| self.place(at))
        };
        (event, at)
    }

    /// Milliseconds since the Unix epoch for the event about to be produced.
    ///
    /// The `fixed` clock advances one millisecond per event *this lane* has
    /// produced, so a lane's timestamps are a function of its position in its
    /// own stream and nothing else — no wall clock, no interleaving, no
    /// dependence on how the runtime scheduled the threads.
    fn event_time(&self) -> i64 {
        match self.clock {
            Clock::Fixed => self.epoch_ms.saturating_add(self.produced as i64),
            Clock::Wall => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as i64),
        }
    }

    fn place(&mut self, at: i64) -> StorefrontEvent {
        let order_id = self.minted * self.partitions + self.lane_index;
        self.minted += 1;

        let line_count = self.rng.between(1, 5);
        let mut lines = Vec::with_capacity(line_count as usize);
        let mut amount_cents = 0u64;
        for _ in 0..line_count {
            let item = self.rng.below(SKUS.len() as u32) as usize;
            let qty = self.rng.between(1, 5);
            let unit_cents = UNIT_CENTS[item];
            amount_cents += u64::from(qty) * u64::from(unit_cents);
            lines.push(OrderLine {
                sku: Cow::Borrowed(SKUS[item]),
                qty,
                unit_cents,
            });
        }

        self.open.push(Pending {
            order_id,
            amount_cents,
        });
        StorefrontEvent::OrderPlaced(OrderPlaced {
            order_id,
            customer_id: self.rng.below(CUSTOMERS),
            region: Cow::Borrowed(REGIONS[self.rng.below(REGIONS.len() as u32) as usize]),
            placed_at: at,
            lines,
        })
    }

    fn capture(&mut self) -> Option<StorefrontEvent> {
        let pending = self.open.take(&mut self.rng)?;
        self.captured.push(pending);
        Some(StorefrontEvent::PaymentCaptured(PaymentCaptured {
            order_id: pending.order_id,
            amount_cents: pending.amount_cents,
        }))
    }

    fn refund(&mut self) -> Option<StorefrontEvent> {
        let pending = self.captured.take(&mut self.rng)?;
        // A whole, half, third or quarter of what was captured — never more,
        // which is the property a downstream balance check depends on.
        let share = u64::from(self.rng.between(1, 4));
        let reason = REFUND_REASONS[self.rng.below(REFUND_REASONS.len() as u32) as usize];
        Some(StorefrontEvent::RefundIssued(RefundIssued {
            order_id: pending.order_id,
            amount_cents: pending.amount_cents / share,
            reason: Cow::Borrowed(reason),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn config(partitions: u32, seed: u64) -> DatagenSourceConfig {
        DatagenSourceConfig {
            partitions,
            seed,
            ..DatagenSourceConfig::default()
        }
    }

    fn run(config: &DatagenSourceConfig, lane: u32, events: usize) -> Vec<StorefrontEvent> {
        let mut plan = EventPlan::new(config, lane);
        (0..events).map(|_| plan.next().0).collect()
    }

    /// The property the whole crate exists for, checked the way a consumer
    /// would: replay the lane's stream in order and assert every reference
    /// resolves against something already seen.
    #[test]
    fn every_reference_resolves_to_an_earlier_event_in_the_same_lane() {
        let cfg = config(4, 7);
        let mut placed: HashMap<u64, u64> = HashMap::new();
        let mut captured: HashMap<u64, u64> = HashMap::new();
        let mut seen_capture = 0usize;
        let mut seen_refund = 0usize;

        for (index, event) in run(&cfg, 2, 20_000).into_iter().enumerate() {
            match event {
                StorefrontEvent::OrderPlaced(e) => {
                    let total: u64 = e
                        .lines
                        .iter()
                        .map(|l| u64::from(l.qty) * u64::from(l.unit_cents))
                        .sum();
                    assert!(!e.lines.is_empty() && e.lines.len() <= 5);
                    assert!(placed.insert(e.order_id, total).is_none(), "id reused");
                }
                StorefrontEvent::PaymentCaptured(e) => {
                    seen_capture += 1;
                    let total = placed.get(&e.order_id).unwrap_or_else(|| {
                        panic!(
                            "capture at {index} references unplaced order {}",
                            e.order_id
                        )
                    });
                    assert_eq!(
                        e.amount_cents, *total,
                        "a capture must settle the order's line total"
                    );
                    assert!(
                        captured.insert(e.order_id, e.amount_cents).is_none(),
                        "order {} captured twice",
                        e.order_id
                    );
                }
                StorefrontEvent::RefundIssued(e) => {
                    seen_refund += 1;
                    let paid = captured.get(&e.order_id).unwrap_or_else(|| {
                        panic!(
                            "refund at {index} references uncaptured order {}",
                            e.order_id
                        )
                    });
                    assert!(
                        e.amount_cents <= *paid && e.amount_cents > 0,
                        "refund {} is not within the captured {paid}",
                        e.amount_cents
                    );
                    assert!(REFUND_REASONS.contains(&e.reason.as_ref()));
                }
            }
        }
        assert!(
            seen_capture > 1_000 && seen_refund > 100,
            "mix is degenerate"
        );
    }

    /// The disjointness that makes the above work with no coordination: a
    /// lane's ids are its own residue class, so two lanes cannot collide even
    /// though neither can see the other.
    #[test]
    fn lanes_mint_disjoint_order_ids() {
        let cfg = config(4, 11);
        let mut all = std::collections::BTreeSet::new();
        for lane in 0..4u32 {
            for event in run(&cfg, lane, 2_000) {
                let id = event.order_id();
                assert_eq!(
                    id % 4,
                    u64::from(lane),
                    "lane {lane} produced an id outside its slice"
                );
                if matches!(event, StorefrontEvent::OrderPlaced(_)) {
                    assert!(all.insert(id), "id {id} minted twice across lanes");
                }
            }
        }
    }

    /// A reference always lands *after* what it references. The lane assigns
    /// offsets in emission order, so an index comparison here is the offset
    /// property.
    #[test]
    fn a_reference_never_precedes_the_order_it_names() {
        let mut first_seen: HashMap<u64, usize> = HashMap::new();
        for (index, event) in run(&config(2, 3), 1, 10_000).into_iter().enumerate() {
            match &event {
                StorefrontEvent::OrderPlaced(e) => {
                    first_seen.insert(e.order_id, index);
                }
                other => {
                    let placed_at = first_seen[&other.order_id()];
                    assert!(
                        placed_at < index,
                        "reference at {index} precedes its order at {placed_at}"
                    );
                }
            }
        }
    }

    /// Determinism, which the CPU-pinned fan-out has to leave alone: a lane's
    /// stream is a function of its seed and its index, not of scheduling.
    #[test]
    fn a_lane_replays_identically_and_differs_from_its_siblings() {
        let cfg = config(4, 42);
        assert_eq!(run(&cfg, 0, 500), run(&cfg, 0, 500));
        assert_ne!(run(&cfg, 0, 500), run(&cfg, 1, 500));
        assert_ne!(run(&cfg, 0, 500), run(&config(4, 43), 0, 500));

        // Generated on another thread, which is where a lane actually runs.
        let cloned = cfg.clone();
        let elsewhere = std::thread::spawn(move || run(&cloned, 3, 500))
            .join()
            .expect("generator thread");
        assert_eq!(elsewhere, run(&cfg, 3, 500));
    }

    #[test]
    fn the_rings_stay_bounded_however_long_the_lane_runs() {
        let cfg = config(1, 5);
        let mut plan = EventPlan::new(&cfg, 0);
        for _ in 0..100_000 {
            plan.next();
            assert!(plan.open.len() <= RING_CAPACITY);
            assert!(plan.captured.len() <= RING_CAPACITY);
        }
        assert!(
            plan.open_orders() > 0,
            "a saturated lane still has open work"
        );
    }

    /// The mix is a floor on placements rather than an exact split: an empty
    /// ring falls through to `order_placed`, so placements can only ever be
    /// over-represented.
    #[test]
    fn the_event_mix_follows_the_documented_shares() {
        let mut counts = [0usize; 3];
        for event in run(&config(1, 17), 0, 100_000) {
            counts[match event {
                StorefrontEvent::OrderPlaced(_) => 0,
                StorefrontEvent::PaymentCaptured(_) => 1,
                StorefrontEvent::RefundIssued(_) => 2,
            }] += 1;
        }
        let pct = |i: usize| counts[i] as f64 / 1_000.0;
        assert!((59.0..64.0).contains(&pct(0)), "placed {}%", pct(0));
        assert!((33.0..36.0).contains(&pct(1)), "captured {}%", pct(1));
        assert!((4.0..6.0).contains(&pct(2)), "refunded {}%", pct(2));
    }

    #[test]
    fn the_fixed_clock_advances_one_millisecond_per_event_from_the_epoch() {
        let cfg = DatagenSourceConfig {
            epoch_ms: 1_000,
            ..config(1, 1)
        };
        let mut plan = EventPlan::new(&cfg, 0);
        for expected in 1_000..1_100 {
            assert_eq!(plan.next().1, expected);
        }
    }

    #[test]
    fn the_wall_clock_stamps_a_plausible_present() {
        let cfg = DatagenSourceConfig {
            clock: Clock::Wall,
            ..config(1, 1)
        };
        let at = EventPlan::new(&cfg, 0).next().1;
        // Any real host clock is past the fixed clock's synthetic epoch.
        assert!(at > crate::config::DEFAULT_EPOCH_MS, "wall clock read {at}");
    }
}
