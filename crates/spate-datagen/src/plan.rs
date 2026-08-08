//! One lane's event plan: the next event it generates, and the referential
//! integrity across the ones it has already produced.
//!
//! # The mechanism
//!
//! A lane draws an event type from a fixed categorical mix — 60% place, 35%
//! capture, 5% refund — using its own PRNG, and never touches another lane's
//! state. Two structural properties carry referential integrity:
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
//! A ring is only empty while the lane is warming up, so the fall-through
//! shapes the first few dozen events and none after them. The observed mix is
//! the draw's.
//!
//! # What the rings bound, and what they do not
//!
//! Each ring holds [`RING_CAPACITY`] entries. A push into a full ring
//! overwrites the entry at the rotating cursor, so a lane's footprint is flat
//! in its runtime; the order in that slot is never captured.
//!
//! The rings bound memory, not the backlog. The mix places faster than it
//! captures, so orders placed and not yet paid accumulate at roughly a quarter
//! of every event for as long as the lane runs — [`EventPlan::open_orders`]
//! reports that count. A downstream check reconciles the orders that were
//! captured against their lines; it cannot expect every order to be paid.

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
/// Entries are drawn at random and removed with a swap: both operations are
/// constant-time, and the `Vec` is sized once at construction so neither
/// allocates. Once the ring is full a push overwrites the slot at `cursor`,
/// which advances on every overwrite, so evictions spread evenly over all
/// [`RING_CAPACITY`] slots.
///
/// A slot carries no age: `swap_remove` moves the last entry into the hole it
/// leaves. The entry an overwrite drops is therefore an arbitrary one the lane
/// still remembered, not the oldest.
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
/// same events.
#[derive(Clone, Debug)]
pub(crate) struct EventPlan {
    rng: SplitMix64,
    /// This lane's residue class in the order-id space.
    lane_index: u64,
    /// The modulus of that space — the configured partition count.
    partitions: u64,
    /// How many orders this lane has minted, i.e. the next `n`.
    minted: u64,
    /// How many of those it has captured. Counted rather than read off
    /// `captured`, which is bounded and so cannot answer for the whole run.
    settled: u64,
    /// How many events it has produced, which is the `fixed` clock's offset
    /// from `epoch_ms` for the *next* one.
    produced: u64,
    open: Ring,
    captured: Ring,
    clock: Clock,
    epoch_ms: i64,
}

impl EventPlan {
    /// The plan for lane `lane_index` of `config.partitions`.
    ///
    /// `lane_index` must be below `config.partitions`: it is the lane's
    /// residue class, and one outside the modulus mints ids belonging to
    /// another lane.
    pub(crate) fn new(config: &DatagenSourceConfig, lane_index: u32) -> EventPlan {
        debug_assert!(
            lane_index < config.partitions,
            "lane {lane_index} is outside the {} lanes it takes a residue class from",
            config.partitions
        );
        EventPlan {
            rng: SplitMix64::new(config.lane_seed(lane_index)),
            lane_index: u64::from(lane_index),
            partitions: u64::from(config.partitions),
            minted: 0,
            settled: 0,
            produced: 0,
            open: Ring::new(),
            captured: Ring::new(),
            clock: config.clock,
            epoch_ms: config.epoch_ms,
        }
    }

    /// Orders this lane has placed and not yet captured — the `open_orders`
    /// gauge's per-lane contribution.
    ///
    /// Rises for as long as the lane runs. The mix places faster than it
    /// captures, and an order the ring evicts is never captured, so this is
    /// not a quantity that settles.
    pub(crate) fn open_orders(&self) -> u64 {
        self.minted - self.settled
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
    /// Under [`Clock::Fixed`] this is `epoch_ms` plus one millisecond per
    /// event *this lane* has already produced, so a lane's timestamps are a
    /// function of its position in its own stream and nothing else — no wall
    /// clock, no interleaving, no dependence on how the runtime scheduled the
    /// threads. The sum saturates, so an `epoch_ms` within a stream's length
    /// of [`i64::MAX`] pins every timestamp there.
    ///
    /// Under [`Clock::Wall`] it is the host clock and none of that holds.
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
        self.settled += 1;
        self.captured.push(pending);
        Some(StorefrontEvent::PaymentCaptured(PaymentCaptured {
            order_id: pending.order_id,
            amount_cents: pending.amount_cents,
        }))
    }

    fn refund(&mut self) -> Option<StorefrontEvent> {
        let pending = self.captured.take(&mut self.rng)?;
        // The whole of what was captured, or its half, third or quarter
        // rounded down — never more, which is the property a downstream
        // balance check depends on.
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
        let mut refunded: std::collections::HashSet<u64> = std::collections::HashSet::new();
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
                    assert!(
                        refunded.insert(e.order_id),
                        "order {} refunded twice — a capture settles once",
                        e.order_id
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

    /// The disjointness that referential integrity rests on: a lane's ids are
    /// its own residue class, so two lanes cannot collide even though neither
    /// can see the other.
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

    /// The stream itself, pinned. Determinism against a *second run of the
    /// same build* is what the test above shows; this is what holds the stream
    /// steady across builds, which is what the crate publishes.
    ///
    /// Any change to the order or count of draws in `next` moves these — a
    /// reordered struct literal is enough — so a payload change is a decision
    /// taken here rather than a side effect noticed downstream.
    #[test]
    fn the_generated_stream_is_pinned_across_builds() {
        let events = run(&config(4, 0), 1, 500);
        let encoded: Vec<String> = events
            .iter()
            .map(|e| serde_json::to_string(e).unwrap())
            .collect();

        assert_eq!(
            encoded[0],
            r#"{"type":"order_placed","order_id":1,"customer_id":335,"region":"eu-west","placed_at":1767225600000,"lines":[{"sku":"STD-04","qty":1,"unit_cents":79900}]}"#
        );
        assert_eq!(
            encoded[1], r#"{"type":"payment_captured","order_id":1,"amount_cents":79900}"#,
            "the capture settles the line total of the placement above it"
        );

        // FNV-1a over the whole run, so drift past the events spelled out
        // above is caught too.
        let digest = encoded
            .iter()
            .flat_map(|s| s.bytes())
            .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3)
            });
        assert_eq!(
            digest, 7_739_033_676_478_761_228,
            "the generated stream moved"
        );
    }

    #[test]
    fn the_rings_stay_bounded_however_long_the_lane_runs() {
        assert_eq!(RING_CAPACITY, 256, "the crate docs promise 256 entries");
        let cfg = config(1, 5);
        let mut plan = EventPlan::new(&cfg, 0);
        let mut high_water = 0;
        for _ in 0..100_000 {
            plan.next();
            assert!(plan.open.len() <= RING_CAPACITY);
            assert!(plan.captured.len() <= RING_CAPACITY);
            high_water = high_water.max(plan.open.len());
        }
        // Exactly at the bound, never past it. A ring sitting below its
        // capacity would mean the eviction was never reached, and the flat
        // footprint would be untested.
        assert_eq!(high_water, RING_CAPACITY);
    }

    /// The rings bound memory, not the backlog. `open_orders` answers for the
    /// whole run, so it has to outgrow the ring that holds a window of it —
    /// a gauge reading `RING_CAPACITY` forever would carry no information.
    #[test]
    fn open_orders_counts_the_whole_backlog_rather_than_the_ring() {
        let cfg = config(1, 5);
        let mut plan = EventPlan::new(&cfg, 0);
        let mut placed = 0u64;
        let mut captured = 0u64;
        for _ in 0..100_000 {
            match plan.next().0 {
                StorefrontEvent::OrderPlaced(_) => placed += 1,
                StorefrontEvent::PaymentCaptured(_) => captured += 1,
                StorefrontEvent::RefundIssued(_) => {}
            }
        }
        assert_eq!(
            plan.open_orders(),
            placed - captured,
            "open orders are the placements no capture has settled"
        );
        assert!(
            plan.open_orders() > 20 * RING_CAPACITY as u64,
            "open_orders tracks the ring ({}) rather than the backlog",
            plan.open_orders()
        );
    }

    /// The observed mix is the draw's. A fall-through can only add placements,
    /// but it needs an empty ring, and a lane's rings are empty only while it
    /// warms up — so over a long run the shares sit on the drawn ones.
    #[test]
    fn the_event_mix_follows_the_documented_shares() {
        const EVENTS: usize = 100_000;
        let mut counts = [0usize; 3];
        for event in run(&config(1, 17), 0, EVENTS) {
            counts[match event {
                StorefrontEvent::OrderPlaced(_) => 0,
                StorefrontEvent::PaymentCaptured(_) => 1,
                StorefrontEvent::RefundIssued(_) => 2,
            }] += 1;
        }
        let pct = |i: usize| counts[i] as f64 * 100.0 / EVENTS as f64;
        // Bands of ±1 point around the drawn 60/35/5. The sampling deviation
        // at this many events is under a tenth of a point, and warm-up
        // fall-throughs are a few events in a hundred thousand.
        assert!((59.0..61.0).contains(&pct(0)), "placed {}%", pct(0));
        assert!((34.0..36.0).contains(&pct(1)), "captured {}%", pct(1));
        assert!((4.0..6.0).contains(&pct(2)), "refunded {}%", pct(2));
    }

    /// A lane falls through to a placement when the ring it would draw from is
    /// empty, which is a warm-up behaviour: it happens in the opening handful
    /// of events and then not again.
    #[test]
    fn an_empty_ring_falls_through_to_a_placement_while_the_lane_warms_up() {
        let cfg = config(4, 42);
        let mut plan = EventPlan::new(&cfg, 0);
        let mut fell_through = Vec::new();
        for index in 0..10_000 {
            // A fall-through is a draw outside the placement band that
            // produced a placement anyway.
            let draw_is_placement = plan.rng.clone().below(100) < PLACE_THRESHOLD;
            let event = plan.next().0;
            if !draw_is_placement && matches!(event, StorefrontEvent::OrderPlaced(_)) {
                fell_through.push(index);
            }
        }
        assert!(
            !fell_through.is_empty(),
            "the fall-through never fired, so nothing exercised it"
        );
        assert!(
            fell_through.iter().all(|&i| i < 100),
            "a fall-through outside warm-up at {fell_through:?}"
        );
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
