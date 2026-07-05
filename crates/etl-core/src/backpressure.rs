//! Backpressure: the global in-flight byte budget and the watermark
//! pause/resume controller with hysteresis.
//!
//! Invariant (see `docs/DESIGN.md` § Backpressure): source threads never
//! block on sends. When a `try_send` is rejected or the in-flight budget
//! crosses its high watermark, the poll loop pauses its source lanes and
//! *keeps polling*; it resumes only under hysteresis — usage back below the
//! low watermark, downstream queues drained, and a minimum pause elapsed —
//! so pause/resume cannot flap faster than once per `min_pause`.
//!
//! Everything here is synchronous and tokio-free: pipeline threads call it
//! on every poll iteration, and the [`InflightBudget`] atomics are modeled
//! under [loom](https://docs.rs/loom). Run the loom suite with:
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test -p etl-core --release backpressure::loom_tests
//! ```
//!
//! # Poll-loop integration
//!
//! ```
//! use etl_core::backpressure::{
//!     BackpressureParams, InflightBudget, Transition, WatermarkController,
//! };
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! let budget = Arc::new(InflightBudget::new());
//! let params = BackpressureParams::from_budget(
//!     256 * 1024 * 1024, // max in-flight bytes
//!     0.8,               // pause at 80%
//!     0.5,               // resume below 50%
//!     Duration::from_millis(500),
//! );
//! let mut controller = WatermarkController::new(params);
//!
//! // Inside the poll loop:
//! // - when a try_send to a shard queue is rejected:
//! //     controller.on_send_rejected();
//! //     (stash the undeliverable record; NEVER block)
//! // - once per iteration:
//! let queues_below_low = true; // driver-provided: all shard queues < 50% full
//! match controller.tick(&budget, queues_below_low) {
//!     Some(Transition::Pause) => { /* source.pause(&owned_lanes) */ }
//!     Some(Transition::Resume) => { /* source.resume(&owned_lanes) */ }
//!     None => {}
//! }
//! ```

#![allow(unexpected_cfgs)] // `--cfg loom` is set only by the loom CI job.

use std::time::{Duration, Instant};

#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, Ordering};

/// Global in-flight byte budget, shared by pipeline threads (which add on
/// enqueue to sink queues) and sink workers (which subtract when a batch is
/// acknowledged or abandoned).
///
/// This is a heuristic gauge, not a synchronization point: decisions taken
/// on a slightly stale reading are corrected on the next poll iteration and
/// absorbed by the controller's hysteresis, so all operations use
/// [`Ordering::Relaxed`]. Atomic read-modify-write operations cannot lose
/// updates even under `Relaxed` (every RMW observes the latest value in the
/// modification order); relaxation only permits *stale reads* in
/// [`InflightBudget::usage`], which the hysteresis absorbs. Both directions
/// saturate — `sub` can never underflow past zero even if an
/// acknowledgement races ahead of the bookkeeping that added its bytes.
#[derive(Debug, Default)]
pub struct InflightBudget {
    bytes: AtomicUsize,
}

impl InflightBudget {
    /// An empty budget. Wrap in an `Arc` to share.
    #[must_use]
    pub fn new() -> Self {
        Self {
            bytes: AtomicUsize::new(0),
        }
    }

    /// Record `bytes` entering the in-flight window (saturating).
    pub fn add(&self, bytes: usize) {
        // `fetch_update` never returns `Err` with an always-`Some` closure.
        let _ = self
            .bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(bytes))
            });
    }

    /// Record `bytes` leaving the in-flight window (saturating at zero).
    pub fn sub(&self, bytes: usize) {
        let _ = self
            .bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(bytes))
            });
    }

    /// Current in-flight bytes (possibly slightly stale under contention).
    #[must_use]
    pub fn usage(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }
}

/// Time source for the controller, injectable so hysteresis is testable
/// without sleeping.
pub trait Clock {
    /// Current monotonic instant.
    fn now(&self) -> Instant;
}

/// Default [`Clock`] over [`Instant::now`].
#[derive(Clone, Copy, Debug, Default)]
pub struct MonotonicClock;

impl Clock for MonotonicClock {
    #[inline]
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Hysteresis parameters for one pipeline's watermark controller.
///
/// Struct literals are accepted as-is for config wiring;
/// [`BackpressureParams::from_budget`] validates its inputs. The controller
/// assumes `low_bytes <= high_bytes`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackpressureParams {
    /// Pause when [`InflightBudget::usage`] reaches this many bytes.
    pub high_bytes: usize,
    /// Resume only once usage is at or below this many bytes.
    pub low_bytes: usize,
    /// Minimum time to stay paused. Bounds the pause/resume flap rate and
    /// amortizes the prefetch purge that pausing a source implies (a paused
    /// Kafka partition drops its prefetched messages and refetches on
    /// resume — spike-verified).
    pub min_pause: Duration,
}

impl BackpressureParams {
    /// Derive watermarks from a byte budget and ratios.
    ///
    /// # Panics
    ///
    /// Panics unless `max_inflight_bytes > 0` and
    /// `0.0 < low_ratio <= high_ratio <= 1.0` — these are programmer
    /// errors; user-facing validation happens at config load.
    #[must_use]
    pub fn from_budget(
        max_inflight_bytes: usize,
        high_ratio: f64,
        low_ratio: f64,
        min_pause: Duration,
    ) -> Self {
        assert!(
            max_inflight_bytes > 0,
            "backpressure budget must be non-zero"
        );
        assert!(
            0.0 < low_ratio && low_ratio <= high_ratio && high_ratio <= 1.0,
            "backpressure ratios must satisfy 0 < low ({low_ratio}) <= high ({high_ratio}) <= 1"
        );
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_sign_loss,
            clippy::cast_possible_truncation
        )]
        let scale = |ratio: f64| (max_inflight_bytes as f64 * ratio) as usize;
        Self {
            high_bytes: scale(high_ratio).max(1),
            low_bytes: scale(low_ratio),
            min_pause,
        }
    }
}

/// A pause or resume decision for the poll loop to apply to its source
/// lanes (and mirror into the backpressure metrics).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transition {
    /// Pause the lanes this controller governs; keep polling.
    Pause,
    /// Resume the paused lanes.
    Resume,
}

#[derive(Clone, Copy, Debug)]
enum State {
    Normal,
    Paused { since: Instant },
}

/// Per-pipeline-thread pause/resume state machine with hysteresis.
///
/// The controller never calls anything: [`WatermarkController::tick`]
/// returns a [`Transition`] and the driver applies it, which keeps this
/// module free of source and metrics dependencies. Transitions strictly
/// alternate (`Pause`, `Resume`, `Pause`, ...) and each full cycle takes at
/// least [`BackpressureParams::min_pause`].
#[derive(Debug)]
pub struct WatermarkController<C: Clock = MonotonicClock> {
    params: BackpressureParams,
    state: State,
    /// A `try_send` rejection observed since the last `tick`.
    rejected: bool,
    clock: C,
}

impl WatermarkController<MonotonicClock> {
    /// Controller on the real monotonic clock.
    #[must_use]
    pub fn new(params: BackpressureParams) -> Self {
        Self::with_clock(params, MonotonicClock)
    }
}

impl<C: Clock> WatermarkController<C> {
    /// Controller with an injected clock (tests).
    #[must_use]
    pub fn with_clock(params: BackpressureParams, clock: C) -> Self {
        Self {
            params,
            state: State::Normal,
            rejected: false,
            clock,
        }
    }

    /// Record that a `try_send` to a downstream queue was rejected. Cheap;
    /// call from the poll loop's rejection path. While paused this restarts
    /// the minimum-pause timer — a rejection is proof downstream is still
    /// congested.
    pub fn on_send_rejected(&mut self) {
        self.rejected = true;
        if let State::Paused { since } = &mut self.state {
            *since = self.clock.now();
        }
    }

    /// Evaluate the state machine once per poll iteration.
    ///
    /// `queues_below_low` is the driver's view of its downstream queues
    /// (all below the low-watermark fill ratio). Returns a transition for
    /// the driver to apply, or `None`.
    pub fn tick(&mut self, budget: &InflightBudget, queues_below_low: bool) -> Option<Transition> {
        match self.state {
            State::Normal => {
                if self.rejected || budget.usage() >= self.params.high_bytes {
                    self.rejected = false;
                    self.state = State::Paused {
                        since: self.clock.now(),
                    };
                    Some(Transition::Pause)
                } else {
                    None
                }
            }
            State::Paused { since } => {
                if self.rejected {
                    // Consumed: `on_send_rejected` already restarted the
                    // timer; clear the flag so one rejection is not counted
                    // against two ticks.
                    self.rejected = false;
                    return None;
                }
                let drained = budget.usage() <= self.params.low_bytes && queues_below_low;
                if drained && self.clock.now().duration_since(since) >= self.params.min_pause {
                    self.state = State::Normal;
                    Some(Transition::Resume)
                } else {
                    None
                }
            }
        }
    }

    /// Whether the controller currently holds its lanes paused.
    #[must_use]
    pub fn is_paused(&self) -> bool {
        matches!(self.state, State::Paused { .. })
    }

    /// The hysteresis parameters in force.
    #[must_use]
    pub fn params(&self) -> &BackpressureParams {
        &self.params
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// Manual clock: starts at an arbitrary instant, advanced explicitly.
    struct TestClock {
        base: Instant,
        offset: Cell<Duration>,
    }

    impl TestClock {
        fn new() -> Self {
            Self {
                base: Instant::now(),
                offset: Cell::new(Duration::ZERO),
            }
        }

        fn advance(&self, d: Duration) {
            self.offset.set(self.offset.get() + d);
        }
    }

    impl Clock for &TestClock {
        fn now(&self) -> Instant {
            self.base + self.offset.get()
        }
    }

    const MIN_PAUSE: Duration = Duration::from_millis(500);

    fn params() -> BackpressureParams {
        BackpressureParams {
            high_bytes: 800,
            low_bytes: 500,
            min_pause: MIN_PAUSE,
        }
    }

    fn setup(clock: &TestClock) -> (WatermarkController<&TestClock>, InflightBudget) {
        (
            WatermarkController::with_clock(params(), clock),
            InflightBudget::new(),
        )
    }

    #[test]
    fn budget_saturates_both_directions() {
        let b = InflightBudget::new();
        b.sub(100);
        assert_eq!(b.usage(), 0, "sub never underflows");
        b.add(usize::MAX);
        b.add(100);
        assert_eq!(b.usage(), usize::MAX, "add saturates");
        b.sub(usize::MAX);
        assert_eq!(b.usage(), 0);
    }

    #[test]
    fn rejection_pauses_on_next_tick() {
        let clock = TestClock::new();
        let (mut ctl, budget) = setup(&clock);
        assert_eq!(ctl.tick(&budget, true), None);
        ctl.on_send_rejected();
        assert_eq!(ctl.tick(&budget, true), Some(Transition::Pause));
        assert!(ctl.is_paused());
    }

    #[test]
    fn high_watermark_pauses_without_rejection() {
        let clock = TestClock::new();
        let (mut ctl, budget) = setup(&clock);
        budget.add(800);
        assert_eq!(ctl.tick(&budget, true), Some(Transition::Pause));
    }

    #[test]
    fn no_resume_before_min_pause() {
        let clock = TestClock::new();
        let (mut ctl, budget) = setup(&clock);
        ctl.on_send_rejected();
        ctl.tick(&budget, true);
        clock.advance(MIN_PAUSE - Duration::from_millis(1));
        assert_eq!(ctl.tick(&budget, true), None, "drained but too early");
    }

    #[test]
    fn no_resume_above_low_watermark() {
        let clock = TestClock::new();
        let (mut ctl, budget) = setup(&clock);
        budget.add(900);
        ctl.tick(&budget, true);
        clock.advance(MIN_PAUSE * 2);
        budget.sub(300); // 600 > low (500)
        assert_eq!(ctl.tick(&budget, true), None);
        budget.sub(200); // 400 <= low
        assert_eq!(ctl.tick(&budget, true), Some(Transition::Resume));
    }

    #[test]
    fn no_resume_while_queues_are_full() {
        let clock = TestClock::new();
        let (mut ctl, budget) = setup(&clock);
        ctl.on_send_rejected();
        ctl.tick(&budget, true);
        clock.advance(MIN_PAUSE * 2);
        assert_eq!(ctl.tick(&budget, false), None);
        assert_eq!(ctl.tick(&budget, true), Some(Transition::Resume));
    }

    #[test]
    fn rejection_while_paused_restarts_the_timer() {
        let clock = TestClock::new();
        let (mut ctl, budget) = setup(&clock);
        ctl.on_send_rejected();
        ctl.tick(&budget, true);
        clock.advance(MIN_PAUSE - Duration::from_millis(1));
        ctl.on_send_rejected(); // congestion evidence: restart
        clock.advance(Duration::from_millis(2)); // past original deadline
        assert_eq!(ctl.tick(&budget, true), None);
        clock.advance(MIN_PAUSE);
        assert_eq!(ctl.tick(&budget, true), Some(Transition::Resume));
    }

    #[test]
    fn transitions_strictly_alternate_and_cycles_respect_min_pause() {
        // Adversary: rejects the instant we resume, drains immediately
        // after we pause. Transitions must still alternate and the rate is
        // bounded by min_pause per full cycle.
        let clock = TestClock::new();
        let (mut ctl, budget) = setup(&clock);
        let mut transitions = Vec::new();
        let step = Duration::from_millis(50);
        let total = MIN_PAUSE * 10; // 5s of virtual time
        let mut elapsed = Duration::ZERO;
        while elapsed < total {
            if !ctl.is_paused() {
                ctl.on_send_rejected();
            }
            if let Some(t) = ctl.tick(&budget, true) {
                transitions.push(t);
            }
            clock.advance(step);
            elapsed += step;
        }
        for pair in transitions.chunks(2) {
            assert_eq!(pair[0], Transition::Pause);
            if let Some(second) = pair.get(1) {
                assert_eq!(*second, Transition::Resume);
            }
        }
        let cycles = usize::try_from(total.as_millis() / MIN_PAUSE.as_millis()).unwrap();
        assert!(
            transitions.len() <= 2 * (cycles + 1),
            "flapping: {} transitions in {} min_pause windows",
            transitions.len(),
            cycles
        );
        assert!(transitions.len() >= 2, "controller wedged");
    }

    #[test]
    fn from_budget_computes_thresholds() {
        let p = BackpressureParams::from_budget(1000, 0.8, 0.5, MIN_PAUSE);
        assert_eq!(p.high_bytes, 800);
        assert_eq!(p.low_bytes, 500);
    }

    #[test]
    #[should_panic(expected = "backpressure ratios")]
    fn from_budget_rejects_inverted_ratios() {
        let _ = BackpressureParams::from_budget(1000, 0.5, 0.8, MIN_PAUSE);
    }

    #[test]
    #[should_panic(expected = "non-zero")]
    fn from_budget_rejects_zero_budget() {
        let _ = BackpressureParams::from_budget(0, 0.8, 0.5, MIN_PAUSE);
    }

    mod properties {
        use super::*;
        use proptest::prelude::*;

        #[derive(Clone, Copy, Debug)]
        enum Op {
            Add(usize),
            Sub(usize),
            Reject,
            Advance(u64),
            Tick,
        }

        fn op_strategy() -> impl Strategy<Value = Op> {
            prop_oneof![
                (0usize..2000).prop_map(Op::Add),
                (0usize..2000).prop_map(Op::Sub),
                Just(Op::Reject),
                (1u64..400).prop_map(Op::Advance),
                Just(Op::Tick),
            ]
        }

        proptest! {
            /// The budget matches a saturating single-threaded model, and
            /// transitions strictly alternate starting with Pause.
            #[test]
            fn model_equivalence(ops in proptest::collection::vec(op_strategy(), 1..200)) {
                let clock = TestClock::new();
                let (mut ctl, budget) = setup(&clock);
                let mut model: usize = 0;
                let mut transitions = Vec::new();
                for op in ops {
                    match op {
                        Op::Add(n) => { budget.add(n); model = model.saturating_add(n); }
                        Op::Sub(n) => { budget.sub(n); model = model.saturating_sub(n); }
                        Op::Reject => ctl.on_send_rejected(),
                        Op::Advance(ms) => clock.advance(Duration::from_millis(ms)),
                        Op::Tick => {
                            if let Some(t) = ctl.tick(&budget, true) {
                                transitions.push(t);
                            }
                        }
                    }
                    prop_assert_eq!(budget.usage(), model);
                }
                for (i, t) in transitions.iter().enumerate() {
                    let expected = if i % 2 == 0 { Transition::Pause } else { Transition::Resume };
                    prop_assert_eq!(*t, expected);
                }
            }

            /// Liveness: whatever happened before, once the system drains
            /// and stays quiet past min_pause, the controller resumes.
            #[test]
            fn eventually_resumes_after_drain(ops in proptest::collection::vec(op_strategy(), 1..200)) {
                let clock = TestClock::new();
                let (mut ctl, budget) = setup(&clock);
                for op in ops {
                    match op {
                        Op::Add(n) => budget.add(n),
                        Op::Sub(n) => budget.sub(n),
                        Op::Reject => ctl.on_send_rejected(),
                        Op::Advance(ms) => clock.advance(Duration::from_millis(ms)),
                        Op::Tick => { let _ = ctl.tick(&budget, true); }
                    }
                }
                // Drain the world and go quiet.
                budget.sub(budget.usage());
                let _ = ctl.tick(&budget, true); // consume any pending rejection
                clock.advance(MIN_PAUSE * 2);
                let _ = ctl.tick(&budget, true);
                prop_assert!(!ctl.is_paused(), "controller wedged in Paused");
            }
        }
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::InflightBudget;
    use loom::sync::Arc;
    use loom::thread;

    /// Balanced concurrent add/sub from multiple threads never underflows
    /// and always converges to zero: atomic RMW cannot lose updates, and
    /// saturation bounds every interleaving.
    #[test]
    fn balanced_ops_converge_to_zero() {
        loom::model(|| {
            let budget = Arc::new(InflightBudget::new());
            let handles: Vec<_> = [10usize, 25]
                .into_iter()
                .map(|n| {
                    let b = Arc::clone(&budget);
                    thread::spawn(move || {
                        b.add(n);
                        let _ = b.usage(); // reader interleaves freely
                        b.sub(n);
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
            assert_eq!(budget.usage(), 0);
        });
    }

    /// An unbalanced `sub` racing an `add` saturates at zero rather than
    /// wrapping.
    #[test]
    fn premature_sub_saturates() {
        loom::model(|| {
            let budget = Arc::new(InflightBudget::new());
            let b = Arc::clone(&budget);
            let t = thread::spawn(move || b.sub(40));
            budget.add(15);
            t.join().unwrap();
            assert!(budget.usage() <= 15, "usage bounded by what was added");
        });
    }
}
