//! Backpressure: the global in-flight byte budget and the watermark
//! pause/resume controller with hysteresis.
//!
//! Invariant (INV-2): source threads never block on sends. The poll loop
//! pauses its source lanes and *keeps polling* when the in-flight budget
//! crosses its high watermark, or when a `try_send` is rejected while a shard
//! worker waits for a write permit. A rejection while every worker can take
//! work pauses only once rejections have lasted `min_pause` with no push
//! completing. The loop resumes only under hysteresis: usage back below the
//! low watermark, downstream queues drained, and a minimum pause elapsed. A
//! pause a queue rejection raised while a shard worker waited for a permit,
//! with usage under the high watermark, skips the minimum once no worker
//! waits, so it cycles with the sink's permits. Every other pause/resume cycle
//! takes at least `min_pause`.
//!
//! Everything here is synchronous and tokio-free. Pipeline threads call it
//! on every poll iteration, and the [`InflightBudget`] atomics are modeled
//! under [loom](https://docs.rs/loom). Run the loom suite with:
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test -p spate-core --release backpressure::loom_tests
//! ```
//!
//! # Poll-loop integration
//!
//! ```
//! use spate_core::backpressure::{
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
//! //     controller.on_queue_rejected(&budget);
//! //     (stash the undeliverable record; NEVER block)
//! // - when a push completes with nothing left stashed:
//! //     controller.on_push_done();
//! // - once per iteration, with a reading of whether every shard queue is
//! //   at or below 50% full, which the controller calls only when it decides:
//! let queues_below_low = || true;
//! match controller.tick(&budget, queues_below_low) {
//!     Some(Transition::Pause) => { /* source.pause(&owned_lanes) */ }
//!     Some(Transition::Resume) => { /* source.resume(&owned_lanes) */ }
//!     None => {}
//! }
//! ```

use crate::clock::{Clock, SystemClock};
use std::time::{Duration, Instant};

#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, Ordering};

/// Global in-flight byte budget, shared by pipeline threads (which add on
/// enqueue to sink queues) and sink workers (which subtract when a batch is
/// acknowledged or abandoned).
///
/// This is a heuristic gauge, not a synchronization point. Decisions taken
/// on a slightly stale reading are corrected on the next poll iteration and
/// absorbed by the controller's hysteresis, so all operations use
/// [`Ordering::Relaxed`]. Atomic read-modify-write operations cannot lose
/// updates even under `Relaxed` (every RMW observes the latest value in the
/// modification order); relaxation only permits *stale reads* in
/// [`InflightBudget::usage`], which the hysteresis absorbs. Both directions
/// saturate. `sub` cannot underflow past zero even if an acknowledgment
/// races ahead of the bookkeeping that added its bytes.
///
/// The budget also counts the shards whose worker holds a sealed batch with
/// no free write permit. [`WatermarkController::on_queue_rejected`] reads that
/// count to tell sink pressure from a queue the worker has not reached yet.
#[derive(Debug, Default)]
pub struct InflightBudget {
    bytes: AtomicUsize,
    /// Shards whose worker holds a sealed batch with no free write permit.
    gated: AtomicUsize,
}

impl InflightBudget {
    /// An empty budget. Wrap in an `Arc` to share.
    #[must_use]
    pub fn new() -> Self {
        Self {
            bytes: AtomicUsize::new(0),
            gated: AtomicUsize::new(0),
        }
    }

    /// Record `bytes` entering the in-flight window (saturating).
    pub fn add(&self, bytes: usize) {
        self.update(|v| v.saturating_add(bytes));
    }

    /// Record `bytes` leaving the in-flight window (saturating at zero).
    pub fn sub(&self, bytes: usize) {
        self.update(|v| v.saturating_sub(bytes));
    }

    #[cfg(not(loom))]
    fn update(&self, f: impl FnMut(usize) -> usize) {
        self.bytes.update(Ordering::Relaxed, Ordering::Relaxed, f);
    }

    // loom has no `update`.
    #[cfg(loom)]
    fn update(&self, mut f: impl FnMut(usize) -> usize) {
        let _ = self
            .bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(f(v)));
    }

    /// Current in-flight bytes (possibly slightly stale under contention).
    #[must_use]
    pub fn usage(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Count one more shard whose worker holds a sealed batch with no free
    /// write permit. Each call is paired with one [`Self::ungate_shard`].
    pub(crate) fn gate_shard(&self) {
        self.gated.fetch_add(1, Ordering::Relaxed);
    }

    /// Take back one shard counted by [`Self::gate_shard`].
    pub(crate) fn ungate_shard(&self) {
        let before = self.gated.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(before > 0, "ungate_shard without a matching gate_shard");
    }

    /// How many shards wait for a write permit (possibly slightly stale).
    pub(crate) fn gated_shards(&self) -> usize {
        self.gated.load(Ordering::Relaxed)
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
    /// Minimum time to stay paused. Bounds the pause/resume flap rate, since
    /// resuming a paused lane is not free for every source.
    ///
    /// A pause raised by a queue rejection while a shard worker waits for a
    /// write permit, with usage under `high_bytes`, does not wait it out.
    /// It ends once no worker waits and the other resume conditions hold.
    ///
    /// Also how long queue rejections must last, with no push completing,
    /// before they pause the source while no shard worker waits for a permit
    /// (see [`WatermarkController::on_queue_rejected`]).
    pub min_pause: Duration,
}

impl BackpressureParams {
    /// Derive watermarks from a byte budget and ratios.
    ///
    /// # Panics
    ///
    /// Panics unless `max_inflight_bytes > 0` and
    /// `0.0 < low_ratio <= high_ratio <= 1.0`. User-facing validation of the
    /// same bounds happens at config load.
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
/// [`WatermarkController::tick`] returns a [`Transition`] and the driver
/// applies it; the controller calls nothing itself. Transitions strictly
/// alternate (`Pause`, `Resume`, `Pause`, ...). Each full cycle takes at
/// least [`BackpressureParams::min_pause`], except one whose pause a gated
/// queue rejection raised (see [`Self::on_queue_rejected`]).
#[derive(Debug)]
pub struct WatermarkController<C: Clock = SystemClock> {
    params: BackpressureParams,
    state: State,
    /// A rejection under sink pressure observed since the last `tick`.
    rejected: bool,
    /// While not paused, the rejection behind `rejected` came while a shard
    /// worker waited for a write permit. While paused, that rejection raised
    /// the pause with usage under the high watermark and no run spanning
    /// `min_pause`, so the pause ends once no worker waits. Cleared on
    /// resume.
    gated: bool,
    /// The first and latest rejection of the current run of rejections while
    /// no shard worker waits for a permit. A completed push ends the run.
    contended: Option<(Instant, Instant)>,
    clock: C,
}

impl WatermarkController<SystemClock> {
    /// Controller on the real monotonic clock.
    #[must_use]
    pub fn new(params: BackpressureParams) -> Self {
        Self::with_clock(params, SystemClock)
    }
}

impl<C: Clock> WatermarkController<C> {
    /// Controller on an injected clock.
    #[must_use]
    pub fn with_clock(params: BackpressureParams, clock: C) -> Self {
        Self {
            params,
            state: State::Normal,
            rejected: false,
            gated: false,
            contended: None,
            clock,
        }
    }

    /// Record a rejection under sink pressure: the next `tick` pauses, and
    /// while paused this restarts the minimum-pause timer.
    ///
    /// A poll loop with an [`InflightBudget`] calls
    /// [`Self::on_queue_rejected`] instead, which calls this only when a shard
    /// worker waits for a write permit.
    pub fn on_send_rejected(&mut self) {
        self.rejected = true;
        if let State::Paused { since } = &mut self.state {
            *since = self.clock.now();
        }
    }

    /// Record that a `try_send` to a shard queue was rejected. Cheap; call
    /// from the poll loop's rejection path.
    ///
    /// While `budget` counts a shard whose worker waits for a write permit,
    /// the sink is the limit, and the next `tick` pauses as after
    /// [`Self::on_send_rejected`]. That pause ends once no shard waits and
    /// the other resume conditions hold, with no minimum pause. It keeps
    /// [`BackpressureParams::min_pause`] when usage had reached the high
    /// watermark, or a run of rejections had spanned `min_pause`, on the
    /// tick that raised it.
    ///
    /// While no shard waits, the worker has not reached its queue yet, and
    /// the rejection starts or extends a run that pauses only once it spans
    /// `min_pause`. [`Self::on_push_done`] ends the run.
    ///
    /// While paused, any rejection restarts the minimum-pause timer.
    pub fn on_queue_rejected(&mut self, budget: &InflightBudget) {
        if self.is_paused() {
            self.on_send_rejected();
            return;
        }
        if budget.gated_shards() > 0 {
            self.gated = true;
            self.on_send_rejected();
            return;
        }
        let now = self.clock.now();
        let first = self.contended.map_or(now, |(first, _)| first);
        self.contended = Some((first, now));
    }

    /// Record that a push or flush completed with nothing left parked, which
    /// ends a run of rejections recorded by [`Self::on_queue_rejected`].
    pub fn on_push_done(&mut self) {
        self.contended = None;
    }

    /// Evaluate the state machine once per poll iteration.
    ///
    /// `queues_below_low` reads the driver's downstream queues and returns
    /// whether all of them are at or below the low-watermark fill ratio. It is
    /// called at most once, and only while paused with no rejection since the
    /// last tick and usage at or below the low watermark. Returns a transition
    /// for the driver to apply, or `None`.
    // Without the hint, a split codegen unit can leave `tick` out of line in
    // the poll loop.
    #[inline]
    pub fn tick(
        &mut self,
        budget: &InflightBudget,
        queues_below_low: impl FnOnce() -> bool,
    ) -> Option<Transition> {
        match self.state {
            State::Normal => {
                // A span, so a lone rejection followed by a quiet source does
                // not pause the thread long after.
                let contended = self.contended.is_some_and(|(first, last)| {
                    last.duration_since(first) >= self.params.min_pause
                });
                let over = budget.usage() >= self.params.high_bytes;
                if self.rejected || contended || over {
                    self.gated &= !contended && !over;
                    self.rejected = false;
                    self.contended = None;
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
                    // `on_send_rejected` already restarted the timer; clear
                    // the flag so one rejection is not counted against two
                    // ticks.
                    self.rejected = false;
                    return None;
                }
                let drained = budget.usage() <= self.params.low_bytes && queues_below_low();
                if drained && self.clock.now().duration_since(since) >= self.floor(budget) {
                    self.gated = false;
                    self.state = State::Normal;
                    Some(Transition::Resume)
                } else {
                    None
                }
            }
        }
    }

    /// How long the current pause must last before it can end. A pause the
    /// sink's permits raised ends once no shard waits for one.
    #[inline]
    fn floor(&self, budget: &InflightBudget) -> Duration {
        if self.gated && budget.gated_shards() == 0 {
            Duration::ZERO
        } else {
            self.params.min_pause
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
    use crate::clock::TestClock;
    use std::cell::Cell;

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
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        assert_eq!(ctl.tick(&budget, || true), None);
        ctl.on_send_rejected();
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Pause));
        assert!(ctl.is_paused());
    }

    #[test]
    fn high_watermark_pauses_without_rejection() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        budget.add(800);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Pause));
    }

    #[test]
    fn no_resume_before_min_pause() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        ctl.on_send_rejected();
        ctl.tick(&budget, || true);
        clock.advance(MIN_PAUSE - Duration::from_millis(1));
        assert_eq!(ctl.tick(&budget, || true), None, "drained but too early");
    }

    #[test]
    fn no_resume_above_low_watermark() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        budget.add(900);
        ctl.tick(&budget, || true);
        clock.advance(MIN_PAUSE * 2);
        budget.sub(300); // 600 > low (500)
        assert_eq!(ctl.tick(&budget, || true), None);
        budget.sub(200); // 400 <= low
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Resume));
    }

    #[test]
    fn no_resume_while_queues_are_full() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        ctl.on_send_rejected();
        ctl.tick(&budget, || true);
        clock.advance(MIN_PAUSE * 2);
        assert_eq!(ctl.tick(&budget, || false), None);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Resume));
    }

    /// The queue reading costs an atomic load per shard, so it is taken only
    /// while paused, with no rejection pending and usage at or below the low
    /// watermark.
    #[test]
    fn reads_the_queues_only_while_paused_at_or_below_low() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        let reads = Cell::new(0);
        let reading = || {
            reads.set(reads.get() + 1);
            true
        };

        assert_eq!(ctl.tick(&budget, reading), None);
        assert_eq!(reads.get(), 0, "running");
        ctl.on_send_rejected();
        assert_eq!(ctl.tick(&budget, reading), Some(Transition::Pause));
        assert_eq!(reads.get(), 0, "pausing on a rejection");
        ctl.on_send_rejected();
        assert_eq!(ctl.tick(&budget, reading), None);
        assert_eq!(reads.get(), 0, "paused with a rejection pending");
        budget.add(501);
        assert_eq!(ctl.tick(&budget, reading), None);
        assert_eq!(reads.get(), 0, "paused above the low watermark");
        budget.sub(1); // exactly low
        assert_eq!(ctl.tick(&budget, reading), None, "drained but too early");
        assert_eq!(reads.get(), 1, "paused at the low watermark");
        clock.advance(MIN_PAUSE);
        assert_eq!(ctl.tick(&budget, reading), Some(Transition::Resume));
        assert_eq!(reads.get(), 2, "resuming");
    }

    #[test]
    fn rejection_while_paused_restarts_the_timer() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        ctl.on_send_rejected();
        ctl.tick(&budget, || true);
        clock.advance(MIN_PAUSE - Duration::from_millis(1));
        ctl.on_send_rejected(); // congestion evidence: restart
        clock.advance(Duration::from_millis(2)); // past original deadline
        assert_eq!(ctl.tick(&budget, || true), None);
        clock.advance(MIN_PAUSE);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Resume));
    }

    /// Rejections while no shard waits for a permit pause only once their run
    /// spans `min_pause`: 1 ms short does not pause, exactly `min_pause` does.
    #[test]
    fn a_contended_run_pauses_once_it_spans_min_pause() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        ctl.on_queue_rejected(&budget);
        assert_eq!(ctl.tick(&budget, || true), None, "one rejection");
        clock.advance(MIN_PAUSE - Duration::from_millis(1));
        ctl.on_queue_rejected(&budget);
        assert_eq!(ctl.tick(&budget, || true), None, "1 ms short of min_pause");
        clock.advance(Duration::from_millis(1));
        ctl.on_queue_rejected(&budget);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Pause));
    }

    /// The run is measured between rejections, so a lone rejection followed by
    /// a quiet source never pauses.
    #[test]
    fn a_lone_contended_rejection_never_pauses() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        ctl.on_queue_rejected(&budget);
        assert_eq!(ctl.tick(&budget, || true), None);
        clock.advance(MIN_PAUSE * 100);
        assert_eq!(ctl.tick(&budget, || true), None);
        assert!(!ctl.is_paused());
    }

    #[test]
    fn a_completed_push_ends_a_contended_run() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        ctl.on_queue_rejected(&budget);
        ctl.on_push_done();
        clock.advance(MIN_PAUSE);
        ctl.on_queue_rejected(&budget);
        assert_eq!(ctl.tick(&budget, || true), None);
    }

    /// A shard waiting for a permit makes a queue rejection sink pressure,
    /// which pauses on the next tick as `on_send_rejected` does.
    #[test]
    fn a_rejection_while_a_shard_is_gated_pauses_on_next_tick() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        budget.gate_shard();
        ctl.on_queue_rejected(&budget);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Pause));
        budget.ungate_shard();
    }

    /// A pause raised while a shard waited for a permit resumes once none
    /// waits and the queues have drained, before `min_pause`.
    #[test]
    fn a_gated_pause_resumes_once_no_shard_waits() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        budget.gate_shard();
        ctl.on_queue_rejected(&budget);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Pause));
        assert_eq!(ctl.tick(&budget, || true), None, "a shard still waits");
        budget.ungate_shard();
        assert_eq!(ctl.tick(&budget, || false), None, "queues not drained");
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Resume));
    }

    /// A gated rejection that coincides with usage at the high watermark
    /// raises a pause the budget also asks for, which keeps `min_pause`.
    #[test]
    fn a_gated_rejection_at_the_high_watermark_keeps_min_pause() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        budget.add(800);
        budget.gate_shard();
        ctl.on_queue_rejected(&budget);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Pause));
        budget.ungate_shard();
        budget.sub(800);
        assert_eq!(ctl.tick(&budget, || true), None, "min_pause not elapsed");
        clock.advance(MIN_PAUSE);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Resume));
    }

    /// A gated rejection that arrives during a pause the budget raised
    /// restarts the timer and leaves the pause on `min_pause`.
    #[test]
    fn a_gated_rejection_during_a_high_watermark_pause_keeps_min_pause() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        budget.add(800);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Pause));
        budget.gate_shard();
        ctl.on_queue_rejected(&budget);
        assert_eq!(ctl.tick(&budget, || true), None, "a rejection this tick");
        budget.ungate_shard();
        budget.sub(800);
        assert_eq!(ctl.tick(&budget, || true), None, "min_pause not elapsed");
        clock.advance(MIN_PAUSE);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Resume));
    }

    /// A gated rejection on the tick a run with no shard waiting already
    /// spans `min_pause` raises a pause the run also asks for, which keeps
    /// `min_pause`.
    #[test]
    fn a_gated_rejection_with_a_spanning_run_keeps_min_pause() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        ctl.on_queue_rejected(&budget);
        clock.advance(MIN_PAUSE);
        ctl.on_queue_rejected(&budget);
        budget.gate_shard();
        ctl.on_queue_rejected(&budget);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Pause));
        budget.ungate_shard();
        assert_eq!(ctl.tick(&budget, || true), None, "min_pause not elapsed");
        clock.advance(MIN_PAUSE);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Resume));
    }

    /// A gated rejection does not join a run of rejections with no shard
    /// waiting, so it cannot stretch that run past `min_pause`, and its pause
    /// ends once no shard waits.
    #[test]
    fn a_gated_rejection_after_a_short_run_resumes_once_no_shard_waits() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        ctl.on_queue_rejected(&budget);
        assert_eq!(ctl.tick(&budget, || true), None, "one rejection");
        clock.advance(MIN_PAUSE);
        budget.gate_shard();
        ctl.on_queue_rejected(&budget);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Pause));
        budget.ungate_shard();
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Resume));
    }

    /// The pause a gated rejection raised does not carry over to the next
    /// pause, which `on_send_rejected` raises with no shard waiting.
    #[test]
    fn the_next_pause_after_a_gated_one_keeps_min_pause() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        budget.gate_shard();
        ctl.on_queue_rejected(&budget);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Pause));
        budget.ungate_shard();
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Resume));
        ctl.on_send_rejected();
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Pause));
        assert_eq!(ctl.tick(&budget, || true), None, "min_pause not elapsed");
    }

    /// With no minimum pause, any queue rejection pauses on the next tick.
    #[test]
    fn a_contended_rejection_pauses_at_once_with_zero_min_pause() {
        let clock = TestClock::frozen();
        let mut ctl = WatermarkController::with_clock(
            BackpressureParams {
                min_pause: Duration::ZERO,
                ..params()
            },
            &*clock,
        );
        let budget = InflightBudget::new();
        ctl.on_queue_rejected(&budget);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Pause));
    }

    #[test]
    fn a_contended_rejection_while_paused_restarts_the_timer() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        ctl.on_send_rejected();
        ctl.tick(&budget, || true);
        clock.advance(MIN_PAUSE - Duration::from_millis(1));
        ctl.on_queue_rejected(&budget);
        clock.advance(Duration::from_millis(2)); // past original deadline
        assert_eq!(ctl.tick(&budget, || true), None);
        clock.advance(MIN_PAUSE);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Resume));
    }

    /// A pause ends the run that raised it, so resuming does not pause again
    /// on the run's old span.
    #[test]
    fn a_pause_ends_the_contended_run_that_raised_it() {
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        ctl.on_queue_rejected(&budget);
        clock.advance(MIN_PAUSE);
        ctl.on_queue_rejected(&budget);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Pause));
        clock.advance(MIN_PAUSE);
        assert_eq!(ctl.tick(&budget, || true), Some(Transition::Resume));
        assert_eq!(ctl.tick(&budget, || true), None);
    }

    #[test]
    fn transitions_strictly_alternate_and_cycles_respect_min_pause() {
        // Adversary: rejects the instant we resume, drains immediately
        // after we pause. Transitions must still alternate and the rate is
        // bounded by min_pause per full cycle.
        let clock = TestClock::frozen();
        let (mut ctl, budget) = setup(&clock);
        let mut transitions = Vec::new();
        let step = Duration::from_millis(50);
        let total = MIN_PAUSE * 10; // 5s of virtual time
        let mut elapsed = Duration::ZERO;
        while elapsed < total {
            if !ctl.is_paused() {
                ctl.on_send_rejected();
            }
            if let Some(t) = ctl.tick(&budget, || true) {
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
            QueueReject,
            PushDone,
            Gate,
            Ungate,
            Advance(u64),
            Tick,
        }

        fn op_strategy() -> impl Strategy<Value = Op> {
            prop_oneof![
                (0usize..2000).prop_map(Op::Add),
                (0usize..2000).prop_map(Op::Sub),
                Just(Op::Reject),
                Just(Op::QueueReject),
                Just(Op::PushDone),
                Just(Op::Gate),
                Just(Op::Ungate),
                (1u64..400).prop_map(Op::Advance),
                Just(Op::Tick),
            ]
        }

        proptest! {
            /// The budget matches a saturating single-threaded model, and
            /// transitions strictly alternate starting with Pause.
            #[test]
            fn model_equivalence(ops in proptest::collection::vec(op_strategy(), 1..200)) {
                let clock = TestClock::frozen();
                let (mut ctl, budget) = setup(&clock);
                let mut model: usize = 0;
                let mut gated: usize = 0;
                let mut transitions = Vec::new();
                for op in ops {
                    match op {
                        Op::Add(n) => { budget.add(n); model = model.saturating_add(n); }
                        Op::Sub(n) => { budget.sub(n); model = model.saturating_sub(n); }
                        Op::Reject => ctl.on_send_rejected(),
                        Op::QueueReject => ctl.on_queue_rejected(&budget),
                        Op::PushDone => ctl.on_push_done(),
                        Op::Gate => { budget.gate_shard(); gated += 1; }
                        Op::Ungate => if gated > 0 { budget.ungate_shard(); gated -= 1; },
                        Op::Advance(ms) => clock.advance(Duration::from_millis(ms)),
                        Op::Tick => {
                            if let Some(t) = ctl.tick(&budget, || true) {
                                transitions.push(t);
                            }
                        }
                    }
                    prop_assert_eq!(budget.usage(), model);
                    prop_assert_eq!(budget.gated_shards(), gated);
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
                let clock = TestClock::frozen();
                let (mut ctl, budget) = setup(&clock);
                let mut gated: usize = 0;
                for op in ops {
                    match op {
                        Op::Add(n) => budget.add(n),
                        Op::Sub(n) => budget.sub(n),
                        Op::Reject => ctl.on_send_rejected(),
                        Op::QueueReject => ctl.on_queue_rejected(&budget),
                        Op::PushDone => ctl.on_push_done(),
                        Op::Gate => { budget.gate_shard(); gated += 1; }
                        Op::Ungate => if gated > 0 { budget.ungate_shard(); gated -= 1; },
                        Op::Advance(ms) => clock.advance(Duration::from_millis(ms)),
                        Op::Tick => { let _ = ctl.tick(&budget, || true); }
                    }
                }
                // Drain the world and go quiet.
                budget.sub(budget.usage());
                // Consume any pending rejection, or pause on a run that already
                // spans min_pause.
                let _ = ctl.tick(&budget, || true);
                clock.advance(MIN_PAUSE * 2);
                let _ = ctl.tick(&budget, || true);
                prop_assert!(!ctl.is_paused(), "controller wedged in Paused");
            }

            /// With no shard ever waiting for a permit, every pause lasts at
            /// least `min_pause`.
            #[test]
            fn an_ungated_pause_lasts_min_pause(
                ops in proptest::collection::vec(
                    op_strategy().prop_filter("no gating", |op| !matches!(op, Op::Gate | Op::Ungate)),
                    1..200,
                ),
            ) {
                let clock = TestClock::frozen();
                let (mut ctl, budget) = setup(&clock);
                let mut paused_at = None;
                for op in ops {
                    match op {
                        Op::Add(n) => budget.add(n),
                        Op::Sub(n) => budget.sub(n),
                        Op::Reject => ctl.on_send_rejected(),
                        Op::QueueReject => ctl.on_queue_rejected(&budget),
                        Op::PushDone => ctl.on_push_done(),
                        Op::Gate | Op::Ungate => unreachable!("filtered out"),
                        Op::Advance(ms) => clock.advance(Duration::from_millis(ms)),
                        Op::Tick => match ctl.tick(&budget, || true) {
                            Some(Transition::Pause) => paused_at = Some(clock.now()),
                            Some(Transition::Resume) => {
                                let at = paused_at.take().expect("a resume follows a pause");
                                prop_assert!(clock.now().duration_since(at) >= MIN_PAUSE);
                            }
                            None => {}
                        },
                    }
                }
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
    /// and always converges to zero. Atomic RMW cannot lose updates, and
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

    /// Shard workers gating and ungating while a pipeline thread reads the
    /// count leave it at zero once every worker has taken back its share.
    #[test]
    fn balanced_gates_converge_to_zero() {
        loom::model(|| {
            let budget = Arc::new(InflightBudget::new());
            let handles: Vec<_> = (0..2)
                .map(|_| {
                    let b = Arc::clone(&budget);
                    thread::spawn(move || {
                        b.gate_shard();
                        let _ = b.gated_shards(); // reader interleaves freely
                        b.ungate_shard();
                    })
                })
                .collect();
            let _ = budget.gated_shards();
            for h in handles {
                h.join().unwrap();
            }
            assert_eq!(budget.gated_shards(), 0);
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
