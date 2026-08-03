//! The backpressure bench rig: a fixed script of poll-loop iterations, each
//! one a movement of the in-flight budget plus the watermark controller's
//! `tick` over it.
//!
//! The script is a corpus, not a repeat count. Every entry carries its own
//! budget movement, its own queue reading and its own clock step, and the
//! controller's state after an entry differs from its state before it — the
//! unit of work being counted is a poll iteration, which is what a pipeline
//! thread runs millions of.
//!
//! Nothing else compiles this file — the controller has no wall-clock
//! sibling, and would not be a candid one: a `tick` is tens of instructions,
//! well under the resolution of the timer that would have to measure it.

use spate_core::backpressure::{BackpressureParams, Clock, InflightBudget, WatermarkController};
use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

/// Poll iterations one case drives.
pub(crate) const ITERATIONS: usize = 32_768;

/// The in-flight budget the watermarks are derived from. Small enough that a
/// realistic chunk moves the reading appreciably, which is what lets the
/// script cross a watermark on a schedule the fixture can state.
const MAX_INFLIGHT: usize = 8 * 1024 * 1024;

/// Bytes one sealed chunk contributes, matching
/// [`ChunkConfig`](spate_core::ops::ChunkConfig)'s default target.
const CHUNK: usize = 64 * 1024;

/// Virtual time one poll iteration takes. Ten iterations therefore span
/// [`MIN_PAUSE`], which is what makes the flapping script's resume land where
/// the fixture says it does.
///
/// A whole number of seconds, and that is load-bearing rather than a round
/// number for readability. The clock's origin is a real [`Instant`], whose
/// sub-second part differs every run; adding a step with a nonzero
/// nanosecond part carries into the seconds field for some origins and not
/// others, and the carry is a branch. Measured at a 50-millisecond step, that
/// alone moved the flapping case by 0.1% between two runs of one binary.
/// Advancing whole seconds leaves the origin's nanoseconds untouched by every
/// addition and every difference the controller takes, and the case becomes
/// bit-identical run to run.
const STEP: Duration = Duration::from_secs(1);

/// Minimum time the controller holds a pause.
///
/// Ten seconds where the configuration default is 500 milliseconds, and the
/// difference is a consequence of [`STEP`] rather than a claim about
/// production. Virtual time has to advance in whole seconds to stay
/// deterministic, so the pause floor can only be a whole number of steps; ten
/// of them is the same *ratio* to the step that the shipped default has to a
/// realistic poll interval, which is what decides how many iterations a pause
/// lasts and so what the script measures.
///
/// The value cannot move the count on its own in any case. `tick` compares
/// against it — `duration_since(since) >= min_pause` — so what a different
/// floor changes is which arm the comparison selects, not the work the
/// comparison does, and the three profiles put every arm under measurement
/// regardless.
const MIN_PAUSE: Duration = Duration::from_secs(10);

/// Iterations one phase of the flapping script holds its side of the
/// hysteresis band. Longer than [`MIN_PAUSE`] in virtual time, so a resume is
/// never blocked by the pause floor — the script is adversarial about the
/// watermarks, not about the timer.
const PHASE: usize = 16;

/// Iterations between rejections in the congested script.
const REJECT_EVERY: usize = 64;

/// Injected clock. The controller reads time through a
/// [`Clock`](spate_core::backpressure::Clock) precisely so it can be driven
/// without sleeping, and the bench takes that seam rather than the monotonic
/// default: `Instant::now` is a libc read costing more than the state machine
/// it would be timing, and counting it would put the case under the C library
/// instead of under this crate. Virtual time advances explicitly, once per
/// iteration, inside the measured loop.
#[derive(Clone)]
struct BenchClock {
    base: Instant,
    offset: Rc<Cell<Duration>>,
}

impl BenchClock {
    fn new() -> Self {
        BenchClock {
            base: Instant::now(),
            offset: Rc::new(Cell::new(Duration::ZERO)),
        }
    }

    fn advance(&self, d: Duration) {
        self.offset.set(self.offset.get() + d);
    }
}

impl Clock for BenchClock {
    fn now(&self) -> Instant {
        self.base + self.offset.get()
    }
}

/// One poll iteration's traffic: what the terminal stage handed the budget,
/// what the sink workers gave back, whether a `try_send` bounced, and what
/// the driver saw of its queues.
struct Step {
    add: usize,
    sub: usize,
    reject: bool,
    queues_below_low: bool,
}

/// Which pressure a script puts the controller under.
#[derive(Clone, Copy)]
pub(crate) enum Profile {
    /// In-flight bytes ride well below the low watermark, queues stay
    /// drained, nothing bounces. Every `tick` takes the `Normal` arm and
    /// returns no transition — the iteration a healthy pipeline runs for
    /// hours at a time.
    Quiet,
    /// In-flight bytes climb past the high watermark and stay there, the
    /// queues never report themselves drained, and a `try_send` bounces
    /// periodically. One transition at the top, then every later `tick`
    /// takes the `Paused` arm and finds a reason not to resume.
    Congested,
    /// The adversary: usage slams from above the high watermark to below the
    /// low one and back, on a phase long enough that the minimum-pause floor
    /// never blocks a resume. Both arms run, and the controller transitions
    /// as often as its hysteresis allows.
    Flapping,
}

impl Profile {
    /// In-flight bytes the script holds at iteration `i`.
    fn usage(self, params: &BackpressureParams, i: usize) -> usize {
        match self {
            // A plateau a megabyte up: fifteen iterations climbing a chunk at
            // a time, then one dropping the whole run back. Always far below
            // the low watermark, so no tick has a reason to pause.
            Profile::Quiet => 1024 * 1024 + (i % 16) * CHUNK,
            Profile::Congested => params.high_bytes + (i % 8 + 1) * CHUNK,
            Profile::Flapping => {
                if high_phase(i) {
                    params.high_bytes + CHUNK
                } else {
                    params.low_bytes / 2
                }
            }
        }
    }

    /// Whether the driver saw every queue below its low-watermark fill.
    fn queues_below_low(self, i: usize) -> bool {
        match self {
            Profile::Quiet => true,
            Profile::Congested => false,
            Profile::Flapping => !high_phase(i),
        }
    }

    /// Whether a `try_send` bounced during iteration `i`.
    fn reject(self, i: usize) -> bool {
        match self {
            Profile::Quiet => false,
            Profile::Congested => i.is_multiple_of(REJECT_EVERY),
            Profile::Flapping => high_phase(i) && i.is_multiple_of(PHASE),
        }
    }
}

/// Whether iteration `i` falls in a flapping script's congested phase.
fn high_phase(i: usize) -> bool {
    (i / PHASE).is_multiple_of(2)
}

/// The controller, its budget, and the script that drives them.
pub(crate) struct Rig {
    controller: WatermarkController<BenchClock>,
    budget: InflightBudget,
    clock: BenchClock,
    script: Vec<Step>,
    /// Transitions the script must produce. Asserted rather than returned
    /// unchecked, so a script that stopped crossing a watermark — and so
    /// stopped exercising the arm its name claims — could not pass as a fast
    /// one.
    pub(crate) expect_transitions: usize,
}

impl Rig {
    /// One drive: every iteration of the script, in order. Returns the number
    /// of transitions the controller reported so a caller can keep the work
    /// observable.
    ///
    /// The shape mirrors a pipeline thread's poll loop: bytes enter the
    /// budget as chunks seal, leave it as sink workers acknowledge, a
    /// rejected `try_send` is recorded rather than waited on, and `tick` is
    /// consulted once per iteration for a pause or resume to apply.
    pub(crate) fn drive(&mut self) -> usize {
        let Rig {
            controller,
            budget,
            clock,
            script,
            ..
        } = self;
        let mut transitions = 0;
        for step in script.iter() {
            budget.add(step.add);
            budget.sub(step.sub);
            if step.reject {
                controller.on_send_rejected();
            }
            clock.advance(STEP);
            if controller.tick(budget, step.queues_below_low).is_some() {
                transitions += 1;
            }
        }
        transitions
    }
}

/// Each profile's script must stay inside the watermark band its name claims,
/// or the case silently stops exercising the arm the bench header attributes
/// to it — the reading, not the transition count, is what selects the branches
/// inside `tick`, and a script that drifted across a watermark would still
/// produce a plausible number.
///
/// Checked in the builder, which gungraun evaluates before it starts
/// collecting, so a drifted script fails loudly instead.
///
/// [`Profile::Flapping`] has no band by construction — it is defined by
/// crossing both watermarks — and asserting one would only restate
/// [`Profile::usage`]. What pins that profile is the transition count the
/// bench asserts: a script that stopped crossing could not produce it.
fn assert_in_band(profile: Profile, params: &BackpressureParams, level: usize) {
    match profile {
        Profile::Quiet => assert!(
            level < params.low_bytes,
            "the quiet script reached {level} bytes, at or above the low \
             watermark of {}, so it is no longer the profile that never has a \
             reason to pause",
            params.low_bytes
        ),
        Profile::Congested => assert!(
            level > params.high_bytes,
            "the congested script fell to {level} bytes, at or below the high \
             watermark of {}, so a tick could reach the drained test's second \
             conjunct — which the bench header says it never does",
            params.high_bytes
        ),
        Profile::Flapping => {}
    }
}

/// A rig driving [`ITERATIONS`] poll iterations under `profile`.
///
/// The script is built here, outside anything measured, from the usage
/// trajectory the profile declares: each entry's `add`/`sub` is whatever
/// moves the budget from the previous iteration's level to this one's, so the
/// budget sees genuine movements of varying size rather than a constant
/// rewritten every step. Both are handed to the budget on every iteration —
/// one of them zero — so the count carries two saturating read-modify-writes
/// per iteration whichever way the reading moved.
pub(crate) fn rig(profile: Profile, expect_transitions: usize) -> Rig {
    let params = BackpressureParams::from_budget(MAX_INFLIGHT, 0.8, 0.5, MIN_PAUSE);
    let mut level = 0usize;
    let script = (0..ITERATIONS)
        .map(|i| {
            let target = profile.usage(&params, i);
            assert_in_band(profile, &params, target);
            let (add, sub) = if target >= level {
                (target - level, 0)
            } else {
                (0, level - target)
            };
            level = target;
            Step {
                add,
                sub,
                reject: profile.reject(i),
                queues_below_low: profile.queues_below_low(i),
            }
        })
        .collect();
    let clock = BenchClock::new();
    Rig {
        controller: WatermarkController::with_clock(params, clock.clone()),
        budget: InflightBudget::new(),
        clock,
        script,
        expect_transitions,
    }
}
