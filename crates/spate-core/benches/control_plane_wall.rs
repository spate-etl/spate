//! Wall-clock A/B cases for the control plane: the acknowledgement and
//! checkpoint path, and the backpressure poll loop.
//!
//! These run *beside* the per-record path rather than on it. A record crosses
//! the operator chain; a poll batch's acknowledgement crosses the checkpointer
//! once, and the watermark controller is consulted once per poll iteration
//! whether or not anything moved. `chain_wall.rs` and `split_wall.rs` are the
//! per-record half.
//!
//! Two subjects in one target rather than two binaries. Every `_wall` target
//! is compiled on both legs of every comparison whatever `--filter` selects,
//! so a binary is a permanent cost on every future run; the reason the
//! guidance gives for splitting subjects — the metrics recorder is
//! process-global — does not apply to either rig here, because neither
//! registers a metric. The case ids carry the split instead: `--filter ack_`
//! and `--filter poll_` each select one subject.
//!
//! Both rigs live in `benches/support/`, shared with the instruction-count
//! siblings in `checkpoint_gungraun.rs` and `backpressure_gungraun.rs` and
//! pinned by `tests/bench_fixtures.rs`.
//!
//! Run: `make bench-ab REF=main FILTER=ack_` (or `FILTER=poll_`)
//!
//! # Reading these numbers
//!
//! - **`records_per_s` is the figure to read**, and there is deliberately no
//!   `.bytes()` on any case here. Neither rig moves a payload: a schedule
//!   entry is a partition and an offset, and a script step is two integers and
//!   two flags. A byte denominator would be inventing a throughput out of the
//!   size of a struct.
//! - **The poll cases exist for their allocation counters.** A `tick` is tens
//!   of instructions, which the counted tier resolves and no timer can, so the
//!   wall reading of one is not the point. What this tier adds is
//!   `alloc_count_per_iter` — which must be a flat zero, at a 1% floor, on any
//!   machine rather than only where valgrind runs. A poll loop that starts
//!   allocating is the regression these three cases are here to catch.
//! - **The ack thread axis prices a degradation, not a speed-up.** Adding
//!   acking threads makes this workload *slower* on the reference machine —
//!   about 10.1 M batches/s at one thread against 5.4 M at two and 6.1 M at
//!   four — and the axis is worth measuring for exactly that reason. Issuing
//!   is wait-free but not contention-free: a batch carries two sends on
//!   unbounded MPSC channels, an `Arc` allocation, and a `Sender` clone whose
//!   refcount every issuing thread shares, and each of those is a cache line
//!   moving between cores once more than one thread is on it. The single-owner
//!   `Checkpointer::drain` on top of that leaves nothing to amortise it
//!   against. Read the three together: what a change to the ack path moves is
//!   the shape of that curve.
//! - **That curve is not monotonic**, and two threads measure worse than four
//!   on the reference machine. It reproduces closely run to run, so it is a
//!   property of the workload on this hardware rather than noise, but nothing
//!   here explains it and no claim is made about which count is worst in
//!   general.
//! - **`cpu_ns_per_iter` on the threaded cases is occupancy, not work.** The
//!   workers spin while the controller drains, and spinning is CPU time, so
//!   the figure runs at roughly the party count times the wall figure. Both
//!   legs spin identically and the statistic is a mean of per-pair
//!   differences, so the comparison holds — but the metric stops being
//!   independent of wall time there, and wall time is the one to read.
//! - **The state is a `RefCell`** because the harness hands a case's routine
//!   `&S` while both rigs' `drive` takes `&mut self`. One borrow flag per
//!   iteration against a region of hundreds of microseconds, paid identically
//!   by both legs.
//!
//! # What the measured region carries that production does not
//!
//! The poll routine calls [`poll_traffic::Rig::reset`] before each drive,
//! because the script's budget movements are relative and a drive would
//! otherwise leave the next one measuring a different profile — see that
//! method. It is one saturating subtraction, one `Cell` write and one struct
//! write against 32 768 iterations of work.
//!
//! The threaded ack cases carry their rendezvous: two atomic operations per
//! worker per commit tick, plus whatever each side spins through waiting for
//! the other. It is small at this tick width — `ack_wide_1thread` runs through
//! the same rendezvous and lands where a single-threaded drive of the same
//! schedule does — but it is why the thread axis is measured only at the wide
//! width. At the narrow one there are sixteen times as many ticks over the
//! same batches, and the rendezvous would be most of what the case reported.
//!
//! A pipeline does not synchronise this way. Its threads issue continuously
//! and the runtime drains on its commit interval, where these workers are held
//! to a tick boundary so that the drain count, the watermark count and the
//! batches per worker per tick are all fixed rather than decided by the
//! scheduler. The rendezvous is what buys a comparable measurement, and it is
//! also what keeps `drain` on one code path: every worker has reported before
//! the controller drains, so the deferred-retry arm never fires. A controller
//! draining concurrently with issuing threads would take that arm at a rate
//! the machine decides — a second code path inside the region, appearing and
//! disappearing between replicates.
//!
//! `ack_wide_1thread` runs through the threaded driver too, with one worker,
//! rather than through the single-threaded one. Otherwise the step from one
//! thread to two would confound "a second thread appeared" with "the
//! rendezvous appeared", and the axis would not be a scaling curve. It is
//! therefore not comparable in absolute terms with the counted tier's
//! `wide_ticks`; what the two share is the schedule, which the corpus digest
//! carries.

use spate_bench::{Suite, bench_main};
use std::cell::RefCell;

#[path = "support/ack_traffic.rs"]
mod ack_traffic;
#[path = "support/poll_traffic.rs"]
mod poll_traffic;

use ack_traffic::{BATCHES, Order, Threaded};
use poll_traffic::{ITERATIONS, Profile};

/// Batches per commit tick for the wide cases — the tick width the thread
/// axis is measured at, and the counted tier's `wide_ticks`.
const WIDE: usize = 256;

/// Batches per commit tick for the narrow case, sixteen times as many ticks
/// over the same batches, and the counted tier's `narrow_ticks`.
const NARROW: usize = 16;

/// Iterations the ack cases pin rather than calibrate.
///
/// Not a statement about the workload — it is the harness's degenerate-region
/// guard needing room to work. That guard times an empty loop of the case's
/// own iteration count as its floor, and one drive here costs the better part
/// of a millisecond, so calibrating to the default 50 ms target lands on a few
/// dozen iterations. An empty loop that short is a couple of dozen nanoseconds
/// against a clock whose granularity is tens, so the floor reads as zero and
/// the case is refused for being unmeasurable — *intermittently*, since
/// whether it rounds to zero depends on where the read lands. One A/A run
/// reached its second replicate before hitting it.
///
/// Five hundred and twelve puts the reference loop a comfortable multiple
/// above that granularity while keeping a replicate under a second. The
/// guard's real job is unaffected: it exists to catch a routine the optimiser
/// deleted, and one of those would still sit at the floor whatever the count.
const ACK_ITERS: u64 = 512;

/// Transitions each poll profile's script produces.
///
/// Passed to the rig rather than derived by it: a rig that computed its own
/// expectation from its own model of the state machine would agree with
/// itself however the machine changed. `tests/bench_fixtures.rs` holds the
/// same three numbers for the same reason.
const QUIET_TRANSITIONS: usize = 0;
const CONGESTED_TRANSITIONS: usize = 1;
const FLAPPING_TRANSITIONS: usize = 2048;

/// A case driving the whole schedule through a pinned number of acking
/// threads.
///
/// The watermark assertion stays inside the measured region, as the counted
/// tier's does: it is one comparison against 8192 batches of work, and it is
/// what stops a rig that quietly stopped advancing a partition — or a worker
/// that stopped issuing — passing as a fast case. The count is also what
/// `black_box` holds.
fn ack_case(suite: Suite, id: &str, per_tick: usize, order: Order, threads: usize) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                let rig = ack_traffic::threaded(per_tick, order, threads);
                corpus.absorb("schedule", &rig.corpus());
                RefCell::new(rig)
            },
            |b, rig: &RefCell<Threaded>| {
                b.iter(|| {
                    let mut rig = rig.borrow_mut();
                    let watermarks = rig.drive();
                    assert_eq!(watermarks, rig.expect_watermarks);
                    watermarks
                });
            },
        )
        .items(BATCHES as u64)
        .iters(ACK_ITERS)
        .done()
}

/// A case driving the whole poll script under one pressure profile.
///
/// `reset` is inside the region because it is part of what one iteration
/// costs — leaving it out would report a per-iteration figure for work the
/// case does not actually do per iteration. It is three writes.
fn poll_case(suite: Suite, id: &str, profile: Profile, transitions: usize) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                let rig = poll_traffic::rig(profile, transitions);
                corpus.absorb("script", &rig.corpus());
                RefCell::new(rig)
            },
            |b, rig: &RefCell<poll_traffic::Rig>| {
                b.iter(|| {
                    let mut rig = rig.borrow_mut();
                    rig.reset();
                    let produced = rig.drive();
                    assert_eq!(produced, rig.expect_transitions);
                    produced
                });
            },
        )
        .items(ITERATIONS as u64)
        .done()
}

fn suite() -> Suite {
    let suite = spate_bench::suite("spate-core");

    // The tick-width pair, at one thread. `narrow` pays the fixed per-tick
    // cost — `drain`'s two-pass setup, and `take_watermarks` allocating a
    // vector and sweeping every tracker — sixteen times as often over
    // identical per-batch work, so the pair separates the per-tick term from
    // the per-batch one.
    let suite = ack_case(suite, "ack_wide_1thread", WIDE, Order::Issued, 1);
    let suite = ack_case(suite, "ack_narrow_1thread", NARROW, Order::Issued, 1);

    // Resolution order, against `ack_wide_1thread`. A resolution is an indexed
    // slot write rather than a search, so the tracker itself does not care;
    // what the pair prices is the allocator, since resolving out of order
    // frees a batch's shared state out of allocation order.
    let suite = ack_case(suite, "ack_scrambled_1thread", WIDE, Order::Scrambled, 1);

    // The thread axis. Everything but the acking-thread count is held fixed —
    // the same schedule, the same tick width, the same four partitions — so
    // `records_per_s` across these three and `ack_wide_1thread` is the
    // scaling curve, and its ceiling is the serial drain.
    let suite = ack_case(suite, "ack_wide_2threads", WIDE, Order::Issued, 2);
    let suite = ack_case(suite, "ack_wide_4threads", WIDE, Order::Issued, 4);

    // The three pressure profiles. `quiet` never leaves the `Normal` arm,
    // `congested` pauses once and then finds a reason not to resume on every
    // later tick, and `flapping` crosses both watermarks as often as the
    // hysteresis allows. All three must allocate nothing at all.
    let suite = poll_case(suite, "poll_quiet", Profile::Quiet, QUIET_TRANSITIONS);
    let suite = poll_case(
        suite,
        "poll_congested",
        Profile::Congested,
        CONGESTED_TRANSITIONS,
    );
    poll_case(
        suite,
        "poll_flapping",
        Profile::Flapping,
        FLAPPING_TRANSITIONS,
    )
}

bench_main!(suite);
