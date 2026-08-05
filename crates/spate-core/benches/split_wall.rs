//! Wall-clock A/B cases for the multi-sink split terminal.
//!
//! One shape — a poll batch deserialized straight into a
//! [`split`](spate_core::ops::ChainBuilder::split) terminal and dispatched to
//! typed branches, drained to encoded chunks — parameterised by the two things
//! the terminal varies: how many branches it fans out to, and what share of
//! records match one. This is where the split's per-record cost lives: an
//! `Any` downcast to discriminate the branch, an `AckRef` clone, and a boxed
//! router and encoder call each.
//!
//! The rig lives in `benches/support/split_rig.rs`, shared with the
//! instruction-count sibling in `benches/split_gungraun.rs` and pinned by
//! `tests/bench_fixtures.rs`.
//!
//! Run: `make bench-ab REF=main FILTER=split_`
//!
//! # Reading these numbers
//!
//! - **`bytes_per_s` is the figure comparable across all three cases.** Every
//!   corpus here is the same number of bytes — the tag byte changes which route
//!   arm runs, never the payload width — so the byte denominator is one
//!   constant and the cases differ only in the work behind it.
//! - **`records_per_s` legitimately drops by a quarter** in
//!   `split_four_branches_quarter_unrouted`, because a quarter of the batch
//!   reaches no branch and produces no row. That is the axis, not a
//!   regression: the case prices a route closure that falls through against one
//!   that always matches.
//! - **Both four-branch cases are marked erratic**, so they are measured and
//!   reported but never reach the significant-changes table — see
//!   [`FOUR_BRANCH_ERRATIC`]. They place occasional replicates well above their
//!   own mode on the reference machine, enough to flag a change that never
//!   happened, and nothing in the measured region accounts for it. Read their
//!   rows; do not gate on them. `split_two_branches` is the case here that
//!   gates.
//! - **The counts come from the rig, not from a literal.** `.items_of()` reads
//!   `Rig::expect_rows`. That is a second statement of the tag distribution
//!   rather than a derivation from it — `Tags::routed` is arithmetic written
//!   beside the cycle, not read off it — so the two are tied together
//!   elsewhere: the routine asserts the driven count against it every
//!   iteration, and `tests/bench_fixtures.rs` counts the corpus's tag bytes
//!   against both.
//! - **The state is a `RefCell`** because the harness hands a case's routine
//!   `&S` while `Rig::drive` takes `&mut self`. One borrow flag per iteration
//!   against a region of hundreds of microseconds, paid identically by both
//!   legs.
//!
//! # What the measured region carries that production does not
//!
//! `Rig::drive` mints a fresh `AckRef::test_pair()` and sweeps every branch's
//! receiver inside the region — in production a source owns the first and a
//! shard worker the second. The sweep grows with the branch count whether or
//! not a branch is idle, which is why the four-branch cases are read against
//! each other and against the two-branch case rather than in absolute terms.
//!
//! Nothing returns bytes to the `InflightBudget`, so it climbs across a
//! calibrated run. It stays cost-neutral: the seal path's `add` is one
//! value-independent atomic and nothing in the rig reads `usage()`.

use spate_bench::{Suite, bench_main};
use std::cell::RefCell;

#[path = "support/split_rig.rs"]
mod split_rig;

use split_rig::{Rig, Tags, four_branch_rig, two_branch_rig};

/// Fold a rig's corpus into the harness's digest.
fn absorb(corpus: &mut spate_bench::Corpus, rig: &Rig) {
    corpus.absorb("payloads", &rig.corpus().concat());
}

/// Why both four-branch cases are reported but never flagged.
///
/// Established across five A/A runs — one commit on both legs, corpus digests
/// matched, allocation counters identical to the byte. Both four-branch cases
/// hold a ~163 µs mode and then place occasional replicates at ~200-208 µs,
/// one to three in ten, which is enough to clear the 5% floor whenever two or
/// more land together. It moved between the two cases run to run rather than
/// staying with either, so it is a property they share and not of one corpus.
///
/// It is not the machine drifting under everything: `split_two_branches` held
/// to 1.6% and the ten chain cases to 1% in those same interleaved runs. And it
/// is not something the measured region does — the route closure's
/// fall-through arm drops a `Record` and releases one `AckRef` clone, a
/// decrement rather than a branch, and the corpus and chunk arithmetic are
/// pinned by `tests/bench_fixtures.rs`. What separates these two from the
/// two-branch case is that their per-branch buffers stay under the 64 KiB chunk
/// target, so a chunk seals only at `flush`; the cause beyond that is
/// unexplained.
///
/// This records an observation on one machine, not a diagnosis. It is worth
/// re-testing on dedicated hardware before the marking is lifted.
const FOUR_BRANCH_ERRATIC: &str = "occasional replicates land about 25% above the case's mode on \
     the reference machine; the two-branch and chain cases do not";

/// Rows a case's batch produces, as the rig itself computes it.
fn rows(rig: &RefCell<Rig>) -> u64 {
    rig.borrow().expect_rows as u64
}

/// Payload bytes a case's batch ingests.
///
/// Summed from the corpus rather than stated as a constant, so a payload shape
/// that drifted moves the denominator with it instead of leaving `bytes_per_s`
/// describing a corpus that no longer exists. `tests/bench_fixtures.rs` is what
/// pins the shape itself, and pins that all three corpora are the same size —
/// which is what makes this denominator one constant across the cases.
fn bytes(rig: &RefCell<Rig>) -> u64 {
    rig.borrow().corpus().iter().map(Vec::len).sum::<usize>() as u64
}

/// A case that drives a whole batch through a built split chain.
///
/// The row assertion stays inside the measured region, as the counted tier's
/// does: it is one comparison against thousands of records of work, and it is
/// what stops a corpus that quietly stopped reaching a branch passing as a fast
/// case. The count is also what `black_box` holds.
fn split_case(suite: Suite, id: &str, build: fn() -> Rig, erratic: Option<&str>) -> Suite {
    let case = suite
        .case(
            id,
            move |corpus, _seed| {
                let rig = build();
                absorb(corpus, &rig);
                RefCell::new(rig)
            },
            |b, rig| {
                b.iter(|| {
                    let mut rig = rig.borrow_mut();
                    let produced = rig.drive();
                    assert_eq!(produced, rig.expect_rows);
                    produced
                });
            },
        )
        .items_of(rows)
        .bytes_of(bytes);
    match erratic {
        Some(why) => case.erratic(why).done(),
        None => case.done(),
    }
}

fn suite() -> Suite {
    let suite = spate_bench::suite("spate-core");

    // Every record matches a branch. The two-branch case is the baseline the
    // four-branch one is read against: same records, same bytes, twice the
    // branches for the downcast to discriminate between and twice the buffers,
    // encoders and `AckSet`s the terminal carries.
    let suite = split_case(suite, "split_two_branches", two_branch_rig, None);
    let suite = split_case(
        suite,
        "split_four_branches",
        || four_branch_rig(Tags::FourBranches),
        Some(FOUR_BRANCH_ERRATIC),
    );

    // Four branches at a three-quarter hit rate. The unrouted payloads are
    // spread across the tag cycle rather than aimed at one branch, so all four
    // branches still receive traffic — this is a match-rate case, not a
    // three-branch one.
    split_case(
        suite,
        "split_four_branches_quarter_unrouted",
        || four_branch_rig(Tags::FourBranchesQuarterUnrouted),
        Some(FOUR_BRANCH_ERRATIC),
    )
}

bench_main!(suite);
