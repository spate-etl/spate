//! Deterministic fleet corpora for the coordination benches.
//!
//! An instruction count is only comparable if both legs of a comparison ran
//! on byte-identical input, so nothing here may vary between runs: the
//! weights come from a fixed linear congruential generator rather than a
//! random source, and every id, owner and status is derived from the index.
//! That also means no `rand` dependency, which a bench-only corpus does not
//! justify.
//!
//! The pool sizes are all far above what this crate's property tests explore
//! (fewer than 14 splits, fewer than 5 members), and deliberately so. Neither
//! function's cost is per byte — both scale with the number of splits and the
//! number of members, and a plan of a few splits counts almost nothing but
//! loop entry. A backfill of an object store runs to single-digit thousands
//! of splits, which is where [`SCAN_SPLITS`] and [`ASSIGN_SPLITS`] sit.
//! [`JOIN_SPLITS`] is smaller for a reason stated on it: the pass it exists
//! to measure is quadratic in the pool, so a realistic pool would put it two
//! orders of magnitude past what a bench should count.
//!
//! `reserved` is empty in every case. It is non-empty only inside a
//! departure's grace window, and the tick that matters for balance is the one
//! *after* the window elapses: while work is withheld the leader is
//! deliberately not moving it.

#![allow(dead_code, reason = "each target uses a different subset")]

use spate_coordination::bench_seams::{ClaimCensus, ObservedSplit};
use std::collections::{BTreeMap, BTreeSet};

/// Members in the fleet. Six is past the point where the improving pass's
/// `O(members^2 x splits-per-member)` scan is a straight line through one
/// member's holdings, which is what a two- or three-member fixture would
/// measure.
pub(crate) const MEMBERS: usize = 6;

/// Splits the balance corpora carry. Divisible by [`MEMBERS`], which is what
/// lets [`settled`] hand every member an identical multiset of weights.
pub(crate) const ASSIGN_SPLITS: usize = 1_200;

/// Splits the corpora carry that let the improving pass run to fixpoint from
/// an unbalanced start.
///
/// Smaller than [`ASSIGN_SPLITS`] because that pass is the one term here that
/// is **quadratic** in the pool: it accepts one move per full
/// `O(members^2 x splits-per-member)` scan, and the number of moves it has to
/// accept is itself proportional to the pool. At `ASSIGN_SPLITS` these
/// profiles count in the hundreds of millions of instructions — an order of
/// magnitude past what the rest of this tier costs, and a bench nobody waits
/// for rather than a more sensitive one. The two profiles that use this share
/// it exactly, so the pair stays readable against each other; neither is
/// comparable with the two that use [`ASSIGN_SPLITS`], and nothing asks them
/// to be.
pub(crate) const JOIN_SPLITS: usize = 180;

/// Splits the claim-scan corpora carry. Larger than [`ASSIGN_SPLITS`] because
/// the scan is one pass plus a sort with no `O(members^2)` term, so it needs
/// a wider pool to count anything.
pub(crate) const SCAN_SPLITS: usize = 10_000;

/// Every member's advertised lane budget. Well above `ASSIGN_SPLITS /
/// MEMBERS` on purpose: a cap that binds turns the balance decision into a
/// truncation and stops measuring the part that scales, so no profile here
/// exercises that path and none claims to. It is far above the shipped
/// `max_in_flight` default for the same reason — what a real worker
/// materialises at once is not what makes this decision expensive.
pub(crate) const LANE_CAP: u32 = 512;

/// Delivery attempts before a takeover parks the split instead.
pub(crate) const MAX_ATTEMPTS: u32 = 4;

/// Splits this worker already holds when it scans.
///
/// A busy worker's in-flight set, and short of its lane budget rather than at
/// it: the reconcile only scans when it is **below** the budget or has a
/// quarantine decision pending, so a worker holding exactly its budget is the
/// one state the walk does not happen in. What matters for the count is that
/// the set is bounded by the budget rather than by the pool — production
/// tests membership against the in-flight map, so a set the size of the pool
/// would measure a lookup the worker never does.
pub(crate) const OWNED: usize = 64;

/// The tie-break seed. A job fingerprint hash in production; any fixed value
/// here, since what it must not be is per-leader.
pub(crate) const SEED: u64 = 0x5ea1_edc0_ffee;

/// A linear congruential generator with Knuth's MMIX constants. Reproducible
/// across platforms and architectures, which `DefaultHasher` and `rand` are
/// explicitly not.
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Lcg {
        Lcg(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    /// A value in `[low, high]`, inclusive.
    ///
    /// Scaled off the **high** bits, not reduced modulo the span. A
    /// power-of-two-modulus LCG's low bits have short periods — bit 0
    /// alternates on every step — so `next() % n` for a small `n` correlates
    /// with the draw index. Two draws per item is enough for that to bite:
    /// every second draw lands on the same parity, which is how a
    /// one-in-ten selector here came out selecting nothing at all. The wide
    /// multiply takes the top 64 bits of `state x span`, which is exact for
    /// any span below `u64::MAX` and uses the bits the generator is good at.
    ///
    /// The bounds are checked with `assert!`, not `debug_assert!`: benches
    /// build in the release-derived profile where a debug assertion is
    /// compiled out, and an inverted range would underflow into an enormous
    /// weight that silently changes what the profile measures.
    fn range(&mut self, low: u64, high: u64) -> u64 {
        assert!(low <= high, "range low {low} above high {high}");
        let span = high - low;
        assert!(span < u64::MAX, "range spans the whole u64 domain");
        let scaled = (u128::from(self.next()) * (u128::from(span) + 1)) >> 64;
        low + scaled as u64
    }
}

/// The instance the claim-scan corpora are scanned as.
pub(crate) const INSTANCE: &str = "spate-worker-0";

/// Member names, shaped like the pod names an instance id is usually derived
/// from rather than as single letters: the balance decision hashes every
/// member name once per plan and compares them inside its `BTreeMap`s, and a
/// one-character name measures neither.
pub(crate) fn member(i: usize) -> String {
    format!("spate-worker-{i}")
}

/// The fleet, as the leader reads it off the presence keys.
pub(crate) fn members(n: usize) -> BTreeSet<String> {
    (0..n).map(member).collect()
}

/// Every member's advertised lane budget, as the leader reads it off the
/// same keys. Passed explicitly rather than left to `default_cap` so the
/// per-member lookup the balance decision does is a hit, which is what a
/// fleet that has finished announcing itself produces.
pub(crate) fn caps(n: usize) -> BTreeMap<String, u32> {
    (0..n).map(|i| (member(i), LANE_CAP)).collect()
}

/// A split id shaped like a real one: a short source tag and a base64url
/// digest, 25 bytes in all.
///
/// The digest matters more than the length. Ids are the keys of the map both
/// functions walk, the sort key the claim scan orders on, and the tie-break
/// preimage the improving pass hashes — and a sequential `split-0001` scheme
/// would share a long prefix between neighbors, so every comparison would
/// run to the last byte in one corpus and diverge on the first in production.
/// These diverge early, as a digest does.
fn split_id(i: usize) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut lcg = Lcg::new(0x51d_0000_0000 ^ i as u64);
    let mut id = String::with_capacity(25);
    id.push_str("s3-");
    for _ in 0..22 {
        // The top six bits, for the reason `range` takes the high half.
        id.push(ALPHABET[(lcg.next() >> 58) as usize] as char);
    }
    id
}

/// How the corpus draws split weights.
#[derive(Clone, Copy)]
pub(crate) enum Weights {
    /// Packed splits either side of a 32 MiB target — the ordinary listing,
    /// and the denominator the skewed profile is read against.
    Packed,
    /// One split in twenty is a single multi-gigabyte object, which an object
    /// at or above the packing target becomes. This is the profile the
    /// improving pass has to work at: a heavy split can only reduce imbalance
    /// by moving to a member far lighter than its own weight, so the
    /// admission test `load(from) > load(to) + weight` rejects far more
    /// candidate moves before the scan finds one it accepts.
    Skewed,
}

impl Weights {
    fn draw(self, lcg: &mut Lcg, i: usize) -> u64 {
        match self {
            Weights::Packed => lcg.range(8 << 20, 40 << 20),
            Weights::Skewed if i.is_multiple_of(20) => lcg.range(1 << 30, 8 << 30),
            Weights::Skewed => lcg.range(1 << 20, 16 << 20),
        }
    }
}

/// A freshly planned pool: every split runnable, spec observed, never owned.
///
/// What the fleet sees on the tick after a leader first publishes a plan —
/// the sticky pass places nothing, so the fill pass does the whole job and
/// the improving pass polishes what greedy left.
pub(crate) fn fresh(n: usize, weights: Weights) -> Vec<ObservedSplit> {
    let mut lcg = Lcg::new(0xf00d_0000_0001);
    (0..n)
        .map(|i| {
            (
                split_id(i),
                Some(weights.draw(&mut lcg, i)),
                "runnable",
                None,
                0,
                0,
                None,
            )
        })
        .collect()
}

/// A settled fleet: every split runnable, held by a live member, and the
/// loads exactly equal.
///
/// Split `i` goes to member `i % MEMBERS` and its weight is drawn per
/// *round* of `MEMBERS` splits, so every member holds one split of each
/// round's weight and therefore the identical multiset. Equal loads make this
/// a fixpoint unconditionally — the improving pass admits a move only when
/// `load(from) > load(to) + weight`, which no pair satisfies at equality —
/// without the corpus having to be computed by the balance decision it is
/// used to measure. A corpus derived from that function's own output would
/// move whenever the function did, and the delta between two revisions would
/// be the difference between two different inputs.
///
/// `tests/bench_fixtures.rs` asserts the fixpoint rather than trusting it.
pub(crate) fn settled(n: usize, weights: Weights) -> Vec<ObservedSplit> {
    assert_eq!(n % MEMBERS, 0, "a settled fleet needs whole rounds");
    let mut lcg = Lcg::new(0xf00d_0000_0002);
    let round_weights: Vec<u64> = (0..n / MEMBERS)
        .map(|round| weights.draw(&mut lcg, round))
        .collect();
    (0..n)
        .map(|i| {
            let owner = member(i % MEMBERS);
            (
                split_id(i),
                Some(round_weights[i / MEMBERS]),
                "runnable",
                Some(owner.clone()),
                1,
                0,
                Some(owner),
            )
        })
        .collect()
}

/// The claim-scan pool for a worker whose peers are all busy: every split it
/// does not hold is under a live foreign lease.
///
/// The scan a worker with lane budget to spare pays on every watch event that
/// reaches it, and gets nothing back for: a live foreign lease is the cheapest
/// rejection there is. The [`OWNED`] splits this worker holds are leased to
/// *it*, so they are rejected one branch earlier still.
pub(crate) fn leased(n: usize) -> Vec<ObservedSplit> {
    (0..n)
        .map(|i| {
            let owner = if i < OWNED {
                INSTANCE.to_string()
            } else {
                member(1 + i % (MEMBERS - 1))
            };
            (
                split_id(i),
                Some(1),
                "runnable",
                Some(owner.clone()),
                1,
                0,
                Some(owner),
            )
        })
        .collect()
}

/// The claim-scan pool for a fleet recovering from a disruption: every claim
/// kind present, terminal splits interleaved, and a slice of the takeovers
/// out of delivery attempts.
///
/// The profile that makes the sort do work. `unclaimed` hands the sort an
/// input already in kind-and-id order — every candidate is a `Create`, and
/// the map is walked in id order — so its comparisons all settle on the id.
/// Here the kinds are interleaved through the id order, so the sort has to
/// move most of what it is given.
pub(crate) fn recovering(n: usize) -> Vec<ObservedSplit> {
    let mut lcg = Lcg::new(0xf00d_0000_0003);
    (0..n)
        .map(|i| {
            let id = split_id(i);
            // Drawn rather than taken from `i` so the kinds interleave with
            // the id order the map is walked in, which is what the sort has
            // to undo.
            let roll = lcg.range(0, 99);
            let attempts = if lcg.range(0, 9) == 0 {
                MAX_ATTEMPTS - 1
            } else {
                0
            };
            if i < OWNED {
                // What this worker already holds, leased to it. `owned`
                // rejects these a branch before the lease is even read, but
                // the corpus has to agree with the set for the two to
                // describe one fleet. Both draws happen above regardless, so
                // changing [`OWNED`] cannot shift the rest of the corpus.
                return (
                    id,
                    Some(1),
                    "runnable",
                    Some(INSTANCE.to_string()),
                    2,
                    0,
                    Some(INSTANCE.to_string()),
                );
            }
            let (weight, status, owner, epoch, lease) = match roll {
                0..=14 => (Some(1), "completed", Some(member(2)), 3, None),
                15..=19 => (Some(1), "quarantined", Some(member(3)), 3, None),
                // Running elsewhere: the live foreign lease, still the
                // majority of any real pool.
                20..=64 => {
                    let owner = member(1 + i % (MEMBERS - 1));
                    (Some(1), "runnable", Some(owner.clone()), 2, Some(owner))
                }
                // The owner died and the lease expired.
                65..=79 => (Some(1), "runnable", Some(member(4)), 2, None),
                // Gracefully released.
                80..=89 => (Some(1), "runnable", None, 2, None),
                // Never owned.
                90..=95 => (Some(1), "runnable", None, 0, None),
                // A restarted predecessor under this worker's own id.
                96..=98 => (
                    Some(1),
                    "runnable",
                    Some(INSTANCE.to_string()),
                    2,
                    Some(INSTANCE.to_string()),
                ),
                // Progress record observed before its spec: quarantinable,
                // not claimable.
                _ => (None, "runnable", Some(member(5)), 2, None),
            };
            (id, weight, status, owner, epoch, attempts, lease)
        })
        .collect()
}

/// How [`recovering`] classifies, as `[create, released, reclaim, expired,
/// quarantined]`.
///
/// Two calls in one process only prove the generator is pure. The property
/// the benches need is stronger — that the corpus is the same *across
/// revisions*, since a merge-base leg and a head leg run different builds.
/// This census is the cheapest witness: any edit to a seed, a roll boundary,
/// a count or [`OWNED`] moves it, and moving it silently would re-baseline
/// every comparison without anything failing. `tests/bench_fixtures.rs`
/// checks it, and the bench asserts it on every run.
pub(crate) const RECOVERING_CENSUS: ClaimCensus = [587, 985, 283, 1290, 189];

/// The splits this worker already holds, drawn from the head of a corpus.
///
/// Bounded by [`OWNED`] rather than by the pool: production tests membership
/// against the in-flight map, which the lane budget caps, so a set the size
/// of the pool would measure a lookup the worker never does.
pub(crate) fn owned(corpus: &[ObservedSplit]) -> BTreeSet<String> {
    corpus
        .iter()
        .take(OWNED)
        .map(|(id, ..)| id.clone())
        .collect()
}
