//! Deterministic listing corpora for the planning bench.
//!
//! An instruction count is only comparable if both legs of a comparison
//! packed byte-identical input, so nothing here may vary between runs: the
//! sizes come from a fixed linear congruential generator rather than a random
//! source, and the keys and ETags are derived from the index. That also means
//! no `rand` dependency, which a bench-only corpus does not justify.
//!
//! Sizes are expressed against [`TARGET_BYTES`] so the profiles keep their
//! meaning if the target ever moves: what distinguishes them is where each
//! object sits relative to the split target and to the open-cost floor
//! (`target / 16`), because that is what decides whether it shares a bin.

/// The split target these corpora are packed against. Four times the shipped
/// default, chosen so a bin holds enough members for the open-cost floor and
/// the lookback window to both matter; it is the denominator for every size
/// below rather than a recommendation.
pub(crate) const TARGET_BYTES: u64 = 256 * 1024 * 1024;

/// The open-cost floor `pack` applies: every object costs at least
/// `target / OPEN_COST_DIVISOR`, so a split holds at most ~16 members however
/// small they are. The divisor is `pub(crate)` and cannot be imported from a
/// bench, so it is restated here — `the_profiles_pack_differently` fails if
/// the two ever disagree.
const FLOOR: u64 = TARGET_BYTES / 16;

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
    /// The bounds are checked with `assert!`, not `debug_assert!`: benches
    /// build in the release-derived profile where a debug assertion is
    /// compiled out, and an inverted range would underflow into an enormous
    /// size that silently changes what the profile measures.
    fn range(&mut self, low: u64, high: u64) -> u64 {
        assert!(low <= high, "range low {low} above high {high}");
        let span = high - low;
        assert!(span < u64::MAX, "range spans the whole u64 domain");
        low + self.next() % (span + 1)
    }
}

/// A listing key shaped like a real partitioned prefix, so the digested
/// preimage is a realistic length rather than a two-character stub.
///
/// Monotone in `index`, and that is load-bearing rather than cosmetic: the
/// source sorts the listing by key before packing, so a key whose leading
/// components cycle (a day-of-month, say) would sort into an order unrelated
/// to generation — scattering the runs a profile is built around, and giving
/// `split_id_for`'s per-bin sort a different amount of work than production
/// ever asks of it. The partition components therefore advance with the
/// index instead of cycling.
fn key(index: usize) -> String {
    // The divisors nest: an hour covers 100 parts and a day covers 24 hours,
    // so each component only advances when the one below it wraps. Divisors
    // that do not nest give a key that is not monotone in the index even
    // though every component looks ordered on its own.
    const PER_HOUR: usize = 100;
    const PER_DAY: usize = PER_HOUR * 24;
    let day = index / PER_DAY % 28 + 1;
    let hour = index / PER_HOUR % 24;
    format!("year=2026/month=08/day={day:02}/hour={hour:02}/part-{index:08}.ndjson")
}

/// A quoted 32-character hexadecimal ETag — the shape an S3-compatible store
/// actually reports for a single-part upload, quotes included, which is what
/// the rest of this crate's fixtures use. Length matters: the whole string is
/// digested per member.
///
/// Drawn from its own generator rather than the one sizing objects. Sharing
/// one stream couples the two: a profile whose size closure happened to draw
/// a different number of values would silently re-phase every ETag after it,
/// changing the digest inputs of an unrelated profile.
fn etag(index: usize, lcg: &mut Lcg) -> String {
    format!("\"{:016x}{:016x}\"", lcg.next(), index)
}

fn corpus(
    count: usize,
    seed: u64,
    size: impl Fn(usize, &mut Lcg) -> u64,
) -> Vec<(String, u64, String)> {
    let mut lcg = Lcg::new(seed);
    (0..count)
        .map(|i| {
            let bytes = size(i, &mut lcg);
            (key(i), bytes, etag(i, &mut lcg))
        })
        .collect()
}

/// The ordinary backfill listing: many objects well under the target, each
/// paying the open-cost floor, so bins fill to ~16 members. The denominator
/// every other profile is read against.
pub(crate) fn uniform_small() -> Vec<(String, u64, String)> {
    corpus(10_000, 0x5EED_0001, |_, lcg| {
        lcg.range(FLOOR / 4, FLOOR / 2)
    })
}

/// Objects at or above the split target. Today each closes a bin on its own
/// the moment it is placed, so packing is one bin per object and the digest
/// count equals the listing length — which is the whole reason a very large
/// object bounds worst-case split duration. Subdividing by byte range turns
/// each of these into several members, moving both the input cardinality and
/// the open-bin scan, so this is the profile a subdivision change shows up in.
pub(crate) fn big_objects() -> Vec<(String, u64, String)> {
    corpus(2_000, 0x5EED_0002, |_, lcg| {
        lcg.range(TARGET_BYTES * 4, TARGET_BYTES * 16)
    })
}

/// Long runs of tiny objects interleaved with objects just under the target.
///
/// This is the profile that exercises the open-bin deque. An object at or
/// above the target closes its bin the instant it is placed, so a corpus of
/// tiny-plus-huge leaves the deque holding one bin and the linear
/// `open.iter().position(...)` scan never runs — measured, that shape is
/// indistinguishable from `uniform_small` (deque 1, mean scan 0.94). Sizing
/// the large objects *just below* the target instead leaves each bin open but
/// nearly full, so the deque fills to `PACKING_LOOKBACK` and the scan walks
/// most of it: deque 10, mean scan 5.4, for the same 407 splits.
///
/// That scan is where an implementation splicing subdivided members back into
/// listing order would show first, which is why the profile has to reach it.
pub(crate) fn mixed_tail() -> Vec<(String, u64, String)> {
    corpus(5_000, 0x5EED_0003, |i, lcg| {
        if i % 50 == 49 {
            // Just under the target: enough to nearly fill a bin, not enough
            // to close it.
            lcg.range(TARGET_BYTES - FLOOR + 1, TARGET_BYTES - 1)
        } else {
            lcg.range(1_024, 64 * 1_024)
        }
    })
}
