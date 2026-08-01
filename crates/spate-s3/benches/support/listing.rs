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

/// The split target these corpora are packed against — a plausible
/// production value, and the denominator for every size below.
pub(crate) const TARGET_BYTES: u64 = 256 * 1024 * 1024;

/// The open-cost floor `pack` applies: every object costs at least this, so
/// a split holds at most ~16 members however small they are.
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
    fn range(&mut self, low: u64, high: u64) -> u64 {
        debug_assert!(low <= high);
        low + self.next() % (high - low + 1)
    }
}

/// A listing key shaped like a real partitioned prefix, so the digested
/// preimage is a realistic length rather than a two-character stub.
fn key(index: usize) -> String {
    let day = index % 28 + 1;
    let hour = index % 24;
    format!("year=2026/month=08/day={day:02}/hour={hour:02}/part-{index:06}.ndjson")
}

/// A 32-character hexadecimal ETag, the shape an S3-compatible store reports
/// for a single-part upload. Length matters: it is digested per member.
fn etag(index: usize, lcg: &mut Lcg) -> String {
    format!("{:016x}{:016x}", lcg.next(), index)
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

/// Long runs of tiny objects interleaved with occasional large ones. The
/// tiny runs keep the open-bin deque saturated at `PACKING_LOOKBACK`, so the
/// linear `open.iter().position(...)` scan runs at its worst case on every
/// placement — where an implementation that splices subdivided members back
/// into listing order would show first.
pub(crate) fn mixed_tail() -> Vec<(String, u64, String)> {
    corpus(5_000, 0x5EED_0003, |i, lcg| {
        if i % 50 == 49 {
            lcg.range(TARGET_BYTES, TARGET_BYTES * 2)
        } else {
            lcg.range(1_024, 64 * 1_024)
        }
    })
}
