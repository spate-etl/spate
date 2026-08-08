//! SplitMix64 — the generator's whole source of randomness.
//!
//! Hand-rolled rather than pulled from a crate. A load generator needs a
//! reproducible stream of bits and nothing else: no cryptographic strength, no
//! distributions, no entropy source. Forty lines buy that, and they keep
//! `deny.toml`, `about.toml` and `THIRD-PARTY.md` untouched by this crate —
//! which is the point of a crate whose job is to remove prerequisites.
//!
//! The algorithm is Steele/Lea/Flood's SplitMix64: one additive Weyl step over
//! a 64-bit state, then a finalizing avalanche. Its period is 2^64 and it
//! passes the usual statistical batteries; what matters here is that the same
//! seed yields the same sequence on every platform, forever, because a lane's
//! determinism is a tested property.

/// The Weyl increment: the 64-bit odd integer nearest to 2^64 / φ.
const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// A seeded SplitMix64 stream. One per lane, never shared.
#[derive(Clone, Debug)]
pub(crate) struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// A stream from `seed`. Every seed is valid, including zero.
    pub(crate) fn new(seed: u64) -> SplitMix64 {
        SplitMix64 { state: seed }
    }

    /// The next 64 bits.
    pub(crate) fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GAMMA);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `0..n`. Lemire's multiply-shift reduction: one 64×64→128
    /// multiply instead of a modulo, taking the *high* bits, which are the
    /// well-mixed ones. The residual bias is below 2^-32 for any `n` a
    /// dimension table here could have, and no rejection loop means the cost
    /// of a draw does not depend on the draw.
    ///
    /// `n == 0` yields 0 rather than dividing by zero; no caller passes it.
    pub(crate) fn below(&mut self, n: u32) -> u32 {
        if n == 0 {
            return 0;
        }
        let product = (self.next_u64() >> 32) * u64::from(n);
        (product >> 32) as u32
    }

    /// A value in `lo..=hi`. Panics in debug if `hi < lo`.
    pub(crate) fn between(&mut self, lo: u32, hi: u32) -> u32 {
        debug_assert!(lo <= hi, "empty range {lo}..={hi}");
        lo + self.below(hi - lo + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pinned vectors from the reference implementation. A lane's stream is a
    /// tested property of this crate, so changing the algorithm is a conscious
    /// break, not a refactor.
    #[test]
    fn seed_zero_matches_the_reference_stream() {
        let mut rng = SplitMix64::new(0);
        assert_eq!(rng.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(rng.next_u64(), 0x6E78_9E6A_A1B9_65F4);
        assert_eq!(rng.next_u64(), 0x06C4_5D18_8009_454F);
    }

    #[test]
    fn the_same_seed_replays_and_a_different_one_diverges() {
        let draw = |seed| {
            let mut rng = SplitMix64::new(seed);
            (0..64).map(|_| rng.next_u64()).collect::<Vec<_>>()
        };
        assert_eq!(draw(7), draw(7));
        assert_ne!(draw(7), draw(8));
    }

    #[test]
    fn below_stays_in_range_and_covers_it() {
        let mut rng = SplitMix64::new(42);
        let mut seen = [false; 5];
        for _ in 0..10_000 {
            let v = rng.below(5);
            assert!(v < 5, "below(5) returned {v}");
            seen[v as usize] = true;
        }
        assert!(seen.iter().all(|&s| s), "every value in 0..5 is reachable");
        assert_eq!(rng.below(1), 0, "a single-valued range needs no bits");
        assert_eq!(rng.below(0), 0, "an empty range does not divide by zero");
    }

    #[test]
    fn between_is_inclusive_at_both_ends() {
        let mut rng = SplitMix64::new(99);
        let mut lo_seen = false;
        let mut hi_seen = false;
        for _ in 0..10_000 {
            let v = rng.between(1, 5);
            assert!((1..=5).contains(&v), "between(1, 5) returned {v}");
            lo_seen |= v == 1;
            hi_seen |= v == 5;
        }
        assert!(lo_seen && hi_seen);
        assert_eq!(rng.between(3, 3), 3);
    }
}
