//! A seeded SplitMix64 stream, for draws that must replay from a seed: retry
//! jitter, generated data, test corpora.
//!
//! This is not a cryptographic generator and must not be used as one.

/// The Weyl increment: the 64-bit odd integer nearest to 2^64 / φ.
const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// The SplitMix64 generator: an additive step over a 64-bit state, then a
/// finalizing avalanche.
///
/// The same seed yields the same stream on every platform and build, and every
/// seed is valid, including zero.
///
/// ```
/// use spate_core::rng::SplitMix64;
///
/// let mut a = SplitMix64::new(7);
/// let mut b = SplitMix64::new(7);
/// assert_eq!(a.next_u64(), b.next_u64());
/// ```
// No `Copy`: an implicit copy would fork the stream.
#[derive(Clone, Debug)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// A stream from `seed`.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        SplitMix64 { state: seed }
    }

    /// The next 64 bits.
    #[inline]
    pub const fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GAMMA);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reference implementation's first three outputs for seed 0.
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

    /// The seed whose first draw is zero, which the backoff's xorshift state
    /// replaces.
    #[test]
    fn one_seed_draws_zero_first() {
        assert_eq!(SplitMix64::new(0x61C8_8646_80B5_83EB).next_u64(), 0);
    }
}
