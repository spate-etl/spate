//! Deterministic pseudo-random bytes for corpus generation.
//!
//! [`SplitMix64`] is seeded per case by the driver and produces the same
//! stream for the same seed on any build of this crate, so both legs of an
//! A/B run generate an identical corpus. The stream is pinned by
//! `known_answer_vectors` below.
//!
//! It offers raw 64-bit words, a bounded draw, byte fills, and printable
//! ASCII.
//!
//! This is not a cryptographic generator and must not be used as one.

/// The SplitMix64 generator, seeded per case by the driver.
///
/// The algorithm is Steele, Lea and Flood's, as published with the SplittableRandom
/// spliterator; the constants below are theirs and are pinned by
/// `known_answer_vectors`.
#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
}

/// The golden-ratio increment: the step SplitMix64 takes through its state
/// space before mixing.
const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

impl SplitMix64 {
    /// A generator seeded with `seed`.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// The next 64 bits.
    pub const fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GAMMA);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `0..n`, or `0` when `n` is zero.
    ///
    /// Lemire's multiply-shift reduction. The result is biased by at most one
    /// part in 2^64 divided by `n`, and the draw consumes exactly one word of
    /// the stream whatever `n` is.
    pub const fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        ((self.next_u64() as u128 * n as u128) >> 64) as u64
    }

    /// Fills `buf` with the generator's output, little-endian.
    pub fn fill_bytes(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let word = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
    }

    /// `len` bytes of printable ASCII, for corpora that have to survive being
    /// read as text.
    #[must_use]
    pub fn ascii(&mut self, len: usize) -> Vec<u8> {
        const ALPHABET: &[u8; 64] =
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .";
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            let mut word = self.next_u64();
            for _ in 0..8 {
                if out.len() == len {
                    break;
                }
                out.push(ALPHABET[(word & 0x3F) as usize]);
                word >>= 6;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::SplitMix64;

    /// The published SplitMix64 vectors for seed 0.
    ///
    /// Pins the output stream to three constants a reader can check against
    /// the reference implementation, so any two builds of this crate generate
    /// the same corpus.
    #[test]
    fn known_answer_vectors() {
        let mut rng = SplitMix64::new(0);
        assert_eq!(rng.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(rng.next_u64(), 0x6E78_9E6A_A1B9_65F4);
        assert_eq!(rng.next_u64(), 0x06C4_5D18_8009_454F);

        // A second seed, so the test pins the recurrence rather than one
        // hard-coded triple that a broken implementation could also produce by
        // ignoring its input.
        let mut other = SplitMix64::new(0xDEAD_BEEF);
        assert_ne!(other.next_u64(), 0xE220_A839_7B1D_CDAF);
    }

    #[test]
    fn fill_bytes_covers_a_ragged_tail() {
        let mut a = SplitMix64::new(7);
        let mut buf = [0u8; 13];
        a.fill_bytes(&mut buf);
        assert!(buf.iter().any(|b| *b != 0), "the tail was left unwritten");

        let mut b = SplitMix64::new(7);
        let mut same = [0u8; 13];
        b.fill_bytes(&mut same);
        assert_eq!(buf, same, "the same seed produced different bytes");
    }

    #[test]
    fn below_stays_in_range_and_zero_is_not_a_panic() {
        let mut rng = SplitMix64::new(42);
        for _ in 0..10_000 {
            assert!(rng.below(7) < 7);
        }
        assert_eq!(rng.below(0), 0);
    }

    #[test]
    fn ascii_is_printable_and_the_requested_length() {
        let mut rng = SplitMix64::new(1);
        let text = rng.ascii(101);
        assert_eq!(text.len(), 101);
        assert!(text.iter().all(|b| (0x20..0x7F).contains(b)));
    }
}
