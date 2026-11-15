//! A seeded SplitMix64 stream, for draws that must replay from a seed: retry
//! jitter, generated data, test corpora. [`fresh_seed`] gives a seed that
//! differs between calls and between processes, for draws that must not
//! line up across replicas.
//!
//! This is not a cryptographic generator and must not be used as one.

use std::sync::atomic::{AtomicU64, Ordering};

/// The Weyl increment: the 64-bit odd integer nearest to 2^64 / φ.
const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// Calls to [`fresh_seed`] in this process.
static SEEDS_TAKEN: AtomicU64 = AtomicU64::new(0);

/// A seed no other call in this process returns, and that another process
/// returns only by chance.
///
/// It is the wall-clock time in nanoseconds, XOR the process id shifted to
/// bit 48, XOR a per-process call count shifted to bit 40. Two calls in one
/// process differ by the count. Two processes collide only when their clocks
/// agree in the low 40 bits, which the count and the process id leave
/// untouched.
///
/// The value is predictable to anyone who knows the time and the process id.
/// It spreads retries apart and separates runs; it is no secret.
///
/// ```
/// use spate_core::rng::fresh_seed;
///
/// assert_ne!(fresh_seed(), fresh_seed());
/// ```
#[must_use]
pub fn fresh_seed() -> u64 {
    seed_from(
        wall_nanos(),
        std::process::id(),
        SEEDS_TAKEN.fetch_add(1, Ordering::Relaxed),
    )
}

/// The wall-clock time in nanoseconds since the Unix epoch, or zero before it.
fn wall_nanos() -> u64 {
    crate::clock::wall_now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// [`fresh_seed`]'s value for a clock reading, a process id and a call count.
const fn seed_from(nanos: u64, pid: u32, n: u64) -> u64 {
    nanos ^ ((pid as u64) << 48) ^ (n << 40)
}

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

    /// Each term lands on bits another term also sets, so an OR or an AND in
    /// place of an XOR, or a dropped term, changes the value.
    #[test]
    fn a_fresh_seed_xors_the_clock_the_pid_and_the_count() {
        assert_eq!(
            seed_from(0xFFFF_FF00_0000_00FF, 0x00F0, 0x0F),
            0xFF0F_F000_0000_00FF
        );
    }

    /// The clock and the process id are what set two processes' seeds apart;
    /// the count alone gives every process the same sequence. The low 40 bits
    /// carry the clock alone, and bits 48 and up carry the process id XOR the
    /// count's high bits.
    #[test]
    fn a_fresh_seed_carries_the_clock_and_the_pid() {
        const LOW_40: u64 = (1 << 40) - 1;
        let before = wall_nanos();
        let seed = fresh_seed();
        let after = wall_nanos();
        let taken = SEEDS_TAKEN.load(Ordering::Relaxed);

        // Wrapping, so neither a carry out of bit 40 nor a clock stepped back
        // between the reads can fail it.
        let window = after.wrapping_sub(before);
        assert!(
            (seed & LOW_40).wrapping_sub(before & LOW_40) & LOW_40 <= window,
            "{seed:#x} does not carry the clock read between {before:#x} and {after:#x}"
        );

        let pid = u64::from(std::process::id()) & 0xFFFF;
        assert!(
            [before, after]
                .iter()
                .any(|t| (((seed ^ t) >> 48) ^ pid) <= taken >> 8),
            "{seed:#x} does not carry pid {pid:#x}"
        );
    }
}
