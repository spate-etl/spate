//! Draws over [`SplitMix64`], the generator's whole source of randomness.
//!
//! A lane holds one stream and never shares it, so a lane's output is a
//! function of its seed alone. ADR-0042 records why this crate takes no
//! third-party generator.
//!
//! This is not a cryptographic generator and must not be used as one.

pub(crate) use spate_core::rng::SplitMix64;

/// A value in `0..n`, or `0` when `n` is zero.
///
/// Lemire's multiply-shift reduction over the high 32 bits of the stream.
/// The residual bias is below 2^-32 for any `n` a dimension table here
/// could have, and a draw costs the same whatever it returns.
pub(crate) fn below(rng: &mut SplitMix64, n: u32) -> u32 {
    if n == 0 {
        return 0;
    }
    let product = (rng.next_u64() >> 32) * u64::from(n);
    (product >> 32) as u32
}

/// A value in `lo..=hi`. Panics in debug if `hi < lo`.
pub(crate) fn between(rng: &mut SplitMix64, lo: u32, hi: u32) -> u32 {
    debug_assert!(lo <= hi, "empty range {lo}..={hi}");
    lo + below(rng, hi - lo + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn below_stays_in_range_and_covers_it() {
        let mut rng = SplitMix64::new(42);
        let mut seen = [false; 5];
        for _ in 0..10_000 {
            let v = below(&mut rng, 5);
            assert!(v < 5, "below(5) returned {v}");
            seen[v as usize] = true;
        }
        assert!(seen.iter().all(|&s| s), "every value in 0..5 is reachable");
        assert_eq!(below(&mut rng, 1), 0, "a single-valued range needs no bits");
        assert_eq!(
            below(&mut rng, 0),
            0,
            "an empty range does not divide by zero"
        );
    }

    #[test]
    fn between_is_inclusive_at_both_ends() {
        let mut rng = SplitMix64::new(99);
        let mut lo_seen = false;
        let mut hi_seen = false;
        for _ in 0..10_000 {
            let v = between(&mut rng, 1, 5);
            assert!((1..=5).contains(&v), "between(1, 5) returned {v}");
            lo_seen |= v == 1;
            hi_seen |= v == 5;
        }
        assert!(lo_seen && hi_seen);
        assert_eq!(between(&mut rng, 3, 3), 3);
    }
}
