//! Fingerprints that pin a generated benchmark corpus across revisions.

/// FNV-1a (64-bit) over `bytes`.
///
/// The same input yields the same value on any toolchain, which
/// `DefaultHasher` does not promise.
#[must_use]
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

/// A corpus's length and [`fnv1a`] digest.
///
/// The digest catches a change that keeps the length, such as a re-seeded
/// filler or a reordered field list.
#[must_use]
pub fn pin(bytes: &[u8]) -> (usize, u64) {
    (bytes.len(), fnv1a(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The digest matches the published FNV-1a 64 test vectors.
    #[test]
    fn fnv1a_matches_the_reference_vectors() {
        assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a(b"foobar"), 0x8594_4171_f739_67e8);
    }
}
