//! Hash map and set aliases over seeded fast hashers.
//!
//! Every map in Spate picks one of two hashers. [`FastMap`] and [`FastSet`]
//! are for keys Spate produces: partitions, sequence numbers, metric labels.
//! [`QualityMap`] and [`QualitySet`] are for keys an outside party chooses,
//! such as payload object keys or column values: their hasher adds a final
//! mix that spreads every input bit into the bits the table indexes on.
//!
//! Both states draw a fresh seed per instance, so iteration order differs
//! between maps and between runs. Neither hasher claims resistance against an
//! attacker who can observe timing or iteration order.
//!
//! The aliases are std maps with another hasher, so `new` and `with_capacity`
//! do not exist; build them with `default()`, `collect()` or
//! `with_capacity_and_hasher(n, Default::default())`.
//!
//! # Examples
//!
//! ```
//! use spate_core::hash::{FastMap, QualitySet};
//!
//! let mut offsets: FastMap<u32, i64> = FastMap::default();
//! offsets.insert(0, 42);
//! assert_eq!(offsets.get(&0), Some(&42));
//!
//! let keys: QualitySet<&str> = ["id", "name"].into_iter().collect();
//! assert!(keys.contains("name"));
//! ```

use std::hash::{BuildHasher, Hasher};

/// A [`HashMap`](std::collections::HashMap) for keys Spate produces.
pub type FastMap<K, V> = std::collections::HashMap<K, V, FastState>;

/// A [`HashSet`](std::collections::HashSet) for keys Spate produces.
pub type FastSet<T> = std::collections::HashSet<T, FastState>;

/// A [`HashMap`](std::collections::HashMap) for keys an outside party chooses.
pub type QualityMap<K, V> = std::collections::HashMap<K, V, QualityState>;

/// A [`HashSet`](std::collections::HashSet) for keys an outside party chooses.
pub type QualitySet<T> = std::collections::HashSet<T, QualityState>;

macro_rules! seeded_state {
    ($(#[$state_doc:meta])* $state:ident, $(#[$hasher_doc:meta])* $hasher:ident, $inner:ident) => {
        $(#[$state_doc])*
        #[derive(Clone, Debug, Default)]
        pub struct $state(foldhash::$inner::RandomState);

        impl BuildHasher for $state {
            type Hasher = $hasher;

            #[inline(always)]
            fn build_hasher(&self) -> $hasher {
                $hasher(self.0.build_hasher())
            }
        }

        $(#[$hasher_doc])*
        #[derive(Clone)]
        pub struct $hasher(foldhash::$inner::FoldHasher<'static>);

        impl std::fmt::Debug for $hasher {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($hasher)).finish_non_exhaustive()
            }
        }

        impl Hasher for $hasher {
            #[inline(always)]
            fn write(&mut self, bytes: &[u8]) {
                self.0.write(bytes);
            }

            #[inline(always)]
            fn write_u8(&mut self, i: u8) {
                self.0.write_u8(i);
            }

            #[inline(always)]
            fn write_u16(&mut self, i: u16) {
                self.0.write_u16(i);
            }

            #[inline(always)]
            fn write_u32(&mut self, i: u32) {
                self.0.write_u32(i);
            }

            #[inline(always)]
            fn write_u64(&mut self, i: u64) {
                self.0.write_u64(i);
            }

            #[inline(always)]
            fn write_u128(&mut self, i: u128) {
                self.0.write_u128(i);
            }

            #[inline(always)]
            fn write_usize(&mut self, i: usize) {
                self.0.write_usize(i);
            }

            #[inline(always)]
            fn finish(&self) -> u64 {
                self.0.finish()
            }
        }
    };
}

seeded_state!(
    /// The hasher state behind [`FastMap`] and [`FastSet`], seeded per instance.
    FastState,
    /// The hasher a [`FastState`] builds.
    FastHasher,
    fast
);

seeded_state!(
    /// The hasher state behind [`QualityMap`] and [`QualitySet`], seeded per
    /// instance.
    QualityState,
    /// The hasher a [`QualityState`] builds.
    QualityHasher,
    quality
);

#[cfg(test)]
mod tests {
    use super::*;

    /// Two default states hash one key differently, so neither state is a
    /// fixed seed.
    #[test]
    fn fast_state_seeds_each_instance() {
        let key = 0x5eed_u64;
        assert_ne!(
            FastState::default().hash_one(key),
            FastState::default().hash_one(key)
        );
    }

    /// As [`fast_state_seeds_each_instance`], for the quality state.
    #[test]
    fn quality_state_seeds_each_instance() {
        let key = 0x5eed_u64;
        assert_ne!(
            QualityState::default().hash_one(key),
            QualityState::default().hash_one(key)
        );
    }

    /// Whether `write` with the values 1 and 2 gives two different hashes
    /// under one hasher state.
    fn differs<S: BuildHasher>(state: &S, write: impl Fn(&mut S::Hasher, u8)) -> bool {
        let hash = |v| {
            let mut h = state.build_hasher();
            write(&mut h, v);
            h.finish()
        };
        hash(1) != hash(2)
    }

    /// Two inputs differing only in one write's value hash differently, for
    /// every forwarded write width and both states.
    #[test]
    fn each_write_reaches_the_hash() {
        fn check<S: BuildHasher>(state: &S) {
            assert!(differs(state, |h, v| h.write(&[v; 3])), "write");
            assert!(differs(state, |h, v| h.write_u8(v)), "write_u8");
            assert!(differs(state, |h, v| h.write_u16(v.into())), "write_u16");
            assert!(differs(state, |h, v| h.write_u32(v.into())), "write_u32");
            assert!(differs(state, |h, v| h.write_u64(v.into())), "write_u64");
            assert!(differs(state, |h, v| h.write_u128(v.into())), "write_u128");
            assert!(
                differs(state, |h, v| h.write_usize(v.into())),
                "write_usize"
            );
        }
        check(&FastState::default());
        check(&QualityState::default());
    }
}
