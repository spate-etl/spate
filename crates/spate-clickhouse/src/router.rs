//! Distributed-parity shard routing: place each record on the same shard a
//! ClickHouse `Distributed` table with sharding expression
//! `xxHash64(<key column>)` would.
//!
//! Inserting directly into shard-local tables is this sink's whole design;
//! [`DistributedRouter`] is what lets SELECTs through a `Distributed` table
//! still prune shards (`optimize_skip_unused_shards=1`). It reproduces the
//! engine's placement of `xxHash64(key) % sum(weights)`, with the remainder
//! mapped to consecutive half-open weight intervals in **config order** (weights
//! `[9, 10]` → shard 0 owns `[0, 9)`, shard 1 owns `[9, 19)`).
//!
//! **Ordering contract (documented, not verifiable by the framework):**
//! `shards[i]` in the sink config must be the cluster's `shard_num = i + 1`,
//! the same order as the `remote_servers` entry the Distributed DDL
//! names. Weights must match the cluster's `<weight>`s. The opt-in
//! `distributed_check` config block verifies counts, weights, and the
//! sharding expression at startup; the ordering itself is on the operator.

use spate_core::config::ConfigError;
use spate_core::deser::RecFamily;
use spate_core::record::Record;
use spate_core::sink::RecordRouter;
use std::fmt;
use std::sync::Arc;
use twox_hash::XxHash64;

/// The sharding key extracted from one record. Borrows where possible;
/// integer variants carry the value and are hashed from a stack array, so
/// nothing here allocates per record.
///
/// Integer widths matter: ClickHouse hashes a column at its **declared
/// width** as little-endian bytes, so a `UInt32` column key must use
/// [`ShardKey::U32`]; widening it to `U64` hashes 8 bytes instead of 4 and
/// silently breaks parity. Parity also requires a non-`Nullable` key
/// column: ClickHouse hashes the `Nullable` wrapper differently.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum ShardKey<'a> {
    /// A `String` sharding column: hashed as its UTF-8 bytes.
    Str(&'a str),
    /// Raw bytes (`FixedString`, pre-encoded keys): hashed as-is.
    Bytes(&'a [u8]),
    /// A `UInt64` column: hashed as 8 little-endian bytes. For `Int64`
    /// pass `v as u64`, which gives bit-identical little-endian bytes.
    U64(u64),
    /// A `UInt32`/`DateTime` column: hashed as 4 little-endian bytes. For
    /// `Int32` pass `v as u32`.
    U32(u32),
}

/// Pure per-record key extractor for family `F`. The type is a `fn`
/// pointer, so `fn` items and non-capturing closures both coerce to it:
///
/// ```
/// use spate_clickhouse::{KeyExtractor, ShardKey};
/// use spate_core::deser::Owned;
///
/// struct OrderLineRow {
///     order_id: u64,
/// }
///
/// fn order_key(row: &OrderLineRow) -> ShardKey<'_> {
///     ShardKey::U64(row.order_id)
/// }
///
/// let _: KeyExtractor<Owned<OrderLineRow>> = order_key;
/// let _: KeyExtractor<Owned<OrderLineRow>> = |row| ShardKey::U64(row.order_id);
/// ```
///
/// A capturing closure does not coerce, whatever its signature:
///
/// ```compile_fail,E0308
/// use spate_clickhouse::{KeyExtractor, ShardKey};
/// use spate_core::deser::Owned;
///
/// let fallback: u64 = 0;
/// let _: KeyExtractor<Owned<Vec<u8>>> = |_row| ShardKey::U64(fallback);
/// ```
///
/// The extractor must be **total**: return a key for every record the
/// terminal stage can see, and never panic. Routing has no Skip/Fail
/// error policy (unlike encoding), so a panicking extractor fails the
/// in-flight batch and stops the pipeline, and because restart replays
/// the same record, a payload-dependent panic (say, slicing an empty
/// field) is a deterministic crash loop until a code fix ships. For a
/// malformed or absent key, return a deterministic fallback **in the same
/// variant as the key**, such as `ShardKey::U64(0)` for the extractor above. A
/// fallback of another variant hashes a different domain, so those records
/// land where the server would not put them.
pub type KeyExtractor<F> = for<'a, 'buf> fn(&'a <F as RecFamily>::Rec<'buf>) -> ShardKey<'a>;

/// Routes records to shards exactly as a ClickHouse `Distributed` table
/// with sharding expression `xxHash64(<key column>)` would (see the
/// [module docs](self) for the algorithm and the ordering contract).
///
/// Construct via [`ClickHouseSink::router`](crate::ClickHouseSink::router)
/// so weights come from the validated config and cannot disagree with the
/// endpoint topology; [`DistributedRouter::new`] is the programmatic
/// escape hatch. Cloning is free (a fn pointer and a shared weights
/// table); the terminal stage clones one router per pipeline thread.
pub struct DistributedRouter<F: RecFamily> {
    extract: KeyExtractor<F>,
    /// Exclusive prefix-sum interval ends; `ends[last]` = total weight.
    ends: Arc<[u64]>,
    /// All-weights-1 fast path (`hash % N`), the default config.
    uniform: bool,
}

impl<F: RecFamily> DistributedRouter<F> {
    /// Programmatic constructor: weights in shard-config order, each
    /// `>= 1`. Prefer [`ClickHouseSink::router`](crate::ClickHouseSink::router),
    /// which sources weights from the validated YAML.
    pub fn new(extract: KeyExtractor<F>, weights: &[u32]) -> Result<Self, ConfigError> {
        if weights.is_empty() {
            return Err(ConfigError::Validation(
                "DistributedRouter requires at least one shard weight".into(),
            ));
        }
        if let Some(i) = weights.iter().position(|&w| w == 0) {
            return Err(ConfigError::Validation(format!(
                "DistributedRouter shard {i} has weight 0; ClickHouse weights are \
                 interval widths and must be at least 1"
            )));
        }
        let mut ends = Vec::with_capacity(weights.len());
        let mut acc = 0u64;
        for &w in weights {
            acc += u64::from(w);
            ends.push(acc);
        }
        Ok(DistributedRouter {
            extract,
            ends: ends.into(),
            uniform: weights.iter().all(|&w| w == 1),
        })
    }

    /// Hash a key exactly as ClickHouse's `xxHash64(col)`: canonical XXH64,
    /// seed 0, over the key's canonical bytes (strings as UTF-8, integers
    /// as little-endian fixed-width bytes).
    #[must_use]
    pub fn hash_key(key: ShardKey<'_>) -> u64 {
        match key {
            ShardKey::Str(s) => XxHash64::oneshot(0, s.as_bytes()),
            ShardKey::Bytes(b) => XxHash64::oneshot(0, b),
            ShardKey::U64(v) => XxHash64::oneshot(0, &v.to_le_bytes()),
            ShardKey::U32(v) => XxHash64::oneshot(0, &v.to_le_bytes()),
        }
    }

    /// Weight-interval selection for a precomputed hash: `hash % total`
    /// mapped onto consecutive half-open intervals in config order.
    #[must_use]
    pub fn shard_for_hash(&self, hash: u64) -> usize {
        let total = *self.ends.last().expect("non-empty by construction");
        let r = hash % total;
        if self.uniform {
            // All weights 1: total == N and the scan is the identity.
            return usize::try_from(r).expect("r < num_shards");
        }
        // Shard counts are small; a branch-predictable scan over one cache
        // line of u64 interval ends beats a binary search at this size.
        self.ends
            .iter()
            .position(|&end| r < end)
            .expect("r < total by construction")
    }

    /// The shard count this router was constructed for.
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.ends.len()
    }
}

impl<F: RecFamily> RecordRouter<F> for DistributedRouter<F> {
    #[inline]
    fn route_record<'buf>(&self, rec: &Record<F::Rec<'buf>>, num_shards: usize) -> usize {
        // Silent misrouting is the failure this router prevents;
        // constructing via `ClickHouseSink::router` makes this unreachable.
        assert_eq!(
            num_shards,
            self.ends.len(),
            "DistributedRouter was built for {} shard(s) but the sink has {num_shards}; \
             construct it via ClickHouseSink::router so the topologies cannot drift",
            self.ends.len()
        );
        self.shard_for_hash(Self::hash_key((self.extract)(&rec.payload)))
    }
}

// Manual impls: derives would demand `F: Clone`/`F: Debug`, and family
// markers are bare unit structs.
impl<F: RecFamily> Clone for DistributedRouter<F> {
    fn clone(&self) -> Self {
        DistributedRouter {
            extract: self.extract,
            ends: Arc::clone(&self.ends),
            uniform: self.uniform,
        }
    }
}

impl<F: RecFamily> fmt::Debug for DistributedRouter<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DistributedRouter")
            .field("ends", &self.ends)
            .field("uniform", &self.uniform)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spate_core::checkpoint::AckRef;
    use spate_core::deser::Owned;
    use spate_core::record::{PartitionId, RecordMeta};

    /// The owned family every test routes over.
    type OwnedBytes = Owned<Vec<u8>>;

    // `&Vec<u8>` (not `&[u8]`) is forced: the fn must coerce to
    // `KeyExtractor<Owned<Vec<u8>>>`, whose argument is `&'a Rec<'buf>`.
    #[allow(clippy::ptr_arg)]
    fn first_byte_key(rec: &Vec<u8>) -> ShardKey<'_> {
        ShardKey::Bytes(&rec[..1])
    }

    fn record(payload: Vec<u8>) -> Record<Vec<u8>> {
        let (ack, _rx) = AckRef::test_pair();
        Record {
            payload,
            meta: RecordMeta {
                partition: PartitionId(0),
                offset: 0,
                event_time_ms: 0,
                key_hash: None,
            },
            ack,
        }
    }

    /// A borrowing family: the record holds a key borrowed from the payload
    /// buffer, and an extractor over it returns a key borrowed from the
    /// record it is given.
    struct Ev<'a> {
        key: &'a str,
    }
    struct EvFam;
    impl RecFamily for EvFam {
        type Rec<'buf> = Ev<'buf>;
    }

    fn ev_record(key: &str) -> Record<Ev<'_>> {
        let (ack, _rx) = AckRef::test_pair();
        Record {
            payload: Ev { key },
            meta: RecordMeta {
                partition: PartitionId(0),
                offset: 0,
                event_time_ms: 0,
                key_hash: None,
            },
            ack,
        }
    }

    /// `xxHash64("abc")`, the key hash the borrowed-family tests route on.
    const ABC_HASH: u64 = 0x44BC_2CF5_AD77_0999;

    #[test]
    fn equal_weights_reduce_to_hash_modulo_shard_count() {
        let router = DistributedRouter::<OwnedBytes>::new(first_byte_key, &[1, 1, 1, 1]).unwrap();
        for h in [0u64, 1, 3, 4, 17, u64::MAX, 0xEF46_DB37_51D8_E999] {
            assert_eq!(router.shard_for_hash(h), (h % 4) as usize, "hash {h}");
        }
    }

    #[test]
    fn weight_intervals_match_the_distributed_nine_ten_example() {
        // The ClickHouse docs' example: weights 9 and 10, with remainders
        // [0, 9) belonging to the first shard and [9, 19) to the second.
        let router = DistributedRouter::<OwnedBytes>::new(first_byte_key, &[9, 10]).unwrap();
        for r in 0..9u64 {
            assert_eq!(router.shard_for_hash(r), 0, "remainder {r}");
        }
        for r in 9..19u64 {
            assert_eq!(router.shard_for_hash(r), 1, "remainder {r}");
        }
        // And the remainder wraps at the total weight.
        assert_eq!(router.shard_for_hash(19), 0);
    }

    #[test]
    fn hash_key_matches_known_xxh64_vectors() {
        // Reference XXH64 seed-0 vectors, confirmed against a live
        // ClickHouse 26.3 server (`SELECT hex(xxHash64('abc')), ...`);
        // the container suite re-pins them against the server.
        assert_eq!(
            DistributedRouter::<OwnedBytes>::hash_key(ShardKey::Str("")),
            0xEF46_DB37_51D8_E999
        );
        assert_eq!(
            DistributedRouter::<OwnedBytes>::hash_key(ShardKey::Str("abc")),
            0x44BC_2CF5_AD77_0999
        );
        assert_eq!(
            DistributedRouter::<OwnedBytes>::hash_key(ShardKey::Bytes(b"abc")),
            DistributedRouter::<OwnedBytes>::hash_key(ShardKey::Str("abc")),
            "Str and Bytes hash identically for the same bytes"
        );
    }

    #[test]
    fn u64_and_u32_keys_hash_their_little_endian_bytes() {
        // The absolute values are `SELECT hex(xxHash64(toUInt64(42)))` /
        // `toUInt32(42)` from a live ClickHouse 26.3 server, pinning the
        // little-endian fixed-width byte interpretation rather than internal
        // consistency alone.
        assert_eq!(
            DistributedRouter::<OwnedBytes>::hash_key(ShardKey::U64(42)),
            0xB556_806F_B6D1_4353,
        );
        assert_eq!(
            DistributedRouter::<OwnedBytes>::hash_key(ShardKey::U32(42)),
            0xD756_D7B6_2FC5_0BF1,
        );
        assert_eq!(
            DistributedRouter::<OwnedBytes>::hash_key(ShardKey::U64(42)),
            DistributedRouter::<OwnedBytes>::hash_key(ShardKey::Bytes(&42u64.to_le_bytes())),
        );
        assert_ne!(
            DistributedRouter::<OwnedBytes>::hash_key(ShardKey::U32(42)),
            DistributedRouter::<OwnedBytes>::hash_key(ShardKey::U64(42)),
            "the declared column width is part of the hash input"
        );
    }

    #[test]
    fn empty_string_keys_hash_and_route_without_panicking() {
        fn empty_key(_rec: &Vec<u8>) -> ShardKey<'_> {
            ShardKey::Str("")
        }
        let router = DistributedRouter::<OwnedBytes>::new(empty_key, &[1, 1]).unwrap();
        let rec = record(vec![7]);
        let shard = RecordRouter::route_record(&router, &rec, 2);
        assert_eq!(shard, (0xEF46_DB37_51D8_E999u64 % 2) as usize);
    }

    #[test]
    fn new_rejects_zero_weights_and_empty_weight_lists() {
        let empty = DistributedRouter::<OwnedBytes>::new(first_byte_key, &[]);
        assert!(empty.is_err(), "empty weights must be rejected");
        let zero = DistributedRouter::<OwnedBytes>::new(first_byte_key, &[1, 0, 1]);
        let msg = zero.unwrap_err().to_string();
        assert!(msg.contains("shard 1"), "names the offending shard: {msg}");
    }

    #[test]
    #[should_panic(expected = "built for 2 shard(s) but the sink has 3")]
    fn route_panics_when_topology_disagrees_with_construction() {
        let router = DistributedRouter::<OwnedBytes>::new(first_byte_key, &[1, 1]).unwrap();
        let rec = record(vec![7]);
        let _ = RecordRouter::route_record(&router, &rec, 3);
    }

    #[test]
    fn borrowed_fn_item_extractor_coerces_and_routes() {
        fn key_of<'a>(rec: &'a Ev<'_>) -> ShardKey<'a> {
            ShardKey::Str(rec.key)
        }

        let router = DistributedRouter::<EvFam>::new(key_of, &[1, 1]).unwrap();
        let rec = ev_record("abc");
        assert_eq!(
            RecordRouter::route_record(&router, &rec, 2),
            (ABC_HASH % 2) as usize,
        );
    }

    #[test]
    fn borrowed_non_capturing_closure_extractor_coerces_and_routes() {
        // The argument type is left to inference, so this covers inference
        // over the `Rec<'buf>` GAT as well as the coercion.
        let extract: KeyExtractor<EvFam> = |rec| ShardKey::Str(rec.key);

        let router = DistributedRouter::<EvFam>::new(extract, &[1, 1]).unwrap();
        let rec = ev_record("abc");
        assert_eq!(
            RecordRouter::route_record(&router, &rec, 2),
            (ABC_HASH % 2) as usize,
        );
    }
}
