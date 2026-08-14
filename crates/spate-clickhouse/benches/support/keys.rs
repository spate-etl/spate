//! Sharding-key corpora for the routing bench.
//!
//! Routing is the one piece of this sink that runs on literally every record
//! before anything is batched, and its cost is dominated by one `xxHash64`
//! over the key's canonical bytes. Two things about the key therefore decide
//! the cost, and both are parameters here.
//!
//! **Length**, because XXH64 changes shape at 32 bytes: below it the digest
//! is a single accumulator seeded from a prime, at or above it four lanes are
//! initialized, striped over, and merged. `SHORT_LEN` sits in the first
//! regime and `LONG_LEN` in the second, so a change that helps one and hurts
//! the other cannot hide in an average.
//!
//! **Width**, because ClickHouse hashes an integer column at its *declared*
//! width as little-endian bytes: a `UInt32` key hashes four bytes and a
//! `UInt64` eight, and passing the wrong one silently breaks Distributed
//! parity. The two integer corpora are separate for the same reason the two
//! string lengths are.
//!
//! Key *content* changes no instruction count, since XXH64's work is a
//! function of length alone, but it does decide which shard each record lands
//! on, and so
//! how far the weighted interval scan walks before it exits. That is why the
//! bytes come from a fixed generator rather than from the index directly: a
//! corpus whose keys differ only in a trailing digit still hashes uniformly,
//! but nothing here should depend on believing that.

/// Records per routing case. A single short-key route is tens of
/// nanoseconds, so the measured region has to be a realistic run of records
/// rather than one call, which is also what the sink's terminal stage does
/// between seals. It is the same reasoning as the encoder's block size, at
/// the scale routing actually operates on.
pub(crate) const KEYS: usize = 100_000;

/// A `String` key below XXH64's 32-byte threshold: the single-accumulator
/// regime, and the length a tenant or session identifier has.
pub(crate) const SHORT_LEN: usize = 8;

/// A `String` key above it: two full 32-byte stripes through the four-lane
/// regime, the length a composite path-shaped key reaches.
pub(crate) const LONG_LEN: usize = 64;

/// A `Bytes` key: the width of a pre-encoded binary identifier, and what a
/// `FixedString(16)` sharding column hands the router.
pub(crate) const BLOB_LEN: usize = 16;

/// Shards in every routing case. Held constant across the weight cases so
/// the only thing that differs between them is which branch of
/// `shard_for_hash` runs.
pub(crate) const SHARDS: usize = 8;

/// The default cluster: every shard weight 1, which is the `hash % N` fast
/// path.
pub(crate) const UNIFORM: [u32; SHARDS] = [1; SHARDS];

/// A cluster of two hardware generations, four nodes each. The weights sum
/// to 30 and the heavy shards sit late in config order, so the interval scan
/// walks a little over five entries on average, enough for its cost to be
/// visible against the uniform case rather than lost in the hash.
pub(crate) const TIERED: [u32; SHARDS] = [1, 2, 4, 8, 1, 2, 4, 8];

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
}

/// `n` keys of exactly [`SHORT_LEN`] bytes: a `u32` of entropy rendered as
/// hex, which is eight digits wide whatever the value.
pub(crate) fn short_strings(n: usize) -> Vec<String> {
    let mut lcg = Lcg::new(0x5EED_0001);
    (0..n)
        .map(|_| format!("{:0width$x}", lcg.next() as u32, width = SHORT_LEN))
        .collect()
}

/// `n` keys of exactly [`LONG_LEN`] bytes, shaped like the composite
/// tenant/device/stream key a multi-tenant ingest shards on.
///
/// The length is checked rather than assumed, and with `assert!` rather than
/// `debug_assert!`: a bench builds in the release-derived profile, where a
/// debug assertion is compiled out, and a literal edited by one character
/// would move this corpus into the other XXH64 regime while still producing a
/// number.
pub(crate) fn long_strings(n: usize) -> Vec<String> {
    let mut lcg = Lcg::new(0x5EED_0002);
    let keys: Vec<String> = (0..n)
        .map(|_| {
            format!(
                "tenant-{:08x}/device-{:016x}/streams-{:016x}",
                lcg.next() as u32,
                lcg.next(),
                lcg.next()
            )
        })
        .collect();
    assert!(
        keys.iter().all(|k| k.len() == LONG_LEN),
        "a long key is not {LONG_LEN} bytes"
    );
    keys
}

/// `n` keys of exactly [`BLOB_LEN`] raw bytes.
pub(crate) fn blobs(n: usize) -> Vec<Vec<u8>> {
    let mut lcg = Lcg::new(0x5EED_0003);
    (0..n)
        .map(|_| {
            let mut v = Vec::with_capacity(BLOB_LEN);
            v.extend_from_slice(&lcg.next().to_le_bytes());
            v.extend_from_slice(&lcg.next().to_le_bytes());
            v
        })
        .collect()
}

/// `n` `UInt64` column keys.
pub(crate) fn u64s(n: usize) -> Vec<u64> {
    let mut lcg = Lcg::new(0x5EED_0004);
    (0..n).map(|_| lcg.next()).collect()
}

/// `n` `UInt32` column keys.
pub(crate) fn u32s(n: usize) -> Vec<u32> {
    let mut lcg = Lcg::new(0x5EED_0005);
    (0..n).map(|_| lcg.next() as u32).collect()
}
