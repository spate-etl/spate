//! Wall-clock A/B cases for the operator-chain hot path.
//!
//! Measures the production path: `Box<dyn RunnableChain>` fed borrowed payload
//! batches through deserialize → filter → flat_map → encode → handoff, against
//! the owned-payload equivalent. The boundary is one virtual call per batch by
//! construction; what that contrast establishes is recorded in ADR-0013, the
//! zero-copy seam.
//!
//! Beyond that contrast, the borrowed rig sweeps the three parameters the
//! terminal stage varies in production: the router (a constant stub against the
//! production key-hash router over a keyed corpus), the shard count, and the
//! chunk target that decides how often a chunk seals mid-batch. Wall time is
//! cheap enough to carry the interior points of both sweeps; the
//! instruction-count sibling in `benches/chain_gungraun.rs` takes only their
//! endpoints. The last two cases are the microscopic floor the router sweep is
//! read against — the hash and the modulo on their own, with no chain around
//! them.
//!
//! The rigs live in `benches/support/chain_rig.rs`, shared with the counted
//! tier and pinned by `tests/bench_fixtures.rs`.
//!
//! Run: `make bench-ab REF=main FILTER=chain_`
//!
//! # Reading these numbers
//!
//! - **No wall-clock figure is comparable between `chain_borrowed` and
//!   `chain_owned`.** The two arms fan out differently — the borrowed one's
//!   `flat_map` emits three sub-records per payload against the owned one's
//!   one — so neither `records_per_s` nor `bytes_per_s` is like-for-like, and a
//!   ratio between them is not a quantity this rig measures. The shared input
//!   denominator makes `bytes_per_s` read as though it were; it is not. What
//!   the pair establishes is the **allocation** contrast — a fixed handful per
//!   batch against one per record — which is what ADR-0013 records the
//!   zero-copy seam on. Each arm is read against its own history across two
//!   builds, which is all an A/B comparison ever claims.
//! - **The keyed arms declare payload bytes only**, not payload plus key.
//!   Folding the keys in would give those arms a 21% larger numerator and
//!   flatten routing in exactly the cases that exist to price it. The
//!   denominator is what the chain ingests as *payload*, held fixed across
//!   every case here so the sweeps move against one constant.
//! - **The state is a `RefCell`** because the harness hands a case's routine
//!   `&S` while `Rig::drive` takes `&mut self`. One borrow flag per iteration
//!   against a region of tens of microseconds, paid identically by both legs.
//!
//! # What the measured region carries that production does not
//!
//! `Rig::drive` mints a fresh `AckRef::test_pair()` and sweeps every shard
//! receiver inside the region — in production a source owns the first and a
//! shard worker the second. Both legs pay it, so it sits in the baseline rather
//! than in the difference, but it is why a shard-count case is read against its
//! own single-shard sibling rather than in absolute terms.
//!
//! Nothing returns bytes to the `InflightBudget`, so it climbs across a
//! calibrated run — further than it ever did under the retired weekly target,
//! which drove far fewer iterations. It stays cost-neutral: the seal path's
//! `add` is one value-independent atomic on a bare `AtomicUsize` with no cap,
//! and nothing in the rig reads `usage()`, so it cannot flip `push_batch` onto
//! the park path. A rig that grew a component which *does* read it would need a
//! real consumer instead.

use spate_bench::{Suite, bench_main};
use spate_core::ops::ChunkConfig;
use spate_core::record::{PartitionId, RawPayload, RecordMeta, stable_key_hash};
use spate_core::sink::{KeyHashRouter, ShardRouter};
use std::cell::RefCell;

#[path = "support/chain_rig.rs"]
mod chain_rig;

use chain_rig::{BATCH, BORROWED_BATCH_BYTES, Rig, Routing, borrowed_rig, borrowed_rig_with};

/// Payload bytes one batch ingests: `BATCH` payloads of
/// `"payload-{i:04}-abcdefgh|ijklmnop|qrstuvwx"`.
///
/// The throughput denominator for every chain case, keyed and keyless alike.
/// Pinned by `tests/bench_fixtures.rs` rather than trusted, because a payload
/// shape that drifted would leave every `bytes_per_s` here describing a corpus
/// that no longer exists.
const INPUT_BYTES: u64 = BATCH as u64 * 39;

/// Rows the borrowed chain emits per batch: `split3` cuts each payload at its
/// two separators, and `keep` rejects none.
const BORROWED_ROWS: u64 = BATCH as u64 * 3;

/// Shards the standalone routing case spreads over.
///
/// Sixteen rather than one, so the modulo has something to divide and the
/// result is a real spread — the keyed corpus hits all sixteen residues.
const ROUTE_SHARDS: usize = 16;

/// The default chunk target: above everything one batch encodes, so a chunk
/// seals only at `flush`.
fn default_target() -> usize {
    ChunkConfig::default().target_bytes
}

/// Fold a rig's corpus into the harness's digest.
///
/// Both blobs are absorbed unconditionally. A keyless corpus contributes a
/// zero-length `"keys"`, which still carries its label and length into the
/// digest — so every case built on a rig absorbs the same shape, and a keyed
/// corpus can never digest equal to a keyless one. The two floor cases at the
/// end of the file do not use this helper: they drive a corpus without a chain
/// around it and absorb only the keys.
fn absorb(corpus: &mut spate_bench::Corpus, rig: &Rig) {
    let built = rig.corpus();
    corpus.absorb("payloads", &built.payloads().concat());
    corpus.absorb("keys", &built.keys().concat());
}

/// A case that drives a whole batch through a built chain.
///
/// Every borrowed-rig case shares one setup and one routine; only the rig
/// differs, so the sweeps below read as the parameters they vary rather than as
/// seven copies of one body. `chain_owned` is written out instead because its
/// chain emits one record per payload rather than three, so it declares a
/// different `.items()`.
fn batch_case(suite: Suite, id: &str, build: fn() -> Rig) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                let rig = build();
                absorb(corpus, &rig);
                RefCell::new(rig)
            },
            |b, rig| {
                b.iter(|| rig.borrow_mut().drive());
            },
        )
        .items(BORROWED_ROWS)
        .bytes(INPUT_BYTES)
        .done()
}

fn suite() -> Suite {
    let suite = spate_bench::suite("spate-core");

    // --- the borrowed/owned contrast ---------------------------------------
    //
    // The headline pair. Same corpus, same shape, and one virtual call per
    // batch either way; what separates them is a copy per record, which is the
    // cost the zero-copy design exists to avoid.
    let suite = batch_case(suite, "chain_borrowed", borrowed_rig);
    let suite = suite
        .case(
            "chain_owned",
            |corpus, _seed| {
                let rig = chain_rig::owned_rig();
                absorb(corpus, &rig);
                RefCell::new(rig)
            },
            |b, rig| {
                b.iter(|| rig.borrow_mut().drive());
            },
        )
        // One record out per payload: the owned chain has no `flat_map`.
        .items(BATCH as u64)
        .bytes(INPUT_BYTES)
        .done();

    // --- the production router, swept over shard counts ---------------------
    //
    // Read `chain_keyed_one_shard` against `chain_borrowed`: same records, same
    // chunking, but every key hashed during deserialization and a real modulo
    // per record. The shard cases then hold routing fixed and vary only how
    // many buffers, encoder clones and `AckSet`s the stage carries — and how
    // many chunks `flush` seals.
    let suite = batch_case(suite, "chain_keyed_one_shard", || {
        borrowed_rig_with(Routing::KeyHash, 1, default_target())
    });
    let suite = batch_case(suite, "chain_keyed_four_shards", || {
        borrowed_rig_with(Routing::KeyHash, 4, default_target())
    });
    let suite = batch_case(suite, "chain_keyed_sixteen_shards", || {
        borrowed_rig_with(Routing::KeyHash, 16, default_target())
    });

    // --- chunk sealing ------------------------------------------------------
    //
    // A target of `BORROWED_BATCH_BYTES / n` seals `n` chunks per batch, all of
    // them inside `push`: each target divides the batch's encoding exactly, so
    // the buffer reaches it on a sub-record boundary and `flush` finds the
    // shard empty. Everything else matches `chain_borrowed`, so the difference
    // is `seal_and_send` — `BytesMut::split`, the fresh `reserve`, the
    // in-flight budget update, the `AckSet` hand-off, the next chunk's
    // `Instant::now` and the queue `try_send`.
    let suite = batch_case(suite, "chain_chunk_half_batch", || {
        borrowed_rig_with(Routing::Fixed, 1, BORROWED_BATCH_BYTES / 2)
    });
    let suite = batch_case(suite, "chain_chunk_quarter_batch", || {
        borrowed_rig_with(Routing::Fixed, 1, BORROWED_BATCH_BYTES / 4)
    });
    let suite = batch_case(suite, "chain_chunk_sixteenth_batch", || {
        borrowed_rig_with(Routing::Fixed, 1, BORROWED_BATCH_BYTES / 16)
    });

    // --- the microscopic floor ----------------------------------------------
    //
    // The two costs the router sweep above pays per record, measured with
    // nothing else around them. Both fold the whole corpus into one iteration
    // rather than measuring a single call: a lone `stable_key_hash` is a few
    // nanoseconds, which the harness refuses as indistinguishable from an empty
    // loop — and rightly, since a timer cannot resolve it. Folding is also what
    // gives `black_box` a value to hold, so neither call can be optimized away.
    suite
        .case(
            "chain_stable_key_hash",
            |corpus, _seed| {
                let keys = chain_rig::corpus(Routing::KeyHash).keys().to_vec();
                corpus.absorb("keys", &keys.concat());
                keys
            },
            |b, keys| {
                b.iter(|| {
                    keys.iter()
                        .fold(0u64, |acc, key| acc ^ stable_key_hash(key))
                });
            },
        )
        .items(BATCH as u64)
        // The keys are what this case ingests, so here they *are* the corpus —
        // unlike the chain cases above, where they sit beside the payload.
        .bytes(BATCH as u64 * 8)
        .done()
        .case(
            "chain_route_key_hash",
            |corpus, _seed| {
                let built = chain_rig::corpus(Routing::KeyHash);
                corpus.absorb("keys", &built.keys().concat());
                built
                    .keys()
                    .iter()
                    .map(|key| {
                        RawPayload {
                            bytes: b"",
                            key: Some(key),
                            partition: PartitionId(0),
                            offset: 0,
                            timestamp_ms: 0,
                        }
                        .meta()
                    })
                    .collect::<Vec<RecordMeta>>()
            },
            |b, metas| {
                b.iter(|| {
                    // The shard count has to stay opaque. `KeyHashRouter::route`
                    // takes it modulo the record's hash, and a literal power of
                    // two folds that division into a mask — roughly four times
                    // cheaper than the divide the terminal stage issues, where
                    // the count is `self.shards.len()` at run time. Without this
                    // the case still runs and still reports a stable figure; it
                    // is just not a floor the sweep above can be read against.
                    let shards = std::hint::black_box(ROUTE_SHARDS);
                    metas
                        .iter()
                        .fold(0usize, |acc, meta| acc + KeyHashRouter.route(meta, shards))
                });
            },
        )
        .items(BATCH as u64)
        // No `.bytes()`: a `RecordMeta` is a hash and three integers, not bytes
        // the pipeline ingested. Declaring a byte count here would invent a
        // throughput out of the size of a struct.
        .done()
}

bench_main!(suite);
