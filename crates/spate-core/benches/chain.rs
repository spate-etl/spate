//! Operator-chain throughput and allocation benches (divan).
//!
//! Measures the production path: `Box<dyn RunnableChain>` fed borrowed
//! payload batches through deserialize → filter → flat_map → encode →
//! handoff, against the owned-payload equivalent. The boundary is one
//! virtual call per batch by construction; what that contrast establishes
//! is recorded under "Performance gates" in
//! `docs/user-guide/07-reference/ci.mdx`.
//!
//! Beyond that contrast, the borrowed rig sweeps the three parameters the
//! terminal stage varies in production: the router (a constant stub against
//! the production key-hash router over a keyed corpus), the shard count, and
//! the chunk target that decides how often a chunk seals mid-batch. Wall time
//! is cheap enough to carry the interior points of both sweeps; the
//! instruction-count sibling takes only their endpoints.
//!
//! The rigs live in `benches/support/chain_rig.rs`, shared with the
//! instruction-count bench in `benches/chain_gungraun.rs`.
//!
//! Run: `cargo bench -p spate-core --bench chain`

use divan::AllocProfiler;
use spate_core::ops::ChunkConfig;

#[path = "support/chain_rig.rs"]
mod chain_rig;

use chain_rig::{
    BATCH, BORROWED_BATCH_BYTES, Rig, Routing, borrowed_rig, borrowed_rig_with, owned_rig,
};

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn main() {
    divan::main();
}

/// Drive `rig` repeatedly, counting the records the borrowed chain emits
/// (three sub-records per payload after `flat_map`).
fn bench_borrowed(bencher: divan::Bencher<'_, '_>, mut rig: Rig) {
    rig.drive();
    bencher
        .counter(divan::counter::ItemsCount::new(BATCH * 3))
        .bench_local(|| rig.drive());
}

/// The default chunk target: above everything one batch encodes, so a chunk
/// seals only at `flush`.
fn default_target() -> usize {
    ChunkConfig::default().target_bytes
}

#[divan::bench]
fn borrowed_chain_batch(bencher: divan::Bencher<'_, '_>) {
    bench_borrowed(bencher, borrowed_rig());
}

#[divan::bench]
fn owned_chain_batch(bencher: divan::Bencher<'_, '_>) {
    let mut rig = owned_rig();
    rig.drive();
    bencher
        .counter(divan::counter::ItemsCount::new(BATCH))
        .bench_local(|| rig.drive());
}

// --- the production router, swept over shard counts -------------------------
//
// Read `keyed_one_shard` against `borrowed_chain_batch`: same records, same
// chunking, but every key hashed during deserialization and a real modulo per
// record. The shard cases then hold routing fixed and vary only how many
// buffers, encoder clones and `AckSet`s the stage carries — and how many
// chunks `flush` seals.

#[divan::bench]
fn keyed_one_shard(bencher: divan::Bencher<'_, '_>) {
    bench_borrowed(
        bencher,
        borrowed_rig_with(Routing::KeyHash, 1, default_target()),
    );
}

#[divan::bench]
fn keyed_four_shards(bencher: divan::Bencher<'_, '_>) {
    bench_borrowed(
        bencher,
        borrowed_rig_with(Routing::KeyHash, 4, default_target()),
    );
}

#[divan::bench]
fn keyed_sixteen_shards(bencher: divan::Bencher<'_, '_>) {
    bench_borrowed(
        bencher,
        borrowed_rig_with(Routing::KeyHash, 16, default_target()),
    );
}

// --- chunk sealing ----------------------------------------------------------
//
// A target of `BORROWED_BATCH_BYTES / n` seals `n` chunks per batch, all of
// them inside `push`: each target divides the batch's encoding exactly, so the
// buffer reaches it on a sub-record boundary and `flush` finds the shard
// empty. Everything else matches `borrowed_chain_batch`, so the difference is
// `seal_and_send` — `BytesMut::split`, the fresh `reserve`, the in-flight
// budget update, the `AckSet` hand-off, the next chunk's `Instant::now` and
// the queue `try_send`.

#[divan::bench]
fn chunk_half_batch(bencher: divan::Bencher<'_, '_>) {
    bench_borrowed(
        bencher,
        borrowed_rig_with(Routing::Fixed, 1, BORROWED_BATCH_BYTES / 2),
    );
}

#[divan::bench]
fn chunk_quarter_batch(bencher: divan::Bencher<'_, '_>) {
    bench_borrowed(
        bencher,
        borrowed_rig_with(Routing::Fixed, 1, BORROWED_BATCH_BYTES / 4),
    );
}

#[divan::bench]
fn chunk_sixteenth_batch(bencher: divan::Bencher<'_, '_>) {
    bench_borrowed(
        bencher,
        borrowed_rig_with(Routing::Fixed, 1, BORROWED_BATCH_BYTES / 16),
    );
}
