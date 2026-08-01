//! Operator-chain throughput and allocation benches (divan).
//!
//! Measures the production path: `Box<dyn RunnableChain>` fed borrowed
//! payload batches through deserialize → filter → flat_map → encode →
//! handoff, against the owned-payload equivalent. The static-vs-dyn
//! boundary delta was measured by the seam prototype (~+9% on a trivial
//! chain, amortizing to 1–2% with realistic work; see
//! `docs/benchmarks/zero-copy-seam.mdx`) — the boundary is one virtual call per batch by
//! construction.
//!
//! The rigs live in `benches/support/chain_rig.rs`, shared with the
//! instruction-count bench in `benches/chain_gungraun.rs`.
//!
//! Run: `cargo bench -p spate-core --bench chain`

use divan::AllocProfiler;

#[path = "support/chain_rig.rs"]
mod chain_rig;

use chain_rig::{BATCH, borrowed_rig, owned_rig};

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn main() {
    divan::main();
}

#[divan::bench]
fn borrowed_chain_batch(bencher: divan::Bencher<'_, '_>) {
    let mut rig = borrowed_rig();
    rig.drive();
    bencher
        .counter(divan::counter::ItemsCount::new(BATCH * 3))
        .bench_local(|| rig.drive());
}

#[divan::bench]
fn owned_chain_batch(bencher: divan::Bencher<'_, '_>) {
    let mut rig = owned_rig();
    rig.drive();
    bencher
        .counter(divan::counter::ItemsCount::new(BATCH))
        .bench_local(|| rig.drive());
}
