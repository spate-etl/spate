//! The driver bench rig: the cases the counted tier, the allocation snapshot
//! and `tests/bench_fixtures.rs` all run, and the counts each must return.
//!
//! Included with `#[path]` by `driver_gungraun.rs`, `driver_alloc.rs` and
//! `tests/bench_fixtures.rs`.

#![allow(dead_code, reason = "each target uses a different subset")]

use spate_core::bench_seams::{PollLoop, poll_loop};

/// Poll-loop iterations one case runs.
pub(crate) const POLLS: usize = 10_000;

/// One shape of pipeline thread.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Case {
    pub(crate) name: &'static str,
    pub(crate) lanes: usize,
    pub(crate) shards: usize,
    /// Payloads per batch, or `None` for polls that return nothing.
    pub(crate) payloads: Option<usize>,
}

/// One lane and one shard, every poll empty: the cost every iteration pays.
pub(crate) const EMPTY_ONE_LANE: Case = Case {
    name: "empty_one_lane",
    lanes: 1,
    shards: 1,
    payloads: None,
};

/// Many lanes, every poll empty: work that scales with the lanes a thread owns.
pub(crate) const EMPTY_64_LANES: Case = Case {
    name: "empty_64_lanes",
    lanes: 64,
    shards: 1,
    payloads: None,
};

/// Many shards, every poll empty: work that scales with the sink's queues.
pub(crate) const EMPTY_256_SHARDS: Case = Case {
    name: "empty_256_shards",
    lanes: 1,
    shards: 256,
    payloads: None,
};

/// Many lanes, every poll a batch: the per-batch path on top of the loop.
pub(crate) const BATCHES_64_LANES: Case = Case {
    name: "batches_64_lanes",
    lanes: 64,
    shards: 1,
    payloads: Some(16),
};

pub(crate) const CASES: [Case; 4] = [
    EMPTY_ONE_LANE,
    EMPTY_64_LANES,
    EMPTY_256_SHARDS,
    BATCHES_64_LANES,
];

impl Case {
    /// Build the case's thread, outside any measured region.
    pub(crate) fn build(self) -> PollLoop {
        poll_loop(POLLS, self.lanes, self.shards, self.payloads)
    }

    /// `(polls, min_lane_polls, batches, payloads)` a run must return. Lanes
    /// are polled in turn, so the fewest polls any lane sees is the share
    /// rounded down.
    pub(crate) fn expected(self) -> (u64, u64, u64, u64) {
        let polls = POLLS as u64;
        let batches = if self.payloads.is_some() { polls } else { 0 };
        (
            polls,
            polls / self.lanes as u64,
            batches,
            batches * self.payloads.unwrap_or(0) as u64,
        )
    }
}
