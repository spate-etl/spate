//! Checkpointing: acknowledgement tracking and watermark commits.
//!
//! One [`AckRef`] is created per source poll batch (by an [`AckIssuer`] on
//! the polling thread) and cloned into every record derived from that batch
//! — filter drops, `flat_map` fan-out, and multi-sink routing all compose
//! through plain `Clone`/`Drop`. When the last clone drops, the batch
//! resolves with the worst status observed and a message flows to the
//! [`Checkpointer`], which advances per-partition contiguous watermarks
//! ([`PartitionTracker`]) and hands `Source::commit` its positions on an
//! interval.
//!
//! Invariants (see `docs/DESIGN.md` and `CLAUDE.md`):
//! - the tracker stays synchronous and tokio-free (loom-tested);
//! - the ack path never blocks (unbounded channels, atomics only);
//! - a watermark never advances past an unacknowledged or failed batch,
//!   including across rebalances (assignment epochs make stale
//!   acknowledgements harmless).
//!
//! The concurrency-bearing primitives are model-checked with
//! [loom](https://docs.rs/loom):
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test -p etl-core --release checkpoint::loom_tests
//! ```

mod ack;
#[cfg(not(loom))]
mod checkpointer;
#[cfg(all(test, loom))]
mod loom_tests;
mod sync;
mod tracker;

pub use ack::{AckMsg, AckRef, AckStatus, BatchId};
#[cfg(not(loom))]
pub use checkpointer::{AckIssuer, Checkpointer, DrainStats};
pub use tracker::{PartitionTracker, ResolveOutcome};
