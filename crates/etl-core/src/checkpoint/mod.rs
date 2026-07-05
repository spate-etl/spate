//! Checkpointing: acknowledgement tracking and watermark commits.
//!
//! One [`AckRef`] is created per source poll batch and cloned into every
//! record derived from that batch (filter drops, `flat_map` fan-out, and
//! multi-sink routing all compose through plain `Clone`/`Drop`). When the
//! last clone drops, the batch resolves with the worst status observed and
//! a message flows to the checkpointer, which advances per-partition
//! contiguous watermarks and drives source offset commits on an interval.
//!
//! Invariants (see `docs/DESIGN.md` and `CLAUDE.md`):
//! - the tracker stays synchronous and tokio-free (loom-tested);
//! - the ack path never blocks (unbounded channel, atomics only);
//! - a watermark never advances past an unacknowledged or failed batch.

mod ack;

pub use ack::{AckMsg, AckRef, AckStatus, BatchId};
