//! Property-testing strategies for framework types (feature `proptest`).
//!
//! Use these to drive the mocks from generated inputs, e.g. pushing
//! arbitrary payload sets through a pipeline and asserting invariants like
//! "every acknowledged offset was durably written".

use crate::sink::WriteOutcome;
use etl_core::record::PartitionId;
use etl_core::source::LaneId;
use proptest::prelude::*;
use std::time::Duration;

/// Up to `max_items` payloads of up to `max_len` bytes each.
pub fn payloads(max_items: usize, max_len: usize) -> impl Strategy<Value = Vec<Vec<u8>>> {
    prop::collection::vec(
        prop::collection::vec(any::<u8>(), 0..=max_len),
        0..=max_items,
    )
}

/// A lane layout: 1..=`max_lanes` lanes with dense ids, each serving a
/// distinct partition drawn from `0..max_partitions`. Suitable for
/// [`SourceHandle::assign_lanes`](crate::SourceHandle::assign_lanes).
pub fn lane_layout(
    max_lanes: usize,
    max_partitions: u32,
) -> impl Strategy<Value = Vec<(LaneId, PartitionId)>> {
    let all: Vec<u32> = (0..max_partitions).collect();
    prop::sample::subsequence(all, 1..=max_lanes.min(max_partitions as usize)).prop_map(
        |partitions| {
            partitions
                .into_iter()
                .enumerate()
                .map(|(i, p)| {
                    (
                        LaneId(u32::try_from(i).expect("dense lane id")),
                        PartitionId(p),
                    )
                })
                .collect()
        },
    )
}

/// One step of a generated source script. Indices are relative to a
/// [`lane_layout`] — the consuming test resolves them and skips steps that
/// are invalid in its current state (e.g. revoking an already-revoked
/// lane).
#[derive(Clone, Debug)]
pub enum ScriptOp {
    /// Push a payload to the partition at this layout index.
    Push {
        /// Index into the layout.
        partition_index: usize,
        /// Payload bytes.
        payload: Vec<u8>,
    },
    /// Revoke the lane at this layout index.
    Revoke {
        /// Index into the layout.
        lane_index: usize,
    },
    /// Re-assign the lane at this layout index (after a revoke).
    Assign {
        /// Index into the layout.
        lane_index: usize,
    },
}

/// A push-heavy script of up to `max_ops` steps over `layout_len` lanes,
/// with payloads up to `max_payload_len` bytes.
pub fn source_script(
    max_ops: usize,
    layout_len: usize,
    max_payload_len: usize,
) -> impl Strategy<Value = Vec<ScriptOp>> {
    let op = prop_oneof![
        8 => (0..layout_len, prop::collection::vec(any::<u8>(), 0..=max_payload_len))
            .prop_map(|(partition_index, payload)| ScriptOp::Push { partition_index, payload }),
        1 => (0..layout_len).prop_map(|lane_index| ScriptOp::Revoke { lane_index }),
        1 => (0..layout_len).prop_map(|lane_index| ScriptOp::Assign { lane_index }),
    ];
    prop::collection::vec(op, 0..=max_ops)
}

/// Up to `max` scripted write outcomes, weighted towards success:
/// 8 ok : 2 retryable : 1 delayed-ok (1..50 ms) : 1 fatal.
pub fn write_outcomes(max: usize) -> impl Strategy<Value = Vec<WriteOutcome>> {
    let outcome = prop_oneof![
        8 => Just(WriteOutcome::ok()),
        2 => Just(WriteOutcome::retryable("scripted retryable failure")),
        1 => (1u64..50)
            .prop_map(|ms| WriteOutcome::ok().after(Duration::from_millis(ms))),
        1 => Just(WriteOutcome::fatal("scripted fatal failure")),
    ];
    prop::collection::vec(outcome, 0..=max)
}
