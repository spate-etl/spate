//! Property tests exercising the `proptest`-feature strategies against the
//! mocks — both to test the strategies and to demonstrate their use.
#![cfg(feature = "proptest")]

use proptest::prelude::*;
use spate_core::record::PartitionId;
use spate_core::source::LaneId;
use spate_test::strategies;
use spate_test::{ScriptedResult, TestEncoder, decode_rows};
use std::collections::HashSet;

proptest! {
    /// Arbitrary payload sets round-trip through the length-prefixed
    /// encoder.
    #[test]
    fn payloads_round_trip_through_the_encoder(payloads in strategies::payloads(32, 256)) {
        use spate_core::sink::RowEncoder;
        let mut buf = bytes::BytesMut::new();
        for p in &payloads {
            let (ack, _rx) = spate_core::checkpoint::AckRef::test_pair();
            let rec = spate_core::record::Record {
                payload: p.clone(),
                meta: spate_core::record::RecordMeta {
                    partition: PartitionId(0),
                    offset: 0,
                    event_time_ms: 0,
                    key_hash: None,
                },
                ack,
            };
            TestEncoder.encode(&rec, &mut buf).unwrap();
        }
        prop_assert_eq!(decode_rows(&buf), payloads);
    }

    /// Layouts have unique dense lanes and unique partitions — directly
    /// usable with `SourceHandle::assign_lanes`.
    #[test]
    fn lane_layouts_are_unique_and_dense(layout in strategies::lane_layout(8, 64)) {
        prop_assert!(!layout.is_empty());
        let lanes: HashSet<LaneId> = layout.iter().map(|&(l, _)| l).collect();
        let partitions: HashSet<PartitionId> = layout.iter().map(|&(_, p)| p).collect();
        prop_assert_eq!(lanes.len(), layout.len());
        prop_assert_eq!(partitions.len(), layout.len());
        prop_assert!(layout.iter().enumerate().all(|(i, &(l, _))| l == LaneId(i as u32)));
    }

    /// Script indices stay within the layout they were generated for.
    #[test]
    fn source_scripts_reference_the_layout(script in strategies::source_script(64, 5, 16)) {
        for op in &script {
            let idx = match op {
                strategies::ScriptOp::Push { partition_index, .. } => *partition_index,
                strategies::ScriptOp::Revoke { lane_index }
                | strategies::ScriptOp::Assign { lane_index } => *lane_index,
            };
            prop_assert!(idx < 5);
        }
    }

    /// Outcome scripts only contain constructible outcomes and lean
    /// towards success.
    #[test]
    fn write_outcome_scripts_are_wellformed(outcomes in strategies::write_outcomes(64)) {
        for o in &outcomes {
            if let Some(d) = o.delay {
                prop_assert!(!d.is_zero());
            }
            match &o.result {
                ScriptedResult::Ok => {}
                ScriptedResult::Retryable(r) | ScriptedResult::Fatal(r) => {
                    prop_assert!(!r.is_empty());
                }
                _ => prop_assert!(false, "unknown scripted result variant"),
            }
        }
    }
}
