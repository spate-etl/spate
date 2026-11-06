//! E2E scenario: a transactional writer. One partition carries committed and
//! aborted transactions, ending on an aborted one so a marker sits at its tail, and
//! another carries plain records. Only committed records reach the sink, and the
//! group commits past the tail marker with lag reading zero.
//!
//! Selecting it:
//!
//! ```sh
//! cargo test -p spate --features kafka,clickhouse,avro --locked \
//!   --test e2e_transactions -- --ignored
//! ```

#[path = "e2e_support/mod.rs"]
mod support;

use std::ops::Range;
use support::*;

const TRANSACTIONAL: i32 = 0;
const PLAIN: i32 = 1;
const SIZE: i64 = 2_000;

/// Events whose ids equal their offsets, as the sampler compares them.
fn records(partition: i32, offsets: Range<i64>) -> Vec<(i32, Event)> {
    offsets
        .map(|offset| {
            (
                partition,
                Event {
                    id: event_id(partition, offset),
                    name: format!("evt-{partition}-{offset}"),
                },
            )
        })
        .collect()
}

/// Pins that aborted records never reach the sink, and that markers, which take
/// offsets and deliver nothing, neither hold the group back nor hold lag above zero.
#[test]
#[ignore = "requires Docker"]
fn aborted_records_stay_out_and_markers_do_not_hold_the_group_back() {
    let h = Harness::up();
    let mut params = PipelineParams::defaults("e2e-transactions", 19195);

    // Each transaction's marker takes the one offset after its records.
    let committed_first = 0..SIZE;
    let aborted_middle = SIZE + 1..2 * SIZE + 1;
    let committed_second = 2 * SIZE + 2..3 * SIZE + 2;
    let aborted_tail = 3 * SIZE + 3..4 * SIZE + 3;
    let end = aborted_tail.end + 1;
    params.holes = vec![
        (TRANSACTIONAL, committed_first.end..committed_second.start),
        (TRANSACTIONAL, committed_second.end..end),
    ];

    h.create_topic(&params.topic, 2);
    h.create_table(&params.table);
    let topic = &params.topic;
    h.produce_transaction(topic, &records(TRANSACTIONAL, committed_first), true);
    h.produce_transaction(topic, &records(TRANSACTIONAL, aborted_middle), false);
    h.produce_transaction(topic, &records(TRANSACTIONAL, committed_second), true);
    h.produce_transaction(topic, &records(TRANSACTIONAL, aborted_tail), false);
    h.produce(topic, &records(PLAIN, 0..SIZE));
    assert_eq!(
        h.end_offsets(topic, 2),
        [end, SIZE],
        "the partitions hold the layout the holes declare"
    );

    let pipeline = h.spawn_pipeline(&params);
    pipeline.assert_drained(&h, 3 * SIZE as u64);
    let report = pipeline.stop();
    assert!(
        matches!(report.state, spate::pipeline::ExitState::Completed),
        "clean exit: {report:?}"
    );
}
