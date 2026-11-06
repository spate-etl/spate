//! E2E scenario: a rolling restart of three brokers under load. Each controlled
//! stop moves that broker's partition leaders, and restarting all three moves the
//! group coordinator too. The pipeline reads on through the new leaders and loses
//! nothing.
//!
//! Selecting it:
//!
//! ```sh
//! cargo test -p spate --features kafka,clickhouse,avro --locked \
//!   --test e2e_leader_move -- --ignored
//! ```

#[path = "e2e_support/mod.rs"]
mod support;

use std::time::Duration;
use support::*;

/// The exit code of a container the runtime had to kill.
const KILLED: i64 = 137;
const BROKERS: i32 = 3;
const IN_SYNC: usize = 3;
const WAVE: usize = 15_000;
const DEADLINE: Duration = Duration::from_secs(180);

/// Pins that a rolling restart loses no record and leaves the run able to drain.
/// A wave follows each restart, and has to reach the sink through the moved leaders
/// before the next broker stops.
#[test]
#[ignore = "requires Docker"]
fn a_rolling_restart_moves_leaders_and_loses_nothing() {
    let h = Harness::up_cluster([19201, 19202, 19203]);
    let params = PipelineParams::defaults("e2e-leader-move", 19194);
    let partitions = 6;
    let waves = 1 + BROKERS as usize;
    let total = waves * WAVE;

    h.create_topic(&params.topic, partitions);
    h.create_table(&params.table);
    let all = events(partitions, total);
    let mut waves = all.chunks(WAVE);
    h.produce(&params.topic, waves.next().expect("the first wave"));

    let pipeline = h.spawn_pipeline(&params);
    wait_until(DEADLINE, "the first wave", || {
        h.uniq(&params.table) == WAVE as u64
    });

    for (node, wave) in (1..=BROKERS).zip(waves) {
        let states = h.partition_states(&params.topic).expect("partition states");
        assert!(
            states.iter().any(|p| p.leader == node),
            "node {node} leads a partition before it stops: {states:?}"
        );

        let exit = h.stop_broker(node);
        assert_ne!(exit, KILLED, "node {node} stopped under its own shutdown");
        wait_until(DEADLINE, "every partition to lead elsewhere", || {
            h.partition_states(&params.topic)
                .is_some_and(|states| states.iter().all(|p| p.leader > 0 && p.leader != node))
        });

        h.start_broker(node);
        wait_until(DEADLINE, "the replicas to catch up", || {
            h.partition_states(&params.topic)
                .is_some_and(|states| states.iter().all(|p| p.in_sync == IN_SYNC))
        });

        let before = h.uniq(&params.table);
        h.produce(&params.topic, wave);
        wait_until(DEADLINE, "the wave through the moved leaders", || {
            h.uniq(&params.table) >= before + WAVE as u64
        });
    }

    pipeline.assert_drained(&h, total as u64);
    let report = pipeline.stop();
    assert!(
        matches!(report.state, spate::pipeline::ExitState::Completed),
        "clean exit: {report:?}"
    );
}
