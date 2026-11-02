//! E2E scenario: graceful drain and resume. SIGTERM-equivalent trigger
//! mid-stream; every committed offset is backed by durable rows; a second
//! pipeline instance resumes from the committed positions and finishes
//! with zero loss.
//!
//! Selecting it:
//!
//! ```sh
//! cargo test -p spate --features kafka,clickhouse,avro --locked \
//!   --test e2e_drain_restart -- --ignored
//! ```

// Scenario reports (row/duplicate counts) go to the test log by design.
#![allow(clippy::print_stdout)]

#[path = "e2e_support/mod.rs"]
mod support;

use std::time::Duration;
use support::*;

#[test]
#[ignore = "requires Docker"]
fn drain_commits_only_durable_rows_and_restart_resumes() {
    drain_then_resume(("e2e-drain", 19184, 1), ("e2e-drain-resume", 19185, 1), 3);
}

/// A restart that lowers the Kafka consumer count from four to one loses
/// nothing: the four consumers commit only durable rows as they drain and
/// leave the group, and the one that replaces them resumes every partition
/// from those commits.
#[test]
#[ignore = "requires Docker"]
fn fewer_consumers_after_a_restart_lose_nothing() {
    drain_then_resume(("e2e-fewer", 19188, 4), ("e2e-fewer-resume", 19189, 1), 4);
}

/// A restart that raises the Kafka consumer count from one to four loses
/// nothing, however many rebalances the four consumers' joins take.
#[test]
#[ignore = "requires Docker"]
fn more_consumers_after_a_restart_lose_nothing() {
    drain_then_resume(("e2e-more", 19190, 1), ("e2e-more-resume", 19191, 4), 4);
}

/// Drain a first instance mid-stream, check every committed offset is backed
/// by durable rows, then resume with a second instance on the same topic,
/// group and table and check nothing was lost. Each instance is `(name,
/// admin port, Kafka consumers)`.
fn drain_then_resume(
    first: (&'static str, u16, usize),
    second: (&'static str, u16, usize),
    partitions: i32,
) {
    let h = Harness::up();
    let mut params = PipelineParams::defaults(first.0, first.1);
    params.kafka_consumers = first.2;
    let total = 120_000;

    h.create_topic(&params.topic, partitions);
    h.create_table(&params.table);
    h.produce(&params.topic, &events(partitions, total));

    // ── First instance: drain mid-stream ───────────────────────────────
    let first = h.spawn_pipeline(&params);
    wait_until(Duration::from_secs(60), "mid-stream", || {
        h.count(&params.table) >= 10_000
    });
    let report = first.stop();
    assert!(
        matches!(report.state, spate::pipeline::ExitState::Completed),
        "drain completes cleanly mid-stream: {report:?}"
    );
    println!("drain report: {:?}", report.sink_drain);

    // Every partition committed progress, and every committed offset is
    // backed by durable rows: for partition p
    // committed at C_p, ids [p*1e6, p*1e6 + C_p) must all be present
    // (produced seq == offset on a fresh topic with one ordered producer).
    let committed = h.committed(&params.topic, &params.group, partitions);
    for (p, c) in committed.iter().enumerate() {
        let lo = event_id(i32::try_from(p).expect("partition"), 0);
        assert!(
            *c > 0,
            "partition {p}: nothing committed after a drain that wrote rows: {committed:?}"
        );
        let hi = lo + c;
        let present = h.scalar(&format!(
            "SELECT uniqExact(id) FROM {} WHERE id >= {lo} AND id < {hi}",
            params.table
        ));
        assert_eq!(
            present,
            u64::try_from(*c).expect("offset"),
            "partition {p}: committed to {c} but rows are missing — \
             the watermark ran ahead of durable writes"
        );
    }
    println!("drain scenario: committed {committed:?} all backed by rows");

    // ── Second instance: resume and finish ─────────────────────────────
    let mut resumed_params = PipelineParams::defaults(second.0, second.1);
    resumed_params.kafka_consumers = second.2;
    resumed_params.topic = params.topic.clone();
    resumed_params.group = params.group.clone();
    resumed_params.table = params.table.clone();
    let second = h.spawn_pipeline(&resumed_params);
    wait_until(
        Duration::from_secs(120),
        "every distinct id present",
        || h.uniq(&params.table) == total as u64,
    );
    let report = second.stop();
    assert!(
        matches!(report.state, spate::pipeline::ExitState::Completed),
        "resumed instance exits cleanly: {report:?}"
    );

    let rows = h.count(&params.table);
    let uniq = h.uniq(&params.table);
    assert_eq!(uniq, total as u64, "zero loss across drain + restart");
    let final_committed: i64 = h
        .committed(&params.topic, &params.group, partitions)
        .iter()
        .sum();
    assert_eq!(
        final_committed, total as i64,
        "everything committed at the end"
    );
    println!(
        "drain scenario: {rows} rows, {uniq} distinct, {} duplicates",
        rows - uniq
    );
}
