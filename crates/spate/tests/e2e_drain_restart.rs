//! E2E scenario: graceful drain and resume. SIGTERM-equivalent trigger
//! mid-stream; every committed offset is backed by durable rows; a second
//! pipeline instance resumes from the committed positions and finishes
//! with zero loss.
//!
//! `cargo test -p spate --test e2e_drain_restart -- --ignored` (requires Docker).

// Scenario reports (row/duplicate counts) go to the test log by design.
#![allow(clippy::print_stdout)]

#[path = "e2e_support/mod.rs"]
mod support;

use std::time::Duration;
use support::*;

#[test]
#[ignore = "requires Docker"]
fn drain_commits_only_durable_rows_and_restart_resumes() {
    let h = Harness::up();
    let params = PipelineParams::defaults("e2e-drain", 19184);
    let partitions = 3;
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

    // Every committed offset is backed by durable rows: for partition p
    // committed at C_p, ids [p*1e6, p*1e6 + C_p) must all be present
    // (produced seq == offset on a fresh topic with one ordered producer).
    let committed = h.committed(&params.topic, &params.group, partitions);
    for (p, c) in committed.iter().enumerate() {
        let lo = event_id(i32::try_from(p).expect("partition"), 0);
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
    let mut resumed_params = PipelineParams::defaults("e2e-drain-resume", 19185);
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
