//! E2E scenario: rebalance under load. A second consumer joins the group
//! mid-stream and leaves again; the pipeline survives both rebalances with
//! zero loss, and at no sampled instant do committed offsets exceed rows
//! durably written — the at-least-once invariant, observed live.
//!
//! `cargo test -p spate --test e2e_rebalance -- --ignored` (requires Docker).

// Scenario reports (row/duplicate counts) go to the test log by design.
#![allow(clippy::print_stdout)]

#[path = "e2e_support/mod.rs"]
mod support;

use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::time::{Duration, Instant};
use support::*;

#[test]
#[ignore = "requires Docker"]
fn rebalance_under_load_preserves_every_record() {
    let h = Harness::up();
    let params = PipelineParams::defaults("e2e-rebalance", 19183);
    let partitions = 6;
    let total = 30_000;

    h.create_topic(&params.topic, partitions);
    h.create_table(&params.table);
    h.produce(&params.topic, &events(partitions, total));

    let pipeline = h.spawn_pipeline(&params);
    wait_until(Duration::from_secs(60), "pipeline mid-stream", || {
        h.count(&params.table) >= 5_000
    });

    // ── The intruder: joins the group, steals partitions, leaves ───────
    // It never commits (that would corrupt the group's offsets); whatever
    // it consumes is redelivered to the pipeline after it leaves.
    let intruder = {
        let brokers = h.brokers.clone();
        let topic = params.topic.clone();
        let group = params.group.clone();
        std::thread::spawn(move || {
            let consumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &brokers)
                .set("group.id", &group)
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "earliest")
                .create()
                .expect("intruder consumer");
            consumer.subscribe(&[&topic]).expect("intruder subscribe");
            let leave_at = Instant::now() + Duration::from_secs(10);
            while Instant::now() < leave_at {
                let _ = consumer.poll(Duration::from_millis(100));
            }
            // Dropping the consumer leaves the group: second rebalance.
        })
    };

    // ── Invariant sampling through both rebalances ─────────────────────
    // Read committed BEFORE counting rows: writes only grow, so
    // committed(t1) <= rows(t2) must hold for t1 < t2 iff the pipeline
    // never commits past unwritten data.
    let sample_until = Instant::now() + Duration::from_secs(20);
    let mut samples = 0u32;
    let mut violations = Vec::new();
    while Instant::now() < sample_until {
        let committed: i64 = h
            .committed(&params.topic, &params.group, partitions)
            .iter()
            .sum();
        let rows = h.uniq(&params.table) as i64;
        samples += 1;
        if committed > rows {
            violations.push((samples, committed, rows));
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    intruder.join().expect("intruder thread");

    assert!(
        violations.is_empty(),
        "committed offsets exceeded durable rows at samples: {violations:?}"
    );
    println!("rebalance scenario: invariant held across {samples} samples");

    // ── Conservation after the storm ───────────────────────────────────
    wait_until(
        Duration::from_secs(120),
        "every distinct id present",
        || h.uniq(&params.table) == total as u64,
    );
    let report = pipeline.stop();
    assert!(
        matches!(report.state, spate::pipeline::ExitState::Completed),
        "clean exit: {report:?}"
    );
    let rows = h.count(&params.table);
    let uniq = h.uniq(&params.table);
    assert_eq!(uniq, total as u64, "no record lost across rebalances");
    println!(
        "rebalance scenario: {rows} rows, {uniq} distinct, {} duplicates",
        rows - uniq
    );
}
