//! E2E scenario: sink outage. ClickHouse is paused mid-stream; backpressure
//! engages (observed via /metrics), and after recovery every record is
//! delivered, at-least-once with bounded duplicates.
//!
//! Selecting it:
//!
//! ```sh
//! cargo test -p spate --features full --locked \
//!   --test e2e_sink_outage -- --ignored
//! ```

// Scenario reports (row/duplicate counts) go to the test log by design.
#![allow(clippy::print_stdout)]

#[path = "e2e_support/mod.rs"]
mod support;

use std::time::{Duration, Instant};
use support::*;

#[test]
#[ignore = "requires Docker"]
fn sink_outage_backpressures_and_loses_nothing() {
    let h = Harness::up();
    let mut params = PipelineParams::defaults("e2e-outage", 19182);
    // Small queues and chunks so the outage visibly backs up into the
    // source instead of hiding in buffers.
    params.queue_capacity = 2;
    params.chunk_bytes = 2 * 1024;
    params.batch_max_rows = 1_000;
    let partitions = 3;
    let total = 40_000;

    h.create_topic(&params.topic, partitions);
    h.create_table(&params.table);
    // Two waves: the second is produced only after the sink is frozen, so
    // the outage is guaranteed unwritable inflow. Producing everything
    // upfront raced the pipeline: a fast run drained all rows before the
    // pause landed, and an idle sink feels no outage.
    let all = events(partitions, total);
    let (first_wave, second_wave) = all.split_at(15_000);
    h.produce(&params.topic, first_wave);

    let pipeline = h.spawn_pipeline(&params);

    wait_until(Duration::from_secs(60), "first 10k rows", || {
        h.count(&params.table) >= 10_000
    });

    // ── Outage ─────────────────────────────────────────────────────────
    h.pause_clickhouse();
    h.produce(&params.topic, second_wave);
    let outage = Duration::from_secs(15);
    let start = Instant::now();
    let mut saw_paused = false;
    while start.elapsed() < outage {
        let (_, body) = http_get(pipeline.admin, "/metrics");
        if metric_sum(&body, "spate_backpressure_paused") >= 1.0 {
            saw_paused = true;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    h.unpause_clickhouse();

    let (_, body) = http_get(pipeline.admin, "/metrics");
    let pause_events = metric_sum(&body, "spate_backpressure_pause_events_total");
    assert!(
        saw_paused || pause_events >= 1.0,
        "backpressure engaged during the outage (gauge seen: {saw_paused}, events: {pause_events})"
    );

    // ── Recovery: zero loss, duplicates bounded and reported ───────────
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
    assert_eq!(uniq, total as u64, "no record lost across the outage");
    let duplicates = rows - uniq;
    // At-least-once: duplicates are legal; the dedup window absorbs
    // same-token retries so the residue should be small. Report it.
    println!("outage scenario: {rows} rows, {uniq} distinct, {duplicates} duplicates");
    assert!(
        duplicates < total as u64 / 10,
        "duplicate volume implausibly high: {duplicates}"
    );
}
