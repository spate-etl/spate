//! E2E scenario: sink outage. ClickHouse is paused mid-stream; backpressure
//! engages (observed via /metrics), and after recovery every record is
//! delivered, at-least-once with bounded duplicates.
//!
//! Selecting it:
//!
//! ```sh
//! cargo test -p spate --features kafka,clickhouse,avro --locked \
//!   --test e2e_sink_outage -- --ignored
//! ```

// Scenario reports (row/duplicate counts) go to the test log by design.
#![allow(clippy::print_stdout)]

#[path = "e2e_support/mod.rs"]
mod support;

use spate_test::{metric_sum, metric_value};
use std::time::{Duration, Instant};
use support::*;

/// Records the two scenarios deliver, split across a wave before the outage and
/// a wave during it.
const TOTAL: usize = 40_000;

/// Small queues and chunks so the outage visibly backs up into the source
/// instead of hiding in buffers.
fn outage_params(name: &'static str, admin_port: u16) -> PipelineParams {
    let mut params = PipelineParams::defaults(name, admin_port);
    params.queue_capacity = 2;
    params.chunk_bytes = 2 * 1024;
    params.batch_max_rows = 1_000;
    params
}

/// Run a pipeline until the first wave is mostly written, freeze ClickHouse and
/// produce the second wave.
///
/// The second wave is produced only after the sink is frozen, so the outage is
/// guaranteed unwritable inflow. Producing everything upfront raced the pipeline:
/// a fast run drained all rows before the pause landed, and an idle sink feels no
/// outage.
fn start_outage(h: &Harness, params: &PipelineParams) -> RunningPipeline {
    let partitions = 3;
    h.create_topic(&params.topic, partitions);
    h.create_table(&params.table);
    let all = events(partitions, TOTAL);
    let (first_wave, second_wave) = all.split_at(15_000);
    h.produce(&params.topic, first_wave);

    let pipeline = h.spawn_pipeline(params);

    wait_until(Duration::from_secs(60), "first 10k rows", || {
        h.count(&params.table) >= 10_000
    });

    h.pause_clickhouse();
    h.produce(&params.topic, second_wave);
    pipeline
}

#[test]
#[ignore = "requires Docker"]
fn sink_outage_backpressures_and_loses_nothing() {
    let h = Harness::up();
    let params = outage_params("e2e-outage", 19182);
    let total = TOTAL;
    let pipeline = start_outage(&h, &params);

    // ── Outage ─────────────────────────────────────────────────────────
    let outage = Duration::from_secs(15);
    let start = Instant::now();
    let mut saw_paused = false;
    while start.elapsed() < outage {
        let (_, body) = http_get(pipeline.admin, "/metrics");
        if metric_sum(&body, "spate_backpressure_paused", &[]).unwrap_or(0.0) >= 1.0 {
            saw_paused = true;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    h.unpause_clickhouse();

    let (_, body) = http_get(pipeline.admin, "/metrics");
    let pause_events =
        metric_sum(&body, "spate_backpressure_pause_events_total", &[]).unwrap_or(0.0);
    assert!(
        saw_paused || pause_events >= 1.0,
        "backpressure engaged during the outage (gauge seen: {saw_paused}, events: {pause_events})"
    );

    // ── Recovery: zero loss, duplicates bounded and reported ───────────
    pipeline.assert_drained(&h, total as u64);
    let (report, sampled) = pipeline.stop_sampled();
    assert!(
        matches!(report.state, spate::pipeline::ExitState::Completed),
        "clean exit: {report:?}"
    );
    assert!(
        sampled.peak_pending > 0.0,
        "the outage left batches unacknowledged: {sampled:?}"
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

/// Pins that `checkpoint.max_pending_batches` holds a real backlog at the limit.
/// The limit sits below what the outage's queues let build up, so without it the
/// reading passes the limit and the sampler fails the run. It can stop the source
/// before the queues fill, so this runs apart from the backpressure scenario.
#[test]
#[ignore = "requires Docker"]
fn sink_outage_holds_pending_work_at_its_limit() {
    let h = Harness::up();
    let mut params = outage_params("e2e-outage-limit", 19192);
    params.max_pending_batches = 4;
    let pipeline = start_outage(&h, &params);

    let limit = params.max_pending_batches as f64;
    wait_until(
        Duration::from_secs(90),
        "the pending reading to reach the limit",
        || {
            let (_, body) = http_get(pipeline.admin, "/metrics");
            metric_value(
                &body,
                "spate_checkpoint_pending_batches",
                &[("pipeline", params.name)],
            )
            .is_some_and(|pending| pending >= limit)
        },
    );
    h.unpause_clickhouse();

    pipeline.assert_drained(&h, TOTAL as u64);
    let (report, sampled) = pipeline.stop_sampled();
    assert!(
        matches!(report.state, spate::pipeline::ExitState::Completed),
        "clean exit: {report:?}"
    );
    assert_eq!(sampled.peak_pending, limit, "{sampled:?}");
}
