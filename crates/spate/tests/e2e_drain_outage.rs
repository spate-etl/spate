//! E2E scenario: shutdown *during* a sink outage (#83).
//!
//! [`e2e_sink_outage`](e2e_sink_outage.rs) recovers the sink before stopping,
//! so it never exercises the drain against a sink that is still down. That
//! is the case the graceful-shutdown guide names ("a ClickHouse outage
//! mid-shutdown, say").
//!
//! With ClickHouse paused, `write_batch` does not return: the container holds
//! the connection open and answers nothing, and the client's default `end`
//! timeout is 180s, longer than any sane `drain_timeout`. Every in-flight
//! permit is therefore held by a write that will not finish, so the sink
//! worker reaches `dispatch` with no permit available and nothing to release
//! one. The drain deadline is the only thing that can free it, which is why
//! `acquire_owned()` has to be polled inside the `select!` that watches that
//! deadline rather than awaited outside it. A worker parked outside it never
//! wakes, and the process runs past `terminationGracePeriodSeconds` to
//! SIGKILL.
//!
//! The assertion is the one an operator cares about: SIGTERM with the sink
//! down still exits, under its own deadline, having abandoned loudly.
//!
//! Selecting it:
//!
//! ```sh
//! cargo test -p spate --features full --locked \
//!   --test e2e_drain_outage -- --ignored
//! ```

// Scenario reports go to the test log by design.
#![allow(clippy::print_stdout)]

#[path = "e2e_support/mod.rs"]
mod support;

use std::time::{Duration, Instant};
use support::*;

/// The shutdown wait is bounded in the test itself, so a regression fails
/// rather than hangs.
const EXIT_BOUND: Duration = Duration::from_secs(60);

#[test]
#[ignore = "requires Docker"]
fn shutdown_during_a_sink_outage_still_exits_under_its_deadline() {
    let h = Harness::up();
    let mut params = PipelineParams::defaults("e2e-drain-outage", 19187);
    // One permit per shard, small batches and queues: the outage saturates the
    // in-flight window within a batch or two instead of depending on volume.
    params.inflight_max_per_shard = 1;
    params.queue_capacity = 2;
    params.chunk_bytes = 2 * 1024;
    params.batch_max_rows = 500;
    params.linger = "200ms";
    // Short enough that "honored the deadline" and "hung" are far apart.
    params.drain_timeout = "10s";
    let partitions = 3;
    let total = 30_000;

    h.create_topic(&params.topic, partitions);
    h.create_table(&params.table);
    let all = events(partitions, total);
    let (first_wave, second_wave) = all.split_at(10_000);
    h.produce(&params.topic, first_wave);

    let pipeline = h.spawn_pipeline(&params);
    wait_until(Duration::from_secs(60), "first 5k rows", || {
        h.count(&params.table) >= 5_000
    });

    // ── Freeze the sink and keep feeding it ────────────────────────────
    h.pause_clickhouse();
    h.produce(&params.topic, second_wave);

    // Backpressure engaging is the precondition this scenario needs. The
    // queues only fill once the worker has stopped consuming them, which is
    // the state of having sealed a batch it cannot get a permit for.
    wait_until(
        Duration::from_secs(90),
        "backpressure engaged (worker holds a batch it cannot dispatch)",
        || {
            let (_, body) = http_get(pipeline.admin, "/metrics");
            metric_sum(&body, "spate_backpressure_paused") >= 1.0
        },
    );

    // ── SIGTERM with the sink still down ───────────────────────────────
    let RunningPipeline { shutdown, join, .. } = pipeline;
    let started = Instant::now();
    shutdown.trigger();
    while !join.is_finished() && started.elapsed() < EXIT_BOUND {
        std::thread::sleep(Duration::from_millis(100));
    }
    let exited_in = started.elapsed();
    let finished = join.is_finished();

    // Unpause before asserting: on failure this lets the wedged drain
    // complete so the thread joins and the container tears down cleanly,
    // rather than leaving a frozen container behind a hung test.
    h.unpause_clickhouse();
    let report = join
        .join()
        .expect("pipeline thread panicked")
        .expect("pipeline start failed");

    assert!(
        finished,
        "pipeline did not exit within {EXIT_BOUND:?} of shutdown while the sink \
         was down; drain_timeout was {} (#83)",
        params.drain_timeout
    );
    println!("drain-during-outage: exited in {exited_in:?}, report {report:?}");

    // The exit sits inside the drain's own budget, not some looser bound.
    assert!(
        exited_in < Duration::from_secs(30),
        "exit took {exited_in:?} against a 10s drain_timeout"
    );

    // Abandoning is the correct outcome here. Asserting it shows the drain
    // ran its deadline sweep rather than finding nothing to do.
    let drain = report.sink_drain.expect("a sink drain report");
    assert!(
        drain.abandoned > 0,
        "the frozen sink's batches must be abandoned loudly, got {drain:?}"
    );
    assert!(
        matches!(report.state, spate::pipeline::ExitState::Completed),
        "an abandoned batch is a completed drain, not a failure: {report:?}"
    );
}
