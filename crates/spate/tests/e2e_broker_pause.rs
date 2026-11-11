//! E2E scenarios: a silent coordinator. The broker is paused mid-stream, so its
//! sockets stay open and answer nothing. Paused long enough that the member's
//! session expires, the pipeline survives the silence, rejoins once the broker
//! answers, and loses nothing. Paused before a stop, the run still ends.
//!
//! Selecting it:
//!
//! ```sh
//! cargo test -p spate --features kafka,clickhouse,avro --locked \
//!   --test e2e_broker_pause -- --ignored
//! ```

#[path = "e2e_support/mod.rs"]
mod support;

use spate_test::metric_sum;
use std::time::Duration;
use support::*;

/// Pins that a broker going silent mid-stream ends neither the run nor a record.
/// The pause lasts until the client has counted a timed-out request and the member
/// has left its settled state, and the second wave is produced after it, so those
/// rows reach the sink only through the state the silence left.
#[test]
#[ignore = "requires Docker"]
fn a_silent_broker_mid_stream_loses_nothing() {
    let h = Harness::up();
    let mut params = PipelineParams::defaults("e2e-broker-pause", 19193);
    params.rdkafka = vec![
        ("socket.timeout.ms", "5000"),
        ("session.timeout.ms", "6000"),
        ("heartbeat.interval.ms", "2000"),
    ];
    let partitions = 3;
    let total = 40_000;

    h.create_topic(&params.topic, partitions);
    h.create_table(&params.table);
    let all = events(partitions, total);
    let (first_wave, second_wave) = all.split_at(20_000);
    h.produce(&params.topic, first_wave);

    let pipeline = h.spawn_pipeline(&params);
    wait_until(Duration::from_secs(60), "pipeline mid-stream", || {
        h.count(&params.table) >= 5_000
    });

    h.pause_broker();
    wait_until(
        Duration::from_secs(120),
        "the client to time out a request and leave its settled state",
        || {
            let (_, body) = http_get(pipeline.admin, "/metrics");
            let labels = [("pipeline", params.name)];
            let timed_out = metric_sum(
                &body,
                "spate_kafka_source_broker_req_timeouts_total",
                &labels,
            );
            let healthy = metric_sum(&body, "spate_kafka_source_group_healthy", &labels);
            timed_out.is_some_and(|n| n >= 1.0) && healthy == Some(0.0)
        },
    );
    h.unpause_broker();

    h.produce(&params.topic, second_wave);
    pipeline.assert_drained(&h, total as u64);
    let (report, sampled) = pipeline.stop_sampled();
    assert!(
        matches!(report.state, spate::pipeline::ExitState::Completed),
        "clean exit: {report:?}"
    );
    assert!(
        sampled.discarded > 0,
        "the sampler's own probes met the silence: {sampled:?}"
    );
}

/// Pins that a stop with the broker silent ends the run: the final commit and the
/// consumer's close each wait a bounded time for the group's answer.
#[test]
#[ignore = "requires Docker"]
fn a_stop_during_a_silent_broker_ends_the_run() {
    let h = Harness::up();
    let mut params = PipelineParams::defaults("e2e-stop-pause", 19197);
    params.rdkafka = vec![
        ("socket.timeout.ms", "5000"),
        ("session.timeout.ms", "6000"),
        ("heartbeat.interval.ms", "2000"),
    ];
    h.create_topic(&params.topic, 3);
    h.create_table(&params.table);
    h.produce(&params.topic, &events(3, 20_000));
    let pipeline = h.spawn_pipeline(&params);
    wait_until(Duration::from_secs(60), "mid-stream", || {
        h.count(&params.table) >= 5_000
    });
    let RunningPipeline {
        shutdown,
        join,
        sampler,
        ..
    } = pipeline;
    h.pause_broker();
    let started = std::time::Instant::now();
    shutdown.trigger();
    while !join.is_finished() && started.elapsed() < Duration::from_secs(120) {
        std::thread::sleep(Duration::from_millis(100));
    }
    let waited = started.elapsed();
    let finished = join.is_finished();
    h.unpause_broker();
    join.join().expect("thread").expect("start");
    sampler.finish();
    assert!(finished, "the run had not ended {waited:?} after its stop");
}
