//! E2E scenario: rebalance under load. An outside consumer joins the group
//! of a pipeline running two consumers mid-stream and leaves again; the
//! pipeline survives both rebalances with zero loss, with the harness's sampler
//! watching committed offsets against durable rows throughout.
//!
//! Selecting it:
//!
//! ```sh
//! cargo test -p spate --features kafka,clickhouse,avro --locked \
//!   --test e2e_rebalance -- --ignored
//! ```

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
#[expect(
    clippy::disallowed_methods,
    reason = "the test joins the intruder thread"
)]
fn rebalance_under_load_preserves_every_record() {
    let h = Harness::up();
    let mut params = PipelineParams::defaults("e2e-rebalance", 19183);
    // Two consumers in the pipeline, so the intruder's join and leave each
    // revoke both of them at once.
    params.kafka_consumers = 2;
    let partitions = 6;
    let total = 30_000;

    h.create_topic(&params.topic, partitions);
    h.create_table(&params.table);
    // The second half is produced only after the intruder has left, so it
    // reaches the sink only through the assignment that follows both
    // rebalances.
    let all = events(partitions, 2 * total);
    h.produce(&params.topic, &all[..total]);

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
            #[expect(
                clippy::disallowed_methods,
                reason = "the intruder is a plain group member outside the connector's client factory"
            )]
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

    intruder.join().expect("intruder thread");

    // ── Conservation after the storm ───────────────────────────────────
    h.produce(&params.topic, &all[total..]);
    pipeline.assert_drained_ignoring_lag(&h, 2 * total as u64);
    let report = pipeline.stop();
    assert!(
        matches!(report.state, spate::pipeline::ExitState::Completed),
        "clean exit: {report:?}"
    );
    let rows = h.count(&params.table);
    let uniq = h.uniq(&params.table);
    assert_eq!(uniq, 2 * total as u64, "no record lost across rebalances");
    println!(
        "rebalance scenario: {rows} rows, {uniq} distinct, {} duplicates",
        rows - uniq
    );
}
