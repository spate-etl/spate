//! E2E scenario: the happy path. Kafka → Avro (stub registry) → chain →
//! two-shard ClickHouse; exact delivery, committed offsets, live probes.
//!
//! `cargo test -p etl --test e2e_happy -- --ignored` (requires Docker).

#[path = "e2e_support/mod.rs"]
mod support;

use std::time::Duration;
use support::*;

#[test]
#[ignore = "requires Docker"]
fn happy_path_delivers_exactly_and_commits() {
    let h = Harness::up();
    let params = PipelineParams::defaults("e2e-happy", 19181);
    let partitions = 6;
    let total = 50_000;

    h.create_topic(&params.topic, partitions);
    h.create_table(&params.table);
    let produced = events(partitions, total);
    h.produce(&params.topic, &produced);

    let pipeline = h.spawn_pipeline(&params);

    // All rows land (dedup window makes in-session retries invisible).
    wait_until(Duration::from_secs(120), "all rows in ClickHouse", || {
        h.count(&params.table) == total as u64
    });

    // Probes and metrics are live while running.
    let (status, _) = http_get(pipeline.admin, "/healthz");
    assert_eq!(status, 200, "healthy while consuming");
    let (status, _) = http_get(pipeline.admin, "/readyz");
    assert_eq!(status, 200, "ready: assignment received, sinks probed");
    let (status, _) = http_get(pipeline.admin, "/metrics");
    assert_eq!(status, 200);
    // Two instrumentation gaps are FLAGGED for hardening rather than
    // asserted around here: (1) the driver never increments
    // etl_source_records_total; (2) sink-shard metric handles are created
    // before the runtime installs the recorder (flagship pattern), binding
    // them to the noop recorder — etl_sink_* families render empty. The
    // chain's operator metrics are registered post-install and are live:
    wait_until(Duration::from_secs(30), "operator counted records", || {
        let (_, body) = http_get(pipeline.admin, "/metrics");
        metric_sum(&body, "etl_operator_records_in_total") >= total as f64
    });

    // Let a commit tick pass so watermarks reach Kafka, then drain.
    wait_until(Duration::from_secs(30), "watermarks committed", || {
        let committed = h.committed(&params.topic, &params.group, partitions);
        committed.iter().sum::<i64>() == total as i64
    });
    let report = pipeline.stop();
    assert!(
        matches!(report.state, etl::pipeline::ExitState::Completed),
        "clean exit: {report:?}"
    );

    // Committed offset per partition == records produced to it.
    let mut per_partition = vec![0i64; partitions as usize];
    for (p, _) in &produced {
        per_partition[*p as usize] += 1;
    }
    assert_eq!(
        h.committed(&params.topic, &params.group, partitions),
        per_partition,
        "every partition committed exactly its produced count"
    );

    // Content spot checks.
    assert_eq!(h.count(&params.table), total as u64, "no duplicates");
    let name =
        h.rt.block_on(
            h.ch_client()
                .query(&format!(
                    "SELECT name FROM {} WHERE id = {}",
                    params.table,
                    event_id(3, 100)
                ))
                .fetch_one::<String>(),
        )
        .expect("spot check row");
    assert_eq!(name, "evt-3-100");
}
