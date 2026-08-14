//! E2E scenario: poison payloads. Malformed Avro among valid records with
//! the Skip policy: the pipeline never stalls, drops are counted in
//! metrics, and every valid record lands.
//!
//! Selecting it:
//!
//! ```sh
//! cargo test -p spate --features full --locked \
//!   --test e2e_poison -- --ignored
//! ```

#[path = "e2e_support/mod.rs"]
mod support;

use std::time::Duration;
use support::*;

#[test]
#[ignore = "requires Docker"]
fn poison_payloads_are_skipped_counted_and_do_not_stall() {
    let h = Harness::up();
    let params = PipelineParams::defaults("e2e-poison", 19186);
    let valid_before = 1_000;
    let poison = 50;
    let valid_after = 10;

    h.create_topic(&params.topic, 1);
    h.create_table(&params.table);
    h.produce(&params.topic, &events(1, valid_before));
    h.produce_poison(&params.topic, 0, poison);
    // A tail of valid records proves the pipeline progressed past the
    // poison instead of stalling on it.
    let tail: Vec<(i32, Event)> = (0..valid_after)
        .map(|i| {
            let seq = valid_before as i64 + poison as i64 + i as i64;
            (
                0,
                Event {
                    id: event_id(0, seq),
                    name: format!("tail-{i}"),
                },
            )
        })
        .collect();
    h.produce(&params.topic, &tail);

    let pipeline = h.spawn_pipeline(&params);

    wait_until(Duration::from_secs(90), "all valid rows present", || {
        h.uniq(&params.table) == (valid_before + valid_after) as u64
    });

    let (status, body) = http_get(pipeline.admin, "/metrics");
    assert_eq!(status, 200);
    let dropped = metric_sum(&body, "spate_deser_records_dropped_total");
    assert!(
        dropped >= poison as f64,
        "poison drops surface in metrics (saw {dropped}, expected >= {poison})"
    );
    let (status, _) = http_get(pipeline.admin, "/healthz");
    assert_eq!(status, 200, "healthy after skipping poison");

    let report = pipeline.stop();
    assert!(
        matches!(report.state, spate::pipeline::ExitState::Completed),
        "clean exit: {report:?}"
    );
    assert_eq!(
        h.uniq(&params.table),
        (valid_before + valid_after) as u64,
        "every valid record delivered"
    );
}
