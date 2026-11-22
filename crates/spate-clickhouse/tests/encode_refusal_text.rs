//! A row the encoder refuses under the fail policy stops the pipeline with a
//! failure reason that holds none of the row's values.

use chrono::{DateTime, Utc};
use serde::Serialize;
use spate_clickhouse::testing::ClickHouseRowBinaryEncoder;
use spate_core::config::PipelineConfig;
use spate_core::deser::Owned;
use spate_core::error::ErrorPolicy;
use spate_core::ops::chain_owned;
use spate_core::pipeline::{ExitState, Pipeline, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::sink::KeyHashRouter;
use spate_core::source::LaneId;
use spate_test::{HANG, PipelineRun, TestDeserializer, capture_sink, memory_source};

const CONFIG: &str = r#"
pipeline: { name: encode-refusal-text, threads: 1, io_threads: 1 }
admin: { listen: none }
metrics: { exporter: none }
checkpoint: { interval: 100ms }
source: { memory: {} }
sink: { capture: {} }
"#;

/// Seconds since the epoch for 2200-01-01T00:00:00Z, past what `DateTime`
/// (`UInt32` seconds) holds.
const PAYLOAD: &[u8] = b"7258118400";

#[derive(Serialize)]
struct Row {
    #[serde(with = "spate_clickhouse::serde::chrono::datetime")]
    at: DateTime<Utc>,
}

fn row(payload: Vec<u8>) -> Row {
    let seconds: i64 = std::str::from_utf8(&payload)
        .expect("utf-8")
        .parse()
        .expect("seconds");
    Row {
        at: DateTime::from_timestamp(seconds, 0).expect("timestamp"),
    }
}

#[test]
fn the_failure_reason_omits_the_refused_value() {
    let (source, handle) = memory_source();
    let (sink, _script) = capture_sink(1, 1);
    let runtime = Pipeline::from_config(PipelineConfig::from_str(CONFIG).expect("config"))
        .expect("builder")
        .sink(sink)
        .expect("sink")
        .chains(move |ctx| {
            let mut chunk = ctx.chunk();
            chunk.encode_policy = ErrorPolicy::Fail;
            chain_owned::<Vec<u8>, _>(TestDeserializer::passthrough())
                .map(row)
                .sink(
                    ClickHouseRowBinaryEncoder::<Owned<Row>>::unchecked(),
                    KeyHashRouter,
                    chunk,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        })
        .into_runtime(source)
        .expect("into_runtime");
    let run = PipelineRun::spawn(move || runtime.run());
    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);
    handle.push(p, None, PAYLOAD);
    let report = run
        .wait_exit(HANG)
        .expect("the pipeline did not stop on the refused record")
        .expect("run");
    let ExitState::Failed(failure) = report.state else {
        panic!("expected a failed exit, got {:?}", report.state);
    };
    assert!(
        failure
            .reason
            .ends_with("serialize error: value out of range for DateTime"),
        "{}",
        failure.reason
    );
    assert!(!failure.reason.contains("2200-"), "{}", failure.reason);
}
