//! The warning for a row the encoder skips holds none of the row's values.
//!
//! One test, because it installs the process's global log capture: the
//! pipeline's threads log to the global subscriber.

use chrono::{DateTime, Utc};
use serde::Serialize;
use spate_clickhouse::ClickHouseEncoder;
use spate_core::config::PipelineConfig;
use spate_core::deser::Owned;
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::sink::KeyHashRouter;
use spate_core::source::LaneId;
use spate_test::{HANG, LogCapture, PipelineRun, TestDeserializer, capture_sink, memory_source};
use tracing::Level;

const CONFIG: &str = r#"
pipeline: { name: encode-skip-line, threads: 1, io_threads: 1 }
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
fn the_skip_line_omits_the_refused_value() {
    let logs = LogCapture::install_global(Level::WARN);

    let (source, handle) = memory_source();
    let (sink, _script) = capture_sink(1, 1);
    let runtime = Pipeline::from_config(PipelineConfig::from_str(CONFIG).expect("config"))
        .expect("builder")
        .sink(sink)
        .expect("sink")
        .chains(move |ctx| {
            let chunk = ctx.chunk();
            chain_owned::<Vec<u8>, _>(TestDeserializer::passthrough())
                .map(row)
                .sink(
                    ClickHouseEncoder::<Owned<Row>>::unchecked(),
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
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());
    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);
    let last = handle.push(p, None, PAYLOAD);
    assert!(
        handle.wait_committed(p, last + 1, HANG),
        "the pipeline did not commit past the skipped row"
    );
    shutdown.trigger();
    let report = run
        .wait_exit(HANG)
        .expect("the pipeline did not stop")
        .expect("run");
    assert_eq!(report.exit_code(), 0);

    let lines = logs.lines();
    let skipped: Vec<&String> = lines
        .iter()
        .filter(|l| l.contains("record skipped by sink encoder error policy"))
        .collect();
    assert_eq!(skipped.len(), 1, "one skip line: {lines:#?}");
    assert!(
        skipped[0].contains("serialize error: value out of range for DateTime"),
        "{}",
        skipped[0]
    );
    let quoted: Vec<&String> = lines.iter().filter(|l| l.contains("2200-")).collect();
    assert!(
        quoted.is_empty(),
        "a line holds the row's value: {quoted:#?}"
    );
}
