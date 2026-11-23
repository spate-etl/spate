//! The warning for a record skipped under `on_error: skip` locates the
//! record and holds none of its content.
//!
//! One test, because it installs the process's global log capture: the
//! pipeline's threads log to the global subscriber.

use serde::Deserialize;
use spate_core::config::PipelineConfig;
use spate_core::deser::{Deserializer, Owned};
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::sink::KeyHashRouter;
use spate_core::source::LaneId;
use spate_json::{JsonDeserializerBuilder, JsonDeserializerConfig, JsonOnError};
use spate_test::{HANG, LogCapture, PipelineRun, TestEncoder, capture_sink, memory_source};
use tracing::Level;

const SKIPPED: &str = "json record skipped by on_error policy";

fn config(name: &str) -> String {
    format!(
        r#"
pipeline: {{ name: {name}, threads: 1, io_threads: 1 }}
admin: {{ listen: none }}
metrics: {{ exporter: none }}
checkpoint: {{ interval: 100ms }}
source: {{ memory: {{}} }}
sink: {{ capture: {{}} }}
"#
    )
}

#[derive(Deserialize)]
enum Status {
    Active,
}

#[derive(Deserialize)]
#[expect(dead_code, reason = "decoded, never read")]
struct Typed {
    n: u32,
    #[serde(default)]
    status: Option<Status>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[expect(dead_code, reason = "decoded, never read")]
struct Strict {
    n: u32,
}

fn builder(reject_duplicate_keys: bool) -> JsonDeserializerBuilder {
    let mut config = JsonDeserializerConfig::default();
    config.on_error = JsonOnError::Skip;
    config.reject_duplicate_keys = reject_duplicate_keys;
    JsonDeserializerBuilder::new(config).expect("the config validates")
}

/// Runs `good` and then `refused` through a pipeline on partition 3, so the
/// refused record sits at offset 1.
fn run<T, D>(name: &str, deser: D, good: &[u8], refused: &[u8])
where
    T: Send + 'static,
    D: Deserializer<Owned<T>> + Clone + Send + 'static,
{
    let (source, handle) = memory_source();
    let (sink, _script) = capture_sink(1, 1);
    let runtime = Pipeline::from_config(PipelineConfig::from_str(&config(name)).expect("config"))
        .expect("builder")
        .sink(sink)
        .expect("sink")
        .chains(move |ctx| {
            let chunk = ctx.chunk();
            chain_owned::<T, _>(deser.clone())
                .map(|_: T| b"row".to_vec())
                .sink(TestEncoder, KeyHashRouter, chunk, ctx.queues, ctx.budget)
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
    let p = PartitionId(3);
    handle.assign_lanes(&[(LaneId(0), p)]);
    handle.push(p, None, good);
    let refused_at = handle.push(p, None, refused);
    assert_eq!(refused_at, 1, "the refused record's offset");
    assert!(
        handle.wait_committed(p, refused_at + 1, HANG),
        "{name}: the pipeline did not commit past the refused record"
    );
    shutdown.trigger();
    let report = run
        .wait_exit(HANG)
        .expect("the pipeline did not stop")
        .expect("run");
    assert_eq!(report.exit_code(), 0, "{name}");
}

#[test]
fn the_skip_line_locates_the_record_and_omits_its_content() {
    let logs = LogCapture::install_global(Level::WARN);

    run::<Typed, _>(
        "skip-wrong-type",
        builder(false).build_serde::<Typed>(),
        br#"{"n":1}"#,
        br#"{"n":"hunter2"}"#,
    );
    run::<Typed, _>(
        "skip-unknown-variant",
        builder(false).build_serde::<Typed>(),
        br#"{"n":1}"#,
        br#"{"n":1,"status":"hunter2"}"#,
    );
    run::<Strict, _>(
        "skip-unknown-field",
        builder(false).build_serde::<Strict>(),
        br#"{"n":1}"#,
        br#"{"n":1,"hunter2":2}"#,
    );
    run::<serde_json::Value, _>(
        "skip-duplicate-key",
        builder(true).build_value(),
        br#"{"n":1}"#,
        br#"{"hunter2":1,"hunter2":2}"#,
    );

    let lines = logs.lines();
    let skipped: Vec<&String> = lines.iter().filter(|l| l.contains(SKIPPED)).collect();
    assert_eq!(skipped.len(), 4, "one skip line per case: {lines:#?}");
    for line in skipped {
        assert!(
            line.contains("partition=3 offset=1"),
            "the skip line does not locate the record: {line}"
        );
    }
    let quoted: Vec<&String> = lines.iter().filter(|l| l.contains("hunter2")).collect();
    assert!(
        quoted.is_empty(),
        "a line holds record content: {quoted:#?}"
    );
}
