//! A sink's pool keys, from YAML beside the backend key or from
//! `SinkOptions`, reach the pool the builder spawns.

use spate_core::config::PipelineConfig;
use spate_core::deser::Owned;
use spate_core::error::ErrorPolicy;
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, RuntimeOptions, SinkOptions};
use spate_core::record::PartitionId;
use spate_core::sink::{BatchSection, KeyHashRouter, SinkPoolConfig};
use spate_core::source::LaneId;
use spate_test::{BytesPassthrough, HANG, PipelineRun, TestEncoder, capture_sink, memory_source};
use std::time::Duration;

/// Records pushed in each run.
const ROWS: u64 = 3;

/// A pipeline whose sink section holds `sink_keys` beside `capture:`.
fn config(sink_keys: &str) -> PipelineConfig {
    PipelineConfig::from_str(&format!(
        r#"
pipeline: {{ name: sink-pool-keys, threads: 1, io_threads: 1 }}
admin: {{ listen: none }}
metrics: {{ exporter: none }}
checkpoint: {{ interval: 100ms, drain_timeout: 2s }}
source: {{ memory: {{}} }}
sinks:
  a:
    chunk: {{ target_bytes: 1 }}
    {sink_keys}
    capture: {{}}
"#
    ))
    .expect("config")
}

/// Run `config` with `options` over a capture sink whose base pool never
/// seals a batch before shutdown, and wait for every row to be written. Only
/// a `max_rows: 1` that reaches the spawned pool writes them.
fn rows_are_written(config: PipelineConfig, options: SinkOptions) {
    let (source, handle) = memory_source();
    let mut base = SinkPoolConfig::default();
    base.batch.linger = Duration::from_secs(3600);
    let (sink, script) = capture_sink(1, 1);
    let sink = sink.with_pool_config(base);

    let runtime = Pipeline::from_config(config)
        .expect("builder")
        .add_sink_with("a", sink, options)
        .expect("sink a")
        .chains(|ctx| {
            let mut split = chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .with_metrics(ctx.pipeline.clone(), "main")
                .split(ErrorPolicy::Fail);
            let a = split.add::<Owned<Vec<u8>>, _, _>(TestEncoder, KeyHashRouter, ctx.sink("a"));
            split
                .route(move |row: Vec<u8>, out| out.emit(a, row))
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
    for i in 0..ROWS {
        handle.push(p, None, &[b'a' + i as u8]);
    }

    spate_test::wait_until(HANG, "every row written before shutdown", || {
        script.writes().iter().map(|w| w.rows).sum::<u64>() == ROWS
    });
    shutdown.trigger();
    run.wait_exit(HANG)
        .expect("pipeline did not exit")
        .expect("run");
}

/// A `batch:` block beside the backend key overlays the connector's base in
/// the spawned pool.
#[test]
fn a_yaml_batch_block_reaches_the_spawned_pool() {
    rows_are_written(config("batch: { max_rows: 1 }"), SinkOptions::default());
}

/// `SinkOptions::with_batch` replaces the connector's base in the spawned
/// pool.
#[test]
fn a_batch_set_in_code_reaches_the_spawned_pool() {
    let mut batch = BatchSection::default();
    batch.max_rows = 1;
    batch.linger = Duration::from_secs(3600);
    rows_are_written(config(""), SinkOptions::default().with_batch(batch));
}
