//! A record the typed JSON deserializer refuses under `on_error: fail` stops
//! the pipeline with a failure reason that holds none of the record.

use serde::Deserialize;
use spate_core::config::PipelineConfig;
use spate_core::error::ErrorPolicy;
use spate_core::ops::chain_owned;
use spate_core::pipeline::{ExitState, Pipeline, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::sink::KeyHashRouter;
use spate_core::source::LaneId;
use spate_json::JsonDeserializerBuilder;
use spate_test::{HANG, PipelineRun, TestEncoder, capture_sink, memory_source};

const CONFIG: &str = r#"
pipeline: { name: json-refusal-text, threads: 1, io_threads: 1 }
admin: { listen: none }
metrics: { exporter: none }
checkpoint: { interval: 100ms }
source: { memory: {} }
deserializer:
  json: { on_error: fail }
sink: { capture: {} }
"#;

#[derive(Debug, Deserialize)]
enum Status {
    Active,
}

#[derive(Debug, Deserialize)]
struct Login {
    #[expect(dead_code, reason = "decoded, never read")]
    status: Status,
}

/// `serde_json` decodes unless the builder calls `with_simd_json`, whatever
/// features the build turns on.
#[test]
fn an_unknown_variant_fails_the_pipeline_without_quoting_it() {
    assert_eq!(
        refusal(|b| b),
        "malformed payload: malformed: the document does not match the record type \
         at line 1 column 19"
    );
}

/// `simd-json` gives a serde message no position.
#[cfg(feature = "simd-json")]
#[test]
fn under_simd_json_an_unknown_variant_fails_the_pipeline_without_quoting_it() {
    assert_eq!(
        refusal(JsonDeserializerBuilder::with_simd_json),
        "malformed payload: malformed: the document does not match the record type"
    );
}

/// The failure reason a pipeline stops with after the typed deserializer,
/// built through `choose`, refuses one record.
fn refusal(choose: impl FnOnce(JsonDeserializerBuilder) -> JsonDeserializerBuilder) -> String {
    let (source, handle) = memory_source();
    let (sink, _script) = capture_sink(1, 1);
    let pipeline =
        Pipeline::from_config(PipelineConfig::from_str(CONFIG).expect("config")).expect("builder");
    let deser = choose(
        JsonDeserializerBuilder::from_component_config(
            pipeline
                .config()
                .deserializer
                .as_ref()
                .expect("deserializer"),
        )
        .expect("json builder"),
    )
    .build_serde::<Login>();
    let runtime = pipeline
        .sink(sink)
        .expect("sink")
        .chains(move |ctx| {
            let chunk = ctx.chunk();
            chain_owned::<Login, _>(deser.clone())
                .deser_error_policy(ErrorPolicy::Fail)
                .map(|_: Login| b"login".to_vec())
                .sink(TestEncoder, KeyHashRouter, chunk, ctx.queues, ctx.budget)
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
    handle.push(p, None, br#"{"status":"hunter2"}"#);

    let report = run
        .wait_exit(HANG)
        .expect("the pipeline did not stop on the refused record")
        .expect("run");
    let ExitState::Failed(failure) = report.state else {
        panic!("expected a failed exit, got {:?}", report.state);
    };
    failure.reason
}
