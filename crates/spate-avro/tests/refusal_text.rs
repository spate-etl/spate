//! A record an Avro deserializer refuses stops a Fail-policy pipeline with a
//! failure reason that holds none of the record.

use serde::Deserialize;
use spate_avro::{AvroDeserializerBuilder, AvroValue};
use spate_core::config::PipelineConfig;
use spate_core::deser::{Deserializer, Owned};
use spate_core::error::ErrorPolicy;
use spate_core::ops::chain_owned;
use spate_core::pipeline::{ExitState, Pipeline, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::sink::KeyHashRouter;
use spate_core::source::LaneId;
use spate_test::{HANG, PipelineRun, TestEncoder, capture_sink, memory_source};

const SCHEMA: &str =
    r#"{"type":"record","name":"Login","fields":[{"name":"status","type":"string"}]}"#;
const READER_WITH_REGION: &str = r#"{"type":"record","name":"Login","fields":[{"name":"status","type":"string"},{"name":"region","type":"string"}]}"#;

fn config(name: &str, reader: Option<&str>) -> String {
    let reader = reader.map_or(String::new(), |schema| {
        format!("    reader_schema:\n      inline: '{schema}'\n")
    });
    format!(
        r#"
pipeline: {{ name: {name}, threads: 1, io_threads: 1 }}
admin: {{ listen: none }}
metrics: {{ exporter: none }}
checkpoint: {{ interval: 100ms }}
source: {{ memory: {{}} }}
deserializer:
  avro:
    mode: raw
    schema:
      inline: '{SCHEMA}'
{reader}sink: {{ capture: {{}} }}
"#
    )
}

#[derive(Debug, Deserialize)]
enum Status {
    Active,
}

#[derive(Debug, Deserialize)]
struct Login {
    #[expect(dead_code, reason = "decoded, never read")]
    status: Status,
}

/// The datum `{status: "hunter2"}`: a zigzag length and the bytes.
fn datum() -> Vec<u8> {
    let mut d = vec![14];
    d.extend(b"hunter2");
    d
}

/// Runs the datum through a Fail-policy pipeline over the deserializer
/// `build` makes, and returns the failure reason.
fn failure_reason<T, D>(
    name: &str,
    reader: Option<&str>,
    build: impl FnOnce(&AvroDeserializerBuilder) -> D,
) -> String
where
    T: Send + 'static,
    D: Deserializer<Owned<T>> + Clone + Send + 'static,
{
    let (source, handle) = memory_source();
    let (sink, _script) = capture_sink(1, 1);
    let pipeline =
        Pipeline::from_config(PipelineConfig::from_str(&config(name, reader)).expect("config"))
            .expect("builder");
    let builder = AvroDeserializerBuilder::from_component_config(
        pipeline
            .config()
            .deserializer
            .as_ref()
            .expect("deserializer"),
        &pipeline.io_handle(),
    )
    .expect("avro builder");
    let deser = build(&builder);
    let runtime = pipeline
        .sink(sink)
        .expect("sink")
        .chains(move |ctx| {
            let chunk = ctx.chunk();
            chain_owned::<T, _>(deser.clone())
                .deser_error_policy(ErrorPolicy::Fail)
                .map(|_: T| b"login".to_vec())
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
    handle.push(p, None, &datum());
    let report = run
        .wait_exit(HANG)
        .expect("the pipeline did not stop on the refused record")
        .expect("run");
    let ExitState::Failed(failure) = report.state else {
        panic!("expected a failed exit, got {:?}", report.state);
    };
    failure.reason
}

/// The single-pass datum deserializer names the variants the type has.
#[test]
fn the_datum_path_does_not_quote_an_unknown_symbol() {
    let reason = failure_reason::<Login, _>("avro-refusal-datum", None, |b| {
        b.build_serde_datum::<Login>().expect("datum")
    });
    assert_eq!(
        reason,
        "malformed payload: avro datum decode failed: unknown variant, expected one of: `Active`"
    );
}

/// The two-pass serde deserializer.
#[test]
fn the_serde_path_does_not_quote_an_unknown_symbol() {
    let reason =
        failure_reason::<Login, _>("avro-refusal-serde", None, |b| b.build_serde::<Login>());
    assert_eq!(
        reason,
        "malformed payload: avro record does not match the target type"
    );
}

/// A reader field the record lacks is named, and the record's own field is
/// not quoted.
#[test]
fn a_missing_reader_field_is_named() {
    let reason =
        failure_reason::<AvroValue, _>("avro-refusal-reader", Some(READER_WITH_REGION), |b| {
            b.build_value()
        });
    assert_eq!(
        reason,
        "malformed payload: avro datum decode failed: reader field `region` is missing from the \
         record"
    );
}
