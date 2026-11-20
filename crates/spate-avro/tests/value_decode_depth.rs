//! A datum nested past the decode depth limit is dropped on the value and
//! serde paths, counted, and the pipeline goes on to the next record.
//!
//! The tests share the process-wide metrics recorder, so each pipeline has a
//! name of its own.

use spate_avro::{AvroDeserializerBuilder, AvroValue};
use spate_core::config::PipelineConfig;
use spate_core::deser::Deserializer;
use spate_core::deser::Owned;
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::sink::KeyHashRouter;
use spate_core::source::LaneId;
use spate_test::{
    HANG, PipelineRun, TestEncoder, capture_sink, decode_rows, memory_source, metric_sum,
    unique_name,
};

/// A linked list: an ordinary recursive schema.
const LIST: &str = r#"{"type":"record","name":"Node","fields":[{"name":"value","type":"long"},{"name":"next","type":["null","Node"]}]}"#;

#[derive(Debug, serde::Deserialize)]
struct Node {
    value: i64,
    #[expect(dead_code, reason = "decoded for its depth")]
    next: Option<Box<Node>>,
}

/// A list of `depth` nodes whose head carries `head`: zigzag longs, the union
/// index of `Node` per link, then the trailing `null` branch.
fn list(depth: usize, head: u8) -> Vec<u8> {
    let mut datum = vec![head << 1, 0x02];
    for _ in 1..depth {
        datum.extend([0x00, 0x02]);
    }
    datum.extend([0x00, 0x00]);
    datum
}

fn head_of(value: &AvroValue) -> i64 {
    match value {
        AvroValue::Record(fields) => match fields[0].1 {
            AvroValue::Long(n) => n,
            ref other => panic!("value field is {other:?}"),
        },
        other => panic!("datum is {other:?}"),
    }
}

/// Run `list(1, 1)`, a 50,000-node list and `list(1, 2)` through a pipeline
/// whose chain maps each record to its head value. Return the heads the sink
/// was written, and the pipeline's count of records the deserializer dropped.
fn run<T, D>(
    build: impl FnOnce(&AvroDeserializerBuilder) -> D,
    head: fn(&T) -> i64,
) -> (Vec<i64>, Option<f64>)
where
    T: Send + 'static,
    D: Deserializer<Owned<T>> + Clone + Send + 'static,
{
    let name = unique_name("value-depth");
    let yaml = format!(
        "pipeline: {{ name: {name}, threads: 1, io_threads: 1 }}\n\
         admin: {{ listen: none }}\n\
         metrics: {{ exporter: prometheus }}\n\
         checkpoint: {{ interval: 100ms }}\n\
         source: {{ memory: {{}} }}\n\
         deserializer:\n\
         \x20 avro:\n\
         \x20   mode: raw\n\
         \x20   schema:\n\
         \x20     inline: '{LIST}'\n\
         sink: {{ capture: {{}} }}\n"
    );
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(&yaml).unwrap()).unwrap();
    let render = pipeline.metrics().render_fn();
    let section = pipeline.config().deserializer.clone().unwrap();
    let deser = build(
        &AvroDeserializerBuilder::from_component_config(&section, &pipeline.io_handle()).unwrap(),
    );
    let (source, handle) = memory_source();
    let (sink, script) = capture_sink(1, 1);
    let runtime = pipeline
        .sink(sink)
        .unwrap()
        .chains(move |ctx| {
            let chunk = ctx.chunk();
            chain_owned::<T, _>(deser.clone())
                .with_metrics(ctx.pipeline.clone(), "main")
                .map(move |rec: T| head(&rec).to_le_bytes().to_vec())
                .sink(TestEncoder, KeyHashRouter, chunk, ctx.queues, ctx.budget)
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        })
        .into_runtime(source)
        .unwrap();
    let shutdown = runtime.shutdown_handle();
    let running = PipelineRun::spawn(move || runtime.run());

    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);
    handle.push(p, None, &list(1, 1));
    handle.push(p, None, &list(50_000, 0));
    let last = handle.push(p, None, &list(1, 2));
    assert!(
        handle.wait_committed(p, last + 1, HANG),
        "the pipeline committed past the deep datum"
    );
    shutdown.trigger();
    running
        .wait_exit(HANG)
        .expect("run returns")
        .expect("clean exit");
    let heads = script
        .writes()
        .iter()
        .flat_map(|w| decode_rows(&w.payload))
        .map(|row| i64::from_le_bytes(row.try_into().unwrap()))
        .collect();
    let dropped = metric_sum(
        &render(),
        "spate_deser_records_dropped_total",
        &[("pipeline", &name)],
    );
    (heads, dropped)
}

#[test]
fn a_datum_past_the_depth_limit_is_dropped_on_the_value_path() {
    let (heads, dropped) = run(|b| b.build_value(), head_of);
    assert_eq!(
        heads,
        [1, 2],
        "the deep datum is dropped and the rest delivered"
    );
    assert_eq!(dropped, Some(1.0), "the drop is counted");
}

#[test]
fn a_datum_past_the_depth_limit_is_dropped_on_the_serde_path() {
    let (heads, dropped) = run(|b| b.build_serde::<Node>(), |n: &Node| n.value);
    assert_eq!(
        heads,
        [1, 2],
        "the deep datum is dropped and the rest delivered"
    );
    assert_eq!(dropped, Some(1.0), "the drop is counted");
}
