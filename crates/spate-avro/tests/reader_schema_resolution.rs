//! With a `reader_schema` set, a datum whose nesting recurses through the
//! reader schema's unions past the resolution limit is dropped before it is
//! resolved, and every other datum resolves to the value apache-avro's own
//! reader gives.

#![expect(deprecated, reason = "fixtures call the datum free functions directly")]

use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::types::Value;
use apache_avro::{Schema, to_avro_datum};
use spate_avro::{AvroDeserializerBuilder, AvroDeserializerConfig, AvroValue, SchemaSource};
use spate_core::config::PipelineConfig;
use spate_core::deser::{Deserializer, Owned};
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::sink::KeyHashRouter;
use spate_core::source::LaneId;
use spate_test::{
    EmitCollector, HANG, PipelineRun, TestEncoder, capture_sink, confluent_frame, decode_rows,
    memory_source, metric_sum, raw_payload, test_ack, unique_name,
};
use spate_test_support::RegistryStub;

/// A linked list: an ordinary recursive schema.
const LIST: &str = r#"{"type":"record","name":"Node","fields":[{"name":"value","type":"long"},{"name":"next","type":["null","Node"]}]}"#;

/// Nodes in the deep datum: past the nine the resolution limit admits, and
/// within the ten the depth limit admits, so the resolution limit is what
/// refuses it. apache-avro decodes this depth on any thread's stack in an
/// unoptimized build.
const DEEP: usize = 10;

#[derive(Debug, serde::Deserialize)]
struct Node {
    next: Option<Box<Node>>,
}

/// A list of `nodes` nodes: per node a zigzag long `0` and the union index of
/// `Node`, with the `null` branch on the last.
fn list(nodes: usize) -> Vec<u8> {
    let mut datum = Vec::with_capacity(nodes * 2);
    for _ in 1..nodes {
        datum.extend([0x00, 0x02]);
    }
    datum.extend([0x00, 0x00]);
    datum
}

/// A writer schema of maps nested `levels` deep. It does not recurse, and a
/// datum under it can stop at any level.
fn maps_schema(levels: usize) -> String {
    let mut schema = r#"{"type":"map","values":["long","null"]}"#.to_owned();
    for _ in 1..levels {
        schema = format!(r#"{{"type":"map","values":["long","null",{schema}]}}"#);
    }
    schema
}

/// The list as nested maps with the keys `value` and `next`, `nodes` deep.
fn maps(nodes: usize) -> Vec<u8> {
    let mut datum = Vec::new();
    for node in 1..=nodes {
        // One block of two entries: `value` on the long branch, then `next`.
        datum.extend([0x04, 0x0a]);
        datum.extend(b"value");
        datum.extend([0x00, 0x00, 0x08]);
        datum.extend(b"next");
        // The map branch, or `null` on the last node.
        datum.push(if node == nodes { 0x02 } else { 0x04 });
    }
    datum.extend(std::iter::repeat_n(0x00, nodes));
    datum
}

fn value_nodes(value: &AvroValue) -> u64 {
    let mut count = 0;
    let mut at = value;
    loop {
        let AvroValue::Record(fields) = at else {
            panic!("node is {at:?}")
        };
        count += 1;
        match &fields[1].1 {
            AvroValue::Union(_, inner) if matches!(**inner, AvroValue::Null) => return count,
            AvroValue::Union(_, inner) => at = inner,
            other => panic!("next is {other:?}"),
        }
    }
}

fn serde_nodes(node: &Node) -> u64 {
    1 + node.next.as_deref().map_or(0, serde_nodes)
}

/// Run `payloads` through a pipeline whose chain maps each record to its node
/// count. Return the counts the sink was written, and the pipeline's count of
/// records the deserializer dropped.
fn run<T, D>(
    avro: &str,
    build: impl FnOnce(&AvroDeserializerBuilder) -> D,
    nodes: fn(&T) -> u64,
    payloads: &[Vec<u8>],
) -> (Vec<u64>, Option<f64>)
where
    T: Send + 'static,
    D: Deserializer<Owned<T>> + Clone + Send + 'static,
{
    let name = unique_name("reader-resolution");
    let yaml = format!(
        "pipeline: {{ name: {name}, threads: 1, io_threads: 1 }}\n\
         admin: {{ listen: none }}\n\
         metrics: {{ exporter: prometheus }}\n\
         checkpoint: {{ interval: 100ms }}\n\
         source: {{ memory: {{}} }}\n\
         deserializer:\n\
         \x20 avro:\n\
         {avro}\
         \x20   reader_schema:\n\
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
                .map(move |rec: T| nodes(&rec).to_le_bytes().to_vec())
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
    let mut last = 0;
    for payload in payloads {
        last = handle.push(p, None, payload);
    }
    assert!(
        handle.wait_committed(p, last + 1, HANG),
        "the pipeline committed past the deep datum"
    );
    shutdown.trigger();
    running
        .wait_exit(HANG)
        .expect("run returns")
        .expect("clean exit");
    let counts = script
        .writes()
        .iter()
        .flat_map(|w| decode_rows(&w.payload))
        .map(|row| u64::from_le_bytes(row.try_into().unwrap()))
        .collect();
    let dropped = metric_sum(
        &render(),
        "spate_deser_records_dropped_total",
        &[("pipeline", &name)],
    );
    (counts, dropped)
}

fn raw() -> String {
    format!(
        "\x20   mode: raw\n\
         \x20   schema:\n\
         \x20     inline: '{LIST}'\n"
    )
}

#[test]
fn a_list_past_the_resolution_limit_is_dropped_on_the_value_path() {
    let (counts, dropped) = run(
        &raw(),
        |b| b.build_value(),
        value_nodes,
        &[list(1), list(DEEP), list(2)],
    );
    assert_eq!(
        counts,
        [1, 2],
        "the deep list is dropped and the rest delivered"
    );
    assert_eq!(dropped, Some(1.0), "the drop is counted");
}

#[test]
fn a_list_past_the_resolution_limit_is_dropped_on_the_serde_path() {
    let (counts, dropped) = run(
        &raw(),
        |b| b.build_serde::<Node>(),
        serde_nodes,
        &[list(1), list(DEEP), list(2)],
    );
    assert_eq!(
        counts,
        [1, 2],
        "the deep list is dropped and the rest delivered"
    );
    assert_eq!(dropped, Some(1.0), "the drop is counted");
}

#[test]
fn nested_maps_from_a_registry_schema_are_dropped_past_the_resolution_limit() {
    let stub = RegistryStub::start();
    let body = serde_json::json!({ "schema": maps_schema(DEEP) }).to_string();
    stub.script("/schemas/ids/7", 200, body, 0);
    let avro = format!(
        "\x20   mode: confluent\n\
         \x20   registry:\n\
         \x20     url: http://{}\n",
        stub.addr()
    );
    let frame = |nodes| confluent_frame(7, &maps(nodes));
    let (counts, dropped) = run(
        &avro,
        |b| b.build_value(),
        value_nodes,
        &[frame(1), frame(DEEP), frame(2)],
    );
    assert_eq!(
        counts,
        [1, 2],
        "the deep maps are dropped and the rest delivered"
    );
    assert_eq!(dropped, Some(1.0), "the drop is counted");
}

/// `datum` through the value deserializer.
fn spate(writer: &str, reader: &str, datum: &[u8]) -> Result<AvroValue, String> {
    let mut settings = AvroDeserializerConfig::raw(SchemaSource::inline(writer));
    settings.reader_schema = Some(SchemaSource::inline(reader));
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut deser = AvroDeserializerBuilder::new(settings, rt.handle())
        .unwrap()
        .build_value();
    let mut out = EmitCollector::<AvroValue>::new();
    deser
        .deserialize(&raw_payload(datum), &test_ack(), &mut out)
        .map_err(|e| e.to_string())?;
    Ok(out.records.pop().expect("one record").payload)
}

/// `datum` through apache-avro's reader with the reader schema.
fn upstream(writer: &str, reader: &str, datum: &[u8]) -> Result<AvroValue, String> {
    let writer = Schema::parse_str(writer).unwrap();
    let reader = Schema::parse_str(reader).unwrap();
    GenericDatumReader::builder(&writer)
        .reader_schema(&reader)
        .build()
        .unwrap()
        .read_value(&mut &datum[..])
        // The deserializer leaves apache-avro's text out of a refusal, since
        // it prints the value, and keeps only a missing reader field's name.
        .map_err(|e| match e.details() {
            apache_avro::error::Details::GetField(field) => format!(
                "malformed payload: avro datum decode failed: reader field `{field}` is missing from the record"
            ),
            _ => "malformed payload: avro datum decode failed".to_owned(),
        })
}

fn encode(writer: &str, value: Value) -> Vec<u8> {
    to_avro_datum(&Schema::parse_str(writer).unwrap(), value).unwrap()
}

#[test]
fn union_shapes_resolve_as_apache_avro_resolves_them() {
    let inner = r#"{"type":"record","name":"Inner","fields":[{"name":"n","type":"long"}]}"#;
    let record =
        |fields: &str| format!(r#"{{"type":"record","name":"Outer","fields":[{fields}]}}"#);
    let n = |n| Value::Record(vec![("n".into(), Value::Long(n))]);
    let cases: Vec<(String, String, Value)> = vec![
        // A reader union over a writer value that is no union.
        (
            record(&format!(
                r#"{{"name":"a","type":"long"}},{{"name":"b","type":{inner}}}"#
            )),
            record(&format!(
                r#"{{"name":"a","type":["null","long"]}},{{"name":"b","type":["null",{inner}]}}"#
            )),
            Value::Record(vec![("a".into(), Value::Long(7)), ("b".into(), n(8))]),
        ),
        // An array branch and a map branch.
        (
            record(
                r#"{"name":"a","type":["null",{"type":"array","items":"int"}]},{"name":"b","type":["null",{"type":"map","values":"int"}]}"#,
            ),
            record(
                r#"{"name":"a","type":["null",{"type":"array","items":"long"}]},{"name":"b","type":["null",{"type":"map","values":"long"}]}"#,
            ),
            Value::Record(vec![
                (
                    "a".into(),
                    Value::Union(
                        1,
                        Box::new(Value::Array(vec![Value::Int(1), Value::Int(2)])),
                    ),
                ),
                (
                    "b".into(),
                    Value::Union(
                        1,
                        Box::new(Value::Map([("k".to_owned(), Value::Int(3))].into())),
                    ),
                ),
            ]),
        ),
        // A reader field the writer lacks, with a default, under a union of
        // two record branches.
        (
            record(&format!(r#"{{"name":"b","type":{inner}}}"#)),
            record(&format!(
                r#"{{"name":"b","type":["null",{{"type":"record","name":"Other","fields":[{{"name":"m","type":"long"}}]}},{inner}]}},{{"name":"added","type":"string","default":"x"}}"#
            )),
            Value::Record(vec![("b".into(), n(9))]),
        ),
        // A value the reader cannot resolve fails the same way.
        (
            record(r#"{"name":"a","type":"string"}"#),
            record(r#"{"name":"a","type":["null","long"]}"#),
            Value::Record(vec![("a".into(), Value::String("no".into()))]),
        ),
    ];
    for (writer, reader, value) in cases {
        let datum = encode(&writer, value);
        assert_eq!(
            spate(&writer, &reader, &datum),
            upstream(&writer, &reader, &datum)
        );
    }
}

#[test]
fn recursive_shapes_within_the_limit_resolve_as_apache_avro_resolves_them() {
    let namespaced = LIST.replace(r#""name":"Node","#, r#""name":"Node","namespace":"a.b","#);
    assert_eq!(spate(LIST, LIST, &list(9)), upstream(LIST, LIST, &list(9)));
    assert_eq!(
        spate(&namespaced, &namespaced, &list(9)),
        upstream(&namespaced, &namespaced, &list(9))
    );
    let writer = maps_schema(8);
    assert_eq!(
        spate(&writer, LIST, &maps(8)),
        upstream(&writer, LIST, &maps(8))
    );
    assert_eq!(
        spate(LIST, LIST, &list(10)),
        Err(
            "malformed payload: avro datum refused: resolving its nesting against the reader \
             schema's recursive unions exceeds the resolution limit"
                .to_owned()
        )
    );
}

/// The refusal comes before the resolve. This list cannot resolve, because
/// its `value` is a string and the reader's is a long, and the error it
/// fails with is the limit's.
#[test]
fn a_datum_past_the_limit_is_refused_before_it_is_resolved() {
    let strings = LIST.replace(r#""type":"long""#, r#""type":"string""#);
    let mismatch = spate(&strings, LIST, &list(2)).unwrap_err();
    assert!(mismatch.contains("avro datum decode failed"), "{mismatch}");
    let refused = spate(&strings, LIST, &list(DEEP)).unwrap_err();
    assert_eq!(refused, spate(LIST, LIST, &list(DEEP)).unwrap_err());
    assert!(
        refused.contains("exceeds the resolution limit"),
        "{refused}"
    );
}

/// The list with an array of nulls on its first node. A null takes no bytes,
/// so the array adds to the datum's size and nothing to its encoded length.
fn null_padded(nodes: usize) -> (String, Vec<u8>) {
    let writer = LIST.replace(
        r#"{"name":"next","#,
        r#"{"name":"pad","type":{"type":"array","items":"null"},"default":[]},{"name":"next","#,
    );
    fn node(nodes: usize, pad: usize) -> Value {
        let next = match nodes {
            1 => Value::Union(0, Box::new(Value::Null)),
            _ => Value::Union(1, Box::new(node(nodes - 1, 0))),
        };
        Value::Record(vec![
            ("value".into(), Value::Long(0)),
            ("pad".into(), Value::Array(vec![Value::Null; pad])),
            ("next".into(), next),
        ])
    }
    let datum = encode(&writer, node(nodes, 2000));
    (writer, datum)
}

/// The check is given the datum's encoded length. Were it given no cap, the
/// 2,000 nulls would buy the ten nodes their doubling.
#[test]
fn values_that_take_no_bytes_buy_no_doubling_through_the_deserializer() {
    let (writer, datum) = null_padded(10);
    assert_eq!(datum.len(), 32);
    let refused = spate(&writer, LIST, &datum).unwrap_err();
    assert!(
        refused.contains("exceeds the resolution limit"),
        "{refused}"
    );
    let (writer, datum) = null_padded(8);
    assert_eq!(
        spate(&writer, LIST, &datum),
        upstream(&writer, LIST, &datum)
    );
}

/// A later reader field whose alias is `next` does not lift the limit: the
/// resolve still recurses through the field named `next`.
#[test]
fn an_alias_that_shadows_the_recursive_field_does_not_lift_the_limit() {
    let shadowed = LIST.replace(
        "]}",
        r#",{"name":"x","aliases":["next"],"type":"long","default":0}]}"#,
    );
    let admitted = list(9);
    assert_eq!(
        spate(LIST, &shadowed, &admitted),
        upstream(LIST, &shadowed, &admitted)
    );
    let refused = spate(LIST, &shadowed, &list(DEEP)).unwrap_err();
    assert!(
        refused.contains("exceeds the resolution limit"),
        "{refused}"
    );
}

/// Bytes after the datum are not part of it, and buy no doubling either.
#[test]
fn bytes_after_the_datum_buy_no_doubling() {
    let (writer, mut datum) = null_padded(10);
    datum.extend([0u8; 2000]);
    let refused = spate(&writer, LIST, &datum).unwrap_err();
    assert!(
        refused.contains("exceeds the resolution limit"),
        "{refused}"
    );
}
