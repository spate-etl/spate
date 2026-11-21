//! The chain's `with_metrics` hands each JSON deserializer its `Meter`, so the
//! JSON drop family carries the same `component` and `component_type` as the
//! chain's deserializer stage and the two join in a query.

use serde::Deserialize;
use spate_core::backpressure::InflightBudget;
use spate_core::checkpoint::Checkpointer;
use spate_core::deser::{Deserializer, Owned};
use spate_core::ops::{ChunkConfig, PushOutcome, chain_owned};
use spate_core::record::PartitionId;
use spate_core::sink::{KeyHashRouter, shard_queues};
use spate_core::source::{LaneId, Source, SourceCtx, SourceEvent, SourceLane};
use spate_json::{JsonDeserializerBuilder, JsonDeserializerConfig, JsonFraming, OnError};
use spate_test::{TestEncoder, memory_source, metric_series, metric_value, render_metrics};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct Event {
    #[expect(dead_code, reason = "decoded to type-check the line, never read")]
    id: i64,
}

fn builder() -> JsonDeserializerBuilder {
    let mut config = JsonDeserializerConfig::default();
    config.framing = JsonFraming::Ndjson;
    config.on_error = OnError::Skip;
    config.reject_duplicate_keys = false;
    JsonDeserializerBuilder::new(config)
}

/// Decode one NDJSON payload with one malformed line through a chain built with
/// `with_metrics(pipeline, "main")`, the way the runtime drives a source.
fn decode_one_poison_line<T, D>(pipeline: &'static str, deser: D)
where
    T: Send + 'static,
    D: Deserializer<Owned<T>> + Send + 'static,
{
    const P0: PartitionId = PartitionId(0);
    const L0: LaneId = LaneId(0);

    let (queues, _rxs) = shard_queues(1, 4096);
    let mut chain = chain_owned::<T, _>(deser)
        .with_metrics(pipeline, "main")
        .map(|_: T| b"row".to_vec())
        .sink(
            TestEncoder,
            KeyHashRouter,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    let mut cp = Checkpointer::new();
    let (mut source, handle) = memory_source();
    source.open(SourceCtx::new(cp.handle())).unwrap();
    cp.begin_epoch(&[P0], 1);
    handle.assign_lanes(&[(L0, P0)]);
    let mut lanes = match source.poll_events(Duration::from_millis(100)).unwrap() {
        SourceEvent::LanesAssigned(lanes) => lanes,
        other => panic!("expected assignment, got {other:?}"),
    };
    handle.push(P0, None, b"{\"id\":1}\nNOT JSON\n{\"id\":3}");
    let mut batch = lanes[0]
        .poll(512, Duration::from_millis(100))
        .unwrap()
        .expect("one batch");
    assert!(matches!(chain.push_batch(&mut batch, 0), PushOutcome::Done));
    assert!(matches!(chain.flush(), PushOutcome::Done));
}

fn assert_family_joins_stage(rendered: &str, pipeline: &str) {
    let labels = [
        ("pipeline", pipeline),
        ("component", "main.deserializer"),
        ("component_type", "json"),
    ];
    let dropped = [labels[0], labels[1], labels[2], ("reason", "malformed")];
    assert_eq!(
        metric_value(rendered, "spate_json_deser_records_dropped_total", &dropped),
        Some(1.0),
        "{rendered}"
    );
    assert!(
        !metric_series(rendered, "spate_deser_records_total", &labels).is_empty(),
        "{rendered}"
    );
}

#[test]
fn a_dynamic_json_drop_joins_the_deser_stage() {
    const PIPELINE: &str = "join-value";
    let rendered = render_metrics(|| {
        decode_one_poison_line::<serde_json::Value, _>(PIPELINE, builder().build_value());
    });
    assert_family_joins_stage(&rendered, PIPELINE);
}

#[test]
fn a_typed_json_drop_joins_the_deser_stage() {
    const PIPELINE: &str = "join-serde";
    let rendered = render_metrics(|| {
        decode_one_poison_line::<Event, _>(PIPELINE, builder().build_serde::<Event>());
    });
    assert_family_joins_stage(&rendered, PIPELINE);
}
