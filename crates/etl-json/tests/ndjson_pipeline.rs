//! End-to-end: an NDJSON payload decoded through a real operator chain via the
//! public API and `etl-test`'s memory source — one payload → N records, with a
//! malformed line skipped and a filtered record dropped. Driven the way the
//! runtime drives a source (lane poll → `push_batch`), so it is deterministic
//! and needs no threads. This is the template for testing your own JSON
//! pipelines.

use etl_core::backpressure::InflightBudget;
use etl_core::checkpoint::Checkpointer;
use etl_core::ops::{ChunkConfig, PushOutcome, chain_owned};
use etl_core::record::PartitionId;
use etl_core::sink::{KeyHashRouter, shard_queues};
use etl_core::source::{LaneId, Source, SourceCtx, SourceEvent, SourceLane};
use etl_json::{JsonDeserializerBuilder, JsonFraming, JsonSettings, OnError};
use etl_test::{TestEncoder, decode_rows, memory_source};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct Event {
    name: String,
    value: i64,
}

#[test]
fn ndjson_payload_explodes_and_skips_poison_through_the_chain() {
    const P0: PartitionId = PartitionId(0);
    const L0: LaneId = LaneId(0);

    // Build the deserializer through the public builder — NDJSON, skip policy.
    let deser = JsonDeserializerBuilder::from_settings(JsonSettings {
        framing: JsonFraming::Ndjson,
        on_error: OnError::Skip,
        reject_duplicate_keys: false,
    })
    .build_serde::<Event>();

    // Chain: decode → filter → project to a byte row → sink into one shard.
    let (queues, mut rxs) = shard_queues(1, 4096);
    let mut chain = chain_owned::<Event, _>(deser)
        .filter(|e: &Event| e.value >= 0)
        .map(|e: Event| format!("{}={}", e.name, e.value).into_bytes())
        .sink(
            TestEncoder,
            KeyHashRouter,
            ChunkConfig::default(),
            queues,
            Arc::new(InflightBudget::new()),
        )
        .build();

    // Drive it the way the runtime drives a source.
    let mut cp = Checkpointer::new();
    let (mut source, handle) = memory_source();
    source.open(SourceCtx::new(cp.handle())).unwrap();
    cp.begin_epoch(&[P0], 1);
    handle.assign_lanes(&[(L0, P0)]);
    let mut lanes = match source.poll_events(Duration::from_millis(100)).unwrap() {
        SourceEvent::LanesAssigned(lanes) => lanes,
        other => panic!("expected assignment, got {other:?}"),
    };

    // One payload, four NDJSON lines: line 2 malformed (skipped), line 3
    // negative (filtered), lines 1 and 4 survive.
    let payload = concat!(
        "{\"name\":\"alpha\",\"value\":10}\n",
        "NOT JSON\n",
        "{\"name\":\"beta\",\"value\":-5}\n",
        "{\"name\":\"gamma\",\"value\":30}"
    )
    .as_bytes();
    handle.push(P0, None, payload);

    let mut batch = lanes[0]
        .poll(512, Duration::from_millis(100))
        .unwrap()
        .expect("one batch");
    // Skip policy never fails the batch: the malformed line is dropped.
    assert!(
        matches!(chain.push_batch(&mut batch, 0), PushOutcome::Done),
        "skip policy must complete the batch"
    );
    assert!(matches!(chain.flush(), PushOutcome::Done));

    let mut frames = Vec::new();
    while let Ok(chunk) = rxs[0].try_recv() {
        frames.extend_from_slice(&chunk.frame);
    }
    let rows: Vec<String> = decode_rows(&frames)
        .into_iter()
        .map(|r| String::from_utf8(r).unwrap())
        .collect();
    assert_eq!(rows, vec!["alpha=10".to_string(), "gamma=30".to_string()]);
}
