//! Kafka → chain → **Kafka**: filter/reshape a stream and fan it out to
//! two topics with the producer sink.
//!
//! One `orders` stream carries region-prefixed records (`eu:cust-7:...`,
//! `us:cust-9:...`); the chain routes each to its region's topic through a
//! [`split`](spate::ops::ChainBuilder) terminal — one Kafka sink per
//! destination topic, each a full sink with its own producer, batching,
//! and `spate_sink_*` series. Records with an unknown prefix follow the
//! `unmatched` policy (`Skip`: dropped and counted on
//! `spate_operator_records_dropped_total{reason="unrouted"}`).
//!
//! # Delivery semantics
//!
//! At-least-once, end to end: a source offset commits only once **every**
//! destination that received a derived record has confirmed the delivery
//! reports for it. Duplicates can appear on the output topics after a
//! crash replay or a partially-delivered batch retry — never loss. The
//! sink forces `acks=all` + `enable.idempotence=true`; nothing here is
//! exactly-once (see the delivery-guarantees guide).
//!
//! # Keys
//!
//! Source message keys do not survive deserialization (records carry only
//! the key hash), so this example re-derives the produce key from the
//! payload — [`KafkaBytesEncoder::with_key_fn`] extracts the customer
//! segment, keeping per-customer ordering within each output topic.
//!
//! # Run it
//!
//! Needs Kafka (`KAFKA_BROKERS`) with the three topics (or topic
//! auto-creation). SIGTERM drains gracefully; probes:
//! `curl localhost:9090/readyz`.
//!
//! ```sh
//! cargo run --release -p spate --example kafka_to_kafka_split --features kafka
//! ```

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  production
// INDEX-GOAL:  fan an order stream out to per-region topics
// INDEX-TECH:  Kafka
// INDEX-NEEDS: Kafka

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use spate::deser::BytesPassthrough;
use spate::kafka::sink::KafkaBytesEncoder;
use spate::kafka::{KafkaSource, KafkaSourceConfig};
use spate::prelude::*;
use std::path::Path;

/// Produce key: the customer segment of `region:customer:rest` payloads
/// (a plain `fn` item — the encoder seam takes these so borrowed families
/// work too).
fn customer_key(payload: &[u8]) -> Option<&[u8]> {
    let mut parts = payload.splitn(3, |b| *b == b':');
    let _region = parts.next()?;
    parts.next()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = std::env::var("SPATE_CONFIG")
        .unwrap_or_else(|_| "crates/spate/examples/kafka_to_kafka_split.yaml".to_string());
    let pipeline = Pipeline::from_path(Path::new(&config_path))?;

    // ── Source: Kafka ───────────────────────────────────────────────────
    let source_cfg = KafkaSourceConfig::from_component_config(&pipeline.config().source)?;
    let source = KafkaSource::new(source_cfg);

    // ── Sinks: one Kafka producer per destination topic ─────────────────
    // Each `sinks:` entry is a full `kafka:` sink section (topic, tuning,
    // rdkafka passthrough). `encoder_with` bakes the sink's configured
    // `max_message_bytes` guard into the encoder.
    // ANCHOR: encoder
    let eu_sink = spate::kafka::sink::from_component_config(pipeline.config().sink_config("eu")?)?;
    let us_sink = spate::kafka::sink::from_component_config(pipeline.config().sink_config("us")?)?;
    let eu_enc = eu_sink.encoder_with(KafkaBytesEncoder::with_key_fn(customer_key));
    let us_enc = us_sink.encoder_with(KafkaBytesEncoder::with_key_fn(customer_key));
    // ANCHOR_END: encoder

    // ── The chain, and run ──────────────────────────────────────────────
    let report = pipeline
        .add_sink("eu", eu_sink)?
        .add_sink("us", us_sink)?
        .chains(move |ctx| {
            let mut split = chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .with_metrics(ctx.pipeline.clone(), "main")
                .split(ErrorPolicy::Skip);
            let eu =
                split.add::<Owned<Vec<u8>>, _, _>(eu_enc.clone(), KeyHashRouter, ctx.sink("eu"));
            let us =
                split.add::<Owned<Vec<u8>>, _, _>(us_enc.clone(), KeyHashRouter, ctx.sink("us"));
            split
                .route(move |payload: Vec<u8>, out| {
                    match payload.split(|b| *b == b':').next() {
                        Some(b"eu") => out.emit(eu, payload),
                        Some(b"us") => out.emit(us, payload),
                        // Unknown region → `unmatched` (Skip: counted, dropped).
                        _ => {}
                    }
                })
                .build()
        })
        .run(source)?;

    report.log();
    std::process::exit(report.exit_code());
}
