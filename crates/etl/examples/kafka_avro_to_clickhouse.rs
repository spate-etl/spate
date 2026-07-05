//! The flagship pipeline: Kafka → Avro → chain → sharded ClickHouse.
//!
//! This example is the assembly reference — every wiring step a production
//! binary performs, commented. The YAML next to it
//! (`kafka_avro_to_clickhouse.yaml`) carries all tuning; point `ETL_CONFIG`
//! elsewhere to reconfigure without recompiling.
//!
//! # Run it
//!
//! Needs Kafka, a Confluent-compatible schema registry, and ClickHouse
//! (set `KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, `CLICKHOUSE_URL`), plus a
//! target table — note the deduplication window, without which retry
//! idempotency silently does nothing on plain MergeTree:
//!
//! ```sql
//! CREATE TABLE orders (
//!     id           UInt64,
//!     customer     String,
//!     amount_cents Int64,
//!     ts_ms        Int64
//! ) ENGINE = MergeTree ORDER BY id
//!   SETTINGS non_replicated_deduplication_window = 100;
//! ```
//!
//! ```sh
//! cargo run --release -p etl --example kafka_avro_to_clickhouse --features full
//! ```
//!
//! SIGTERM drains gracefully: lanes stop, chains flush, sink batches
//! complete (bounded by `checkpoint.drain_timeout`), offsets commit —
//! at-least-once end to end. Probes: `curl localhost:9090/readyz`.

use etl::avro::AvroDeserializerBuilder;
use etl::backpressure::InflightBudget;
use etl::clickhouse::ClickHouseEncoder;
use etl::config::PipelineConfig;
use etl::error::ErrorPolicy;
use etl::kafka::KafkaSource;
use etl::metrics::{ComponentLabels, SinkShardMetrics};
use etl::ops::{ChunkConfig, chain_owned};
use etl::pipeline::{ExitState, PipelineRuntime, SinkProbeFn, SinkRuntime};
use etl::sink::{KeyHashRouter, ShardWriter, SinkPool, shard_queues};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

/// One record, end to end: `Deserialize` reads it from Avro (field names
/// match the writer schema), `Serialize` writes it as RowBinary — where
/// **field order must match the `columns` list in the YAML** (RowBinary
/// carries no names; order is the wire contract).
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Order {
    id: u64,
    customer: String,
    amount_cents: i64,
    ts_ms: i64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // JSON logs for Kubernetes; RUST_LOG overrides the default filter.
    etl::telemetry::init(etl::telemetry::LogFormat::Json, "info");

    // ── Configuration ───────────────────────────────────────────────────
    let config_path = std::env::var("ETL_CONFIG")
        .unwrap_or_else(|_| "crates/etl/examples/kafka_avro_to_clickhouse.yaml".to_string());
    let config = PipelineConfig::from_path(Path::new(&config_path))?;
    let pipeline_name = config.pipeline.name.clone();

    // ── Connector runtime ───────────────────────────────────────────────
    // Sink workers and the schema-registry fetcher live on this tokio
    // runtime; pipeline threads stay pure CPU.
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.pipeline.io_threads)
        .thread_name("etl-connectors")
        .enable_all()
        .build()?;

    // ── Source: Kafka ───────────────────────────────────────────────────
    // One consumer per process; partitions become lanes fanned across
    // pipeline threads. The `source: { kafka: ... }` section is the
    // connector's own schema.
    let source = KafkaSource::from_component_config(&config.source)?;

    // ── Deserializer: Confluent-framed Avro ─────────────────────────────
    // Schemas come from the registry via an async fetcher on the connector
    // runtime; a cache miss never blocks a pipeline thread — the batch
    // retries once the schema lands.
    let deser_section = config
        .deserializer
        .as_ref()
        .ok_or("this pipeline requires a `deserializer` section")?;
    let avro = AvroDeserializerBuilder::from_component(deser_section, io.handle())?;
    let deserializer = avro.build_serde::<Order>();

    // ── Sink: sharded ClickHouse ────────────────────────────────────────
    // The connector turns its section into a ShardWriter, per-shard replica
    // endpoints, and pool tuning. Rows are encoded to RowBinary on the
    // pipeline threads; workers merge chunks into big batches and write
    // one deduplication-tokened INSERT per batch, rotating replicas.
    let sink = etl::clickhouse::config::from_component_config(&config.sink)?;
    let num_shards = sink.endpoints.len();
    let (queues, receivers) = shard_queues(num_shards, 8);
    let budget = Arc::new(InflightBudget::new());

    let sink_labels = ComponentLabels::new(pipeline_name.clone(), "sink", "clickhouse");
    let shard_metrics = sink
        .endpoints
        .iter()
        .enumerate()
        .map(|(shard, replicas)| {
            let urls: Vec<String> = replicas.iter().map(|e| e.url().to_string()).collect();
            SinkShardMetrics::new(&sink_labels, u32::try_from(shard).unwrap_or(0), &urls)
        })
        .collect();

    let pool = SinkPool::spawn(
        Arc::new(sink.writer),
        sink.endpoints,
        receivers,
        sink.pool,
        Arc::clone(&budget),
        shard_metrics,
        &pipeline_name,
        io.handle(),
    );

    // Readiness: a second, independent client set probes every replica;
    // the runtime flips /readyz sinks-connected accordingly.
    let probe: SinkProbeFn = {
        let probe_sink = etl::clickhouse::config::from_component_config(&config.sink)?;
        let writer = Arc::new(probe_sink.writer);
        let endpoints = Arc::new(probe_sink.endpoints);
        Box::new(move || {
            let writer = Arc::clone(&writer);
            let endpoints = Arc::clone(&endpoints);
            Box::pin(async move {
                for shard in endpoints.iter() {
                    for endpoint in shard {
                        writer.probe(endpoint).await?;
                    }
                }
                Ok(())
            })
        })
    };

    let sink_runtime = SinkRuntime {
        queues: queues.clone(),
        drain: Box::new(move |deadline| {
            Box::pin(async move {
                let r = pool.drain(deadline).await;
                etl::pipeline::DrainReport {
                    flushed_batches: r.flushed,
                    abandoned_batches: r.abandoned,
                }
            })
        }),
        probe: Some(probe),
    };

    // ── The chain ───────────────────────────────────────────────────────
    // One identical chain per pipeline thread. Record-level failures here
    // follow the per-stage policy: Skip counts and continues; Fail stops
    // the pipeline.
    let chains = {
        let queues = queues;
        let budget = Arc::clone(&budget);
        let name = pipeline_name.clone();
        move |_thread: usize| {
            chain_owned::<Order, _>(deserializer.clone())
                .with_metrics(name.clone(), "main")
                .try_map(
                    |order: Order| {
                        if order.amount_cents >= 0 {
                            Ok(order)
                        } else {
                            Err("negative amount")
                        }
                    },
                    ErrorPolicy::Skip,
                )
                .sink(
                    ClickHouseEncoder::<Order>::new(),
                    KeyHashRouter,
                    ChunkConfig::default(),
                    queues.clone(),
                    Arc::clone(&budget),
                )
                .build()
        }
    };

    // ── Run ─────────────────────────────────────────────────────────────
    // Blocks until SIGTERM/SIGINT (drain) or a fatal error. The runtime
    // owns pipeline threads, the controller, metrics, and the admin server.
    let report = PipelineRuntime::new(config, source, chains, sink_runtime, budget).run()?;

    tracing::info!(
        state = ?report.state,
        drain = ?report.sink_drain,
        watermarks = ?report.final_watermarks,
        "pipeline finished"
    );
    match report.state {
        ExitState::Completed => Ok(()),
        ExitState::Failed(failure) => {
            eprintln!(
                "pipeline failed in {}: {}",
                failure.component, failure.reason
            );
            std::process::exit(1);
        }
    }
}
