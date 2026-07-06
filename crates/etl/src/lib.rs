//! High-performance, at-least-once ETL pipeline framework.
//!
//! `etl` is the facade crate for the `etl-rs` framework and the only crate
//! applications need to depend on. The engine ([`etl_core`]) is re-exported
//! at the root; connectors are enabled through cargo features:
//!
//! | Feature | Enables |
//! |---|---|
//! | `kafka` | [`kafka`] — Kafka source built on `rdkafka` (single consumer, partition-queue lanes) |
//! | `clickhouse` | [`clickhouse`] — ClickHouse sink (RowBinary, dedup tokens, replica rotation) |
//! | `clickhouse-uuid` | `uuid::Uuid` fields for `UUID` columns (`clickhouse::serde::uuid`) |
//! | `clickhouse-chrono` | `chrono` fields for `Date`/`DateTime`/`DateTime64`/`Time` columns |
//! | `clickhouse-time` | `time` crate fields for the same date/time columns |
//! | `clickhouse-rust-decimal` | `rust_decimal::Decimal` conversions for `Decimal` columns |
//! | `avro` | [`avro`] — Avro deserialization (Confluent wire format, schema registry) |
//! | `full` | All connectors (`avro`, `kafka`, `clickhouse`) |
//!
//! # Anatomy of a pipeline
//!
//! One process runs one pipeline (see `docs/DESIGN.md` in the repository
//! for the full architecture and its rationale):
//!
//! ```text
//!                     ┌───────────────────────────────────────────────┐
//!  pinned std thread  │ lane.poll → deserialize (borrowed) → operator │──try_send──▶ per-shard
//!  (× N)              │ chain (map/filter/flat_map, monomorphized)    │             bounded queues
//!                     └───────────────────────────────────────────────┘                  │
//!                          ▲          full? pause lanes, keep polling                    ▼
//!                          │                                            ┌───────────────────────────┐
//!                     ┌──────────┐   acks (never block)                 │ sink workers: merge chunks,│
//!                     │ source   │◀───────────────────────────────────── │ seal batches, rotate       │
//!                     │ control  │   watermarks → store/commit          │ replicas, retry; admin     │
//!                     └──────────┘                                      │ server (/metrics, probes)  │
//!                                                                       └───────────────────────────┘
//! ```
//!
//! Delivery is **at-least-once**: a batch's offsets commit only after every
//! record derived from it was durably written (or intentionally dropped).
//! Duplicates are possible after a crash — design target tables to
//! tolerate replays.
//!
//! # A minimal pipeline
//!
//! Operators are stateful closures chained Flink/Streams-style; the YAML
//! carries tuning and connector configuration. Sketch (see
//! `examples/memory_pipeline.rs` for the complete runnable version and
//! `examples/kafka_avro_to_clickhouse.rs` for the production assembly):
//!
//! ```ignore
//! let config = PipelineConfig::from_path("pipeline.yaml".as_ref())?;
//! let source = KafkaSource::from_component_config(&config.source)?;
//! let (queues, receivers) = shard_queues(num_shards, 8);
//! let budget = Arc::new(InflightBudget::new());
//! let pool = SinkPool::spawn(writer, endpoints, receivers, /* … */);
//!
//! let chains = move |_thread| {
//!     chain_owned::<Order, _>(deserializer.clone())
//!         .with_metrics("orders", "main")
//!         .filter(|o: &Order| o.amount_cents >= 0)
//!         .map(enrich)
//!         .sink(encoder, KeyHashRouter, ChunkConfig::default(),
//!               queues.clone(), budget.clone())
//!         .build()
//! };
//!
//! PipelineRuntime::new(config, source, chains, sink_runtime, budget).run()?;
//! ```
//!
//! # Where things live
//!
//! - [`ops`] — the chain builder and operator combinators.
//! - [`source`] / [`sink`] — the connector traits ([`source::Source`],
//!   [`source::SourceLane`], [`sink::RowEncoder`], [`sink::ShardWriter`])
//!   and the framework-owned sink pool.
//! - [`pipeline`] — the runtime: pinned threads, controller, shutdown.
//! - [`checkpoint`] / [`backpressure`] — acknowledgements and flow control.
//! - [`config`] — YAML with `${VAR:-default}` interpolation and opaque
//!   per-connector sections.
//! - [`metrics`] / [`admin`] / [`telemetry`] — observability (the
//!   [`metrics`](https://crates.io/crates/metrics) facade is the
//!   instrumentation API; see `docs/METRICS.md` for the taxonomy).
//! - Testing your pipelines: the `etl-test` crate (in-memory sources and
//!   sinks with scripting handles).

pub use etl_core::*;

/// Avro deserialization support (Confluent wire format, schema registry).
#[cfg(feature = "avro")]
pub use etl_avro as avro;

/// Kafka source connector.
#[cfg(feature = "kafka")]
pub use etl_kafka as kafka;

/// ClickHouse sink connector.
#[cfg(feature = "clickhouse")]
pub use etl_clickhouse as clickhouse;
