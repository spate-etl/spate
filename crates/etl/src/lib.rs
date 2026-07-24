//! High-performance, at-least-once ETL pipeline framework.
//!
//! `etl` is the facade crate for the `etl-rs` framework and the only crate
//! applications need to depend on. The engine ([`etl_core`]) is re-exported
//! at the root; connectors are enabled through cargo features:
//!
//! | Feature | Enables |
//! |---|---|
//! | `kafka` | [`kafka`] — Kafka source built on `rdkafka` (single consumer, partition-queue lanes) |
//! | `s3` | [`s3`] — coordinated bounded object-storage (S3) backfill source: the fleet's leader plans the prefix into splits, workers lease them through an injected coordinator (solo in-process by default; progress is ephemeral without a durable store), and the job self-terminates once the plan is final and every split completes (`refresh_listing` keeps it open) |
//! | `clickhouse` | [`clickhouse`] — ClickHouse sink (RowBinary, dedup tokens, replica rotation) |
//! | `clickhouse-uuid` | `uuid::Uuid` fields for `UUID` columns (`clickhouse::serde::uuid`) |
//! | `clickhouse-chrono` | `chrono` fields for `Date`/`DateTime`/`DateTime64`/`Time` columns |
//! | `clickhouse-time` | `time` crate fields for the same date/time columns |
//! | `clickhouse-rust-decimal` | `rust_decimal::Decimal` conversions for `Decimal` columns |
//! | `avro` | [`avro`] — Avro deserialization (Confluent wire format, schema registry) |
//! | `json` | [`json`] — JSON deserialization (single-document, NDJSON, top-level array) |
//! | `json-float-roundtrip`, `json-arbitrary-precision`, `json-raw-value` | Opt-in `serde_json` fidelity knobs for [`json`]. Not part of `full` — `arbitrary-precision` is crate-wide |
//! | `coordination` | [`coordination`] backend — multi-instance leader-assigned work distribution for broker-less sources: the protocol, `StoreCoordinator`, and the in-memory store (zero-infrastructure embedding). The seam types and the `CoordinationDriver` live in `etl::coordination` without any feature |
//! | `coordination-nats` | The production NATS JetStream KV store (server >= 2.11) on top of [`coordination`]; pulls the async-nats dependency tree |
//! | `full` | All connectors (`avro`, `json`, `kafka`, `clickhouse`, `coordination-nats`, `s3`) |
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
//! carries tuning and connector configuration; [`pipeline::Pipeline`]
//! assembles and runs the process. This compiles and runs against the
//! `etl-test` mocks — swap in `KafkaSource::from_component_config` and a
//! ClickHouse sink for the production version (see
//! `examples/kafka_avro_to_clickhouse.rs`):
//!
//! ```
//! use etl::prelude::*;
//! use etl_test::{BytesPassthrough, TestEncoder, capture_sink, memory_source};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let config = PipelineConfig::from_str(
//!     "pipeline: { name: demo, threads: 1, io_threads: 1 }\n\
//!      checkpoint: { interval: 100ms }\n\
//!      metrics: { exporter: none }\n\
//!      source: { memory: {} }\n\
//!      sink: { capture: {} }",
//! )?;
//! let (source, handle) = memory_source();
//! let (sink, script) = capture_sink(1, 1);
//!
//! let runtime = Pipeline::from_config(config)?
//!     .sink(sink)?
//!     .chains(|ctx| {
//!         // The sink's YAML `chunk:` block (or the default), bound before
//!         // `with_metrics` moves `ctx.pipeline`.
//!         let chunk_cfg = ctx.chunk();
//!         chain_owned::<Vec<u8>, _>(BytesPassthrough)
//!             .with_metrics(ctx.pipeline, "main")
//!             .filter(|payload: &Vec<u8>| !payload.is_empty())
//!             .sink(TestEncoder, KeyHashRouter, chunk_cfg,
//!                   ctx.queues, ctx.budget)
//!             .build()
//!     })
//!     .runtime_options(RuntimeOptions { handle_signals: false, ..Default::default() })
//!     .into_runtime(source)?;
//!
//! // Drive it: assign a lane, produce, wait for the durable commit.
//! let shutdown = runtime.shutdown_handle();
//! let join = std::thread::spawn(move || runtime.run());
//! handle.assign_lanes(&[(etl::source::LaneId(0), PartitionId(0))]);
//! let last = handle.push(PartitionId(0), None, b"hello");
//! while handle.last_committed(PartitionId(0)) != Some(last + 1) {
//!     std::thread::sleep(std::time::Duration::from_millis(5));
//! }
//! shutdown.trigger();
//! let report = join.join().expect("join")?;
//! assert_eq!(report.exit_code(), 0);
//! assert!(!script.writes().is_empty());
//! # Ok(())
//! # }
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

/// Curated imports for pipeline assemblies: one `use etl::prelude::*;`
/// brings in the builder, chain entry points, and the types every
/// assembly touches.
///
/// Connector constructors are deliberately excluded — import those from
/// their feature-gated modules ([`kafka`], [`clickhouse`], [`avro`]).
/// Additions to this module are semver-additive; nothing is ever removed.
pub mod prelude {
    pub use etl_core::config::PipelineConfig;
    pub use etl_core::deser::{Owned, RecFamily};
    pub use etl_core::error::ErrorPolicy;
    pub use etl_core::ops::{ChunkConfig, chain, chain_owned};
    pub use etl_core::pipeline::{
        BuildError, ChainCtx, ExitReport, ExitState, Pipeline, PipelineError, PipelineRuntime,
        RuntimeOptions, ShutdownHandle, SinkOptions,
    };
    pub use etl_core::record::PartitionId;
    pub use etl_core::sink::{
        KeyHashRouter, RetryConfig, RetryConfigError, SinkBundle, SinkParts, SinkPoolConfig,
    };
    pub use etl_core::telemetry::LogFormat;
}

/// Avro deserialization support (Confluent wire format, schema registry).
#[cfg(feature = "avro")]
pub use etl_avro as avro;

/// JSON deserialization support (single-document, NDJSON, top-level array).
#[cfg(feature = "json")]
pub use etl_json as json;

/// Kafka source and producer-sink connector.
#[cfg(feature = "kafka")]
pub use etl_kafka as kafka;

/// ClickHouse sink connector.
#[cfg(feature = "clickhouse")]
pub use etl_clickhouse as clickhouse;

/// Bounded object-storage (S3) backfill source.
#[cfg(feature = "s3")]
pub use etl_s3 as s3;

// Without the feature, `etl::coordination` resolves to the seam module
// (traits, events, the driver) through the `etl_core::*` glob above. With
// it, this explicit re-export shadows the glob with the backend crate —
// which itself re-exports the same seam types — so the one path serves
// both worlds and the backend types simply appear alongside.
/// Multi-instance leader-assigned coordination backend (NATS JetStream KV
/// + in-memory store). The seam and driver need no feature.
#[cfg(feature = "coordination")]
pub use etl_coordination as coordination;
