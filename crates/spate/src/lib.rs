//! High-performance, at-least-once ETL pipeline framework.
//!
//! `spate` is the facade crate for the Spate framework and the only crate
//! applications need to depend on. The engine ([`spate_core`]) is re-exported
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
//! | `coordination` | [`coordination`] backend — multi-instance leader-assigned work distribution for broker-less sources: the protocol, `StoreCoordinator`, and the in-memory store (zero-infrastructure embedding). The seam types and the `CoordinationDriver` live in `spate::coordination` without any feature |
//! | `coordination-nats` | The production NATS JetStream KV store (server >= 2.11) on top of [`coordination`]; pulls the async-nats dependency tree |
//! | `full` | All connectors (`avro`, `json`, `kafka`, `clickhouse`, `coordination-nats`, `s3`) |
//!
//! # Anatomy of a pipeline
//!
//! One process runs one pipeline (the [user guide] carries the full
//! architecture and its rationale):
//!
//! [user guide]: https://spate.kainth.dev/docs/user-guide/
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
//! Operators are stateful closures chained into a graph; the YAML
//! carries tuning and connector configuration; [`pipeline::Pipeline`]
//! assembles and runs the process. This compiles and runs against the
//! `spate-test` mocks — swap in `KafkaSource::from_component_config` and a
//! ClickHouse sink for the production version (see
//! `examples/kafka_avro_to_clickhouse.rs`):
//!
//! ```
//! use spate::prelude::*;
//! use spate_test::{BytesPassthrough, TestEncoder, capture_sink, memory_source};
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
//! handle.assign_lanes(&[(spate::source::LaneId(0), PartitionId(0))]);
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
//! - Testing your pipelines: the `spate-test` crate (in-memory sources and
//!   sinks with scripting handles).

pub use spate_core::*;

/// Curated imports for pipeline assemblies: one `use spate::prelude::*;`
/// brings in the builder, chain entry points, and the types every
/// assembly touches.
///
/// Connector constructors are deliberately excluded — import those from
/// their feature-gated modules ([`kafka`], [`clickhouse`], [`avro`]).
/// Additions to this module are semver-additive; nothing is ever removed.
pub mod prelude {
    pub use spate_core::config::PipelineConfig;
    pub use spate_core::deser::{Owned, RecFamily};
    pub use spate_core::error::ErrorPolicy;
    pub use spate_core::ops::{ChunkConfig, chain, chain_owned};
    pub use spate_core::pipeline::{
        BuildError, ChainCtx, ExitReport, ExitState, Pipeline, PipelineError, PipelineRuntime,
        RuntimeOptions, ShutdownHandle, SinkOptions,
    };
    pub use spate_core::record::PartitionId;
    pub use spate_core::sink::{
        KeyHashRouter, RetryConfig, RetryConfigError, SinkBundle, SinkParts, SinkPoolConfig,
    };
    pub use spate_core::telemetry::LogFormat;
}

/// Avro deserialization support (Confluent wire format, schema registry).
#[cfg(feature = "avro")]
pub use spate_avro as avro;

/// JSON deserialization support (single-document, NDJSON, top-level array).
#[cfg(feature = "json")]
pub use spate_json as json;

/// Kafka source and producer-sink connector.
#[cfg(feature = "kafka")]
pub use spate_kafka as kafka;

/// ClickHouse sink connector.
#[cfg(feature = "clickhouse")]
pub use spate_clickhouse as clickhouse;

/// Bounded object-storage (S3) backfill source.
#[cfg(feature = "s3")]
pub use spate_s3 as s3;

// Without the feature, `spate::coordination` resolves to the seam module
// (traits, events, the driver) through the `spate_core::*` glob above. With
// it, this explicit re-export shadows the glob with the backend crate —
// which itself re-exports the same seam types — so the one path serves
// both worlds and the backend types simply appear alongside.
/// Multi-instance leader-assigned coordination backend (NATS JetStream KV
/// + in-memory store). The seam and driver need no feature.
#[cfg(feature = "coordination")]
pub use spate_coordination as coordination;

/// Compile-checks the README's example, so it cannot drift from the builder
/// API the way an `ignore`d block silently did.
///
/// `cfg(doctest)` is stripped before macro expansion, so `include_str!` never
/// runs in an ordinary build — the README escaping this crate's directory
/// cannot break `cargo publish`, and the file is not appended to the rendered
/// documentation.
#[cfg(doctest)]
#[doc = include_str!("../../../README.md")]
pub struct ReadmeDoctests;
