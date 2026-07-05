//! ClickHouse sink for the `etl-rs` framework.
//!
//! Writes **directly to shard-local tables** through the official
//! `clickhouse` crate: rows are encoded to RowBinary on the pipeline
//! threads (this crate's own [serializer](rowbinary)), shipped as
//! pre-formatted frames, and inserted one `INSERT ... FORMAT RowBinary`
//! per sealed batch with a deterministic `insert_deduplication_token`.
//! Direct-to-shard writes beat `Distributed`-table inserts for an ETL
//! writer: bigger blocks, less merge pressure, and — crucially for
//! checkpointing — a synchronous server acknowledgement
//! (`wait_end_of_query=1`; `write_batch` returning `Ok` covers
//! materialized views too).
//!
//! # ⚠ Deduplication requires a window — silently off on plain MergeTree
//!
//! Retries reuse the batch's deduplication token, making them idempotent —
//! **but only if the server keeps a deduplication window**:
//!
//! - `Replicated*MergeTree`: deduplication is on by default
//!   (`replicated_deduplication_window = 100`).
//! - **Plain `MergeTree`: the window defaults to `0` and token
//!   deduplication silently does nothing.** Set it explicitly:
//!
//! ```sql
//! CREATE TABLE orders (...) ENGINE = MergeTree ORDER BY id
//! SETTINGS non_replicated_deduplication_window = 100;
//! ```
//!
//! # At-least-once, honestly
//!
//! Tokens cover *same-batch retries* (including on another replica after a
//! timeout). They do **not** cover crash replay: after a restart, data is
//! re-batched with different boundaries and different tokens, and replayed
//! rows will land again. Design target tables to tolerate duplicates —
//! `ReplacingMergeTree` with a version column is the sanctioned pattern.
//!
//! # Column order is the wire contract
//!
//! RowBinary carries no column names. The configured `columns` list and
//! the row struct's **field declaration order** must match; reordering
//! either is a breaking change to the pipeline. See [`rowbinary`] for the
//! full type mapping.
//!
//! # Wiring
//!
//! ```yaml
//! sink:
//!   clickhouse:
//!     table: orders_local
//!     columns: [id, name, amount]
//!     shards:
//!       - replicas: ["http://ch-0-0:8123", "http://ch-0-1:8123"]
//!       - replicas: ["http://ch-1-0:8123", "http://ch-1-1:8123"]
//! ```
//!
//! [`config::from_component_config`] turns that section into a
//! [`ClickHouseWriter`] (the framework's `ShardWriter`), per-shard
//! [`ClickHouseEndpoint`]s, and the sink-pool configuration;
//! [`ClickHouseEncoder`] is the matching `RowEncoder` for any
//! `T: serde::Serialize`.

pub mod config;
mod encoder;
pub mod rowbinary;
mod writer;

pub use config::{ClickHouseSink, ClickHouseSinkConfig, from_component_config};
pub use encoder::{ClickHouseEncoder, PreEncodedRows};
pub use rowbinary::{DateTime64Millis, DateTimeSeconds, RowBinaryError, serialize_row};
pub use writer::{ClickHouseEndpoint, ClickHouseWriter};
