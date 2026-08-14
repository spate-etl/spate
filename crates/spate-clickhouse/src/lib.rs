//! ClickHouse sink for the Spate framework.
//!
//! Writes **directly to shard-local tables** through the official
//! `clickhouse` crate: rows are encoded to RowBinary on the pipeline
//! threads (this crate's own [serializer](rowbinary)), shipped as
//! pre-formatted frames, and inserted one `INSERT ... FORMAT RowBinary`
//! per sealed batch with a deterministic `insert_deduplication_token`.
//! Direct-to-shard writes beat `Distributed`-table inserts for an ETL
//! writer: bigger blocks, less merge pressure, and a synchronous server
//! acknowledgment that checkpointing depends on (`wait_end_of_query=1`;
//! `write_batch` returning `Ok` covers materialized views too).
//!
//! # ⚠ Deduplication requires a window, and is silently off on plain MergeTree
//!
//! Retries reuse the batch's deduplication token, making them idempotent,
//! **but only if the server keeps a deduplication window**:
//!
//! - `Replicated*MergeTree`: deduplication is on by default
//!   (`replicated_deduplication_window` is non-zero).
//! - **Plain `MergeTree`: the window defaults to `0` and token
//!   deduplication silently does nothing.** Set it explicitly:
//!
//! ```sql
//! CREATE TABLE orders (...) ENGINE = MergeTree ORDER BY id
//! SETTINGS non_replicated_deduplication_window = 100;
//! ```
//!
//! # At-least-once delivery
//!
//! Tokens cover *same-batch retries* (including on another replica after a
//! timeout). They do **not** cover crash replay: after a restart, data is
//! re-batched with different boundaries and different tokens, and replayed
//! rows will land again. Design target tables to tolerate duplicates;
//! `ReplacingMergeTree` with a version column is the sanctioned pattern.
//!
//! Re-homing is a different hazard: a key that moves to another shard
//! (changed shard weights, a changed sharding key, a changed shard count)
//! leaves its already-written rows behind on the old shard, and
//! nothing collapses them there. Dedup tokens are shard-scoped, and
//! `ReplacingMergeTree` merges within one shard's local table only, so
//! the old copies persist indefinitely: unpruned reads through a
//! `Distributed` table return both homes' rows, while pruned reads return
//! only the new home's. Treat a re-homing change as a planned migration,
//! backfilling or deleting the moved keys' old-shard rows, rather than as
//! replay noise that settles. A plain restart with unchanged topology is the
//! benign case: replayed rows land on the *same* shard, where the
//! patterns above apply.
//!
//! # Shard-affinity routing (Distributed parity)
//!
//! [`DistributedRouter`] (built via [`ClickHouseSink::router`]) places each
//! record on the same shard a ClickHouse `Distributed` table with sharding
//! expression `xxHash64(<key column>)` would, so SELECTs through the
//! Distributed table can prune shards (`optimize_skip_unused_shards=1`)
//! while inserts keep going direct-to-local. The ordering contract is the
//! operator's: `shards[i]` in the sink config must be the cluster's
//! `shard_num = i + 1` (the `remote_servers` order), with matching
//! `weight`s. The opt-in `distributed_check` config block verifies shard
//! count, weights, and the DDL's sharding expression at startup
//! ([`ClickHouseSink::validate_distributed`]); the ordering itself it can
//! only warn about. See [`router`] for the algorithm.
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
//!     # Only when inserting into JSON columns (as String fields of JSON
//!     # text — see the rowbinary type table):
//!     settings: { input_format_binary_read_json_as_string: "1" }
//! ```
//!
//! [`config::from_component_config`] turns that section into a
//! [`ClickHouseWriter`] (the framework's `ShardWriter`), per-shard
//! [`ClickHouseEndpoint`]s, and the sink-pool configuration;
//! [`ClickHouseEncoder`] is the matching `RowEncoder`, parameterized by a
//! record family: owned rows use `Owned<Row>`, and the encode path itself
//! needs only `Row: serde::Serialize`.
//!
//! # Wire format
//!
//! The default is row-wise **RowBinary** ([`rowbinary`]). An opt-in columnar
//! **Native** encoder ([`native`], `format: native`) transposes each chunk
//! into one self-describing block: it costs more client CPU but cuts server
//! parse CPU and compressed wire size substantially. Build it from a fetched
//! schema via [`ClickHouseSink::native_schema`] and [`NativeEncoder`].

pub mod config;
mod distributed;
mod encoder;
pub mod native;
pub mod router;
pub mod rowbinary;
mod schema;
pub mod serde;
mod types;
mod writer;

pub use config::{
    ClickHouseSink, ClickHouseSinkConfig, Compression, DistributedCheckSection, Format,
    SchemaValidation, from_component_config,
};
pub use distributed::DistributedCheckError;
pub use encoder::{ClickHouseEncoder, PreEncodedRows};
pub use native::{NativeEncoder, NativeError, NativeSchema};
pub use router::{DistributedRouter, KeyExtractor, ShardKey};
pub use rowbinary::{RowBinaryError, serialize_row};
pub use schema::{RowSchema, SchemaError};
pub use types::*;
pub use writer::{ClickHouseEndpoint, ClickHouseWriter};
