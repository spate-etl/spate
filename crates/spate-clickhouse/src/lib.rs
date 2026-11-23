#![cfg_attr(docsrs, feature(doc_cfg))]
// `loom` is never set in a published build, so its cfg carries no badge.
#![cfg_attr(docsrs, doc(auto_cfg(hide(loom))))]
//! ClickHouse sink for the Spate framework.
//!
//! Writes **directly to shard-local tables** through the official
//! `clickhouse` crate: rows are encoded to RowBinary on the pipeline
//! threads (this crate's own serializer), shipped as
//! pre-formatted frames, and inserted one `INSERT ... FORMAT
//! RowBinaryWithNamesAndTypes` per sealed batch with a deterministic
//! `insert_deduplication_token`.
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
//! timeout). They do **not** cover crash replay: every token carries an id
//! minted once per process run, so after a restart replayed rows carry
//! different tokens and land again. Two runs' ids colliding is improbable;
//! inside the deduplication window, a collision makes the server discard the
//! later run's rows as repeats. Design target tables to tolerate duplicates;
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
//! [`ClickHouseDistributedRouter`] (built via [`ClickHouseSink::router`])
//! places each record on the same shard a ClickHouse `Distributed` table with
//! sharding expression `xxHash64(<key column>)` would, so SELECTs through the
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
//! The rows carry no column names: the row struct's **field declaration
//! order** is the insert column order, and reordering it is a breaking
//! change to the pipeline. `#[derive(ClickHouseRow)]` generates the column
//! list from that order, honoring `#[serde(rename = "...")]` for a name no
//! Rust identifier can spell (a flattened `Nested` table's dotted
//! `outer.inner`). [Row types](#row-types) has the full type mapping.
//!
//! Each request body opens with a header naming those columns and the types
//! `system.columns` reported for them, so the server checks the description
//! against the table on every insert and rejects one that has drifted.
//!
//! # Wiring
//!
//! ```yaml
//! sink:
//!   clickhouse:
//!     table: orders_local
//!     shards:
//!       - replicas: ["http://ch-0-0:8123", "http://ch-0-1:8123"]
//!       - replicas: ["http://ch-1-0:8123", "http://ch-1-1:8123"]
//!     # Only when inserting into JSON columns (as String fields of JSON
//!     # text; see the row type table below):
//!     settings: { input_format_binary_read_json_as_string: "1" }
//! ```
//!
//! ```rust,ignore
//! #[derive(Serialize, ClickHouseRow)]
//! struct OrderRow { id: u64, name: String, amount: f64 }
//!
//! let sink = pipeline.block_on(
//!     spate_clickhouse::ClickHouseSinkBuilder::from_component_config(section)?
//!         .connect::<Owned<OrderRow>>(),
//! )?;
//! let encoder = sink.encoder::<Owned<OrderRow>>();
//! ```
//!
//! [`ClickHouseSinkBuilder::from_component_config`] turns that section into a
//! [`ClickHouseSinkBuilder`], holding per-shard
//! [`ClickHouseEndpoint`]s and the sink-pool configuration but no row type
//! yet; [`ClickHouseSinkBuilder::connect`] supplies it, reads the
//! table's columns from every replica, and generates the
//! [`ClickHouseWriter`] (the framework's `ShardWriter`) from both.
//! [`ClickHouseSink::encoder`] hands out the matching `RowEncoder`, a
//! [`ClickHouseFormatEncoder`], parameterized by a record family: owned rows
//! use `Owned<Row>`, and the encode path itself needs only
//! `Row: serde::Serialize`.
//!
//! # Wire format
//!
//! The default is row-wise **RowBinary**, sent as
//! `RowBinaryWithNamesAndTypes`: one header per request body, then the rows.
//! The opt-in columnar **Native** format (`format: native`) transposes each
//! chunk into one self-describing block instead: it costs more client CPU but
//! cuts server parse CPU and compressed wire size substantially.
//!
//! The encoder from [`ClickHouseSink::encoder`] writes whichever format the
//! sink's `format` names. Under `native`, [`ClickHouseSinkBuilder::connect`]
//! refuses a declared column whose type the Native format does not support.
//!
//! # Row types
//!
//! How each Rust or serde type encodes in RowBinary:
//!
//! | Rust / serde type | RowBinary encoding |
//! |---|---|
//! | `bool` | 1 byte, `0`/`1` |
//! | `i8..i128`, `u8..u128` | little-endian fixed width |
//! | `f32`, `f64` | little-endian IEEE 754 |
//! | `String`, `&str` | LEB128 byte length + UTF-8 bytes (`String`) |
//! | `serde_bytes`-style `&[u8]` | LEB128 length + bytes (`String`) |
//! | `Option<T>` (T not itself `Option`) | 1 prefix byte: `1` = NULL, `0` = value follows (`Nullable(T)`) |
//! | `Vec<T>` / slices (seq) | LEB128 element count + elements (`Array(T)`) |
//! | tuples, `[T; N]`, tuple structs | elements back-to-back, no length (`Tuple`, `FixedString(N)` for `[u8; N]`) |
//! | structs | fields in **declaration order**, no header |
//! | maps | LEB128 entry count + alternating key, value (`Map(K, V)`) |
//! | newtype structs | transparent |
//! | newtype enum variants | 1 discriminant byte (serde `variant_index`) + value (`Variant`) |
//! | `char`, `()`, unit structs/variants, struct/tuple variants, `Option<Option<T>>` | **unsupported** — record-level error |
//!
//! ## Column-type coverage beyond the primitives
//!
//! | ClickHouse type | Rust field |
//! |---|---|
//! | `Date` / `Date32` | [`DateDays`] / [`Date32Days`], or [⚠](crate::serde) `chrono::NaiveDate` / `time::Date` via [`crate::serde`] (feature-gated) |
//! | `DateTime` | [`DateTimeSeconds`], or [⚠](crate::serde) `chrono::DateTime<Utc>` / `time::OffsetDateTime` via [`crate::serde`] |
//! | `DateTime64(0/3/6/9)` | [`DateTime64Secs`] / [`DateTime64Millis`] / [`DateTime64Micros`] / [`DateTime64Nanos`], or [⚠](crate::serde) the `datetime64::*` serde modules |
//! | `Time` / `Time64(p)` | [`TimeSeconds`] / [`Time64Secs`]-family, or [⚠](crate::serde) the `time`/`time64::*` serde modules (server ≥ 25.6; where required, set `enable_time_time64_type=1` for `CREATE TABLE`; inserts need no such setting) |
//! | `UUID` | [⚠](crate::serde) `uuid::Uuid` with `#[serde(with = "spate_clickhouse::serde::uuid")]` (feature `uuid`) |
//! | `IPv4` | [⚠](crate::serde) `std::net::Ipv4Addr` with `#[serde(with = "spate_clickhouse::serde::ipv4")]` |
//! | `IPv6` | `std::net::Ipv6Addr` bare (its default impl — 16 network-order bytes — is correct) |
//! | `Enum8` / `Enum16` | `serde_repr` enums: `#[derive(Serialize_repr)] #[repr(i8)]` (or `i16`) |
//! | `Decimal(P, S)` | [`Decimal32<S>`](crate::Decimal32) for precision 9 and below, [`Decimal64<S>`](crate::Decimal64) for 10 through 18, [`Decimal128<S>`](crate::Decimal128) for 19 through 38 (or the matching raw int); `rust_decimal` conversions behind the `rust_decimal` feature |
//! | `Int256` / `UInt256` | [`Int256`] / [`UInt256`] (32 raw LE bytes) |
//! | `JSON` | a `String` of JSON text **plus** the per-insert setting `input_format_binary_read_json_as_string: "1"` (see the crate docs' wiring example) |
//! | `LowCardinality(T)` | the plain `T` — transparent on insert; the server builds the dictionary |
//! | Geo (`Point`, `Ring`, `LineString`, `Polygon`, ...) | the [`Point`]/[`Ring`]/... aliases (tuples and `Vec`s of `Float64`) |
//! | `Dynamic`, `AggregateFunction(...)`, `Decimal256` | **unsupported** — no serde shape maps to them (`Decimal256`: scale manually into an [`Int256`]) |
//!
//! ⚠ marks a field type whose bare `Serialize` impl encodes successfully
//! into bytes the column cannot read. [`crate::serde`] lists them and the
//! attribute each one needs.
//!
//! A `Decimal(P, S)` column's precision picks its wire width, so each
//! wrapper covers a band rather than an upper bound. A `Decimal(9, 2)`
//! column is four bytes wide and takes `Decimal32<2>`, where `Decimal64<2>`
//! writes eight and shifts every following column.
//!
//! ## Unsupported serde attributes (they misalign columns)
//!
//! Because rows are positional and fixed-width, several serde attributes
//! that make a struct emit a variable or unexpected number of fields are
//! **not supported** on row types:
//!
//! - **`#[serde(skip)]` / `#[serde(skip_serializing_if = ...)]`** — a skipped
//!   field emits *fewer* columns for some rows, shifting every following
//!   byte into the wrong column. This serializer **cannot reliably detect
//!   it**: serde reports the already-reduced field count to
//!   `serialize_struct`, so a short row looks self-consistent. Never put a
//!   skip attribute on a row struct; model absent values as `Option<T>`
//!   (`Nullable`) instead. (A `Serialize` impl that *lies* about its field
//!   count, declaring N but writing a different number, fails as a
//!   record-level error.)
//! - **`#[serde(flatten)]`** — serde lowers a flattened struct to a
//!   length-less map, which fails as a record-level error.
//!   Inline the fields into the row struct in column order instead.
//! - **internally/adjacently-tagged enums (`#[serde(tag = ...)]`)** — emit
//!   the tag as a phantom string column. Map enums to an explicit integer or
//!   `String` column, or use a newtype-variant `Variant` (below).
//!
//! ## `Variant` discriminant ordering
//!
//! A newtype enum variant writes serde's `variant_index`, the enum's
//! **declaration order**, as the `Variant` discriminant byte. ClickHouse
//! does **not** use declaration order: it sorts a `Variant(T1, T2, ...)`'s
//! member types **alphabetically by type name** and numbers the
//! discriminators in that sorted order. There is no way to infer that
//! mapping here, so **you must declare your Rust enum variants in the same
//! order ClickHouse will sort the column's types.**
//!
//! For a `Variant(Int64, String)` column (ClickHouse order: `Int64` = 0,
//! `String` = 1, sorted alphabetically), declare:
//!
//! ```
//! # use serde::Serialize;
//! #[derive(Serialize)]
//! enum Payload {
//!     Num(i64),     // variant_index 0 -> Int64
//!     Text(String), // variant_index 1 -> String
//! }
//! # let _ = (Payload::Num(0), Payload::Text(String::new()));
//! ```
//!
//! Also note: `Option<Enum>` against such a column emits the `Nullable`
//! `0`/`1` prefix, but ClickHouse encodes a `Variant` NULL as discriminant
//! `255` and forbids `Nullable(Variant)`, so a nullable variant column is
//! not representable; use the `Variant`'s own NULL support instead.
//!
//! [RowBinary]: https://clickhouse.com/docs/en/interfaces/formats#rowbinary

// `#[derive(ClickHouseRow)]` used inside this crate's own unit tests (part of
// the lib target's own compilation, unlike an integration test under
// `tests/`, which Cargo already links against this library under its crate
// name without help) needs this to resolve `::spate_clickhouse::ClickHouseRow`.
#[cfg(test)]
extern crate self as spate_clickhouse;

mod codec;
pub mod config;
mod distributed;
// `native` and `rowbinary` items are `pub` for the `testing` re-export, and
// unreachable from outside the crate without it.
#[cfg_attr(not(feature = "testing"), allow(unreachable_pub))]
mod encoder;
mod http;
#[cfg_attr(not(feature = "testing"), allow(unreachable_pub))]
mod native;
pub mod router;
mod row;
#[cfg_attr(not(feature = "testing"), allow(unreachable_pub))]
mod rowbinary;
mod schema;
pub mod serde;
#[cfg(test)]
mod test_startup;
#[cfg(test)]
mod test_tls;
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod testing;
mod types;
mod writer;

pub use config::{
    ClickHouseSink, ClickHouseSinkBuilder, ClickHouseSinkConfig, Compression,
    DistributedCheckSection, Format,
};
pub use distributed::DistributedCheckError;
pub use encoder::ClickHouseFormatEncoder;
pub use router::{ClickHouseDistributedRouter, KeyExtractor, ShardKey};
pub use row::{ClickHouseRow, ClickHouseRowFamily};
pub use schema::SchemaError;
pub use spate_clickhouse_derive::ClickHouseRow;
pub use types::*;
pub use writer::{ClickHouseEndpoint, ClickHouseWriter};
