//! High-performance, at-least-once ETL pipeline framework.
//!
//! `etl` is the facade crate for the `etl-rs` framework and the only crate
//! applications need to depend on. The engine ([`etl_core`]) is re-exported
//! at the root, and connectors are enabled through cargo features:
//!
//! | Feature | Enables |
//! |---|---|
//! | `kafka` | [`kafka`] — Kafka source built on `rdkafka` |
//! | `clickhouse` | [`clickhouse`] — ClickHouse sink |
//! | `avro` | [`avro`] — Avro deserialization (Confluent wire format) |
//! | `full` | All of the above |

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
