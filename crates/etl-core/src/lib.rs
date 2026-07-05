//! The engine of the `etl-rs` framework.
//!
//! `etl-core` contains the pipeline runtime and every technology-neutral
//! abstraction: records and their checkpoint tokens, the operator chain,
//! the source and sink traits, checkpointing, backpressure, configuration
//! loading, metrics, and the admin server.
//!
//! Applications should depend on the [`etl`](https://crates.io/crates/etl)
//! facade crate rather than on `etl-core` directly.
//!
//! The architecture and its invariants are documented in `docs/DESIGN.md`;
//! the metric taxonomy in `docs/METRICS.md`.

pub mod admin;
pub mod backpressure;
pub mod checkpoint;
pub mod config;
pub mod error;
pub mod metrics;
pub mod record;
pub mod telemetry;

pub use error::{DeserError, ErrorClass, ErrorPolicy, SinkError, SourceError};
pub use record::{Flow, PartitionId, RawPayload, Record, RecordMeta};
