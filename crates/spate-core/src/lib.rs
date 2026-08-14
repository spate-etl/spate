//! The engine of the Spate framework.
//!
//! `spate-core` contains the pipeline runtime and every technology-neutral
//! abstraction: records and their checkpoint tokens, the operator chain,
//! the source and sink traits, checkpointing, backpressure, configuration
//! loading, metrics, and the admin server.
//!
//! Applications should depend on the [`spate`](https://crates.io/crates/spate)
//! facade crate rather than on `spate-core` directly.
//!
//! The properties the engine is arranged around are published as
//! [Invariants], the decisions behind them as [Decisions], and the metric
//! taxonomy as [Metrics]. Module documentation below cites the first two by
//! identifier alone (`INV-2`, `ADR-0013`).
//!
//! [Invariants]: https://spate.kainth.dev/docs/INVARIANTS
//! [Decisions]: https://spate.kainth.dev/docs/adr/
//! [Metrics]: https://spate.kainth.dev/docs/METRICS

// tokio's own sources change shape under `--cfg loom` (net disappears), so
// anything touching tokio::net is compiled out of loom model builds.
#[cfg(not(loom))]
pub mod admin;
/// Re-export of the [`bytes`] crate: [`RowEncoder`](sink::RowEncoder)
/// signatures take [`bytes::BytesMut`], so connector authors can use this
/// re-export instead of declaring their own `bytes` dependency (declaring
/// one is also fine; versions are compatible per the workspace pin).
pub use bytes;

pub mod backpressure;
pub mod checkpoint;
// Resolves the reserved `chunk:` block into `ops::ChunkConfig`, so it follows
// `ops` out of loom model builds (nothing loom-modelled reads config).
#[cfg(not(loom))]
pub mod config;
// References `source` types, so it shares the source module's loom gate.
#[cfg(not(loom))]
pub mod coordination;
pub mod deser;
pub mod error;
pub mod framing;
pub mod metrics;
#[cfg(not(loom))]
pub mod ops;
#[cfg(not(loom))]
pub mod pipeline;
pub mod record;
#[cfg(not(loom))]
pub mod sink;
#[cfg(not(loom))]
pub mod source;
pub mod telemetry;

pub use error::{DeserError, ErrorClass, ErrorPolicy, FatalError, SinkError, SourceError};
pub use framing::FramingContract;
pub use record::{Flow, PartitionId, RawPayload, Record, RecordMeta};
