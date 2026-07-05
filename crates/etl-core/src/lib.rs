//! The engine of the `etl-rs` framework.
//!
//! `etl-core` contains the pipeline runtime and every technology-neutral
//! abstraction: records and their checkpoint tokens, the operator chain,
//! the source and sink traits, checkpointing, backpressure, configuration
//! loading, metrics, and the admin server.
//!
//! Applications should depend on the [`etl`](https://crates.io/crates/etl)
//! facade crate rather than on `etl-core` directly.
