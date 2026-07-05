//! Testing utilities for the `etl-rs` framework.
//!
//! Provides in-memory [`Source`](etl_core) and sink implementations with
//! paired scripting handles — inject records and failures, observe pause,
//! resume, acknowledgements, and committed watermarks — so pipelines can be
//! tested deterministically without external infrastructure.
