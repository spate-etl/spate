//! ClickHouse sink for the `etl-rs` framework.
//!
//! Writes directly to shard-local tables through the official `clickhouse`
//! crate, one `INSERT` per sealed batch with a deterministic
//! `insert_deduplication_token` so retries across replicas are idempotent
//! within the server's deduplication window. Batches are acknowledged to the
//! checkpointer only after the server confirms the insert.
