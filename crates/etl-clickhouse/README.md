# etl-clickhouse

ClickHouse sink for the [etl-rs](https://github.com/marcuskainth/etl-rs)
framework: rows encoded to RowBinary **on the pipeline threads** (this
crate ships its own serde serializer), written directly to shard-local
tables as one `INSERT ... FORMAT RowBinary` per sealed batch with a
deterministic `insert_deduplication_token`, rotating replicas with
circuit-breaker failover.

Key types: `ClickHouseEncoder<T: Serialize>` (the CPU half),
`ClickHouseWriter` (the I/O half), `config::from_component_config` (the
`sink: { clickhouse: ... }` section).

## ⚠ Deduplication needs a window

Retries reuse the batch token, making them idempotent — **but only if the
server keeps a deduplication window**. `Replicated*MergeTree` does by
default; **plain `MergeTree` defaults the window to `0` and token
deduplication silently does nothing**:

```sql
CREATE TABLE orders (...) ENGINE = MergeTree ORDER BY id
SETTINGS non_replicated_deduplication_window = 100;
```

Crash replay re-batches with new tokens and lands rows twice regardless —
at-least-once by contract. `ReplacingMergeTree` with a version column is
the sanctioned pattern for replay tolerance.

## Column order is the wire contract

RowBinary carries no column names: the configured `columns` list and the
row struct's field declaration order must match, and reordering either is
a breaking change to the pipeline.
