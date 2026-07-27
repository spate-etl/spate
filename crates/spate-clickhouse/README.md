# spate-clickhouse

ClickHouse sink for the [Spate](https://github.com/spate-etl/spate)
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

## Transport compression

Insert (and schema-probe) request/response bodies are compressed on the wire.
This is **transport compression only** — negotiated per connection with the
server — and is independent of the on-disk column `CODEC`s you declare in table
DDL, which remain your responsibility.

```yaml
sink:
  clickhouse:
    compression: lz4      # off | none | lz4 (default) | zstd | zstd:<1-22>
```

`lz4` (the default) is fast and low-CPU — the right choice for high-throughput
ingest. `zstd` trades CPU for a better ratio, worth it when network egress is
the bottleneck (e.g. cross-region); `zstd` alone uses level 3, or pin a level
with `zstd:9`. `off` disables compression entirely.

## Column order is the wire contract

RowBinary carries no column names: the configured `columns` list and the
row struct's field declaration order must match, and reordering either is
a breaking change to the pipeline.
