# ClickHouse sink

The ClickHouse sink (`etl-clickhouse`, feature `clickhouse` on the `etl`
facade) writes **directly to shard-local tables** over HTTP: rows are
encoded to RowBinary on the pipeline threads, shipped to sink workers as
pre-formatted frames, and inserted one `INSERT ... FORMAT RowBinary` per
sealed batch with a deterministic `insert_deduplication_token`.

Construct it from the pipeline's opaque section; the result is a
`SinkBundle` that drops straight into the builder:

```rust
let sink = etl::clickhouse::config::from_component_config(pipeline.config().sink_config("default")?)?;
// optional pre-flight — see the schema validation guide
let schema = pipeline.block_on(sink.validate_schema())?;
let pipeline = pipeline.sink(sink)?;
```

To write to **several tables** from one pipeline (one per record kind), see
[multi-table split](./multi-table.md).

## Configuration

The `sink: { clickhouse: ... }` section deserializes into
`ClickHouseSinkConfig` (`crates/etl-clickhouse/src/config.rs`); unknown
fields are rejected with the offending key, and the retry/breaker values
are validated at load (a bad multiplier or zero threshold fails startup
instead of the write loop).

```yaml
sink:
  clickhouse:
    table: orders_local            # or db.orders_local
    columns: [id, name, amount]    # MUST match the row struct's field order
    shards:
      - replicas: ["http://ch-0-0:8123", "http://ch-0-1:8123"]
      - replicas: ["http://ch-1-0:8123", "http://ch-1-1:8123"]
    user: default
    password: ${CLICKHOUSE_PASSWORD}
    batch: { max_rows: 500000, max_bytes: 128MiB, linger: 1s }
    inflight: { max_per_shard: 2 }
    compression: lz4               # off | lz4 | zstd | zstd:<1-22>
    validate_schema: full          # off | names | full
```

| Key | Type | Default | Meaning |
|---|---|---|---|
| `table` | string | required | Target table, optionally `database.table`-qualified. |
| `columns` | string list | required | Insert column list. **Order is the wire contract** — must match the row struct's field declaration order. Duplicates rejected at load. |
| `shards` | list of `{ replicas: [url] }` | required | One entry per shard, each with its replica HTTP(S) URLs. |
| `database` | string | none | Default database for unqualified tables. |
| `user` | string | none | Username (interpolate secrets via `${VAR}`). |
| `password` | string | none | Password (interpolate secrets via `${VAR}`). |
| `settings` | string map | `{}` | Extra per-insert ClickHouse settings. `insert_deduplication_token`, `insert_deduplicate`, and `wait_end_of_query` are managed by the sink and cannot be overridden. |
| `batch.max_rows` | integer | `500000` | Seal a batch after this many rows. |
| `batch.max_bytes` | byte size | `128MiB` | Seal after this many encoded (uncompressed) bytes. |
| `batch.linger` | duration | `1s` | Seal a partial batch after this long — bounds latency at low throughput. |
| `inflight.max_per_shard` | integer | `2` | Concurrent in-flight batches per shard (to different replicas). |
| `retry.initial` | duration | `100ms` | First backoff delay. |
| `retry.max` | duration | `10s` | Backoff cap. |
| `retry.multiplier` | float | `2.0` | Backoff growth factor. |
| `retry.jitter` | float | `0.2` | Jitter fraction (`0..1`). |
| `retry.max_attempts` | integer | `0` | Attempt cap; `0` = unbounded (retry until the drain deadline — the at-least-once default). |
| `breaker.failure_threshold` | integer | `3` | Consecutive failures before a replica is quarantined. |
| `breaker.open_for` | duration | `5s` | Quarantine duration before a half-open probe. |
| `breaker.half_open_probes` | integer | `1` | Probe writes allowed while half-open. |
| `timeouts.send` | duration | `30s` | Per-send timeout (one frame reaching the socket). |
| `timeouts.end` | duration | `180s` | End-to-end insert timeout: the server fully processing the insert, materialized views included. |
| `compression` | string | `lz4` | Transport (HTTP-body) compression: `off`/`none`, `lz4`, `zstd` (level 3), or `zstd:N` with N in 1..22. Unrelated to on-disk column codecs. |
| `validate_schema` | string | `off` | Startup + first-record schema validation: `off`, `names`, `full`. See [Schema validation](../../03-guides/schema-validation.md). |
| `format` | string | `rowbinary` | Insert wire format: `rowbinary` (row-wise, default) or `native` (columnar blocks). See [Native format](./native-format.md). |

## RowBinary on pipeline threads

Encoding is CPU work, so it happens on the pinned pipeline threads —
`ClickHouseEncoder` (the sink's `RowEncoder`) serializes each row to RowBinary
using the crate's own serializer. Its type parameter is the record *family*,
not the row type: owned rows are wrapped as `Owned<Row>` (e.g.
`ClickHouseEncoder::<Owned<Order>>::new()`), while the encode path itself needs
only `Row: serde::Serialize`. Sink workers on the I/O runtime only merge
frames, seal batches, and drive inserts; they never touch a record.

> [!IMPORTANT]
> RowBinary carries no column names — position is everything. The
> configured `columns` list and the row struct's **field declaration
> order** must match; reordering either is a breaking change to the
> pipeline. `validate_schema: names` (or `full`) plus
> `ClickHouseEncoder::with_schema` turns this contract into a fail-fast
> check — see [Schema validation](../../03-guides/schema-validation.md). The
> full type mapping is in the crate's `rowbinary` module docs.

## Deduplication tokens — and the window warning

Every sealed batch carries a deterministic `insert_deduplication_token`, so
a retry of the same batch (including on another replica after a timeout) is
idempotent — the server drops the duplicate insert.

> [!WARNING]
> **Deduplication is silently off on plain `MergeTree`.** Token
> deduplication only works if the server keeps a deduplication window.
> `Replicated*MergeTree` defaults to a window of 10000;
> **plain `MergeTree` defaults to `0`, and the token does nothing** — no
> error, no warning, duplicates on every retry. Set it explicitly:
>
> ```sql
> CREATE TABLE orders (...) ENGINE = MergeTree ORDER BY id
> SETTINGS non_replicated_deduplication_window = 100;
> ```

And the honest limit: tokens cover *same-batch retries* only. Crash replay
re-batches with different boundaries and different tokens, so replayed rows
land again. Design target tables to tolerate duplicates —
`ReplacingMergeTree` with a version column is the sanctioned pattern. See
[Delivery guarantees](../../02-concepts/02-delivery-guarantees.md).

## Replica rotation and the circuit breaker

Each shard has one worker that seals batches and dispatches up to
`inflight.max_per_shard` concurrent writes, rotating round-robin across
that shard's **healthy** replicas. A failed write retries the *same sealed
batch* (same token) on the next healthy replica with capped exponential
backoff. Per-replica circuit breakers quarantine a replica after
`failure_threshold` consecutive failures for `open_for`, then admit
`half_open_probes` trial writes before closing again. Per-shard workers
(rather than per-replica) keep batches full-sized — ClickHouse wants few
big inserts — while `max_per_shard` still provides replica parallelism.

## Why direct-to-shard, not a Distributed table

Writes go to shard-local tables (with `internal_replication=true` topology)
rather than through a `Distributed` table: bigger blocks, less merge
pressure, and — crucially for checkpointing — a **synchronous server
acknowledgement**. The sink always sets `wait_end_of_query=1`, so a
successful write means the server fully processed the insert, materialized
views included; that success is the durable-ack point that lets watermarks
advance. Sharding across shards is the pipeline's job (the chain's router,
e.g. `KeyHashRouter`), which also makes batch boundaries deterministic for
the dedup tokens.

## `SinkBundle` and the readiness probe

`ClickHouseSink` implements `SinkBundle`, so `pipeline.sink(sink)` derives
everything: per-shard queues, per-shard metrics labeled by replica URL, the
workers, the drain hook — and a readiness probe (`probe_fn`) over **every
replica of every shard** that drives the sinks-connected half of
`/readyz`. The probe deliberately uses its own client set: sharing the
insert clients would report the write path healthy merely because probing
keeps its connections warm. Manual assemblies hand `probe_fn()` to
`SinkRuntime.probe` directly (see
[Manual assembly](../../03-guides/manual-assembly.md)).

## Related

- [Multi-table split](./multi-table.md) — write to one table per record kind
  from a single pipeline (the `sinks:` map + the split terminal).
- [Native format](./native-format.md) — the opt-in columnar
  `format: native`: when to use it, the supported type matrix, and the
  measured trade-off (server CPU/wire vs client CPU).
- [Performance tuning](./performance-tuning.md) — batch/part size, writer
  parallelism, `async_insert`, and the reasoning behind each starting-point
  setting.
- [Distributed parity](./distributed-parity.md) — match the router's shard
  placement to a `Distributed` table's sharding key so cluster reads can
  prune shards, plus the opt-in startup check that keeps the two aligned.
- [Schema validation](../../03-guides/schema-validation.md) — the
  `validate_schema` modes end to end.
- [Tuning](../../05-deployment/tuning.md) — batch sizing and its coupling to
  `backpressure.max_inflight_bytes` (the sizing rule).
- [Graceful shutdown](../../03-guides/graceful-shutdown.md) — what happens to
  in-flight batches at the drain deadline.
