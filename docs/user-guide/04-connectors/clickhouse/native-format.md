# ClickHouse Native format

The ClickHouse sink defaults to **RowBinary** (row-wise). It can instead
encode the **Native** format — ClickHouse's own columnar, block-framed
representation — with one config knob:

```yaml
sink:
  clickhouse:
    format: native   # rowbinary (default) | native
```

Native transposes each chunk of rows into per-column buffers and emits one
self-describing **block** (`INSERT … FORMAT Native`). Because a Native insert
body is a stream of complete blocks, the sink's batching, deduplication, and
at-least-once semantics are **unchanged** — only the encoding differs.

## When to use it

Native **moves the row→column pivot off the ClickHouse server onto the (more
easily scaled) ETL workers**. That is a deliberate trade, measured on a dev
laptop (ClickHouse 25.6, 200k-row inserts, a realistic mixed schema; full
methodology and numbers in the repo's `docs/BENCHMARKS.md`):

| Axis | RowBinary | Native | Delta |
|---|---|---|---|
| Client encode CPU | 32.9 ns/row | 64.3 ns/row | ~2× **more** (still 15.6M rows/s) |
| Compressed wire (lz4 / zstd) | baseline | — | **49% / 71% smaller** |
| Server parse CPU (`ENGINE=Null`) | 92.5 ms | 7.8 ms | **92% less** |
| Server CPU end-to-end (`MergeTree`) | 122 ms | 36 ms | **70% less** |

The server win is largest when the destination has `LowCardinality`,
`Array`, or `Map` columns — RowBinary makes the *server* build those
dictionaries and columnar structures row-by-row; Native hands it the finished
block.

**Choose `native` when** the ClickHouse cluster is CPU-bound, or when network
egress / wire size is the constraint (cross-region, metered links).
**Keep `rowbinary` (the default) when** ETL-worker CPU is the constraint —
RowBinary's row encode is near-memcpy-fast and hard to beat.

## Enabling it

`format: native` is **type-driven**: the encoder needs each column's
ClickHouse type to lay bytes out columnar, so selecting it always fetches
`system.columns` (it upgrades `validate_schema: off` to `names`). Build the
encoder from the fetched schema:

```rust
let sink = etl::clickhouse::config::from_component_config(&pipeline.config().sink)?;
let native_schema = pipeline.block_on(sink.native_schema())?; // Arc<NativeSchema>
let encoder = etl::clickhouse::NativeEncoder::<MyRow>::new(native_schema);
// hand `encoder` to the chain's terminal `.sink(...)` stage
```

`MyRow` is the same `serde::Serialize` row struct RowBinary uses — its field
order must match the configured `columns`. Because Native blocks are larger
per unit of work, set a larger terminal-stage chunk target
(`ChunkConfig::target_bytes`, e.g. 512 KiB) so each block is substantial; the
sink still merges blocks up to `batch.max_bytes` per insert.

## Supported column types

Bool; `Int`/`UInt` 8–256; `Float32/64`; `Date`, `Date32`; `DateTime`,
`DateTime64`; `Decimal32/64/128`; `Enum8/16`; `String`; `FixedString(N)`;
`UUID`; `IPv4`; `IPv6`; `Nullable(T)`; `Array(T)`; `Tuple(…)`; `Map(K, V)`;
`Nested` (as parallel dotted `Array` columns); `LowCardinality(String)` and
`LowCardinality(Nullable(String))`; and the Geo aliases (`Point`, `Ring`,
`Polygon`, …). Field encodings match the [RowBinary type table](./README.md)
— use the same wire-wrapper newtypes (`DateTime64Millis`, `Decimal64<S>`, …)
and `#[serde(with = "…")]` modules (`uuid`, `ipv4`).

## Not supported (yet)

`Variant`, `Dynamic`, `JSON`, `Time`/`Time64`, `Decimal256`,
`AggregateFunction`, `LowCardinality` of non-`String` inner, and `Nested`
with `flatten_nested = 0`. A column of an unsupported type is rejected
**at construction** (before any row is sent) with a clear error naming the
column and type. Fall back to `format: rowbinary` (which supports the wider
set — see the [RowBinary type table](./README.md)) for tables that use
them.

## Semantics that do **not** change

- **At-least-once / deduplication.** Same `insert_deduplication_token`,
  `insert_deduplicate=1`, `wait_end_of_query=1`. The
  [dedup-window warning](./README.md#deduplication-tokens--and-the-window-warning)
  applies identically.
- **Replica rotation, circuit breaker, retries, batching, backpressure** —
  all identical; the format only changes the bytes in the request body.
- **Compression** (`compression: lz4|zstd|off`) applies to the Native body
  exactly as to RowBinary, and Native compresses noticeably smaller.

## Related

- [ClickHouse sink](./README.md) — the full sink configuration.
- [Schema validation](../../03-guides/schema-validation.md) — the schema fetch
  Native relies on.
