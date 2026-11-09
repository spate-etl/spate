# spate-clickhouse

The ClickHouse sink (`spate-clickhouse`, feature `clickhouse` on the `spate`
facade) writes rows directly to shard-local tables over HTTP. Each sealed batch
is one `INSERT` with a deduplication token that every retry of that batch
reuses. Rows are encoded on the pipeline threads, to RowBinary by default or to
columnar Native blocks with `format: native`.

## Enable

Turn on feature `clickhouse` on the [`spate`](https://crates.io/crates/spate)
facade. The facade README has the Cargo snippet.

`clickhouse-uuid`, `clickhouse-chrono` and `clickhouse-time` accept fields of
those crates' types for UUID, date and time columns. `clickhouse-rust-decimal`
adds checked conversions between `rust_decimal::Decimal` and the sink's decimal
wrappers. Each of the four turns on `clickhouse`.

## Configuration

The sink reads the pipeline's `sink: { clickhouse: ... }` section into
`ClickHouseSinkConfig`. `table` and `shards` are required, and an unknown key is
rejected.

```yaml
sink:
  clickhouse:
    table: orders_local            # or analytics.orders_local
    database: analytics            # for an unqualified table
    shards:                        # one entry per shard
      - replicas: ["http://ch-0-0:8123", "http://ch-0-1:8123"]
        weight: 1                  # Distributed-parity weight, at least 1
      - replicas: ["http://ch-1-0:8123", "http://ch-1-1:8123"]
    user: spate
    password: ${CLICKHOUSE_PASSWORD}
    compression: lz4               # off | none | lz4 | zstd | zstd:<1-22>
    format: rowbinary              # rowbinary | native
    timeouts: { send: 30s, end: 180s }
    settings: { insert_quorum: "auto" }   # extra per-insert settings
    # batch, inflight, retry and breaker are the framework's sink-pool sections
```

`weight`, `compression`, `format` and `timeouts` show their defaults. Leaving
out `batch`, `inflight`, `retry` or `breaker` gives the framework's defaults,
which the
[sink-pool reference](https://spate.kainth.dev/docs/user-guide/reference/configuration#sink-pool)
lists.

Loading fails when a shard has no replicas, a replica URL is not `http://` or
`https://` or carries credentials, a `weight` is 0, or `table` or `database` is
not a plain identifier. It also fails when `settings` names a setting the sink
manages (listed under [Writes and retries](#writes-and-retries)), and when
`tls.root_ca` is set and no URL is `https://`. `tls.root_ca` names a PEM bundle
of CAs trusted beside the system store.

`compression` applies to the wire only. The sink compresses insert bodies on the
pipeline threads as each chunk seals, and the table's column `CODEC`s are
separate. `zstd` alone means level 3.

The insert column list is not a key. `#[derive(ClickHouseRow)]` generates it
from the row struct, as [Column contract](#column-contract) describes.
`config::from_component_config` turns the section into a builder, and its
`with_row` supplies the row type and fetches the table's schema.
[`kafka_avro_to_clickhouse.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/kafka_avro_to_clickhouse.rs)
wires the sink into a whole pipeline.

The
[Configuration section of the ClickHouse sink page](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse#configuration)
has the full key table. The `distributed_check` block is on
[Distributed parity](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse/distributed-parity).

## Delivery

- A batch counts as written once the server answers its `INSERT`. The sink sets
  `wait_end_of_query=1`, so that answer comes after the server has processed
  the insert, materialized views included. Only then are the batch's records
  acknowledged.
- A restart or a crash replays the records the source had not committed. The
  replay seals new batches with new tokens, so rows already written land a
  second time. Design target tables to tolerate duplicates. `ReplacingMergeTree`
  with a version column collapses them within one shard.
- A batch the sink abandons fails its records' acknowledgements. The watermark
  stops there, and the records replay after a restart. A fatal error abandons a
  batch at once. A retryable one abandons it when `retry.max_attempts` is
  reached, and the default `0` retries until the drain deadline.

[Delivery guarantees](https://spate.kainth.dev/docs/user-guide/concepts/delivery-guarantees)
covers the framework's side.

## Writes and retries

Each sealed batch is one `INSERT ... FORMAT RowBinaryWithNamesAndTypes`, or
`FORMAT Native`, carrying an `insert_deduplication_token`. The token names the
pipeline, the process run, the shard and the batch's sequence number. A retry
goes to the next healthy replica of the same shard and sends the same bytes
under the same token, so a server that keeps a deduplication window drops the
second copy.

**Plain `MergeTree` keeps no deduplication window by default, and the token
does nothing there.** There is no error, and a retried batch lands twice.
`Replicated*MergeTree` keeps a window by default. On a plain `MergeTree`, set
one on the table:

```sql
CREATE TABLE orders (...) ENGINE = MergeTree ORDER BY id
SETTINGS non_replicated_deduplication_window = 100;
```

A network error, a timeout and most server exceptions, such as too many parts,
are retried. A server exception for a schema mismatch, unparseable data, a
missing table or database, a setting the server does not know, a rejected login
or grant, or a compressed body the server cannot decode is fatal, and so is a
rejected TLS handshake. A fatal error abandons the batch without a retry.

When no replica of a shard is healthy, writes to that shard wait for the
circuit breaker's next probe, and backpressure reaches the source. Records never
move to another shard.

The sink sets `insert_deduplicate`, `insert_deduplication_token`,
`wait_end_of_query`, `input_format_with_names_use_header` and
`input_format_with_types_use_header` on every insert, and `decompress` on a
compressed one. `settings` cannot override them, and cannot name `database`,
which the typed `database` field owns.

[Deduplication tokens and the window warning](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse#deduplication-tokens-and-the-window-warning)
and
[Replica rotation and the circuit breaker](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse#replica-rotation-and-the-circuit-breaker)
cover the mechanism.

## Column contract

RowBinary rows carry no column names. Each row's bytes follow the order in which
the struct's `Serialize` emits its fields, and the `INSERT` lists the columns
in the struct's field declaration order. `#[derive(ClickHouseRow)]` generates
that list. It honours `#[serde(rename = "...")]` for a name no Rust identifier
can spell, such as a flattened `Nested` column's `outer.inner`. The table's own
column order does not matter, because the column list maps by name.

Three checks hold the struct to the table:

- `with_row` reads `system.columns` from every replica of every shard before
  the pipeline starts. It fails on a column the table lacks, a column that
  cannot be inserted into, or replicas that disagree.
- Each request body opens with a header naming every column and the type
  `system.columns` reported for it, and the sink sets the two settings that
  make the server check it. An `ALTER` that changes a column's type fails the
  next insert as fatal, even when the new type has the same width. Under
  `format: native`, each block carries the same names and types.
- On the first record each pipeline thread encodes, the encoder compares the
  struct's field names, order and type classes with the fetched schema. A
  mismatch is fatal before any batch is sent.

[Schema validation](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse/schema-validation)
walks through the three checks, and
[ClickHouse column types](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse/column-types)
lists the column types each format writes.

## Sharp edges

- **Re-homing.** Changing shard weights, the sharding key or the shard count
  sends some keys to a different shard, and their rows already written stay on
  the old one. Plan the change as a migration. See
  [Distributed parity](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse/distributed-parity#weights-the-add-a-shard-knob).
- **One confirming replica.** The sink writes each batch to one replica and
  relies on replication for the rest. A batch acknowledged by a replica that is
  lost before the part replicates is lost. `insert_quorum` in `settings`
  requires more replicas. See
  [Durability and replica loss](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse#durability-and-replica-loss).

## Metrics

The sink registers no metric families of its own. The framework's sink families
carry `component_type="clickhouse"`, and the replica-level ones a redacted
replica URL. The
[Metrics section of the ClickHouse sink page](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse#metrics)
says which ones to watch.

## Related

- [`spate`](https://crates.io/crates/spate) — the facade an application depends
  on, with the Cargo snippet.
- [ClickHouse sink page](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse)
  — every key, the `settings` passthrough, TLS and the supported server
  versions.
- [API documentation on docs.rs](https://docs.rs/spate-clickhouse) — the
  configuration types, the encoders, the writer and the RowBinary type mapping.
- [Native format](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse/native-format)
  — when the columnar format pays for its client CPU.
- [Multi-table split](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse/multi-table)
  — one table per record kind from one pipeline.
- [Performance tuning](https://spate.kainth.dev/docs/user-guide/connectors/sinks/clickhouse/performance-tuning)
  — batch size, writer parallelism and compression.
- [`kafka_avro_to_clickhouse.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/kafka_avro_to_clickhouse.rs)
  — an Avro order stream from Kafka into ClickHouse.
- [`clickhouse_aggregating_mv.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/clickhouse_aggregating_mv.rs)
  — orders rolled up through a Null landing table into an
  `AggregatingMergeTree`.
- [Examples index](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/README.md)
  — every example, with the features and infrastructure each needs.
- [`spate-test`](https://crates.io/crates/spate-test) — an in-memory sink whose
  writes a test scripts per replica, for testing a pipeline with no ClickHouse
  server.
