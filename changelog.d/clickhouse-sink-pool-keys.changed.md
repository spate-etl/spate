**Breaking:** **The ClickHouse sink's `batch`, `inflight`, `retry` and `breaker` blocks move beside `clickhouse:`** (`spate-clickhouse`)

These four blocks tune the framework's sink pool, so they now sit in the sink section beside `clickhouse:`, with the other framework sink keys. A block left inside `clickhouse:` fails at load as an unknown field. Move it up one level:

```yaml
# before
sink:
  clickhouse:
    table: orders
    shards: [{ replicas: ["http://clickhouse:8123"] }]
    batch: { max_rows: 500000, linger: 1s }

# after
sink:
  batch: { max_rows: 500000, linger: 1s }
  clickhouse:
    table: orders
    shards: [{ replicas: ["http://clickhouse:8123"] }]
```

`ClickHouseSinkConfig` no longer has the `batch`, `inflight`, `retry` and `breaker` fields, and `ClickHouseSink` no longer has `pool`. In code, set a block per sink with `SinkOptions::with_batch`, `with_inflight`, `with_retry` or `with_breaker`, or build a `SinkPoolConfig` for a pipeline assembled from the primitives. The pool's bounds are checked when the pipeline installs the sink, and an error starts at `sink` or `sinks.<name>` in place of `sink.clickhouse` or `sinks.<name>.clickhouse`. Pool values are unchanged for a config that set none of the four blocks.
