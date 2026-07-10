# Configuration reference

One YAML file configures one process. The framework owns four typed
sections — `pipeline`, `checkpoint`, `backpressure`, `metrics` — validated
strictly (`deny_unknown_fields` at every level, so typos fail at startup
with the offending YAML path). The `source`, `deserializer`, and `sink`
sections are single-key mappings selecting a component type; their bodies
are opaque to the framework and deserialized by the component's own
factory.

Defaults below are the exact values from
`crates/etl-core/src/config/mod.rs` (framework sections) and
`crates/etl-core/src/sink/config.rs` /
`crates/etl-core/src/pipeline/builder.rs` (code-level sink knobs). The
how-to lives in
[Configuring pipelines](../03-guides/configuring-pipelines.md).

```yaml
pipeline: { name: orders, threads: 4, io_threads: 2 }
checkpoint: { interval: 5s, max_pending_batches: 1024 }
backpressure: { max_inflight_bytes: 256MiB }
metrics: { exporter: prometheus, listen: 0.0.0.0:9090 }
source:
  kafka: { ... }          # body owned by etl-kafka
deserializer:
  avro: { ... }           # body owned by etl-avro (optional section)
sink:
  clickhouse: { ... }     # body owned by etl-clickhouse
```

Durations use humantime syntax (`5s`, `100ms`, `2m`); byte sizes accept
suffixes (`256MiB`, `1GiB`).

## Environment interpolation

The raw text interpolates against the process environment before parsing,
anywhere in the file (including connector bodies):

| Form | Meaning |
|---|---|
| `${VAR}` | The value of `VAR`; startup error if unset. |
| `${VAR:-default}` | The value of `VAR`, or `default` if unset. |
| `${VAR:?message}` | The value of `VAR`; startup error citing `message` if unset. |
| `$$` | A literal `$`. |

## `pipeline:`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | *required* | Pipeline identity; the `pipeline` label on every metric. Must be non-blank. |
| `threads` | integer | derived | Pinned pipeline thread count. Unset: `available_parallelism` minus the I/O reserve (`io_threads` + 1 controller), at least 1. Cgroup-aware — set explicitly on pods without CPU limits ([Tuning](../05-deployment/tuning.md)). Must be ≥ 1 when set. |
| `io_threads` | integer | `2` | Worker threads for the I/O runtime (sink workers, checkpointer, admin server). Must be ≥ 1. |
| `pinning` | `off` \| `compact` | `off` | Core pinning for pipeline threads. `compact` pins thread *i* to core *i* — only useful with exclusive cores (Kubernetes static CPU manager + Guaranteed QoS). |

## `checkpoint:`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `interval` | duration | `5s` | How often committable watermarks are flushed to the source. Not a durability boundary (the sink write is); shorter intervals only narrow the post-crash replay window. Must be ≥ `100ms`. |
| `max_pending_batches` | integer | `1024` | Maximum unacknowledged batches per partition before that partition pauses (a per-partition backpressure backstop). Must be ≥ 1. |
| `drain_timeout` | duration | `25s` | Shutdown/rebalance drain budget for in-flight sink batches. Keep comfortably below `terminationGracePeriodSeconds` ([Docker](../05-deployment/docker.md)). Must be `> 0`. |
| `stalled_fail_after` | duration | `120s` | A partition watermark stalled behind a *failed* batch this long fails the pipeline, so a permanent sink failure becomes restart-and-replay instead of consuming while committing nothing. Must be `> 0`. |

## `backpressure:`

See [Backpressure](../02-concepts/03-backpressure.md) for the model and
[Tuning](../05-deployment/tuning.md) for the sizing rule tying these to the
sink batch settings.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `max_inflight_bytes` | bytes | `256MiB` | Global cap on bytes admitted into the pipeline but not yet durably written. Must be `> 0`. |
| `high_ratio` | float | `0.8` | Fraction of the budget at which sources are paused. |
| `low_ratio` | float | `0.5` | Fraction of the budget below which sources may resume. Ratios must satisfy `0 < low_ratio < high_ratio <= 1`. |
| `min_pause` | duration | `500ms` | Minimum pause before resuming — avoids flapping (pausing a Kafka partition purges its prefetch, so resume is not free). |

## `metrics:`

See [Monitoring](../05-deployment/monitoring.md).

| Key | Type | Default | Meaning |
|---|---|---|---|
| `exporter` | `prometheus` \| `none` | `prometheus` | `prometheus` mounts `/metrics` on the admin server; `none` records to a no-op recorder. |
| `listen` | socket address | `0.0.0.0:9090` | Admin server bind address (`/metrics`, `/healthz`, `/readyz`). |
| `per_partition_detail` | bool | `false` | Emit per-partition gauge series (`partition` label). Off by default: cardinality grows with the assignment. |
| `e2e_basis` | `ingest` \| `event` | `ingest` | Time basis for `etl_e2e_latency_seconds`. `ingest` is immune to producer clock skew; `event` measures true pipeline lag against record event time. |

## `source:`, `deserializer:`, `sink:`

Each is a single-key mapping — the key selects the component, the body
belongs to it:

| Section | Required | Body documented in |
|---|---|---|
| `source` | yes | [Kafka](../04-connectors/kafka.md), [Memory](../04-connectors/memory.md), or your own ([Custom sources](../06-extending/custom-source.md)) |
| `deserializer` | no — sources that emit ready-made records need none | [Avro](../04-connectors/avro/README.md) |
| `sink` | yes | [ClickHouse](../04-connectors/clickhouse/README.md), [Memory](../04-connectors/memory.md), or your own ([Custom sinks](../06-extending/custom-sink.md)) |

The sink connectors map their `batch`, `inflight`, and `retry` sub-sections
onto the framework's `SinkPoolConfig` (defaults: `batch.max_rows: 500000`,
`batch.max_bytes: 128MiB`, `batch.linger: 1s`, `inflight.max_per_shard: 2`,
`retry.initial: 100ms`, `retry.max: 10s`, `retry.multiplier: 2.0`) — see
each connector's page for its exact schema, and
[Tuning](../05-deployment/tuning.md) for how to size them.

## Code-level knobs (not YAML)

Two knobs live in the assembly code rather than the file, because they are
properties of the chain you write, not the deployment:

| Knob | Type | Default | Meaning |
|---|---|---|---|
| `SinkOptions::queue_capacity` | integer | `8` | Bounded chunk-queue depth between pipeline threads and each shard worker (`Pipeline::sink_with`). Must be non-zero. |
| `ChunkConfig::target_bytes` | integer | `65536` (64 KiB) | Chunk seal size at the chain's terminal stage. Workers merge chunks into full batches, so this does not bound insert sizes. |
| `ChunkConfig::encode_policy` | `Skip` \| `Fail` | `Skip` | Policy for record-level encoder failures ([Error handling](../02-concepts/04-error-handling.md)). |

## A complete example

The flagship pipeline's config,
`crates/etl/examples/kafka_avro_to_clickhouse.yaml`, exercises every
framework section plus the Kafka, Avro, and ClickHouse bodies, with the
backpressure sizing rule worked out in its comments.
