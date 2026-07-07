# Configuring pipelines

One process runs one pipeline; one YAML file configures one process. The
file has two kinds of sections: **typed framework sections** the engine
validates strictly, and **opaque connector sections** handed through to your
connector factories. YAML configures connectors and tuning — never the
operator graph, which you define in code
(see [Assembling a pipeline](assembling-a-pipeline.md)).

This guide covers the layout and the loading semantics. Every key with its
type and default lives in the
[configuration reference](../07-reference/configuration.md).

## Layout

A trimmed version of the flagship example
(`crates/etl/examples/kafka_avro_to_clickhouse.yaml`):

```yaml
pipeline:
  name: orders
  io_threads: 2

checkpoint:
  interval: 5s
  drain_timeout: 25s      # keep below terminationGracePeriodSeconds

backpressure:
  max_inflight_bytes: 1GiB

metrics:
  exporter: prometheus
  listen: 0.0.0.0:9090    # /metrics, /healthz, /readyz

source:
  kafka:
    brokers: ${KAFKA_BROKERS:-localhost:9092}
    topic: ${KAFKA_TOPIC:-orders}
    group_id: ${KAFKA_GROUP:-orders-etl}

deserializer:
  avro:
    mode: confluent
    registry:
      url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}

sink:
  clickhouse:
    table: ${CLICKHOUSE_TABLE:-orders}
    columns: [id, customer, amount_cents, ts_ms]
    shards:
      - replicas: ["${CLICKHOUSE_URL:-http://localhost:8123}"]
```

Load it with `Pipeline::from_path`, or parse a string with
`PipelineConfig::from_str` (handy in tests).

## The typed framework sections

Four top-level sections belong to the framework — `pipeline`, `checkpoint`,
`backpressure`, and `metrics`:

- **`pipeline`** — identity and thread budget: `name` (required; the
  `pipeline` label on every metric), `threads`, `io_threads`, `pinning`.
- **`checkpoint`** — watermark commit policy: `interval`,
  `max_pending_batches`, `drain_timeout`, `stalled_fail_after`.
- **`backpressure`** — the in-flight byte budget and pause/resume
  hysteresis: `max_inflight_bytes`, `high_ratio`, `low_ratio`, `min_pause`.
- **`metrics`** — exporter selection and observability knobs: `exporter`,
  `listen`, `per_partition_detail`, `e2e_basis`.

Only `pipeline`, `source`, and `sink` are required; every other section and
field has a documented default (`deserializer` is optional entirely —
sources that emit ready-made records need none).

Three conventions apply throughout:

- **`deny_unknown_fields` at every level.** A misspelled key (`intervall`,
  `sinks`) is a load-time error naming the offending field, never a silently
  ignored no-op. Parse errors carry the full dotted path
  (`pipeline.io_threads: invalid type ...`).
- **Durations are humantime strings**: `5s`, `250ms`, `2m`, `1h`.
- **Byte sizes are humane too**: `256MiB`, `1GiB`, plain integers for bytes.

Cross-field validation runs at load as well — for example
`checkpoint.interval` has a 100ms floor, and the backpressure ratios must
satisfy `0 < low_ratio < high_ratio <= 1`.

## Environment interpolation

`${VAR}` forms interpolate from the process environment **on the raw text
before YAML parsing** (Vector-compatible semantics) — the standard way to
feed Kubernetes ConfigMap/Secret values in:

| Form | Behavior |
|---|---|
| `${VAR}` | Substitute; **error** if unset (empty is allowed). |
| `${VAR:-default}` | Substitute; use `default` if unset **or empty**. |
| `${VAR:?message}` | Substitute; **error** with `message` if unset **or empty**. |
| `$$` | A literal `$`. |

Because substitution is textual and pre-parse, values are spliced verbatim:
a value containing a newline is rejected outright (it would change the
document structure), and YAML-significant characters in a value are your
responsibility — quote the site (`password: "${PASSWORD}"`) so a password
like `p@ss #1` cannot turn into a YAML comment.

## Opaque connector sections

`source`, `deserializer`, and `sink` are **single-key mappings**: the one
key selects the component type, and the body under it belongs entirely to
that component:

```yaml
source:
  kafka:                 # type tag — selects the component
    brokers: localhost:9092   # body — KafkaSourceConfig's schema, not the framework's
    topic: orders
    group_id: orders-etl
```

The framework records the tag and hands the raw body to your connector
factory (`KafkaSource::from_component_config(&pipeline.config().source)`),
which deserializes it into its own typed config — with its own
`deny_unknown_fields` and its own validation. Errors still carry the full
dotted path from the file root (`source.kafka.brokers: missing field ...`).
This is what keeps connectors fully decoupled from the framework schema; the
single-key shape is what lets every typed struct keep `deny_unknown_fields`.

Each connector's body schema is documented on its page:
[Kafka](../04-connectors/kafka.md) ·
[ClickHouse](../04-connectors/clickhouse.md) ·
[Avro](../04-connectors/avro.md) ·
[Memory](../04-connectors/memory.md).

## No hot reload

Configuration is loaded once at startup. There is deliberately no hot
reload: reconfigure with a rolling restart (the Kubernetes
checksum-annotation pattern — hash the config into a pod annotation so a
config change rolls the deployment). Graceful drain on SIGTERM makes the
restart lossless; see [Graceful shutdown](graceful-shutdown.md) and the
rationale in [docs/DESIGN.md](../../DESIGN.md) § Non-goals.

## Related

- [Configuration reference](../07-reference/configuration.md) — every key,
  type, and default in one table.
- [Tuning](../05-deployment/tuning.md) — which knobs matter under load,
  including the backpressure sizing rule.
