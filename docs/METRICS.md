# Metrics

`etl-rs` instruments every pipeline stage through the [`metrics`](https://crates.io/crates/metrics)
facade. The framework installs an exporter selected by the `metrics` config
section (`prometheus` exposes a scrape endpoint on the admin server; `none`
disables export). Pipeline authors register their own metrics through the same
facade — anything recorded with `metrics` macros or handles is exported
alongside the framework's metrics.

## Conventions

- All framework metrics are prefixed `etl_`. Process metrics
  (`process_*`) follow their own standard and are unprefixed.
- Prometheus naming rules: `_total` suffix on counters, unit suffixes
  (`_seconds`, `_bytes`, `_rows`) on everything measured in a unit.
- **Standard labels on every framework metric:** `pipeline` (pipeline name),
  `component` (instance id from config/builder, e.g. `orders_kafka`),
  `component_type` (implementation, e.g. `kafka`, `clickhouse`, `map`).
- Additional labels are listed per metric. Labels marked ⚠ are
  cardinality-sensitive: `partition` labels appear only when
  `metrics.per_partition_detail: true` (default `false`); `shard` and
  `replica` are bounded by cluster topology and always on.
- Hot-path discipline: all handles are pre-registered at pipeline build time;
  counters are incremented at **batch** boundaries; per-record duration
  histograms are observed per batch (duration ÷ n reported as batch means),
  never per record.

## Source (`etl_source_*`)

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_source_records_total` | counter | | Records emitted by the source (post-poll, pre-deserialization). |
| `etl_source_bytes_total` | counter | | Payload bytes emitted. |
| `etl_source_poll_duration_seconds` | histogram | | Time spent inside `poll` per call. |
| `etl_source_lag_records` | gauge | `partition` ⚠ | Consumer lag (log-end offset − committed), per partition when enabled; the unlabelled series reports the max across partitions. |
| `etl_source_rebalances_total` | counter | `event` (`assign`\|`revoke`) | Rebalance events observed. |
| `etl_source_lanes_active` | gauge | | Currently assigned lanes (partitions). |

## Deserializer (`etl_deser_*`)

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_deser_records_total` | counter | `outcome` (`ok`\|`error`) | Deserialization attempts by outcome. One input payload may yield 0..N records; this counts outputs, plus one `error` per failed payload. |
| `etl_deser_records_dropped_total` | counter | `reason` (`skip_policy`) | Payloads dropped by the Skip error policy. |
| `etl_deser_batch_duration_seconds` | histogram | | Deserialization time per source batch. |

## Operators (`etl_operator_*`)

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_operator_records_in_total` | counter | | Records entering the operator. |
| `etl_operator_records_out_total` | counter | | Records emitted downstream (filter drops and flat_map fan-out make this differ from in). |
| `etl_operator_records_dropped_total` | counter | `reason` (`filtered`\|`skip_policy`) | Records intentionally removed. |
| `etl_operator_errors_total` | counter | `error_type` | User-code errors by taxonomy class. |
| `etl_operator_batch_duration_seconds` | histogram | | Processing time per batch through this operator. |

## Queues (pipeline → sink handoff) (`etl_queue_*`)

Queues are labelled by edge: `queue` = `<upstream>-><downstream>` (e.g.
`chain->sink/shard-3`).

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_queue_depth` | gauge | `queue` | Items currently queued. |
| `etl_queue_capacity` | gauge | `queue` | Configured bound. |
| `etl_queue_full_events_total` | counter | `queue` | `try_send` rejections (each is a backpressure signal, never a block). |

## Backpressure (`etl_backpressure_*`)

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_backpressure_paused` | gauge | | 1 while the source is paused by the watermark controller. |
| `etl_backpressure_paused_seconds_total` | counter | | Cumulative paused time. |
| `etl_backpressure_pause_events_total` | counter | | Pause transitions (flapping indicator when high). |
| `etl_backpressure_inflight_bytes` | gauge | | Current global in-flight byte budget usage. |

## Sink (`etl_sink_*`)

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_sink_records_total` | counter | `shard` | Records durably written (acknowledged flushes only). |
| `etl_sink_bytes_total` | counter | `shard` | Bytes durably written. |
| `etl_sink_batch_rows` | histogram | | Rows per sealed batch. |
| `etl_sink_batch_bytes` | histogram | | Bytes per sealed batch. |
| `etl_sink_flushes_total` | counter | `shard`, `reason` (`rows`\|`bytes`\|`linger`\|`drain`) | Flushes by trigger. |
| `etl_sink_flush_duration_seconds` | histogram | `shard` | Write round-trip per flush (including retries). |
| `etl_sink_retries_total` | counter | `shard` | Flush attempts beyond the first. |
| `etl_sink_errors_total` | counter | `shard`, `error_type` | Write errors by taxonomy class. |
| `etl_sink_inflight_batches` | gauge | `shard` | Sealed batches currently in flight. |
| `etl_sink_replica_healthy` | gauge | `shard`, `replica` | 1 = circuit closed, 0 = open (replica quarantined). |
| `etl_sink_breaker_opens_total` | counter | `shard`, `replica` | Circuit-breaker open transitions. |
| `etl_sink_abandoned_batches_total` | counter | `shard` | Batches abandoned at drain deadline (will replay after restart). |

## Checkpointing (`etl_checkpoint_*`)

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_checkpoint_pending_batches` | gauge | `partition` ⚠ | Unacknowledged batches tracked; unlabelled series is the max across partitions. |
| `etl_checkpoint_commits_total` | counter | `outcome` (`ok`\|`error`) | Source commit calls. |
| `etl_checkpoint_commit_duration_seconds` | histogram | | Commit round-trip. |
| `etl_checkpoint_watermark_age_seconds` | gauge | | Age of the oldest unacknowledged batch — the primary "stuck pipeline" alert signal. |

## End-to-end

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_e2e_latency_seconds` | histogram | | Source-to-durable-write latency, observed per acknowledged batch. Time basis is `metrics.e2e_basis`: `ingest` (default, skew-free) or `event` (uses the record's event time; clock-skew sensitive). |

## Pipeline / process

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_pipeline_info` | gauge | `version` | Constant 1; carries build metadata. |
| `etl_pipeline_state` | gauge | `state` (`starting`\|`running`\|`draining`\|`failed`) | 1 for the current state, 0 otherwise. |
| `etl_pipeline_threads` | gauge | | Pinned pipeline thread count. |
| `process_*` | — | | CPU, memory, fds via `metrics-process`. |

## Histogram buckets

Configured on the exporter by name-suffix matchers (override in the `metrics`
config section):

- `*_duration_seconds` / `etl_e2e_latency_seconds`: exponential
  `0.001 .. 60` (1ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms,
  1s, 2.5s, 5s, 10s, 30s, 60s).
- `etl_sink_batch_rows`: powers of 4 from `64` to `1_048_576`.
- `etl_sink_batch_bytes`: powers of 4 from `4 KiB` to `256 MiB`.

## Alerting starting points

- `etl_checkpoint_watermark_age_seconds > 5m` while
  `rate(etl_source_records_total[5m]) > 0` — pipeline is stuck, not idle.
- `etl_backpressure_paused == 1` sustained — sink capacity problem.
- `rate(etl_deser_records_dropped_total[5m]) > 0` — schema drift or poison
  messages being skipped.
- `etl_sink_replica_healthy == 0` — replica down; sustained across all
  replicas of a shard means the shard channel will fill and pause the source.
