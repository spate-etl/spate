# Metrics

`etl-rs` instruments every pipeline stage through the [`metrics`](https://crates.io/crates/metrics)
facade. The framework installs an exporter selected by the `metrics` config
section (`prometheus` exposes a scrape endpoint on the admin server; `none`
disables export). Pipeline authors and connectors register their own metrics through the same
facade — anything recorded is exported alongside the framework's. The
recommended path is a `Meter`: it attaches the three standard labels below and
**auto-prefixes the name `etl_<namespace>_`**, so custom series live under the
same `etl_` umbrella and an operator finds everything under one root. You pass a
local name (`schema_fetches_total`); the `Meter` adds the prefix. The namespace
is `custom` for pipeline-author metrics (`etl_custom_*`) or a segment a
connector claims (`etl_kafka_*`), and it can never be one of the framework's
reserved stage roots (`source`, `deser`, `operator`, `queue`, `backpressure`,
`sink`, `checkpoint`, `e2e`, `pipeline`), so custom families cannot collide with
a framework metric. A built-in source or sink receives its `Meter` from the
runtime, scoped by its declared `component_type` and its role
(`etl_<component_type>_source_*` / `_sink_*`), so a connector that is both a
source and a sink keeps its families apart. Dropping to the raw `metrics` macros is the escape hatch for
a metric you deliberately want *outside* the `etl_` namespace. See
[Instrumenting connectors](user-guide/06-extending/instrumenting-connectors.mdx).

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

## Kafka source (`etl_kafka_source_*`)

Connector-owned families registered through the Kafka source's `Meter`
(namespace `kafka`, role `source`). They translate the librdkafka statistics
snapshot, emitted every `statistics_interval` (default 5s; `0s` disables the
whole family) and drained on the controller thread — never on the record path.

Counters are **absolute-mapped librdkafka totals** scoped to one consumer
lifetime: the handles mirror the client's cumulative values (idempotent under
duplicate delivery; `rate()`/`increase()` work natively), and after a process
restart the series restart from the new consumer's totals — an ordinary
counter reset to PromQL. The `broker` label is the librdkafka broker name
(`host:port/id`), bounded by cluster topology and always on. The `rtt`/
`throttle` gauges are librdkafka's HDR-histogram rolling-window estimates,
converted to seconds; they are per-broker sampled quantiles and **cannot be
aggregated** across brokers or processes (`max()` is the only defensible
cross-series operator). Series backed by data librdkafka hasn't produced yet
(an empty latency window, an unknown lag) are absent rather than `0`.

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_kafka_source_tx_requests_total` | counter | | Requests sent to brokers. |
| `etl_kafka_source_tx_bytes_total` | counter | | Bytes transmitted to brokers. |
| `etl_kafka_source_rx_responses_total` | counter | | Responses received from brokers. |
| `etl_kafka_source_rx_bytes_total` | counter | | Protocol bytes received. |
| `etl_kafka_source_rx_messages_total` | counter | | Messages consumed (excluding ignored). |
| `etl_kafka_source_rx_message_bytes_total` | counter | | Message bytes consumed (including framing). |
| `etl_kafka_source_broker_tx_retries_total` | counter | | Request retries, summed across brokers. |
| `etl_kafka_source_broker_req_timeouts_total` | counter | | Requests timed out, summed across brokers. |
| `etl_kafka_source_broker_connects_total` | counter | | Connection attempts (including failed), summed across brokers. |
| `etl_kafka_source_broker_disconnects_total` | counter | | Disconnections (broker, network, or otherwise), summed across brokers. |
| `etl_kafka_source_broker_up` | gauge | `broker` | 1 while the broker connection state is `UP`; `sum()` gives the brokers-up count. |
| `etl_kafka_source_broker_tx_errors_total` | counter | `broker` | Transmission errors, attributed per broker. |
| `etl_kafka_source_broker_rtt_avg_seconds` | gauge | `broker` | Mean broker round-trip time over the last statistics window. |
| `etl_kafka_source_broker_rtt_p99_seconds` | gauge | `broker` | p99 broker round-trip time over the last window (non-aggregatable). |
| `etl_kafka_source_broker_throttle_avg_seconds` | gauge | `broker` | Mean broker throttle time over the last window. |
| `etl_kafka_source_broker_throttle_p99_seconds` | gauge | `broker` | p99 broker throttle time over the last window (non-aggregatable). |
| `etl_kafka_source_fetch_queue_messages` | gauge | | Prefetched messages queued client-side for the pipeline's topic. |
| `etl_kafka_source_fetch_queue_bytes` | gauge | | Prefetched bytes queued client-side. |
| `etl_kafka_source_reply_queue_depth` | gauge | | librdkafka ops awaiting `poll()` service (poll starvation indicator). |
| `etl_kafka_source_group_rebalances_total` | counter | | Rebalances counted by librdkafka (`etl_source_rebalances_total` counts the callback events the framework observed). |
| `etl_kafka_source_group_assignment_size` | gauge | | Partitions in the current group assignment. |
| `etl_kafka_source_group_healthy` | gauge | | 1 while the member is settled (`up` + `steady`); 0 during joins/rebalances (state detail goes to debug logs). |
| `etl_kafka_source_partition_fetch_queue_messages` | gauge | `partition` ⚠ | Per-partition prefetch queue depth. |
| `etl_kafka_source_partition_lag_stored_records` | gauge | `partition` ⚠ | Lag against the **stored** (not yet committed) offset — reflects processing progress between commits; `etl_source_lag_records` is the committed-basis view. |

## Kafka sink (`etl_kafka_sink_*`)

Connector-owned families registered through the Kafka sink's `Meter`
(namespace `kafka`, role `sink`). They translate the producer's librdkafka
statistics snapshot, emitted every `statistics_interval` (default 5s; `0s`
disables the whole family) and published from the producer's poll thread —
never on the record path.

The conventions match the Kafka source's families: counters are
**absolute-mapped librdkafka totals** scoped to one producer lifetime
(sound because a sink builds exactly one producer; restarts read as
ordinary counter resets to PromQL), the `broker` label is bounded by
cluster topology, and the latency gauges are per-broker rolling-window
estimates over the last statistics interval that **cannot be aggregated**
across brokers or processes (`max()` is the only defensible cross-series
operator). Windows that sampled nothing are absent rather than `0`.
Batch-level write latency is framework-side
(`etl_sink_flush_duration_seconds`); these families add the producer's
internal view. Per-partition transmit-queue detail is deferred (see the
decision log) — the aggregate queue gauges below cover saturation.

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_kafka_sink_tx_requests_total` | counter | | Requests sent to brokers. |
| `etl_kafka_sink_tx_bytes_total` | counter | | Bytes transmitted to brokers. |
| `etl_kafka_sink_rx_responses_total` | counter | | Responses received from brokers. |
| `etl_kafka_sink_rx_bytes_total` | counter | | Protocol bytes received. |
| `etl_kafka_sink_tx_messages_total` | counter | | Messages produced (delivery-confirmed by librdkafka's accounting). |
| `etl_kafka_sink_tx_message_bytes_total` | counter | | Message bytes produced (including framing). |
| `etl_kafka_sink_produce_queue_messages` | gauge | | Messages waiting in the producer's client-side queue (queue-full pressure indicator). |
| `etl_kafka_sink_produce_queue_bytes` | gauge | | Bytes waiting in the producer's client-side queue. |
| `etl_kafka_sink_broker_tx_retries_total` | counter | | Request retries, summed across brokers. |
| `etl_kafka_sink_broker_req_timeouts_total` | counter | | Requests timed out, summed across brokers. |
| `etl_kafka_sink_broker_up` | gauge | `broker` | 1 while the broker connection state is `UP`; `sum()` gives the brokers-up count. |
| `etl_kafka_sink_broker_tx_errors_total` | counter | `broker` | Transmission errors, attributed per broker. |
| `etl_kafka_sink_broker_rtt_avg_seconds` | gauge | `broker` | Mean broker round-trip time over the last statistics window. |
| `etl_kafka_sink_broker_rtt_p99_seconds` | gauge | `broker` | p99 broker round-trip time over the last window (non-aggregatable). |
| `etl_kafka_sink_broker_int_latency_avg_seconds` | gauge | `broker` | Mean time messages spent in the producer queue before transmission, last window. |
| `etl_kafka_sink_broker_int_latency_p99_seconds` | gauge | `broker` | p99 producer-queue latency over the last window (non-aggregatable). |
| `etl_kafka_sink_broker_outbuf_latency_avg_seconds` | gauge | `broker` | Mean time in the transmit buffer before the socket, last window. |
| `etl_kafka_sink_broker_outbuf_latency_p99_seconds` | gauge | `broker` | p99 transmit-buffer latency over the last window (non-aggregatable). |

## Deserializer (`etl_deser_*`)

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_deser_records_total` | counter | `outcome` (`ok`\|`error`) | Deserialization attempts by outcome. One input payload may yield 0..N records; this counts outputs, plus one `error` per failed payload. |
| `etl_deser_records_dropped_total` | counter | `reason` (`skip_policy`) | Payloads dropped by the Skip error policy. |
| `etl_deser_not_ready_total` | counter | | Payload replays waiting on an upstream dependency (e.g. a schema-registry fetch). Neither an error nor backpressure — the batch retries and completes once the dependency arrives. |
| `etl_deser_batch_duration_seconds` | histogram | | Deserialization time per source batch. |

## JSON deserializer (`etl_json_deser_*`)

Connector-owned family registered through the JSON deserializer's `Meter`
(namespace `json_deser`), minted when the builder is given a metrics scope with
`.with_metrics(pipeline, component)`. It surfaces per-record drops the
payload-granular `etl_deser_*` stage metrics above cannot see: under `ndjson`
(or `array`) with `on_error: skip`, an individual bad record is dropped while the
good records around it are emitted, so the `deserialize` call returns `Ok` and no
`etl_deser_records_total{outcome=error}` is counted.

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_json_deser_records_dropped_total` | counter | `reason` (`malformed`\|`duplicate_key`) | Records dropped by `on_error: skip` because they did not parse / match the target type (`malformed`) or contained a duplicate object key under `reject_duplicate_keys` (`duplicate_key`). |

## Operators (`etl_operator_*`)

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_operator_records_in_total` | counter | | Records entering the operator. |
| `etl_operator_records_out_total` | counter | | Records emitted downstream (filter drops and flat_map fan-out make this differ from in). |
| `etl_operator_records_dropped_total` | counter | `reason` (`filtered`\|`skip_policy`\|`unrouted`) | Records intentionally removed (`unrouted`: matched no split-sink branch under an `unmatched: Skip` policy). |
| `etl_operator_errors_total` | counter | `error_type` | User-code errors by taxonomy class. |
| `etl_operator_batch_duration_seconds` | histogram | | Processing time per batch through this operator. |

## Queues (pipeline → sink handoff) (`etl_queue_*`)

Queues are labelled by edge: `queue` = `<upstream>-><downstream>` (e.g.
`chain->sink/shard-3`).

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_queue_depth` | gauge | `queue` | Items currently queued, sampled on each send. It only advances *on a send*, so while the source is paused (or flow otherwise stalls) it freezes at its last sample — typically `capacity` — even as the workers drain the queue. The live resume decision uses the channel directly, not this gauge. |
| `etl_queue_capacity` | gauge | `queue` | Configured bound. |
| `etl_queue_full_events_total` | counter | `queue` | `try_send` rejections (each is a backpressure signal, never a block). A rejected chunk is parked and retried once per poll cycle while blocked, and every retry that still finds the queue full counts again — so during a sustained stall this tracks retry cadence, not distinct fill episodes. |

## Backpressure (`etl_backpressure_*`)

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_backpressure_paused` | gauge | | 1 while the source is paused by the watermark controller. |
| `etl_backpressure_paused_seconds_total` | gauge (monotonic) | | Cumulative paused time. Exported as a gauge because the `metrics` facade's counter is integer-only; treat as a counter in queries (`rate()` works). |
| `etl_backpressure_pause_events_total` | counter | | Pause transitions (flapping indicator when high). |
| `etl_backpressure_inflight_bytes` | gauge | | Current global in-flight byte budget usage. |

## Sink (`etl_sink_*`)

In a [multi-sink](user-guide/02-concepts/06-multi-sink.mdx) pipeline each sink's
series carry its name as the `component` label (a single sink uses
`component="sink"`), so per-table sink metrics never collide.

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
| `etl_sink_replica_errors_total` | counter | `shard`, `replica` | Failed write attempts attributed to a replica — which endpoint is erroring (`etl_sink_errors_total` gives the class breakdown per shard). |
| `etl_sink_shard_healthy` | gauge | `shard` | 1 = the shard has ≥1 circuit-closed replica; 0 = no replica is circuit-closed (every one quarantined or half-open probing) — intake stalls and the shard back-pressures the source while recovery probes keep firing each `open_for` window. |
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
| `etl_e2e_latency_seconds` | histogram | | Source-to-durable-write latency, observed by the sink worker at each durable flush from the batch's **oldest** record. Time basis is `metrics.e2e_basis`: `ingest` (default, skew-free: time since the record entered the terminal stage) or `event` (against the record's event time; clock-skew sensitive, falls back to ingest when no event time exists). |

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
- `etl_sink_shard_healthy == 0` — no replica of the shard is circuit-closed;
  intake stalls and the shard back-pressures the source (the whole-shard
  escalation of the per-replica `replica_healthy` signal). Recovery probes
  still fire each `open_for` window, so the gauge stays 0 through failed
  probe cycles until one succeeds. Pair with a rising
  `etl_sink_replica_errors_total{replica}` to identify the failing endpoints.
- `etl_kafka_source_group_healthy == 0` sustained — the consumer-group member
  is stuck joining/rebalancing; pair with
  `rate(etl_kafka_source_group_rebalances_total[15m])` to distinguish churn
  from a wedged join.
- `rate(etl_kafka_source_broker_tx_errors_total[5m]) > 0` or
  `etl_kafka_source_broker_up == 0` — broker connectivity trouble, attributed
  by the `broker` label.
- `etl_kafka_sink_produce_queue_messages` sustained near
  `queue.buffering.max.messages` (default 100k) — the producer cannot drain
  to the brokers; batch writes are absorbing queue-full backoff and
  `etl_sink_flush_duration_seconds` will rise before the source pauses.
