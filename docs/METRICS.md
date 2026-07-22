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
  **`etl_source_lag_records` is the one exception** — its `partition` label is
  unmarked because consumer lag is always published. It is the family's only
  representation, and a cardinality knob that could delete a golden signal is
  how a maximally backlogged consumer ends up reporting nothing.
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
| `etl_source_lag_records` | gauge | `partition` | Consumer lag (log-end offset − committed), **always per partition** — there is no aggregate series. Aggregate in the query layer: `sum` for total backlog (comparable with the broker's group lag), `max` for the worst partition. A partition whose lag the client has not measured yet is **absent**; a partition the member no longer owns reads `0`. |
| `etl_source_rebalances_total` | counter | `event` (`assign`\|`revoke`) | Rebalance events observed. |
| `etl_source_lanes_active` | gauge | | Currently assigned lanes (partitions). |

### Alerting on consumer lag

```promql
# Total backlog for a pipeline — the figure to compare with the broker's
# group lag. Every member's owned partitions sum into one number.
sum by (pipeline) (etl_source_lag_records)

# The worst single partition, which a total can hide.
max by (pipeline) (etl_source_lag_records)

# Lag was never measured: statistics are disabled, the consumer has not
# committed yet, or the source is not publishing. Absence is deliberate — a
# `0` here would be indistinguishable from "caught up", which is how this
# family once read zero on a fully backlogged consumer.
absent(etl_source_lag_records{pipeline="orders"})

# Only *some* partitions have reported. The sum above silently omits the
# rest, so it under-reports with no other signal. `lanes_active` is the
# assignment size, so a shortfall here means the total is partial.
count by (pipeline) (etl_source_lag_records)
  < sum by (pipeline) (etl_source_lanes_active)
```

Alert on the total, on absence, **and** on the partial-measurement check.
Rate-of-change matters more than level for a backfill: a large lag that is
shrinking is a catch-up working as intended, while a small lag that is not
shrinking is a stalled pipeline.

Give the absence and partial-measurement alerts a `for:` window of a minute or
so. Both are legitimately true at startup — a series appears only after the
first commit *and* the following statistics tick (with the defaults, `5s +
5s`, longer if the first assignment is slow) — so a bare `absent()` pages on
every deploy.

### Absent, zero, and stale

The exporter has no way to delete a series, and no idle timeout is configured,
so a series renders for the life of the process once registered. The three
states are therefore distinguished by value, not by presence:

- **absent** — never measured. No commit yet, statistics disabled, or the
  source does not publish lag at all.
- **`0`** — measured. Either the consumer is caught up, or the member no
  longer owns that partition: on losing a partition a member zeroes it, so
  that `sum` across members stays equal to the real backlog rather than
  double-counting a partition that has moved.
- **stale** — cannot happen for a partition a member lost, which is what the
  zeroing buys. A frozen non-zero value means the member still owns the
  partition and librdkafka has stopped measuring it (a leader change, say);
  the last known value is held deliberately, because dropping it to `0` would
  look like a drain that never happened.

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
| `etl_kafka_source_partition_lag_stored_records` | gauge | `partition` ⚠ | Lag against the **stored** (not yet committed) offset — reflects processing progress between commits; `etl_source_lag_records` is the committed-basis view. Unlike that one, this series *is* gated by `per_partition_detail`. |

## S3 source (`etl_s3_source_*`)

Connector-owned families registered through the S3 source's `Meter`
(namespace `s3`, role `source`). All handles are resolved once at `open`;
counters are incremented at object/chunk/batch boundaries — never per
record. `etl_source_records_total` / `etl_source_bytes_total` count framed
records as for any source; these families add the object-level view of a
bounded backfill.

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_s3_source_objects_listed_total` | counter | | Objects enumerated by the planner's listing. **Leader-only**: only the instance that runs the plan increments it (once per plan run; open plans re-count on every replan tick). |
| `etl_s3_source_objects_completed_total` | counter | | Objects fully framed and handed to the pipeline by this instance. |
| `etl_s3_source_objects_remaining` | gauge | | Objects not yet completed across **this instance's currently-held splits** (rises on split gain, falls per completed object, settles on split close). Fleet totals come from the `etl_coordination_*` split gauges, not from summing this. |
| `etl_s3_source_bytes_read_total` | counter | | Bytes read from the store, as stored (pre-decompression). |
| `etl_s3_source_bytes_decoded_total` | counter | | Bytes after decompression (equals `bytes_read` for uncompressed objects; the ratio is the effective compression). |
| `etl_s3_source_get_retries_total` | counter | | Object GET attempts beyond the first (transient failures, resumed with ranged reads). A rising rate means a flaky store or network; an exhausted attempt budget poisons the split rather than failing the pipeline. |
| `etl_s3_source_objects_failed_total` | counter | `reason` | Objects that poisoned their split: `not_found` (deleted after planning), `etag_drift` (overwritten under the `If-Match` pin, or content shorter than committed progress), `undecodable` (corrupt/truncated content, over the per-object record limit, or unverifiable without an ETag), `retries_exhausted`. Each report hands the split back; quarantine at the attempt cap shows up in `etl_coordination_splits_quarantined`. |

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

## Coordination (`etl_coordination_*`)

Registered only when a source runs with multi-instance split coordination
(the `coordination`/`coordination-nats` features' backend, or any custom `SplitCoordinator`
handed a `CoordinationMetrics`). They fire alongside the source's own
`etl_source_rebalances_total` / `etl_source_lanes_active`.

| Metric | Type | Extra labels | Meaning |
|---|---|---|---|
| `etl_coordination_splits_owned` | gauge | | Splits this worker currently leases (its working set). |
| `etl_coordination_splits_completed` | gauge | | Splits observed completed across the fleet (bounded jobs). |
| `etl_coordination_splits_quarantined` | gauge | | Splits parked after exhausting delivery attempts — **alert on > 0**: a bounded job with quarantined splits ends stalled, not complete. |
| `etl_coordination_live_workers` | gauge | | Distinct live workers observed (the fleet view), including this one. |
| `etl_coordination_leader` | gauge | | 1 while this worker holds the planner leadership lease. |
| `etl_coordination_idle` | gauge | | 1 while this worker owns no splits and observes as a standby. |
| `etl_coordination_splits_draining` | gauge | | Splits this worker is currently draining away under revocation — the **drain** count, so a drain whose revocation was cancelled keeps counting until it actually lands. It falls when the drain ends by any route: the release, a terminal commit, a `fail`, a fence. Sitting non-zero on one worker while the fleet stays visibly unbalanced means drains are not finishing — look at that worker's sink health, then at `drain_deadline`. |
| `etl_coordination_acquisitions_total` | counter | `reason` (`create`\|`reclaimed`\|`expired`\|`reassigned`) | Split leases acquired, by how the split became claimable. `reassigned` is the healthy rebalance path: the previous owner cleared the record before letting go — a drained revocation, or a shutdown/scale-down hand-back — so the claim replays nothing. (The two are one label because a claiming worker cannot tell them apart: both present as a cleared owner and a vanished lease. Use `revocations_total{outcome="drained"}` on the releasing side to count revocations specifically.) `expired` spikes mean workers are dying — a dead owner's uncommitted tail replays. |
| `etl_coordination_split_losses_total` | counter | `reason` (`fenced`\|`starved`\|`revoked`) | Splits lost involuntarily: `fenced` by a peer's higher epoch, `starved` (self-fenced after a full lease with no successful write — store unreachable), or `revoked` — a cooperative drain that never completed, so this worker forced its own release and the uncommitted tail replays. Either the leader had un-assigned the split and the drain was declined or outran `drain_deadline`, or the revocation was `cancelled` and the drain it left behind then committed nothing for a full `drain_deadline` (a stalled drain is released too, or the split stays owned with nothing reading it — that one is usually re-claimed by this same worker). Renamed from `etl_coordination_revocations_total`, which now names a different family (next row but one). |
| `etl_coordination_releases_total` | counter | | Voluntary hand-backs (graceful shutdown, scale-down). |
| `etl_coordination_revocations_total` | counter | `outcome` (`requested`\|`drained`\|`forced`\|`cancelled`) | Splits the leader moved away from this worker by dropping them from its assignment. All four count on the **releasing** worker — one lifecycle, not two sides of a negotiation. `requested` is the denominator and every revocation terminates in exactly one of the other three — including when the split completes or is `fail`ed mid-drain, or when the process shuts down while draining — so `requested - drained - forced - cancelled` is the **revocations** still in flight. That is *not* `etl_coordination_splits_draining`, which counts **drains**: `cancelled` ends a revocation and leaves its drain running, so the gauge reads higher for as long as that drain takes. `drained` completed cooperatively and replays nothing; the gaining worker counts the matching `acquisitions_total{reason="reassigned"}`. `forced` means the source declined, the drain outran `drain_deadline`, or the split was fenced away before the release landed — the uncommitted tail replays. Sustained `forced` is the alert: `drain_deadline` is too tight for the source's commit interval, or a lane is wedged. `cancelled` means the leader took the revocation back — it named the split for this worker again while this worker still held it — so a drain slower than `drain_deadline` gets to finish cleanly instead of being charged a replay for a move nobody wants. If the source had already stopped intake, that drain still ends by handing the split back and this worker re-claims it replay-free (an `acquisitions_total{reason="reassigned"}` with no matching `drained`); if the source had declined, the split simply stays. Sustained `cancelled` means membership is flapping faster than a drain takes. **This name changed meaning**: before leader-assigned coordination it carried a `reason` label and counted involuntary losses, now `etl_coordination_split_losses_total`. |
| `etl_coordination_splits_planned_total` | counter | | Splits this worker wrote into the plan while leader. |
| `etl_coordination_replans_total` | counter | `outcome` (`ok`\|`error`\|`noop`) | Planner runs while leader; `noop` = the enumeration produced nothing new (the normal steady state of an open plan). |
| `etl_coordination_split_failures_total` | counter | | Explicit poison reports (`fail`) from the source. |
| `etl_coordination_quarantines_total` | counter | | Splits parked at the attempt cap. |
| `etl_coordination_writes_total` | counter | `outcome` (`ok`\|`conflict`\|`error`) | Split-record writes. `conflict` is a lost compare-and-swap — fencing working as designed, alarming only in bulk. |
| `etl_coordination_write_duration_seconds` | histogram | | Split-record write round-trip. |
| `etl_coordination_replan_duration_seconds` | histogram | | One planner run (enumeration included). |
| `etl_coordination_reconcile_duration_seconds` | histogram | | One full reconcile listing (the missed-watch-event backstop). |
| `etl_coordination_store_op_duration_seconds` | histogram | `op` (`get`\|`put`\|`delete`\|`list`\|`watch`) | Store primitive round-trips — the NATS latency view. |
| `etl_coordination_drain_duration_seconds` | histogram | | One cooperative drain, on the **releasing** worker: revocation requested to the release landing — stopping intake at a safe boundary, committing the drained tail, giving the split up. Only drains that end a revocation cooperatively are observed; a forced release is a *failed* drain and is counted as `revocations_total{outcome="forced"}` instead, so `drain_deadline` never shows up as a spike in this distribution. A drain whose revocation was `cancelled` is not observed either — when it lands it is no longer ending a revocation, so timing it would mix "how long a handoff takes" with "how long a withdrawn one took to unwind". Read it against `drain_deadline`: a p99 creeping toward it means forced revocations are imminent. |
| `etl_coordination_assignment_latency_seconds` | histogram | | One assignment wait, on the **gaining** worker: a split appearing in this worker's assignment to this worker holding its lease. This is time-to-balance as an operator experiences it — how long work the leader had already decided this worker should be doing sat undone. It spans whatever stood in the way, including the previous owner's drain, rather than flattering itself by timing only the final claim. |

### The two coordination latencies do not compose

`drain_duration_seconds` and `assignment_latency_seconds` were a single
family split by a `phase` label until the balancer became leader-assigned,
and that shape asserted a containment which no longer exists. Do not add,
subtract, or otherwise combine them.

They are measured in different processes. A drain is timed on the worker
*losing* a split, anchored on that worker observing its own `assign` record
shrink. An assignment wait is timed on the worker *gaining* a split,
anchored on that worker observing its own `assign` record grow. The leader
publishes those two records separately and each worker sees its own on its
own clock, so even for a single split's move neither window reliably
contains the other — a gaining worker that observes its assignment late can
start its wait after the drain has already finished.

Their populations differ too. Every assigned split is waited for, including
brand-new splits nobody has held and a dead owner's work reclaimed after
lease expiry; no revocation was involved in either, and no drain was timed.
`assignment_latency_seconds` is therefore not a revocation measurement at
all, which is the deeper reason it is not a `phase` of one.

Two families rather than one label also makes the meaningless query
unwritable instead of merely discouraged: with a shared family,
`histogram_quantile` over `sum by (le)` — aggregating the `phase` away —
looks like an ordinary panel expression while silently merging two
populations recorded on two machines.

Cardinality note: a coordinated source's checkpoint partitions are minted
per split *tenancy* (monotonic), so per-partition series under
`metrics.per_partition_detail: true` grow over a long, churny job; the
default (off) is unaffected. `etl_source_lag_records` is ungated, so a source
that both publishes lag and mints monotonic partition ids would grow without
bound — no shipped source does (Kafka's partitions are bounded by the topic;
the S3 source publishes no lag), but a new connector must not.

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
