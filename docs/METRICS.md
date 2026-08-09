# Metrics

Spate instruments every pipeline stage through the [`metrics`](https://crates.io/crates/metrics)
facade. The framework installs an exporter selected by the `metrics` config
section (`prometheus` exposes a scrape endpoint on the admin server; `none`
disables export). Pipeline authors and connectors register their own metrics through the same
facade — anything recorded is exported alongside the framework's. The
recommended path is a `Meter`: it attaches the three standard labels below and
**auto-prefixes the name `spate_<namespace>_`**, so custom series live under the
same `spate_` umbrella and an operator finds everything under one root. You pass a
local name (`schema_fetches_total`); the `Meter` adds the prefix. The namespace
is `custom` for pipeline-author metrics (`spate_custom_*`) or a segment a
connector claims (`spate_kafka_*`), and it can never be one of the framework's
reserved stage roots (`source`, `deser`, `operator`, `queue`, `backpressure`,
`sink`, `checkpoint`, `coordination`, `e2e`, `pipeline`), so custom families
cannot collide with a framework metric. A built-in source or sink receives its
`Meter` from the runtime, scoped by its declared `component_type` and its role
(`spate_<component_type>_source_*` / `_sink_*`), so a connector that is both a
source and a sink keeps its families apart. Dropping to the raw `metrics` macros is the escape hatch for
a metric you deliberately want *outside* the `spate_` namespace. See
[Instrumenting connectors](user-guide/06-extending/instrumenting-connectors.mdx).

## Conventions

- All framework metrics are prefixed `spate_`. Process metrics
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
  **`spate_source_lag_records` is the one exception** — its `partition` label is
  unmarked because consumer lag is always published. It is the family's only
  representation, and a cardinality knob that could delete a golden signal is
  how a maximally backlogged consumer ends up reporting nothing.
- Hot-path discipline: all handles are pre-registered at pipeline build time;
  counters are incremented at **batch** boundaries; per-record duration
  histograms are observed per batch (duration ÷ n reported as batch means),
  never per record.

## Connector families

This file documents the framework's taxonomy and its own families. A
connector's families are documented **on that connector's page**, under its
`## Metrics` heading — the same split the `Meter` API enforces in code, where
the framework owns the reserved stage roots and a connector claims a namespace
beneath `spate_`.

| Prefix | Documented in |
|---|---|
| `spate_kafka_source_*` | [Kafka source](user-guide/04-connectors/sources/kafka/README.mdx#metrics) |
| `spate_kafka_sink_*` | [Kafka sink](user-guide/04-connectors/sinks/kafka/README.mdx#metrics) |
| `spate_s3_source_*` | [S3 source](user-guide/04-connectors/sources/s3/README.mdx#metrics) |
| `spate_json_deser_*` | [JSON format](user-guide/04-connectors/formats/json/README.mdx#metrics) |
| `spate_datagen_source_*` | [Datagen source](user-guide/04-connectors/sources/datagen/README.mdx#metrics) |
| `spate_custom_*` | Whatever registered it — the default namespace for pipeline-author metrics. |

A connector that registers no families of its own is fully described by the
framework families below, labeled with its `component_type`.

## Source (`spate_source_*`)

| Metric | Type | Extra labels | Description |
|---|---|---|---|
| `spate_source_records_total` | counter | | Records emitted by the source (post-poll, pre-deserialization). |
| `spate_source_bytes_total` | counter | | Payload bytes emitted. |
| `spate_source_poll_duration_seconds` | histogram | | Time spent inside `poll` per call. |
| `spate_source_lag_records` | gauge | `partition` | Consumer lag (log-end offset − committed), **always per partition** — there is no aggregate series. Aggregate in the query layer: `sum` for total backlog (comparable with the broker's group lag), `max` for the worst partition. A partition whose lag the client has not measured yet is **absent**; a partition the member no longer owns reads `0`. |
| `spate_source_rebalances_total` | counter | `event` (`assign`\|`revoke`) | Rebalance events observed. |
| `spate_source_lanes_active` | gauge | | Currently assigned lanes (partitions). |

### Alerting on consumer lag

```promql
# Total backlog for a pipeline — the figure to compare with the broker's
# group lag. Every member's owned partitions sum into one number.
sum by (pipeline) (spate_source_lag_records)

# The worst single partition, which a total can hide.
max by (pipeline) (spate_source_lag_records)

# Lag was never measured: statistics are disabled, the consumer has not
# committed yet, or the source is not publishing. Absence is deliberate — a
# `0` here would be indistinguishable from "caught up", which is how this
# family once read zero on a fully backlogged consumer.
absent(spate_source_lag_records{pipeline="orders"})

# Only *some* partitions have reported. The sum above silently omits the
# rest, so it under-reports with no other signal. `lanes_active` is the
# assignment size, so a shortfall here means the total is partial.
count by (pipeline) (spate_source_lag_records)
  < sum by (pipeline) (spate_source_lanes_active)
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

### Series ownership

The same "no deletion, no idle timeout" fact has a second consequence: a gauge
series is backed by one shared atomic in the recorder, so two handle sets that
resolve the *same* `(name, labels)` are two writers on one cell. Counters
survive that — they only ever add, so a duplicate degrades into a sum a reader
can still make sense of. Gauges do not: the interesting ones here are
**edge-triggered** (sink shard health flips on a breaker transition, retry
backoff on a retry), so a second writer's reading stands until the owner's next
transition — which for a quarantined shard may be never. A duplicated gauge
degrades into a lie.

So every framework handle struct that owns gauges **claims** its series at
construction, keyed by its stage root and the standard labels (plus `shard` or
the queue name where those identify the instance). One claim per series per
process:

- The **owner** — the first to claim — publishes normally.
- A **shadow** — any later handle set on the same key — still records its
  counters and histograms (they aggregate), but every gauge write is dropped,
  so the owner's readings are never overwritten. This is what lets each pipeline
  thread keep its own `SourceMetrics` for counting polls while only the
  controller's instance publishes lag and active lanes.

On the assembly path (`Pipeline`) a collision is a hard `BuildError` /
`StartError` — two pipelines, or two components, sharing a name in one process
is a wiring mistake caught before any data flows. Constructing a handle struct
directly (a hand-built harness, a test fixture) logs an error and shadows
instead, because a metrics label collision must never take down a healthy data
path.

Two consequences worth stating outright:

- **A shadow is never promoted.** The claim frees when the owner is dropped, so
  a pipeline rebuilt *sequentially* — drop the old one, then build — re-owns its
  series cleanly. But an *overlapping* rebuild (build the replacement while the
  original still runs) leaves the newcomer a permanent shadow: on the assembly
  path it fails to build; on the direct path it runs gauge-silent for its whole
  life. In-process blue-green is therefore drop-then-build, not build-then-drop.
- **Ownership is process-local.** Horizontal replicas never collide: each pod is
  its own recorder and Prometheus attaches `instance` at scrape, so identical
  label sets from different pods are different TSDB series. The one way to
  re-create the collision across pods is an aggregation layer that *strips*
  `instance` — a Pushgateway with `honor_labels`, or a recording rule / remote
  write relabeling that drops it — which is outside the framework's reach.

## Deserializer (`spate_deser_*`)

| Metric | Type | Extra labels | Description |
|---|---|---|---|
| `spate_deser_records_total` | counter | `outcome` (`ok`\|`error`) | Deserialization attempts by outcome. One input payload may yield 0..N records; this counts outputs, plus one `error` per failed payload. |
| `spate_deser_records_dropped_total` | counter | `reason` (`skip_policy`) | Payloads dropped by the Skip error policy. |
| `spate_deser_not_ready_total` | counter | | Payload replays waiting on an upstream dependency (e.g. a schema-registry fetch). Neither an error nor backpressure — the batch retries and completes once the dependency arrives. |
| `spate_deser_batch_duration_seconds` | histogram | | Deserialization time per source batch. |

## Operators (`spate_operator_*`)

| Metric | Type | Extra labels | Description |
|---|---|---|---|
| `spate_operator_records_in_total` | counter | | Records entering the operator. |
| `spate_operator_records_out_total` | counter | | Records emitted downstream (filter drops and flat_map fan-out make this differ from in). |
| `spate_operator_records_dropped_total` | counter | `reason` (`filtered`\|`skip_policy`\|`unrouted`) | Records intentionally removed (`unrouted`: matched no split-sink branch under an `unmatched: Skip` policy). |
| `spate_operator_errors_total` | counter | `error_type` | User-code errors by taxonomy class. |
| `spate_operator_batch_duration_seconds` | histogram | | Processing time per batch through this operator. |

## Queues (pipeline → sink handoff) (`spate_queue_*`)

Queues are labeled by edge: `queue` = `<upstream>-><downstream>` (e.g.
`chain->sink/shard-3`).

| Metric | Type | Extra labels | Description |
|---|---|---|---|
| `spate_queue_depth` | gauge | `queue` | Items currently queued, sampled on each send. It only advances *on a send*, so while the source is paused (or flow otherwise stalls) it freezes at its last sample — typically `capacity` — even as the workers drain the queue. The live resume decision uses the channel directly, not this gauge. |
| `spate_queue_capacity` | gauge | `queue` | Configured bound. |
| `spate_queue_full_events_total` | counter | `queue` | `try_send` rejections (each is a backpressure signal, never a block). A rejected chunk is parked and retried once per poll cycle while blocked, and every retry that still finds the queue full counts again — so during a sustained stall this tracks retry cadence, not distinct fill episodes. |

## Backpressure (`spate_backpressure_*`)

| Metric | Type | Extra labels | Description |
|---|---|---|---|
| `spate_backpressure_paused` | gauge | | 1 while the source is paused by the watermark controller. |
| `spate_backpressure_paused_seconds_total` | gauge (monotonic) | | Cumulative paused time. Exported as a gauge because the `metrics` facade's counter is integer-only; treat as a counter in queries (`rate()` works). |
| `spate_backpressure_pause_events_total` | counter | | Pause transitions (flapping indicator when high). |
| `spate_backpressure_inflight_bytes` | gauge | | Current global in-flight byte budget usage. |

## Sink (`spate_sink_*`)

In a [multi-sink](user-guide/02-concepts/06-multi-sink.mdx) pipeline each sink's
series carry its name as the `component` label (a single sink uses
`component="sink"`), so per-table sink metrics never collide.

| Metric | Type | Extra labels | Description |
|---|---|---|---|
| `spate_sink_records_total` | counter | `shard` | Records durably written (acknowledged flushes only). |
| `spate_sink_bytes_total` | counter | `shard` | Bytes durably written. |
| `spate_sink_batch_rows` | histogram | | Rows per sealed batch. |
| `spate_sink_batch_bytes` | histogram | | Bytes per sealed batch. |
| `spate_sink_flushes_total` | counter | `shard`, `reason` (`rows`\|`bytes`\|`linger`\|`drain`) | Flushes by trigger. |
| `spate_sink_flush_duration_seconds` | histogram | `shard` | **Seal→settle** per durably written batch: the wait for an `inflight.max_per_shard` slot, every attempt, every retry-backoff sleep and quarantine probe wait, and the write that finally succeeded. The right input for a commit-lag budget and the **wrong** first reach for "is the sink slow" — see [What a flush contains](#what-a-flush-contains). Only settled batches are observed; an abandoned one is counted by `spate_sink_abandoned_batches_total` instead. |
| `spate_sink_write_duration_seconds` | histogram | `shard`, `outcome` (`ok`\|`error`) | One **attempt**, wrapping the sink write and nothing else *of the framework's* — no permit wait, no retry backoff, no probe wait. This is the "how fast is the sink system" signal. Note what that does include: a connector that sleeps *inside* its write puts that sleep here (the Kafka sink does, on a full producer queue), and the wall-clock charges whatever the sink's I/O runtime was busy with at each await point. Every attempt is observed, retries included, so the count is per attempt rather than per batch. Split by outcome because a fatal reject in a millisecond and a timeout after thirty seconds are both attempts, and mixing them moves the distribution in opposite directions; the error's taxonomy class stays on `spate_sink_errors_total{error_type}`. An attempt aborted at the drain deadline is never observed — but attempts that completed before the abort are. |
| `spate_sink_permit_wait_duration_seconds` | histogram | `shard` | Time a sealed batch waited for one of its shard's `inflight.max_per_shard` slots before its first write attempt — the queueing share of a flush. Observed for every sealed batch **that starts a write**, the healthy near-zero case included: a family that appeared only under contention would read as absent exactly when you want to confirm there is none. A batch the drain deadline drops before it ever gets a permit is not observed, so during a rough shutdown this count runs *below* the number of batches sealed. The queue is FIFO and normally holds one batch, in which case the wait is roughly one in-flight write's remaining time; it can hold more — up to one intake pass, plus the drain force-seal — and a batch behind others is then observed with a wait spanning several write completions. Reaching more than one requires a single chunk to cross `batch.max_bytes` on its own, so on a sane `chunk.target_bytes` this stays at one. |
| `spate_sink_retries_total` | counter | `shard` | Flush attempts beyond the first. |
| `spate_sink_retry_backoff_seconds` | gauge | `shard` | Retry backoff the shard is currently sleeping between write attempts **on an available replica**; `0` when no write is backing off. The step being served, **not** the time left in it — it does not count down, and jitter puts it in `[(1 - retry.jitter) × step, step]`. A shard writes up to `inflight.max_per_shard` batches at once, each backing off independently, so this is the max across them: it answers "is this shard asleep right now, and for how long", which the counters cannot (`spate_sink_retries_total` only moves on an *attempt*, so a shard parked in a long backoff looks flat and idle). A shard whose every replica is quarantined also sleeps — waiting for the earliest of a probe window and an in-flight probe reporting — and reads `0` here, because no attempt is being backed off; `spate_sink_shard_healthy == 0` covers that state. The implication runs one way: the write loop waits only when no replica is circuit-closed, but a shard with none can still be handing out a half-open probe rather than waiting. That is the safe direction — alerting on shard health cannot miss a parked shard. |
| `spate_sink_errors_total` | counter | `shard`, `error_type` | Write errors by taxonomy class. |
| `spate_sink_inflight_batches` | gauge | `shard` | Sealed batches not yet settled — those being written **plus** any sealed batch still queueing for one of the shard's `inflight.max_per_shard` slots. It can therefore sit one above the cap while a batch waits, and further above it in the pathological case where a single chunk crosses `batch.max_bytes` on its own (see `spate_sink_permit_wait_duration_seconds`). Read it against the cap for saturation, not as an equality. |
| `spate_sink_replica_healthy` | gauge | `shard`, `replica` | 1 = circuit closed, 0 = not usable — open (quarantined) **or** half-open, where a replica is being probed but is not yet carrying ordinary writes. The distinction matters when reading `spate_sink_shard_healthy`, which is `≥1 replica closed` by the same rule. |
| `spate_sink_breaker_opens_total` | counter | `shard`, `replica` | Circuit-breaker open transitions. |
| `spate_sink_replica_errors_total` | counter | `shard`, `replica` | Failed write attempts attributed to a replica — which endpoint is erroring (`spate_sink_errors_total` gives the class breakdown per shard). |
| `spate_sink_shard_healthy` | gauge | `shard` | 1 = the shard has ≥1 circuit-closed replica; 0 = no replica is circuit-closed (every one quarantined or half-open probing) — intake stalls and the shard back-pressures the source while recovery probes keep firing. The probe cadence is `open_for` **plus** the probe's own duration — the re-open deadline is stamped when a failure is reported, not when the attempt began. Concurrency while half-open is budgeted by `breaker.half_open_probes` (default 1) per probe episode, not globally: a failure reported by an earlier attempt re-opens the replica and ends the episode, so a probe still in flight from it can overlap the next one. Size a replica's capacity for that, not for exactly one. |
| `spate_sink_abandoned_batches_total` | counter | `shard` | Batches abandoned at drain deadline (will replay after restart). |
| `spate_sink_drain_overrun_total` | counter | `shard` | Shard workers force-aborted for failing to return by the drain deadline. **Non-zero is a framework bug, not an operating condition** — a worker is supposed to abandon at the deadline under its own power. Shutdown still terminated and the data still replays (the acknowledgments fail with the worker), but that shard's batches are missing from `abandoned` above and from the `ExitReport`'s drain counts, so the two disagree by an unknown amount. Alert on `> 0`. |

### What a flush contains

A flush is a batch's whole journey from the moment it was sealed to the moment
it settled, and only the last leg of that is the sink actually writing:

```
seal ────────────────────────────────────────────────────────► settle
└──────────────── spate_sink_flush_duration_seconds ─────────────────┘

  waiting for an inflight slot  spate_sink_permit_wait_duration_seconds
  attempt 1, failed             spate_sink_write_duration_seconds{outcome="error"}
  sleeping before the retry     spate_sink_retry_backoff_seconds  (gauge)
  every replica quarantined,
    waiting for a probe         spate_sink_shard_healthy == 0     (gauge)
  attempt 2, succeeded          spate_sink_write_duration_seconds{outcome="ok"}
```

Note where the span starts. `linger` and the chunk's time in the shard queue
both elapse *before* the seal, so neither is in this histogram — if you are
budgeting end-to-end freshness rather than commit lag, `spate_e2e_latency_seconds`
is the family that spans them.

So a rising flush p99 is not evidence about the sink. It is equally consistent
with a sink that has not slowed down at all while the shard queues behind its
`inflight.max_per_shard` cap — which is how a healthy ClickHouse cluster
(40% CPU, merges keeping up) came to be investigated as a slow one. Ask the
three questions separately:

```promql
# Is the sink slow? Successful attempts only, so a fast fatal reject cannot
# flatter it and a timeout cannot wreck it.
histogram_quantile(0.99, sum by (le, pipeline, component) (
  rate(spate_sink_write_duration_seconds_bucket{outcome="ok"}[5m])))

# Am I queueing? If this carries the flush p99, the answer is more shards or
# a larger inflight.max_per_shard — not a faster sink.
histogram_quantile(0.99, sum by (le, pipeline, component) (
  rate(spate_sink_permit_wait_duration_seconds_bucket[5m])))

# What is my commit-lag budget? The whole journey, which is what a watermark
# actually waits on.
histogram_quantile(0.99, sum by (le, pipeline, component) (
  rate(spate_sink_flush_duration_seconds_bucket[5m])))
```

Keep `component` in the `by` clause. A pipeline with several sinks puts them
all on these families, distinguished only by that label, and aggregating it
away blends a fast sink's distribution with a slow one's — the tail you are
looking for gets diluted below the threshold, and the quantile cannot tell you
*which* sink it came from. Aggregating `shard` away is fine: those are one
population.

The two legs with no histogram are both sleeps, and deliberately so: a shard
parked is a *level*, not an event. They are published by different gauges,
and reading the wrong one is how the residual becomes a mystery:

- **Backing off between attempts on an available replica** —
  `spate_sink_retry_backoff_seconds` publishes the step live.
- **Every replica quarantined, waiting for a probe** —
  `spate_sink_retry_backoff_seconds` reads **`0`** here, because no attempt is
  being backed off. `spate_sink_shard_healthy == 0` covers this state. It is a
  covering signal, not an equivalent one: the write loop waits only when no
  replica is circuit-closed, but a shard with none can still be handing out a
  half-open probe rather than waiting.

So a flush p99 that exceeds write + permit wait is a shard sleeping rather than
a shard writing — but check `spate_sink_shard_healthy` before you check the
backoff gauge. A quarantined shard can hold a flush open for a whole `open_for`
window with the backoff gauge flat at zero throughout.

Three asymmetries worth knowing before you subtract one family from another.
Flush is observed once per **batch** and only for batches that settled. Write
is observed once per **attempt**, so a batch that retried contributes several
observations — including a batch that is later abandoned, whose completed
attempts stay in `{outcome="error"}` with no matching flush. And "abandoned" is
not only a drain-deadline event: a fatal class, exhausted `retry.max_attempts`,
or a panicking write task all abandon in steady state with no drain in sight
(`spate_sink_abandoned_batches_total` counts all four). The decomposition is a
way to reason about where a flush went, not an identity to compute.

## Checkpointing (`spate_checkpoint_*`)

| Metric | Type | Extra labels | Description |
|---|---|---|---|
| `spate_checkpoint_pending_batches` | gauge | `partition` ⚠ | Unacknowledged batches tracked; unlabeled series is the max across partitions. |
| `spate_checkpoint_commits_total` | counter | `outcome` (`ok`\|`error`) | Source commit calls. |
| `spate_checkpoint_commit_duration_seconds` | histogram | | Commit round-trip. |
| `spate_checkpoint_watermark_age_seconds` | gauge | | Age of the oldest unacknowledged batch — the primary "stuck pipeline" alert signal. |

## Coordination (`spate_coordination_*`)

Registered only when a source runs with multi-instance split coordination
(the `coordination`/`coordination-nats` features' backend, or any custom `SplitCoordinator`
handed a `CoordinationMetrics`). They fire alongside the source's own
`spate_source_rebalances_total` / `spate_source_lanes_active`.

| Metric | Type | Extra labels | Description |
|---|---|---|---|
| `spate_coordination_splits_owned` | gauge | | Splits this worker currently leases (its working set). |
| `spate_coordination_splits_completed` | gauge | | Splits observed completed across the fleet (bounded jobs). |
| `spate_coordination_splits_quarantined` | gauge | | Splits parked after exhausting delivery attempts — **alert on > 0**: a bounded job with quarantined splits ends stalled, not complete. |
| `spate_coordination_live_workers` | gauge | | Distinct live workers observed (the fleet view), including this one. |
| `spate_coordination_leader` | gauge | | 1 while this worker holds the planner leadership lease. |
| `spate_coordination_idle` | gauge | | 1 while this worker owns no splits and observes as a standby. |
| `spate_coordination_splits_draining` | gauge | | Splits this worker is currently draining away under revocation — the **drain** count, so a drain whose revocation was cancelled keeps counting until it actually lands. It falls when the drain ends by any route: the release, a terminal commit, a `fail`, a fence. Sitting non-zero on one worker while the fleet stays visibly unbalanced means drains are not finishing — look at that worker's sink health, then at `drain_deadline`. |
| `spate_coordination_acquisitions_total` | counter | `reason` (`create`\|`reclaimed`\|`expired`\|`reassigned`) | Split leases acquired, by how the split became claimable. `reassigned` is the healthy rebalance path: the previous owner cleared the record before letting go — a drained revocation, or a shutdown/scale-down hand-back — so the claim replays nothing. (The two are one label because a claiming worker cannot tell them apart: both present as a cleared owner and a vanished lease. Use `revocations_total{outcome="drained"}` on the releasing side to count revocations specifically.) `expired` spikes mean workers are dying — a dead owner's uncommitted tail replays. |
| `spate_coordination_split_losses_total` | counter | `reason` (`fenced`\|`starved`\|`revoked`) | Splits lost involuntarily: `fenced` by a peer's higher epoch, `starved` (self-fenced after a full lease with no successful write — store unreachable), or `revoked` — a cooperative drain that never completed, so this worker forced its own release and the uncommitted tail replays. Either the leader had un-assigned the split and the drain was declined or outran `drain_deadline`, or the revocation was `cancelled` and the drain it left behind then committed nothing for a full `drain_deadline` (a stalled drain is released too, or the split stays owned with nothing reading it — that one is usually re-claimed by this same worker). Renamed from `spate_coordination_revocations_total`, which now names a different family (next row but one). |
| `spate_coordination_releases_total` | counter | | Voluntary hand-backs (graceful shutdown, scale-down). |
| `spate_coordination_revocations_total` | counter | `outcome` (`requested`\|`drained`\|`forced`\|`cancelled`) | Splits the leader moved away from this worker by dropping them from its assignment. All four count on the **releasing** worker — one lifecycle, not two sides of a negotiation. `requested` is the denominator and every revocation terminates in exactly one of the other three — including when the split completes or is `fail`ed mid-drain, or when the process shuts down while draining — so `requested - drained - forced - cancelled` is the **revocations** still in flight. That is *not* `spate_coordination_splits_draining`, which counts **drains**: `cancelled` ends a revocation and leaves its drain running, so the gauge reads higher for as long as that drain takes. `drained` completed cooperatively and replays nothing; the gaining worker counts the matching `acquisitions_total{reason="reassigned"}`. `forced` means the source declined, the drain outran `drain_deadline`, or the split was fenced away before the release landed — the uncommitted tail replays. Sustained `forced` is the alert: `drain_deadline` is too tight for the source's commit interval, or a lane is wedged. `cancelled` means the leader took the revocation back — it named the split for this worker again while this worker still held it — so a drain slower than `drain_deadline` gets to finish cleanly instead of being charged a replay for a move nobody wants. If the source had already stopped intake, that drain still ends by handing the split back and this worker re-claims it replay-free (an `acquisitions_total{reason="reassigned"}` with no matching `drained`); if the source had declined, the split simply stays. Sustained `cancelled` means membership is flapping faster than a drain takes. **This name changed meaning**: before leader-assigned coordination it carried a `reason` label and counted involuntary losses, now `spate_coordination_split_losses_total`. |
| `spate_coordination_splits_planned_total` | counter | | Splits this worker wrote into the plan while leader. |
| `spate_coordination_replans_total` | counter | `outcome` (`ok`\|`error`\|`noop`) | Planner runs while leader; `noop` = the enumeration produced nothing new (the normal steady state of an open plan). |
| `spate_coordination_split_failures_total` | counter | | Explicit poison reports (`fail`) from the source. |
| `spate_coordination_quarantines_total` | counter | | Splits parked at the attempt cap. |
| `spate_coordination_writes_total` | counter | `outcome` (`ok`\|`conflict`\|`error`) | Split-record writes. `conflict` is a lost compare-and-swap — fencing working as designed, alarming only in bulk. |
| `spate_coordination_write_duration_seconds` | histogram | | Split-record write round-trip. |
| `spate_coordination_replan_duration_seconds` | histogram | | One planner run (enumeration included). |
| `spate_coordination_reconcile_duration_seconds` | histogram | | One full reconcile listing (the missed-watch-event backstop). |
| `spate_coordination_store_op_duration_seconds` | histogram | `op` (`get`\|`put`\|`delete`\|`list`\|`watch`) | Store primitive round-trips — the NATS latency view. |
| `spate_coordination_drain_duration_seconds` | histogram | | One cooperative drain, on the **releasing** worker: revocation requested to the release landing — stopping intake at a safe boundary, committing the drained tail, giving the split up. Only drains that end a revocation cooperatively are observed; a forced release is a *failed* drain and is counted as `revocations_total{outcome="forced"}` instead, so `drain_deadline` never shows up as a spike in this distribution. A drain whose revocation was `cancelled` is not observed either — when it lands it is no longer ending a revocation, so timing it would mix "how long a handoff takes" with "how long a withdrawn one took to unwind". Read it against `drain_deadline`: a p99 creeping toward it means forced revocations are imminent. |
| `spate_coordination_assignment_latency_seconds` | histogram | | One assignment wait, on the **gaining** worker: a split appearing in this worker's assignment to this worker holding its lease. This is time-to-balance as an operator experiences it — how long work the leader had already decided this worker should be doing sat undone. It spans whatever stood in the way, including the previous owner's drain, rather than flattering itself by timing only the final claim. |

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
default (off) is unaffected. `spate_source_lag_records` is ungated, so a source
that both publishes lag and mints monotonic partition ids would grow without
bound — no shipped source does (Kafka's partitions are bounded by the topic;
the S3 source publishes no lag), but a new connector must not.

## End-to-end

| Metric | Type | Extra labels | Description |
|---|---|---|---|
| `spate_e2e_latency_seconds` | histogram | | Source-to-durable-write latency, observed by the sink worker at each durable flush from the batch's **oldest** record. Time basis is `metrics.e2e_basis`: `ingest` (default, skew-free: time since the record entered the terminal stage) or `event` (against the record's event time; clock-skew sensitive, falls back to ingest when no event time exists). |

## Pipeline / process

| Metric | Type | Extra labels | Description |
|---|---|---|---|
| `spate_pipeline_info` | gauge | `version` | Constant 1; carries build metadata. |
| `spate_pipeline_state` | gauge | `state` (`starting`\|`running`\|`draining`\|`failed`) | 1 for the current state, 0 otherwise. |
| `spate_pipeline_threads` | gauge | | Pinned pipeline thread count. |
| `process_*` | — | | CPU, memory, fds via `metrics-process`. |

## Histogram buckets

Configured on the exporter by name-suffix matchers (override in the `metrics`
config section):

- `*_duration_seconds` / `spate_e2e_latency_seconds`: exponential
  `0.001 .. 60` (1ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms,
  1s, 2.5s, 5s, 10s, 30s, 60s).
- `spate_sink_batch_rows`: powers of 4 from `64` to `1_048_576`.
- `spate_sink_batch_bytes`: powers of 4 from `4 KiB` to `256 MiB`.

## Alerting starting points

- `spate_checkpoint_watermark_age_seconds > 5m` while
  `rate(spate_source_records_total[5m]) > 0` — pipeline is stuck, not idle.
- `spate_backpressure_paused == 1` sustained — sink capacity problem.
- `rate(spate_deser_records_dropped_total[5m]) > 0` — schema drift or poison
  messages being skipped.
- `spate_sink_replica_healthy == 0` — replica down; sustained across all
  replicas of a shard means the shard channel will fill and pause the source.
- `spate_sink_shard_healthy == 0` — no replica of the shard is circuit-closed;
  intake stalls and the shard back-pressures the source (the whole-shard
  escalation of the per-replica `replica_healthy` signal). Recovery probes keep
  firing — every `open_for` plus the failing probe's own duration, since the
  re-open deadline is stamped when the failure is reported — so the gauge stays
  0 through failed probe cycles until one succeeds. Pair with a rising
  `spate_sink_replica_errors_total{replica}` to identify the failing endpoints.
- `spate_sink_retry_backoff_seconds` sustained near `retry.max` — the shard is
  parked between attempts, not idle. Threshold at `(1 - retry.jitter) *
  retry.max`, **not** at `retry.max`: jitter only ever shortens a delay, so at
  the ceiling the published step is spread over `[(1 - jitter) * max, max]`
  and an `== retry.max` rule never fires. With a patient policy
  (`max_attempts: 0` and a `retry.max` over 5m, the combination
  `SinkPool::spawn` warns about at startup) this is the *only* live signal:
  `spate_sink_retries_total` goes flat because it moves on attempts,
  `spate_sink_inflight_batches` stays pinned, and `spate_sink_shard_healthy` can
  read `1` throughout *as long as no replica's breaker opens* — a batch can
  keep failing retryably while other batches keep the breakers closed. Pair
  with `spate_sink_errors_total{error_type="retryable"}` for the cause.
  Complementary, not overlapping: a shard whose every replica *is* quarantined
  also sleeps, and this gauge reads `0` there because no attempt is being
  backed off. `spate_sink_shard_healthy == 0` covers that state — the write loop
  waits only when no replica is circuit-closed, though a shard with none can
  still be handing out a half-open probe rather than waiting, so the gauge is
  the wider of the two. Alert on both to cover every parked state.
- `histogram_quantile(0.99, sum by (le, pipeline, component) (rate(spate_sink_write_duration_seconds_bucket{outcome="ok"}[5m])))`
  above your write budget — the sink itself is slow. Alert on this rather than
  on `spate_sink_flush_duration_seconds`, which also carries the permit wait and
  the backoff sleeps and so fires on a saturated *pipeline* as readily as on a
  slow *sink* ([What a flush contains](#what-a-flush-contains)). If write
  duration is flat while flush p99 climbs, read
  `spate_sink_permit_wait_duration_seconds` next, and `spate_sink_shard_healthy`
  after it: the answer is shards, `inflight.max_per_shard`, or a quarantined
  replica set — not the server. Keep a flush-duration alert too if you have a
  commit-lag SLO — that is the family the watermark waits on — but do not read
  it as sink latency.
Each connector adds alerts over its own families; those live in the
connector's `## Metrics` section, indexed under
[Connector families](#connector-families).
