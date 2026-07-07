# Monitoring

Every stage of a pipeline is instrumented, and the instrumentation API is
deliberately not framework-specific: it is the
[`metrics`](https://crates.io/crates/metrics) facade. Anything you record
with `metrics` macros or handles exports through the same endpoint as the
framework's own series — there is no extra registry to learn.

> [!NOTE]
> This page explains the model. The **canonical metric taxonomy** — every
> metric name, type, label, and histogram bucket — lives in
> [docs/METRICS.md](../../METRICS.md). Link targets there don't move; this
> page intentionally does not duplicate the list.

## The exporter and the admin server

The `metrics` config section selects the exporter:

```yaml
metrics:
  exporter: prometheus    # default; `none` disables export
  listen: 0.0.0.0:9090    # admin server: /metrics, /healthz, /readyz
```

With `prometheus` (the default), the exposition endpoint mounts at
`GET /metrics` on the admin server, next to the probes described in
[Docker](docker.md). Scrape it with a PodMonitor or scrape annotations.

Ordering is handled for you: `Pipeline::from_config` installs the exporter
before you can construct any metric handle, so a handle recording into the
void (built before the recorder existed) is unconstructible through the
builder. If you assemble manually, call `metrics::install` first — see
[Manual assembly](../03-guides/manual-assembly.md).

## Conventions you can rely on

- **Prefix**: all framework metrics start with `etl_`; process metrics
  (`process_*`) follow their own standard. Counters end in `_total`; units
  get suffixes (`_seconds`, `_bytes`, `_rows`).
- **Standard labels on every framework metric**:
  - `pipeline` — the `pipeline.name` from config;
  - `component` — the instance id (e.g. `orders_kafka`, `main.deserializer`);
  - `component_type` — the implementation (e.g. `kafka`, `clickhouse`,
    `map`; custom sinks set it via `SinkParts::with_component_type`).
- **Cardinality is opt-in**: `partition`-labelled series appear only with
  `metrics.per_partition_detail: true` (default `false`), because their
  cardinality grows with the assignment. `shard` and `replica` labels are
  bounded by your cluster topology and always on.

## The hot-path discipline

Two rules keep instrumentation off the per-record path, and they bind your
custom metrics as much as the framework's:

1. **Pre-register handles at build time.** Resolve metric names and labels
   once, keep the returned handle, and only touch the handle afterwards.
   Never call `metrics::counter!(...)` with computed labels inside the
   record loop.
2. **Count at batch boundaries.** Counters increment once per batch with
   the batch's totals; per-record durations are observed as batch means
   (duration divided by n), never per record.

The runnable pattern — custom handles side by side with a framework stage
handle — is `crates/etl/examples/custom_metrics.rs`:

```rust
// Pre-register once, at build time.
let orders_enriched = metrics::counter!("myapp_orders_enriched_total", "region" => "eu");
let enrich_seconds = metrics::histogram!("myapp_enrich_duration_seconds");

// Hot loop: touch only the handles, count per batch.
orders_enriched.increment(batch_size);
enrich_seconds.record(0.012);
```

## Starting alerts

These three catch the failure modes that matter most; verify thresholds
against your latency budget. The full list of starting points is at the end
of [docs/METRICS.md](../../METRICS.md).

- **Stuck pipeline (watermark stall)** —
  `etl_checkpoint_watermark_age_seconds > 300` while
  `rate(etl_source_records_total[5m]) > 0`. The watermark age is the age of
  the oldest unacknowledged batch; a failed batch stalls its partition's
  watermark rather than silently advancing (the at-least-once invariant),
  so this is the primary "data is flowing but nothing is committing"
  signal. The pipeline self-terminates after
  `checkpoint.stalled_fail_after` (default 120s) behind a permanently
  failed batch, but you want the alert regardless — restarts that don't
  clear the cause will loop.
- **Abandoned sink batches** —
  `increase(etl_sink_abandoned_batches_total[10m]) > 0`. A batch is
  abandoned when a drain deadline expires with the sink still down; its
  data replays after restart, so this is a duplicates-incoming and
  sink-health signal, not data loss.
- **Sustained backpressure** — `etl_backpressure_paused == 1` sustained
  (e.g. `avg_over_time(etl_backpressure_paused[10m]) > 0.9`). The source is
  paused because the in-flight budget is above its high watermark: the sink
  cannot keep up, or the budget is undersized for the batch settings — see
  the sizing rule in [Tuning](tuning.md).

For alerting on skipped records (schema drift, poison messages), replica
health, and queue saturation, use the `*_dropped_total`,
`etl_sink_replica_healthy`, and `etl_queue_full_events_total` series
described in [docs/METRICS.md](../../METRICS.md).

## Related

- [Error handling](../02-concepts/04-error-handling.md) — how Skip/Fail
  policies surface in the `*_dropped_total` and `*_errors_total` series.
- [Backpressure](../02-concepts/03-backpressure.md) — what the
  `etl_backpressure_*` gauges are measuring.
- [Tuning](tuning.md) — turning what you see on dashboards into config
  changes.
