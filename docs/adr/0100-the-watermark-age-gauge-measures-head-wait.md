---
description: "The watermark age gauge measures how long a watermark has waited on its head batch, pending or failed. Liveness and the stall watchdog count failed batches only."
---

# ADR-0100 — The watermark age gauge measures how long a watermark has waited on its head batch

- **Status:** accepted
- **Date:** 2026-11-13
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

`spate_checkpoint_watermark_age_seconds` and the `/healthz` watermark check
read one value, which the controller took from the partitions stalled behind a
failed batch. A batch that stays pending, such as one behind a sink retrying
without limit under the default `retry.max_attempts: 0`, left the gauge at 0.
Under the default `checkpoint.stalled_fail_after` of 120 s, a failed batch also
ends the run before the gauge reaches the documented 300 s alert, so that alert
could not fire.

A healthy pipeline also holds a pending head batch: for up to one
`checkpoint.interval` before its chunk seals, then up to the sink's
`batch.linger`, then for the write. The loader accepts an interval up to a day
and a linger up to a year. The `/healthz` limit is a fixed 300 s.

## Considered options

- The gauge measures how long each partition watermark has waited on its
  current head batch, pending or failed, and restarts when the watermark moves
- The gauge measures the age of the oldest unacknowledged batch since it was
  registered
- The code stays, and the docs describe the gauge as time stalled behind a
  failed batch

For `/healthz`, following the gauge was considered with the fixed limit, with a
limit derived from `checkpoint.interval` and the longest `batch.linger`, and
with a new configuration key. For `checkpoint.stalled_fail_after`, failing a
pending head as well as a failed one was considered.

## Decision outcome

Chosen option: "The gauge measures how long each partition watermark has
waited on its current head batch", because it rises on a watermark that has
stopped moving, for any cause, and stays low on one that moves slowly. The age
since registration also counts the time a batch queues behind earlier ones, so
under a deep queue it passes 300 s on a pipeline that is still committing.
Leaving the code as it was keeps the documented alert unable to fire.

`/healthz` and `checkpoint.stalled_fail_after` count failed batches only. A
liveness restart replays into the same linger, so following the gauge at the
fixed 300 s would restart a healthy low-volume pipeline in a loop. A derived
limit holds only for pipelines the builder assembles, since a hand-assembled
runtime brings its own sink pool, and a new key would still restart such a
pipeline on upgrade. Failing a pending head at `stalled_fail_after` would cap
the unbounded default retry at 120 s, and would change what `Source::stalled`
tells a seeded S3 feed under [ADR-0096](0096-a-seeded-run-fails-back-a-unit-the-sink-abandoned.md).

### Consequences

- Good, because a sink retrying without limit shows on the gauge, and the
  documented alert can fire under the default configuration.
- Good, because liveness keeps its behavior, so no deployment starts restarting
  on upgrade.
- Bad, because the gauge's healthy reading depends on configuration: an alert
  threshold has to sit above `checkpoint.interval` plus the longest sink
  `batch.linger` plus the write.
- Bad, because liveness does not restart a pipeline whose sink client is
  wedged on a pending write. The alert is the only signal for that case.
- Bad, because a new assignment epoch starts a fresh head, so a group that
  rebalances during an outage restarts the reading at each rebalance.

### Confirmation

`a_pending_head_ages_the_watermark_gauge_until_the_watermark_moves` and
`a_pending_head_neither_turns_liveness_nor_counts_as_a_stall` in
`crates/spate-core/src/pipeline/tests.rs`, with the head-stamp unit tests in
`crates/spate-core/src/checkpoint/tracker.rs` and `checkpointer.rs`.

## More information

Issue [#1028](https://github.com/spate-etl/spate/issues/1028).
[ADR-0002](0002-at-least-once-delivery.md) accepts a watermark stalled behind a
failed batch as the failure mode. [Monitoring](../user-guide/05-deployment/monitoring.mdx)
gives the alert, and [Metrics](../METRICS.md) describes the gauge.
