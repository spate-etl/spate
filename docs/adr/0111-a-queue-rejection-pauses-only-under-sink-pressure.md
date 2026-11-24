---
description: "A shard-queue rejection pauses the source at once only while a shard waits for a write permit; otherwise only after min_pause of rejections."
---

# ADR-0111 — A queue rejection pauses the source at once only under sink pressure

- **Status:** accepted
- **Date:** 2026-11-24
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A pipeline thread hands chunks to each shard's bounded queue with `try_send`.
Before this decision, one rejected `try_send` made the watermark controller
pause the thread's lanes on its next tick, for at least
`backpressure.min_pause`, and each further rejection restarted that pause.

A shard worker drains its queue in passes. With many pipeline threads on a
small queue, the queue refills between passes, so threads were rejected and
paused while the shard's write permits sat idle. Each such rejection cost the
thread `min_pause` of input, and pipelines fell below the rate their sink could
take.

The worker stops reading its queue only while it holds a sealed batch with no
free write permit. That state means the sink is the limit. A full queue at any
other time means the worker has not reached the queue yet. The question was how
the pipeline thread's controller tells the two apart, and what it does in the
second case.

## Considered options

What tells sink pressure from a busy queue:

- A count, on the in-flight budget every sink pool and pipeline thread already
  shares, of the shards whose worker holds a sealed batch with no permit
- The same reading per shard, so a rejection by one queue reads only that
  shard's worker
- No reading: wait `min_pause` before any queue rejection pauses
- No reading: a new configuration key for the wait

What a rejection does while no shard waits for a permit:

- Pause once rejections have lasted `min_pause` with no push or flush
  completing
- Never pause on it

## Decision outcome

Chosen option: "a count on the in-flight budget", with rejections that pause
"once they have lasted `min_pause` with no push or flush completing", because
the count separates the two causes with one atomic that both layers already
share, and the fallback still stops a source whose worker has stalled.

Each shard worker keeps its share of the count from whether it holds a sealed
batch with no permit. It clears its share before its drain, and a guard clears
it when the worker's task ends, is aborted or unwinds.
`WatermarkController::on_queue_rejected` reads the count. While it is above
zero the rejection is sink pressure and pauses on the next tick, as before.
Otherwise the rejection starts or extends a run, and `tick` pauses once the run
spans `min_pause`. `WatermarkController::on_push_done` ends the run. Every
rejection site in the pipeline thread calls `on_queue_rejected`, and every
completed push or flush calls `on_push_done`.

The per-shard reading was measured beside the count. It paused less on one arm
and as often on the others, and neither changed the rate. A slow shard limits
every pipeline thread through seal-order parking either way, and both readings
then pause at once, because that shard is full and its worker waits for a
permit. The count is one atomic where the per-shard reading is one per shard.

A wait of `min_pause` on every queue rejection was rejected because it also
delays the pause when the sink is the limit. A new key was rejected for the
reason ADR-0107 gives: `min_pause` is already the time the runtime treats as
too short to pause for. Never pausing on a rejection while no shard waits would
leave a source unpaused behind a worker that has stopped taking chunks.

### Consequences

- Good, because a queue the sink keeps up with no longer pauses the source, so
  the source is not duty-cycled against `min_pause` while write permits sit
  idle.
- Good, because a rejection while a shard waits for a permit pauses at once,
  as before, so a saturated sink still stops the source client's prefetch.
- Bad, because the queue still fills. Pipeline threads retry where they used to
  pause, on a fixed 2 ms sleep, so `spate_queue_full_events_total` counts each
  retry and rose five to 150 times on the measured arms. Issue #2395 tracks the
  retry cadence.
- Bad, because every thread runs where some used to sit paused, and on the
  measured host each row then cost 1% to 31% more CPU cycles for the same
  instructions.
- Bad, because the count is per pipeline. One waiting shard in any sink makes
  every rejection sink pressure, so a fan-out with one slow sink pauses at once
  as before.
- Bad, because `min_pause` gains a third meaning beside the resume floor and
  the pending-ceiling wait.
- Bad, because a closed queue rejects like a full one. Once a worker has gone,
  a rejection pauses only after `min_pause`.
- Neutral, because INV-2 holds: no send blocks, and a rejected push is retried
  on the existing retry sleep, so no thread spins.
- Neutral, because INV-5 holds: the worker adds one synchronous comparison per
  pass, and touches the shared count only when its share changes.

### Confirmation

The controller's rule is tested in `crates/spate-core/src/backpressure.rs`
(`a_contended_run_pauses_once_it_spans_min_pause` and the tests beside it, the
two property tests, and the loom model `balanced_gates_converge_to_zero`). Each
rejection site and each completed push or flush is tested in
`crates/spate-core/src/pipeline/driver.rs`
(`a_blocked_push_with_no_shard_gated_does_not_pause` and the tests beside it).
The worker's share is tested in `crates/spate-core/src/sink/pool_tests.rs`
(`a_batch_waiting_for_a_permit_counts_its_shard_as_gated` and the three tests
after it).

## Evidence

Measured by a queue benchmark rig this repository does not carry, on one
18-core host (6 Super and 12 Performance cores) under load from other
processes. Every arm used 512 KiB queues, 32 pipeline threads, and made every
twentieth write four times slower. Figures are per 20-second window, for three
windows per leg, base against the landed code. Process CPU is
`/usr/bin/time -l` over the whole run, divided by the rows it settled.

- 8 shards, 1 ms writes, `min_pause` 50 ms, offered 118.7M rows/s: 8,972 to
  9,262 pauses at 103.4M to 106.1M rows/s before, none at 118.6M rows/s after.
- 1 shard, 1 ms writes, `min_pause` 500 ms, offered 48.4M rows/s: 1,211 to
  1,223 pauses at 35.0M to 39.5M rows/s before, none at 48.4M rows/s after.
- 32 shards, 4 write permits per shard, 16 ms writes at 500 MB/s, `min_pause`
  50 ms, offered 89.7M rows/s: 5,951 to 6,414 pauses before, 128 to 355 after.
  Both held the offered rate.
- A sink saturated at 8 shards, with 16 ms writes at 500 MB/s and `min_pause`
  500 ms: the source was paused 96% to 98% of thread time before and 94% to
  96% after.
- `spate_queue_full_events_total` rose from about 25,000 to 236,000–287,000
  per window on the first arm, and from 27,000–33,000 to 152,000–184,000 on
  the third.
- Instructions retired per settled row stayed at most 1% above base on every
  arm.
  Cycles per row rose 1% to 31%, because instructions
  per cycle fell while every thread ran unpaused. On the third arm the process
  used 6.1 to 6.7 cores against 5.5 to 5.6 for the same rows. Base loses
  instructions per cycle the same way when its queues are made deeper. The
  rig does not separate cache and memory contention from the host's two core
  types.
- A prototype of the per-shard reading, measured beside a prototype of the
  count on the third arm, paused 141 to 426 times per window against 443 to
  1,075 for the count's prototype, and neither changed the rate.

## More information

- Landed in the pull request that added
  `WatermarkController::on_queue_rejected`.
- ADR-0107 is the first decision to wait `min_pause` before a pause, and the
  reason given there applies here.
- The behavior is described in
  [Backpressure](../user-guide/02-concepts/03-backpressure.mdx#detecting-the-pressure-bounded-queues-and-the-byte-budget)
  and [Queues](../user-guide/05-deployment/tuning.mdx#queues).
