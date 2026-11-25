---
description: "A pause a queue rejection raised while a shard waited for a write permit ends once no shard waits, without min_pause; other pauses keep it."
---

# ADR-0112 — A pause under sink pressure ends when a write permit frees

- **Status:** accepted
- **Date:** 2026-11-25
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

ADR-0111 made a shard-queue rejection pause the pipeline thread's lanes at once
while a shard worker waits for a write permit. The watermark controller then
held that pause like any other: until the budget and queues were below their
low marks and `backpressure.min_pause` had passed since the last rejection.

A worker that waits for a permit reads its queue again as soon as one of its
writes returns. With a slow tail of writes, permits free well inside
`min_pause`, and the pause outlasts the pressure that raised it. At the 500 ms
default, sources paced below the sink's capacity fell behind on every measured
arm. The question was what ends a pause raised in that state, in
`WatermarkController::tick`.

## Considered options

- End such a pause once no shard waits for a permit and the budget and queues
  are below their low marks, with no minimum
- The same, with a fixed floor shorter than `min_pause`
- Keep `min_pause` for every pause, and leave operators to lower it
- Lower the `min_pause` default

## Decision outcome

Chosen option: "end such a pause once no shard waits, with no minimum",
because it removed the shortfall on every measured arm at the default
`min_pause`. The measured alternatives either kept the shortfall, or removed
it while adding a constant no config sets. The 50 ms stand-in paused about as
often on the default queues and about a quarter less on the saturated arm.

The controller marks a pause raised by a queue rejection while a shard waits,
unless usage had reached the high watermark or a run of rejections with no
shard waiting had spanned `min_pause` on the same tick. Those two causes keep
`min_pause`, because the budget or a stalled worker asked for the pause as
well. A marked pause resumes on the first tick that finds no shard waiting and
both pressure signals clear. A rejection while paused still restarts the timer
and still holds the pause for that tick.

A fixed floor was weighed through its nearest measured stand-in, `min_pause`
50 ms on unchanged code. On the saturated arm below it paused 9,000 to 9,500
times against 12,000 to 12,400, at the same rate, and it is a constant no
configuration sets. Keeping `min_pause` for every pause left the shortfall in
place at the default. Lowering the default also shortens the pending-ceiling
wait ADR-0107 set, and every high-watermark pause, for a gain only this
pause needs.

### Consequences

- Good, because a source paced below the sink's capacity keeps up at the
  default `min_pause` where it fell behind before.
- Good, because the pending-ceiling wait, high-watermark pauses and pauses a
  stalled worker raises keep `min_pause` unchanged.
- Bad, because under a sink at its limit with many write permits the pause
  cycles with the permits. On the saturated arm below it paused seven times as
  often as before for the same rate, and
  `spate_backpressure_pause_events_total` rises with it.
- Bad, because a Kafka source purges its prefetch on each pause and refetches
  on resume, so more pauses cost it fetch traffic. Kafka was measured only on
  one local host with two pipeline threads, where it paused 60 to 72 times
against 58 to 60.
- Bad, because `min_pause` no longer bounds every pause/resume cycle, and its
  resume-floor meaning gains an exception.
- Neutral, because INV-2 holds: no send blocks, and a resume only re-enables
  polling.
- Neutral, because the shard count is a relaxed read. A stale zero resumes one
  pass early, and the next rejection pauses again.

### Confirmation

`crates/spate-core/src/backpressure.rs` tests the rule
(the `a_gated_*` tests, `the_next_pause_after_a_gated_one_keeps_min_pause`
and the property `an_ungated_pause_lasts_min_pause`).
`crates/spate-core/src/pipeline/driver.rs` tests the pipeline thread's wiring
(`a_gated_pause_resumes_once_the_shard_gets_a_permit`). The counted case
`gated_traffic` in `crates/spate-core/benches/backpressure_gungraun.rs`
measures the controller on this path.

## Evidence

Measured by a queue benchmark rig this repository does not carry: a paced
source, a sink that models its writes, and every twentieth write four times
slower. Each arm ran four interleaved passes per leg on one 64-vCPU cloud
host, comparing unchanged code against a prototype of this rule, both at
`min_pause` 500 ms, with unchanged code at 50 ms as a reference. "Fell behind"
counts passes whose backlog grew by more than one second of offered rows.

- 8 MiB queues, 2 write permits per shard, 1 or 8 shards, 32 pipeline threads,
  three write models: unchanged code fell behind in 4 of 4 passes on all six
  settings, at 0.70 to 0.91 of the offered rate. The prototype kept up in 4 of
  4 on all six, as unchanged code at 50 ms did. On 8 shards the prototype
  paused 352 to 960 times against 672 to 1,248 for unchanged code at 500 ms.
- The default 512 KiB queues, 2 write permits, 8 shards: unchanged code at
  500 ms fell behind in 4 of 4 passes on both settings. The prototype kept up
  in 4 of 4 with 1,086 to 2,740 pauses, against 1,120 to 2,786 for unchanged
  code at 50 ms.
- The default 512 KiB queues, 64 write permits, 1 shard, a store capped at
  500 MB/s and offered 110% of it: every leg settled at the cap. Unchanged code
  paused about 1,730 times at 500 ms and 9,000 to 9,500 at 50 ms. The
  prototype paused 12,000 to 12,400 times.
- Kafka, on one local host with 2 pipeline threads and 8 partitions: the
  prototype paused 60 to 72 times against 58 to 60, with the same rate and the
  same messages received per settled row.

The rig ran the queue path alone. These runs do not show the rule's effect on
a Kafka source at 32 pipeline threads with many write permits, where pauses
would rise most.

## More information

- Landed in the pull request that marks a gated pause in
  `WatermarkController`.
- [ADR-0111](0111-a-queue-rejection-pauses-only-under-sink-pressure.md) decides
  when a queue rejection pauses. This record decides when that pause ends, and
  leaves ADR-0111 in force.
- [ADR-0107](0107-a-partition-held-at-the-pending-ceiling-is-paused-at-its-source.md)
  set the pending-ceiling wait to `min_pause`, which this record leaves alone.
- The behavior is described in
  [Backpressure](../user-guide/02-concepts/03-backpressure.mdx#acting-on-it-the-pause-controller).
