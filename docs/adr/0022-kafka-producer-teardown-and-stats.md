# ADR-0022 — Rely on rdkafka's own producer teardown; defer per-partition statistics

- **Status:** accepted
- **Date:** 2026-07-13 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Two smaller decisions taken with the Kafka sink, both about **not** building
something.

Teardown: the sink holds a producer with messages queued and in flight, and each
carries a raw opaque pointer to its batch's countdown
([ADR-0016](0016-kafka-sink-ack-wiring.md)). Dropping that naively would leak the
countdowns and could hang shutdown waiting for messages that will never be
reported on.

Statistics: librdkafka reports produce-queue depth both in aggregate and per
partition. Per-partition detail is more useful for diagnosing a hot partition,
but it multiplies series count by partition count.

## Considered options

- A custom `Drop` that flushes with a deadline and reclaims the countdowns
- Rely on rdkafka's own `Drop`, which purges the queue and in-flight messages,
  flushes for 500 ms, and joins the poll thread
- For statistics: emit per-partition series now, or aggregate only

## Decision outcome

Chosen for teardown: "Rely on rdkafka's own `Drop`", because it already does the
right thing and the purge closes the loop for free. **Purged messages' delivery
reports still fire, and those reports reclaim the countdown opaques.** A custom
`Drop` would duplicate that logic and then have to be kept correct as the client
changes, for no behavior we do not already get. The whole sequence is bounded at
roughly 600 ms.

Chosen for statistics: "Aggregate only, deferred". Not because per-partition
detail is unwanted, but because the metrics attachment seam has no
per-partition-detail channel today. That is a source-side concept, and the sink
has no equivalent. Adding one is additive and can happen when the need is
concrete rather than anticipated.

### Consequences

- Good, because there is no custom teardown path to maintain or to get subtly
  wrong on a client upgrade.
- Good, because deferring per-partition series keeps sink cardinality
  independent of topic partition count, which is the axis that grows without
  warning.
- Bad, because teardown behavior is inherited rather than owned: a change to
  rdkafka's `Drop` semantics would change ours silently, and the 600 ms bound is
  theirs to move.
- Bad, because a hot partition is currently invisible from the sink's metrics.
  The aggregate queue depth shows pressure but not where.

### Confirmation

Nothing automated for teardown; the bound is measured rather than asserted. The
statistics decision is confirmed only by the absence of the series.

## Evidence

Teardown bounded at roughly 600 ms: a purge, a 500 ms flush, and the poll thread
joined. Measured by a rig this repository no longer carries.

## More information

- Landed in `6af6861` (#24).
- [ADR-0016](0016-kafka-sink-ack-wiring.md) — the countdown opaques that the
  purge reclaims.
