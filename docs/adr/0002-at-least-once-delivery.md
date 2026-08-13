# ADR-0002 — At-least-once delivery, with duplicates pushed onto the destination

- **Status:** accepted
- **Date:** 2026-07-05 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The framework moves records from a source that tracks consumption by offset to a
sink that acknowledges writes. A failure between those two points, whether a
crash, a rebalance or a shutdown, leaves a window where it is not known whether
the data arrived. What the framework promises about that window determines
almost every other structure in it: how acknowledgments are tracked, when
watermarks commit, what a sink connector must implement, and what a destination
schema has to tolerate.

## Considered options

- At-least-once: commit a source watermark only behind acknowledged data, and
  replay whatever was in flight
- Exactly-once through transactional sinks: two-phase commit tying the source
  offset and the destination write into one atomic unit
- At-most-once: commit the watermark on read and accept loss on failure

## Decision outcome

Chosen option: "At-least-once", because it is the only one of the three whose
cost is paid by the destination schema rather than by every component in the
pipeline, and the destinations in view already have the tools to absorb it.

Exactly-once requires every sink to implement a transactional protocol and the
framework to coordinate a two-phase commit across sink and source. That is a
cost paid on every connector, forever, to remove a duplicate window that a
versioned destination table removes for free. At-most-once was never a serious
candidate: silent loss is not a trade an ingestion framework gets to make on its
operator's behalf.

**Duplicates happen, and deduplication tokens do not cover crash replay.** A
token makes a retry of the same sealed batch idempotent, but after a restart the
data re-batches with different boundaries and different tokens, so those rows
land twice. Documentation must never imply exactly-once.

### Consequences

- Good, because sink connectors implement one method, writing a sealed batch,
  with no transactional protocol and no coordination with the source.
- Good, because recovery is replay, which is a path exercised on every rebalance
  rather than a rare branch that only runs during an incident.
- Bad, because the destination has to tolerate duplicates. That is work pushed
  onto whoever designs the target table, and it is why the deduplication
  boundary has to be documented precisely.
- Bad, because a stalled watermark is the failure mode for anything the sink
  cannot write, so a wedged destination stops progress rather than degrading.

### Confirmation

INV-1 — a source watermark is never committed past unacknowledged data,
including across rebalances and shutdown. Enforced structurally by the
checkpoint tracker's contiguous-prefix rule, and by the collection-level
acknowledgment contract that fails handles on teardown rather than delivering
them.

## More information

- Landed in `c8973e6`.
- [ADR-0005](0005-refcounted-per-batch-acknowledgements.md) — the acknowledgment
  design this promise rests on.
- [Delivery guarantees](../user-guide/02-concepts/02-delivery-guarantees.mdx) —
  what the guarantee means when operating a pipeline, including the exact
  boundary of what deduplication covers.
