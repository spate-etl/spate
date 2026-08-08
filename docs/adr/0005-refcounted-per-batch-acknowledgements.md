# ADR-0005 — Refcounted per-batch acknowledgments, resolved by drop

- **Status:** accepted
- **Date:** 2026-07-05 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

At-least-once delivery ([ADR-0002](0002-at-least-once-delivery.md)) requires
knowing, for every source offset, whether the data behind it has been durably
written. That is complicated by everything the chain is allowed to do to a
record between source and sink: `filter` drops it, `flat_map` turns it into many,
and a multi-sink split sends it to more than one destination. Each of those has
a different correct answer for "has this been delivered", and the accounting
runs on the per-record path, so it has to be close to free.

## Considered options

- Per-record acknowledgment objects, one allocation each
- A refcounted handle created per source poll batch, cloned by fan-out, resolved
  when the last clone drops — drop meaning *delivered*
- The same, inverted: drop meaning *failed*, with an explicit `deliver()` call

## Decision outcome

Chosen option: "A refcounted handle created per source poll batch, resolved when
the last clone drops", because it makes every chain operation correct without
any of them knowing about acknowledgments. A `filter` drop resolves as success
because the record legitimately ended its life; `flat_map` children clone the
same handle, so the parent is satisfied only when every child is; a split clones
per destination and merges with worst-status. None of those needed special
cases.

The handle is created **per poll batch** rather than per record — the
Vector-finalizer shape — so the cost is one `Arc` clone and drop per record
rather than an allocation. When the last clone drops, the tuple of partition,
sequence and status flows over an unbounded channel to the checkpointer, which
keeps a ring of outstanding sequence numbers per partition and per epoch, pops
the contiguous acknowledged prefix, and advances the committable watermark.

The inverted default was evaluated as structural hardening and **rejected for
records, adopted for collections**: every legitimate record drop is
framework-mediated and
zero-ceremony today, and inverting would turn each into explicit bookkeeping
whose failure mode — a forgotten `deliver()` — is a watermark stall that halts a
pipeline on correct data. Teardown loss, the thing the inversion would have
fixed, only materializes where acknowledgments travel in **bulk**, so the
inversion is applied there instead: every *collection* of handles on the sink
path is an `AckSet`, which fails its handles on drop and delivers only after a
durable write.

### Consequences

- Good, because `filter`, `flat_map` and multi-sink routing need no
  acknowledgment-specific code, and a new operator gets correct behavior by
  default.
- Good, because the per-record cost is roughly two atomics, not an allocation.
- Bad, because "drop means delivered" is a silent default, so any component
  holding handles for data not yet written must explicitly `fail()` them in its
  `Drop` — the chain's terminal stage does this for parked chunks and partial
  buffers, and sink workers for abandoned batches. Over-failing is always safe;
  it costs replay, never loss.
- Bad, because two different defaults now exist — records deliver on drop,
  collections fail on drop — and which applies has to be learned rather than
  inferred.

### Confirmation

INV-1 and INV-4. The checkpoint tracker is loom-tested (INV-3), which is what
holds the contiguous-prefix logic under concurrent acknowledgment arrival.
Regression tests cover each teardown seam individually: handoff drop, queue
drop, and worker or runtime teardown.

## More information

- Landed in `c8973e6`; the contracts were frozen in `93229b3`.
- The fail-by-default inversion was evaluated and recorded alongside the
  original design rather than as a later change; it is documented here because
  the outcome — records deliver, collections fail — is one decision with two
  halves.
- [Delivery guarantees](../user-guide/02-concepts/02-delivery-guarantees.mdx) —
  the acknowledgment chain from record to watermark.
