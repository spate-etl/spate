---
description: "Each split persists as two records, an immutable spec and a small progress record, so compare-and-set commit cost stays independent of descriptor size."
---

# ADR-0030 — Two durable records per split: an immutable spec and a small progress record

- **Status:** accepted
- **Date:** 2026-07-17 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A split has to persist two very different things. Its **specification** (which
objects it covers, with their keys and versions) is written once at planning
time and never changes, and for a bin-packed object list it can run to hundreds
of kilobytes. Its **progress** (owner, epoch, attempt count, watermark) is
small and is rewritten on every claim, fence and commit.

Under a single-record layout, every commit rewrites the descriptor too. Commit
cost then scales with descriptor size, on the path that runs most often.

## Considered options

- One record per split, holding specification and progress together
- Two records: an immutable spec written once, and a small progress record that
  is the compare-and-set target for every claim, fence and commit
- Progress in the store, specification recomputed from the source on demand

## Decision outcome

Chosen option: "Two records", because it decouples commit cost from descriptor
size. The progress record is the only thing rewritten on the hot path, and it is
small enough that a commit costs the same whether the split covers one object or
sixteen.

Recomputing the specification on demand was rejected because it would make every
claim re-list the source.

A second problem is solved alongside it. The count of planned splits is
**recounted from an authoritative listing at every publish**, rather than carried
forward. Without that, a leader that crashed between seeding the split records
and publishing the plan would leave the two permanently disagreeing, and terminal
detection ("are all splits complete?") would be wrong forever with nothing to
correct it. Recounting makes that failure self-healing: the next publish observes
what exists.

### Consequences

- Good, because commit cost is constant in descriptor size, so a coarse packing
  does not make commits expensive.
- Good, because a leader crash mid-publish heals on the next publish instead of
  desynchronizing terminal detection permanently.
- Bad, because a split is now two keys that must be created and cleaned up
  together, and a spec without progress, or the reverse, is a state the code
  has to tolerate.
- Bad, because recounting at every publish costs an authoritative listing each
  time, which is the expensive operation on an object store.

### Confirmation

The recount is what holds terminal detection, and the failure it prevents, a
crash between seeding and publishing, is exercised directly in the coordination
test suite.

## More information

- Landed in `d92b3d4` (#40).
- [ADR-0026](0026-coordination-fencing.md) — the progress record is the
  compare-and-set target that fencing depends on.
- [ADR-0033](0033-s3-split-packing.md) — why a descriptor is bounded at roughly
  sixteen members, which is what keeps it under store value limits.
