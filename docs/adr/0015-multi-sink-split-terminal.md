---
description: "Additive add_sink calls terminate the chain in a typed split that routes each record to one destination, moving fan-out off the database onto the pipeline."
---

# ADR-0015 — Additive `add_sink` and a typed split terminal

- **Status:** accepted
- **Date:** 2026-07-11 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A single source stream often carries records of several kinds that belong in
several destination tables. The established way to do this on the database side
is to insert everything into one table and let materialized views fan it out.
That works, but it puts the fan-out cost on the database, the least scalable
tier in the deployment, and it means every row is parsed and written twice.

The framework could instead route each record to one of several sinks. The
question is whether the acknowledgment design survives a record having more
than one destination.

## Considered options

- One sink per pipeline; fan out on the database with a null table and
  materialized views
- One sink per pipeline; run several pipelines, one per destination, each
  re-reading the source
- Additive `add_sink`, with the chain terminating in a typed split that routes
  each record to exactly one branch

## Decision outcome

Chosen option: "Additive `add_sink` with a typed split terminal", because it
moves fan-out off the database onto the tier that scales horizontally, and
because the acknowledgment layer needed nothing new to support it.

A pipeline installs one or more named sinks; the chain ends either in a single
sink or in a split whose branches are each a full sink with their own family,
encoder, router and queues, resolved by name. Each branch clones the poll
batch's acknowledgment handle, so a source watermark holds until **every**
destination it touched has durably written, merging with worst status, which is
exactly the behavior [ADR-0005](0005-refcounted-per-batch-acknowledgements.md)
already gave `flat_map`. Unmatched records follow a configurable policy: Fail by
default, or Skip which drops and counts with `reason="unrouted"`.

Running several pipelines was rejected because it multiplies source read cost by
destination count and gives each its own independent watermark.

### Consequences

- Good, because the database does one write per row instead of parsing it once
  and rewriting it per view.
- Good, because the acknowledgment semantics fell out of the existing design
  rather than needing a parallel mechanism.
- Bad, because a branch stores its encoder and router type-erased so the branch
  type keys on the destination family alone. Per record that costs a downcast
  plus a virtual route and encode call. The single-sink terminal uses concrete
  types and pays neither. The user's routing closure stays monomorphic.
- Bad, because a low-volume branch can hold acknowledgments for a long time:
  its chunk buffer fills slowly, and everything behind it waits. That was a real
  defect, fixed later by broadcasting a flush on the commit tick.

### Confirmation

Nothing beyond the existing acknowledgment tests specific to the split; the
worst-status merge is exercised by the multi-sink integration tests.

## Evidence

Against a null table plus materialized views on a skewed type mix: **+56% to
+212% throughput, 4–10× larger parts, and 16–31% lower server CPU per row.**
Measured by a rig this repository no longer carries.

The range is wide because the win depends on how skewed the type mix is. The
more the fan-out favors one branch, the less there is to move off the server.

## More information

- Landed in `89cb31d` (#14).
- [Multi-sink](../user-guide/02-concepts/06-multi-sink.mdx) — the split terminal
  in use, and the trade it makes.
