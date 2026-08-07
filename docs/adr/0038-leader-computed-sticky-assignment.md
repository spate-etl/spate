# ADR-0038 — Leader-computed sticky assignment replaces peer-to-peer work stealing

- **Status:** accepted
- **Date:** 2026-07-21 (recorded 2026-08-06 from the decision log)
- **Supersedes:** [ADR-0023](0023-dynamic-work-stealing-coordination.md)
- **Superseded by:** —

## Context and problem statement

[ADR-0023](0023-dynamic-work-stealing-coordination.md) distributed splits by
peer-to-peer stealing: each worker leased toward a target and stole pairwise from
over-loaded peers. Its convergence argument was sound — the pairwise rule does
converge to within one split and cannot oscillate.

Four days of building on it produced a series of defects, and they all had the
same shape: **state visible to neither side of a negotiation.** Splits promised
but not yet leased. Grant acknowledgments lost on a failed compare-and-set.
Annotations that nothing retired. Each was fixed individually; the shape kept
recurring, because it is inherent to distributing the *decision* along with the
work — every worker negotiates from a partial view, and the gaps between those
views are where the bugs live.

Two symptoms were structural rather than incidental: a per-victim steal cap that
could not usefully be raised, and a stable disbalance that nothing corrected.

## Considered options

- Keep peer-to-peer stealing and continue fixing the partial-view defects
- Leader-computed sticky assignment: the leader runs the planner *and* publishes
  a desired assignment per instance, which workers reconcile toward
- Embed consensus so the negotiation has a consistent view

## Decision outcome

Chosen option: "Leader-computed sticky assignment", because it removes the
partial view rather than patching around it. One party computes the assignment
from the whole picture, and workers reconcile toward it — claiming what they are
named for, cooperatively draining what they are not.

Balance is on split **weight** rather than count, sticky, and converges by
strictly-improving moves. Because the leader sees everything, the balance
calculation becomes a **pure function** that can be property-tested, instead of
behavior emerging from concurrent negotiation.

Centralizing costs nothing in safety, and that is the load-bearing claim: an
assignment **is not a fence**. A stale, split-brained or simply wrong leader can
produce bad balance but never two owners, because the durable record's
compare-and-set still decides ownership
([ADR-0026](0026-coordination-fencing.md)). So the leader needs no consensus and
no correctness guarantees at all
([ADR-0025](0025-embedded-consensus-rejected.md)).

Mature implementations of this shape are unanimous the other way: planning is
centralized in an elected leader. The designs that were genuinely peer-to-peer
have since moved to one, having accumulated the same symptoms — an unraisable
steal cap and an uncorrected disbalance. Arriving independently at a design those
have abandoned is a signal worth acting on.

### Consequences

- Good, because the balance logic is a pure function with property tests, rather
  than emergent behavior in an async task.
- Good, because the partial-view defect class is gone: there is one view, and it
  belongs to the leader.
- Bad, because there is now a leader, and its failure — while never a correctness
  problem — pauses rebalancing until a new one is elected.
- Bad, because a rebalance is a decision made *for* a worker rather than by it,
  so a worker that declines to comply has to be forced eventually
  ([ADR-0039](0039-split-revocation-cooperative-then-forced.md)) where previously
  it could simply decline.

### Confirmation

The balance function is pure and property-tested; its invariants are numbered on
the work-assignment page, which names the test enforcing each. Safety is
unchanged and still rests entirely on
[ADR-0026](0026-coordination-fencing.md)'s compare-and-set.

## More information

- Landed in `e79dfc2` (#64).
- Supersedes [ADR-0023](0023-dynamic-work-stealing-coordination.md), whose record
  is kept: the reasoning that led there was sound for what was known at the time,
  and the convergence proof was correct about a property that turned out not to
  be the problem.
- [Work assignment](../user-guide/02-concepts/08-work-assignment.mdx) — the
  normative specification.
