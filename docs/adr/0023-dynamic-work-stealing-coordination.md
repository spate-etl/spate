# ADR-0023 — Dynamic work-stealing splits for source coordination

- **Status:** superseded
- **Date:** 2026-07-17 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** [ADR-0038](0038-leader-computed-sticky-assignment.md)

> Recovered from the decision-log row as it stood in `d92b3d4`, before
> [ADR-0038](0038-leader-computed-sticky-assignment.md) overwrote it on
> 2026-07-21. The reasoning below is the reasoning as it was at the time,
> including the convergence claim that later turned out not to be the property
> that mattered. It is preserved rather than corrected — a record of what was
> believed is only useful if it says what was believed.

## Context and problem statement

A broker-less source — an object-storage backfill — has no consumer group to
distribute work for it. Several instances reading the same bucket need some way
to divide the objects between them, without two instances reading the same
object and without work sitting unclaimed while an instance is idle.

Unlike a partitioned topic, the unit of work here is created by the framework:
a planner decides how objects are grouped into splits. So the granularity is a
choice, and finer granularity is available if it helps.

## Considered options

- Static per-source partitioning: hash the object list into instance-count
  buckets
- Dynamic work-stealing: a leader-elected worker runs the planner, workers lease
  splits toward a target, and steal pairwise from over-loaded peers when nothing
  is unclaimed
- A fair-share gate: each worker may hold at most its share, and takes nothing
  beyond it

## Decision outcome

Chosen option: "Dynamic work-stealing", because static partitioning cannot
rescale mid-job and bounds skew by luck — an instance that draws the large
objects stays slow for the whole run, and adding an instance does nothing until
the next job.

A leader-elected worker runs the source's planner. Workers lease splits toward
`min(max_in_flight, fair share)`, heartbeat at a third of the lease TTL, and when
nothing is unclaimed, steal pairwise from a victim holding more than one above
their own count.

Fine-grained planner-sized splits and stealing attack skew from two directions:
smaller units mean less variance per unit, and stealing corrects whatever
variance remains. The pairwise rule provably converges to within one split and
cannot oscillate — and unlike a fair-share gate, it cannot starve a newcomer,
because a worker joining an already-distributed job can take work from peers
rather than waiting for unclaimed splits that will never appear.

### Consequences

- Good, because the fleet rebalances mid-job: an instance added to a running
  backfill gets work immediately.
- Good, because skew is attacked twice — by granularity and by stealing — rather
  than being bounded by the planner's initial guess.
- Bad, because the decision is distributed along with the work: each worker
  negotiates pairwise from a partial view of the fleet.
- Bad, because a steal moves a split away from a worker that may be mid-read, so
  the uncommitted tail replays under the new owner.

### Confirmation

The convergence property is argued rather than tested: the pairwise rule's
termination is a proof about the steal condition, not something a property test
exercises against the running system.

## More information

- Landed in `d92b3d4` (#40).
- Superseded by [ADR-0038](0038-leader-computed-sticky-assignment.md) four days
  later. The reason it did not survive is recorded there: every defect found in
  those four days had the same shape — state visible to neither side of a
  pairwise negotiation — and the convergence proof, which was sound, was about a
  property that was never the problem.
