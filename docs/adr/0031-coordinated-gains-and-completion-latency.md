# ADR-0031 — Additive lane gains and eager terminal commits

- **Status:** accepted
- **Date:** 2026-07-18 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The checkpointer's epoch model replaces every tracker when an assignment
changes, because that is what a consumer-group rebalance does — full revocation
followed by full assignment. Applied to coordinated sources it is badly wrong in
two ways.

A worker that merely *gains* a split would have to drain and reassign every lane
it already holds, so a routine gain crashes commits that are mid-flight and costs
a re-read of every uncommitted tail. And a split that finishes has to wait for
its final acknowledgements to be noticed on a commit tick, so completion costs a
tick quantum — with several splits in flight, that is a meaningful fraction of a
short job.

## Considered options

- Reuse the rebalance path: drain everything, then reassign the new set
- Additive gains that join the current epoch, plus an eager terminal commit for
  lanes that have reached end of input
- Shorten the commit interval so the completion quantum matters less

## Decision outcome

Chosen option: "Additive gains plus an eager terminal commit", because both
problems come from forcing a coordinated source through a protocol designed for
a different event.

A gain mints a **fresh, never-reused lane** per tenancy and extends the current
epoch, so in-flight acknowledgements on other lanes are untouched. A lane that
reaches end of input surfaces as ready to commit, and the runtime chases its
final acknowledgements within one commit interval rather than waiting for the
tick that would have noticed. A completed split then leaves without a barrier,
because the terminal commit has already proved nothing is in flight.

Shortening the commit interval was rejected as treating the symptom: it would
increase commit traffic for every pipeline to reduce a latency that only
coordinated sources experience.

### Consequences

- Good, because a gain is free for lanes already running — no drain, no re-read.
- Good, because split completion is bounded by one commit interval instead of a
  tick quantum divided by in-flight count.
- Bad, because there are now two assignment paths — eager replacement and
  additive extension — and a source that uses the wrong one is subtly incorrect
  rather than obviously broken.
- Bad, because lanes are never reused, so lane identifiers grow over a long job.

### Confirmation

The eager-replacement contract is defensive: the controller drains and commits
live lanes itself if a source announces a full assignment that is not in fact
full, so a source violating the contract loses performance rather than
correctness.

## More information

- Landed in `84e1583` (#49).
- [ADR-0005](0005-refcounted-per-batch-acknowledgements.md) — the epoch model
  this extends.
- [Writing a source](../user-guide/06-extending/custom-source.mdx) — which event
  to emit for a gain versus a reassignment.
