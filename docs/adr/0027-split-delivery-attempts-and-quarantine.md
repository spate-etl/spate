# ADR-0027 — Delivery attempts count only non-graceful tenancy ends

- **Status:** accepted
- **Date:** 2026-07-17 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A split that kills whichever worker picks it up (corrupt content, an object that
cannot be read) will be retried forever by a fleet that keeps reassigning it. So
splits need an attempt counter and a quarantine threshold.

The difficulty is deciding what counts as an attempt. A split changes hands for
several reasons, and most of them say nothing about the split: a worker was
scaled down, a rebalance moved it, an instance restarted. Counting those as
evidence of poison would quarantine healthy splits during a rolling restart.

## Considered options

- Count every tenancy end
- Count only non-graceful ends: lease expiry takeover, reclaim, explicit failure
- No attempt counting; rely on an operator noticing

## Decision outcome

Chosen option: "Count only non-graceful ends", because a graceful release is a
statement by the worker that it is done with the split for reasons of its own,
and a rebalance revocation is the leader's decision. Neither is evidence about
the split's content. Only an end the split appears to have *caused* (the owner
vanished mid-read, the lease expired, the source explicitly failed it) is
counted.

At the cap the split is quarantined. **A quarantined split blocks
completion**: a bounded job containing one ends as `Stalled` rather than
`Completed`. Reporting a green completion over planned data that was never
processed would dress loss up as success, which is the one outcome an ingestion
framework must not produce.

### Consequences

- Good, because a rolling restart does not consume attempts, so the counter
  means what it says.
- Good, because a bounded job cannot report success while planned data is
  unprocessed.
- Bad, because a split that fails for environmental reasons (a transient
  credential problem hitting one worker repeatedly) accumulates attempts as if
  it were poison.
- Bad, because `Stalled` is a fatal outcome requiring intervention, so one bad
  object stops a job from completing even though every other split finished.

### Confirmation

The completion sweep refuses `AllComplete` while any split is quarantined, so
the `Stalled` outcome is structural rather than a check that could be skipped.

## More information

- Landed in `d92b3d4` (#40).
- [ADR-0035](0035-s3-poison-policy.md) — which failures the object-storage source
  classifies as split-poisoning rather than pipeline-fatal.
