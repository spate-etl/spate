# ADR-0035 — Object-level failures poison the split; credential and configuration failures are fatal

- **Status:** accepted
- **Date:** 2026-07-18 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

An object-storage backfill fails in two very different ways, and the framework
has to tell them apart.

One object may be unreadable: deleted after planning, changed underneath a
version precondition, corrupt, or too large for the read budget. That affects one
split, and killing a fleet-wide backfill over it wastes everything already done.

Or the credentials may be wrong, or the endpoint misconfigured. That affects
every object on every instance, and retrying it is a fleet spinning against a
wall.

## Considered options

- Treat every storage error as retryable and let the attempt counter sort it out
- Classify by **scope**: object-level failures poison the split; credential and
  configuration failures fail the pipeline
- Treat every storage error as fatal

## Decision outcome

Chosen option: "Classify by scope", because scope is the property that
distinguishes them, and it is knowable at the point of failure. A failure that
can only affect this object poisons this split, via the driver's failure path,
consuming an attempt and eventually quarantining it
([ADR-0027](0027-split-delivery-attempts-and-quarantine.md)). A failure that
would affect every object — authentication, authorisation, endpoint
configuration — fails the pipeline, because no amount of reassignment will help
and a fleet retrying it is a fleet doing nothing loudly.

`Stalled` remains fatal. A bounded job that quarantined a split has planned data
it did not process, and reporting `Completed` over it would present loss as
success.

Letting the attempt counter sort everything out was rejected because a
credentials failure would then take every split to its cap before the job
stopped — the right outcome by the longest possible route, with every split
quarantined and nothing indicating why.

### Consequences

- Good, because one corrupt object costs one split rather than a whole backfill.
- Good, because a misconfiguration fails immediately with the actual error rather
  than as mass quarantine.
- Bad, because the classification is a judgement encoded in a match: a storage
  error whose scope is genuinely ambiguous — a transient permission denial during
  a credential rotation — gets classified one way and will sometimes be wrong.
- Bad, because a job with one bad object ends `Stalled` and needs intervention,
  even though every other split completed.

### Confirmation

The completion sweep refuses to report `AllComplete` while any split is
quarantined, so the `Stalled` outcome cannot be skipped.

## More information

- Landed in `84e1583` (#49).
- [ADR-0027](0027-split-delivery-attempts-and-quarantine.md) — the attempt
  counting and quarantine this feeds.
- [S3 source](../user-guide/04-connectors/sources/s3/README.mdx).
