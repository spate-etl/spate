# ADR-0032 — The object-storage source is always coordinated; the manifest checkpoint is deleted

- **Status:** accepted
- **Date:** 2026-07-18 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The object-storage backfill source originally checkpointed through a manifest
object it wrote alongside the data it was reading. When coordination arrived
([ADR-0029](0029-framework-owned-coordination-driver.md)), per-split fenced
progress became a second, independent checkpoint mechanism.

Keeping both means two commit paths, two resume validators, and a permanent
impedance mismatch between a manifest's notion of position and a split's. Every
change to resume semantics has to be made twice and reconciled.

## Considered options

- Keep the manifest for single-instance runs and use coordination for
  multi-instance
- One always-coordinated path; a solo run uses an in-process store with
  ephemeral progress
- Coordination only, with no single-instance mode at all

## Decision outcome

Chosen option: "One always-coordinated path", because at this stage of the
project a dual path is pure liability: twice the surface for a capability
— durable solo resume — that a durable coordination store already provides.

A solo run gets an in-process store, linked unconditionally, so no configuration
or feature flag is needed to run one instance. Its progress is **ephemeral**, and
that is warned about rather than hidden: a single instance that restarts re-reads
from the beginning. An operator who wants durable solo resume points the source
at a durable coordination store, which is the same mechanism the multi-instance
case uses rather than a second one.

Removing single-instance operation entirely was rejected as needlessly hostile to
the simplest case.

### Consequences

- Good, because there is one commit path and one resume validator, so resume
  semantics are defined once.
- Good, because scaling from one instance to several is a configuration change,
  not a different code path.
- Bad, because a solo run loses durable resume that the manifest used to
  provide. That is a real regression for anyone relying on it, mitigated only by
  a warning and the option of a real store.
- Bad, because the in-process store exists solely to make the solo case work, so
  it is a component with no production role that still has to be correct.

### Confirmation

The solo path exercises the same driver and the same store trait as a
distributed run, so the coordination test suite covers it rather than it having
a separate suite.

## More information

- Landed in `84e1583` (#49). The manifest and its schema were deleted in the same
  change.
- [ADR-0024](0024-coordination-store-external-kv.md) — the store trait the
  in-process implementation satisfies.
