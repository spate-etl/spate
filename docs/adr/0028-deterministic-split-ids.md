---
description: "Split ids are deterministic, content-derived and planner-generated, so a replan after leader failover creates no work and no duplicate identifiers."
---

# ADR-0028 — Split ids are deterministic and planner-derived

- **Status:** accepted
- **Date:** 2026-07-17 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The planner runs more than once: on a leader failover, on a listing refresh, and
whenever a new leader takes over mid-job. If it produced fresh identifiers each
time, every replan would create a parallel set of splits describing the same
work, and progress recorded against the old ids would be stranded.

Ids also become keys in whichever store backs coordination
([ADR-0024](0024-coordination-store-external-kv.md)), so they have to be
expressible as key components on any backend.

## Considered options

- Sequential ids allocated by the leader
- Random ids minted at planning time
- Deterministic ids derived from the split's own content, constrained to
  `[A-Za-z0-9_-]` and at most 128 characters

## Decision outcome

Chosen option: "Deterministic ids derived from the split's content", because it
makes a replan **create-if-absent**: the same work computes the same id, so a
second planner run over unchanged input is a no-op rather than a duplication. A
leader failover mid-job costs nothing.

An id embeds in store keys, and backends differ in what they accept in a key.
The conservative alphabet works everywhere, so a new backend never needs an
escaping scheme.

Sequential ids were rejected because allocation requires a leader to be
authoritative, which [ADR-0025](0025-embedded-consensus-rejected.md) deliberately
avoids. Random ids were rejected because they make replanning destructive.

### Consequences

- Good, because leader failover and listing refresh are both no-ops for
  unchanged work.
- Good, because ids are portable across store backends without escaping.
- Bad, because the id is derived from content, so any change to how a split is
  composed changes every id. A repacking is a new set of splits, and progress
  against the old ones is orphaned. That has to be handled as an explicit epoch
  rather than happening by accident.
- Bad, because a derivation collision would silently merge two distinct splits,
  so the derivation has to be collision-resistant against adversarial input
  rather than merely unlikely.

### Confirmation

The derivation is a pure function of the split's content, so the property is
testable directly: plan twice, compare the id sets.

## More information

- Landed in `d92b3d4` (#40).
- [ADR-0034](0034-s3-split-identity.md) — the concrete derivation used by the
  object-storage source, and why it is a digest rather than a concatenation.
