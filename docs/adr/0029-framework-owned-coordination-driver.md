# ADR-0029 — Seam traits plus a framework-owned coordination driver

- **Status:** accepted
- **Date:** 2026-07-17 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Coordinating a source means running a choreography: reconciling assignments
against what is held, partitioning tenancies, fencing quarantined splits,
sweeping for completion, and turning all of it into the lane assignment and
revocation events the pipeline runtime understands.

None of that is specific to object storage. Any broker-less source needs the
same sequence. But it is also where the subtle bugs live, the ordering
mistakes that only appear under a rebalance during a drain.

## Considered options

- Each coordinated source implements the choreography itself, against the store
  trait
- Seam traits — a planner and a coordinator — plus a framework-owned driver in
  `spate-core` that runs the choreography once for every source
- A single trait that a source implements wholesale

## Decision outcome

Chosen option: "Seam traits plus a framework-owned driver", because the
choreography is source-generic and is where the difficulty is. One audited
implementation is worth more than one per connector, and a second coordinated
source should inherit the fixes found by the first rather than rediscovering
them.

The driver lives in `spate-core` and is **synchronous and free of async
runtimes**, consistent with the rest of the control plane. Backends are injected
at assembly, the same shape as the framer seam
([ADR-0009](0009-yaml-configuration-with-opaque-passthrough.md)'s
connector-owns-its-config principle applied to a different axis): the source
declares what it needs, and the deployer's binary decides what supplies it.

### Consequences

- Good, because a new coordinated source implements a planner and a split reader,
  not a distributed protocol.
- Good, because a coordination bug is fixed once, in a component with its own
  test suite, rather than in each connector.
- Bad, because the driver has to anticipate what sources need, and a source with
  an unusual requirement has to either fit the seam or change it for everyone.
- Bad, because the split between what the driver owns and what the source owns
  is not self-evident from either side, so the boundary needs documenting or it
  gets crossed.

### Confirmation

The driver has its own test suite, including a scripted coordinator in
`spate-test` so a source's behavior under revocation and loss can be exercised
without a store.

## More information

- Landed in `d92b3d4` (#40).
- [ADR-0036](0036-coordinator-wiring-at-assembly.md) — why the backend is
  injected at assembly rather than configured per connector.
- [Source coordination](../user-guide/02-concepts/07-source-coordination.mdx).
