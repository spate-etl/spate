---
description: "A departed instance's splits are withheld for a short delay before reassignment, canceled on its return, with zero handled as an immediate, distinct path."
---

# ADR-0040 — A departed instance's splits are withheld briefly, and zero is a distinct path

- **Status:** accepted
- **Date:** 2026-07-21 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

An instance disappearing is usually not a failure. A rolling restart takes each
pod down and brings it back within seconds, and reassigning its splits the moment
it goes means the fleet churns work that is about to come home, with every pod in
the rollout causing a full rebalance, twice.

But withholding work is not free either: while the splits are held back, nobody
is reading them.

## Considered options

- Reassign immediately on departure
- Withhold a departed instance's splits for a delay, cancelled the moment it
  reappears
- Withhold until an operator intervenes

## Decision outcome

Chosen option: "Withhold for a delay, cancelled the moment it reappears", with a
default of 20 seconds.

The default is deliberately **short**, and the reasoning is specific to this
source model rather than borrowed. Starting a split is cheap here (read a
descriptor, spawn a fetcher), unlike joining a consumer group, which rebuilds a
connector's clients and re-establishes broker connections. **When starting is
cheap, idle work costs more than movement**, so the balance tips toward
reassigning sooner than a broker-based system would.

**Zero takes a distinct code path** meaning "immediately", rather than flowing
through the general path as a delay of zero duration. That is a deliberate shape:
a delay knob whose zero is just another value is how "reassign at once" silently
becomes "withhold indefinitely". An off-by-one or a comparison flipped the wrong
way turns the fast path into a stall, and it looks like nothing at all. Making
zero a separate branch means the bug is not expressible, and a regression test
asserts it.

### Consequences

- Good, because a rolling restart does not churn the fleet.
- Good, because the fast path is structurally distinct, so the worst
  misconfiguration is a delay that is too short rather than one that never ends.
- Bad, because splits held by a genuinely dead instance sit unread for the delay,
  so a crash costs 20 seconds of throughput on those splits.
- Bad, because the default is tuned for cheap-to-start splits, and a future
  coordinated source with expensive startup would want a different one.

### Confirmation

A regression test asserts that zero reassigns immediately, which is the property
the separate code path exists to protect.

## More information

- Landed in `e79dfc2` (#64).
- [ADR-0038](0038-leader-computed-sticky-assignment.md) — the leader that decides
  when the withholding ends.
