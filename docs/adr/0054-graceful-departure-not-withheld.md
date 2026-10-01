---
description: "A crashed instance's splits are withheld briefly before reassignment; a graceful departure hands its splits back and they move at once. Supersedes ADR-0040."
---

# ADR-0054 — A crashed instance's splits are withheld briefly, and a graceful departure's are not

- **Status:** accepted
- **Date:** 2026-10-01
- **Supersedes:** [ADR-0040](0040-rebalance-delay.md)
- **Superseded by:** —

## Context and problem statement

[ADR-0040](0040-rebalance-delay.md) withholds a departed instance's splits for
`rebalance_delay`, so a pod that comes back reclaims its own work, and lists "a
rolling restart does not churn the fleet" as a consequence. The leader withholds
only splits whose progress record still names the departed instance as owner.

A graceful stop hands its splits back through the coordination task: it clears
each owner, deletes the leases and the presence key, and gives up leadership.
None of its splits is then withheld, and peers take them at once. A rolling
restart is a sequence of graceful stops, so it moves each pod's work to its
peers and back. Until the coordinator's hand-back reached the store on a
graceful stop, the presence key it left behind kept the leader assigning work
to the departed pod, which hid this.

## Considered options

- Withhold only a crashed instance's splits; a graceful departure's move at once
- Also withhold a gracefully departed instance's splits for `rebalance_delay`,
  cancelled when it returns

## Decision outcome

Chosen option: "Withhold only a crashed instance's splits", because a graceful
stop is either a scale-down, where the work must move, or a restart, where the
pod's splits restart from committed progress wherever they land. Starting a
split is cheap (the reasoning in ADR-0040), so idle work costs more than
movement. Withholding a graceful departure's splits would also need a record of
who released each one, since a released split names no owner.

ADR-0040's other half stands: a `rebalance_delay` of zero takes a distinct code
path meaning "immediately".

### Consequences

- Good, because a graceful takeover waits only for the store to carry the
  departure and the new assignment, never for a lease or `rebalance_delay`.
- Good, because a released split consumes no delivery attempt when it moves.
- Bad, because a rolling restart moves each pod's work to its peers and back,
  one rebalance per pod each way.

### Confirmation

`a_departure_leaves_nothing_to_expire` (`crates/spate-coordination/tests/scenarios/mod.rs`)
pins that a departure clears every owner and deletes the presence key, on every
store. `a_departed_workers_splits_are_withheld_for_the_grace_window` and
`a_zero_rebalance_delay_reassigns_immediately`
(`crates/spate-coordination/tests/multi_worker.rs`) pin the crash path and its
zero.

## More information

- Landed in #856.
- [ADR-0040](0040-rebalance-delay.md) — the withholding this narrows.
- [ADR-0027](0027-split-delivery-attempts-and-quarantine.md) — why a released
  split's move consumes no attempt.
- [Scaling out](../user-guide/05-deployment/scaling-out.mdx) — the behavior on
  a SIGTERM and on a crash.
