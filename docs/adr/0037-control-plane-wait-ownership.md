# ADR-0037 — The driver owns the wait, not the coordination backend

- **Status:** accepted
- **Date:** 2026-07-18 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A coordinated source has nothing to do until something happens, so something has
to block. The obvious place is the backend: it holds the store connection and
the watch subscription, so it knows when a coordination event arrives.

But coordination events are not the only thing worth waking for. A lane reaching
**end of input**, on a pipeline thread and entirely outside the backend's view, is
what triggers a terminal commit and frees the split. A backend that owns the wait
can only ever be woken by half the things that matter.

That is not hypothetical: a finishing lane was noticed only between waits, so
every split completion sat out the remainder of one. Capping the wait bounded the
damage, at the cost of polling the control plane two hundred times a second.

## Considered options

- The backend owns the wait, with a cap to bound the damage from signals it
  cannot see
- The driver owns the wait: the backend's poll takes no timeout and must not
  block, and the driver holds a single-slot waker that lanes also signal
- Both wait, with the backend's wait nested inside the driver's

## Decision outcome

Chosen option: "The driver owns the wait", because only the driver sees both
sources of wakeup. Backend events arrive through the store; end-of-input and
poison arrive from lanes on pipeline threads. Putting the wait where both are
visible removes the latency *and* the polling, rather than trading one for the
other.

The backend's poll takes no timeout and must not block. The driver hands it a
single-slot waker, which lanes clone and signal on two edges: end of input, and
poison.

The waker setter is deliberately **not defaulted**. A default would let a backend
silently skip it and park internally, which is strictly the worst arrangement.
The backend parks *and* the driver parks on top, so a wakeup has to traverse
both. Making it required means a backend that has not thought about this does not
compile.

### Consequences

- Good, because a finishing lane wakes the control plane immediately rather than
  on the next tick.
- Good, because control-plane polling drops from two hundred times a second to
  zero.
- Bad, because "must not block" is a contract on a trait method that the type
  system cannot express, so a backend violating it compiles and misbehaves
  subtly.
- Bad, because the waker is single-slot, so signals coalesce. The driver learns
  that *something* happened, not what or how many.

### Confirmation

The absence of a default on the waker setter is the enforcement: a backend must
implement it explicitly, so the failure mode is a compile error rather than a
silent double-park.

## More information

- Landed in `84e1583` (#49).
- [ADR-0029](0029-framework-owned-coordination-driver.md) — the driver this makes
  responsible for waiting.
