# ADR-0026 — The durable record's compare-and-set revision is the only correctness mechanism

- **Status:** accepted
- **Date:** 2026-07-17 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Coordination has several mechanisms that *look* like they establish ownership: a
lease with a TTL, a watch event announcing a change, a leader publishing an
assignment. Each is a plausible place to put the safety argument, and each is
wrong in the same way. They all depend on timing.

A lease TTL expiring says the holder *probably* stopped, not that it did; the
holder may be in a long garbage-collection pause and about to resume mid-write.
A watch event may arrive late or not at all. If any of these were
safety-relevant, the store's clock would be in the correctness path.

## Considered options

- Lease TTL as the ownership boundary: the lease is valid, therefore the write is
- Watch events as the ownership signal
- A monotonically increasing revision on the durable record, checked by
  compare-and-set on every write, with TTLs and watches carrying only latency

## Decision outcome

Chosen option: "A revision on the durable record, checked by compare-and-set on
every write", because it is the only one of the three that does not put a clock
in the correctness path.

Every claim, fence and commit targets the split's progress record and carries the
revision it expects. An ownership change bumps an epoch. A worker whose epoch is
stale loses its compare-and-set, and **a fenced commit writes nothing**. It then
receives a `Lost` event and stops. It does not matter how long the worker was
paused, whether its lease had expired, or whether it ever saw the watch event
telling it so: it cannot write, because the revision moved.

Leaders are fenced the same way. A new leader's generation bump moves the plan
record's revision, so a deposed leader's plan write loses its compare-and-set.

Lease TTLs and watch events remain, and what they buy is *speed*. They are how
the fleet notices a change in under a second rather than on the next poll. They
are not allowed to be the reason anything is safe.

### Consequences

- Good, because correctness is independent of clock skew, pause duration and
  message delivery, which are the properties hardest to reason about and
  impossible to test exhaustively.
- Good, because there is exactly one thing to verify for safety, and it is a
  single conditional write.
- Bad, because every write on the coordination path costs a compare-and-set
  round trip, including ones that will obviously succeed.
- Bad, because a fenced worker discovers it has lost only when it tries to
  write, so it may do useful-looking work it will then have to discard.

### Confirmation

Structural: a commit that does not carry the expected revision cannot be
expressed by the store trait. The `Lost` event path is exercised by the
coordination test suite, including the case where a worker returns from a long
pause to find itself fenced.

## More information

- Landed in `d92b3d4` (#40).
- The reasoning follows the standard fencing argument for distributed locks: a
  lock with a TTL is not a lock unless writes carry a fencing token.
- [ADR-0025](0025-embedded-consensus-rejected.md) — this is the guarantee that
  makes consensus unnecessary.
- [Work assignment](../user-guide/02-concepts/08-work-assignment.mdx) — the
  normative specification, whose numbered invariants name the property tests.
