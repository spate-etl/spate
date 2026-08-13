# ADR-0025 — Embedded consensus is not used for work distribution

- **Status:** accepted
- **Date:** 2026-07-17 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Coordinating ownership of work across instances is a distributed-agreement
problem, and the textbook answer to distributed agreement is a consensus
protocol. Embedding one (Raft inside the workers) would remove the external
store dependency that [ADR-0024](0024-coordination-store-external-kv.md)
introduces.

## Considered options

- Embed a Raft implementation in the workers, forming a voter set
- Depend on an external store that provides linearizable compare-and-set

## Decision outcome

Chosen option: "Depend on an external store", because the problem needs
linearizability at exactly one point, the per-split ownership commit, and that
is what a single compare-and-set provides. Consensus would be buying a
guarantee the fence already gives.

The decisive argument is operational rather than theoretical. **A voter set
fights autoscaling.** Consensus needs stable membership and per-node durable
state; a worker fleet is designed to scale to zero and back, and its members are
interchangeable and disposable. Making workers voters would mean a scale-down
event is a membership change requiring quorum, which inverts the property the
fleet exists to have. No comparable worker framework embeds consensus for work
distribution, and that agreement across otherwise different designs is worth
weighing.

**Centralizing assignment does not reopen this.** A later change made the leader
compute the whole assignment rather than workers negotiating it
([ADR-0038](0038-leader-computed-sticky-assignment.md)), which looks like it
needs the leader to be authoritative, and therefore like it needs consensus to
elect. It does not, because an assignment carries **no correctness**. A stale
leader, a split-brained pair of leaders, or a wrong leader can produce bad
balance, but never two owners of one split, because the durable record's
compare-and-set still decides ownership. The worst a bad leader can do is
distribute work poorly.

### Consequences

- Good, because workers stay stateless and disposable, so the fleet scales to
  zero and back without a membership protocol.
- Good, because there is one mechanism to reason about for safety, the fence,
  rather than a consensus layer and a fence that must agree.
- Bad, because a store dependency is a runtime dependency with its own
  availability, and when it is down, coordination stops.
- Bad, because leader election is best-effort, so transient double-leadership is
  possible and the design must remain correct under it rather than preventing
  it.

### Confirmation

The claim that an assignment carries no correctness is what makes this sound,
and it is held by [ADR-0026](0026-coordination-fencing.md): the durable split
record's compare-and-set revision is the *only* correctness mechanism. If an
assignment ever became safety-relevant, this decision would need revisiting.

## More information

- Landed in `d92b3d4` (#40); the leader-assignment argument was added in
  `e79dfc2` (#64) when centralizing assignment made the question live again.
- [ADR-0026](0026-coordination-fencing.md) — the fence that provides the
  guarantee consensus would have bought.
