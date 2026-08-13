# ADR-0024 — An external low-latency KV store behind a six-primitive trait

- **Status:** accepted
- **Date:** 2026-07-17 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Coordinating several instances over a shared body of work needs somewhere to put
the durable record of who owns what. An earlier design put it in the object store
the backfill was already reading, using conditional writes as the compare-and-set
primitive. That needed **zero extra infrastructure**, which is a strong argument
and was the argument at the time.

Then the requirement changed. What began as "coordinate one object-storage
backfill" became "a general work-distribution seam" that any broker-less source
could use. With that, the object store stops being incidental infrastructure the
source already needed and becomes the substrate for *every* coordinated source.
Its poll-based round trips, on the order of 10 to 100 ms, cap how fast work can
be distributed.

## Considered options

- Compare-and-set leases over the object store the source is already reading
- An external low-latency KV store behind a public store trait, with
  compare-and-set, a TTL keyspace, watch and list
- Embedded consensus inside the workers

## Decision outcome

Chosen option: "An external low-latency KV store behind a public store trait",
because the trade that justified the object store did not survive the change in
requirement. Reusing infrastructure is worth latency when it is infrastructure
you already had; it is not worth capping distribution speed for every source
that will ever use the seam.

The store supplies **latency and watch, never safety**. Correctness lives
entirely in the fencing protocol
([ADR-0026](0026-coordination-fencing.md)). The store's job is to make the
protocol fast, not to make it correct. That separation is what keeps the trait
small: compare-and-set, a TTL keyspace, watch, and list, six primitives in all,
so a Redis or etcd backend is one implementation away.

This reverses a decision taken and implemented; the earlier design's pull request
was closed unmerged. The reversal is recorded rather than dropped,
because the reasoning that led to it was sound for the requirement it was given.

### Consequences

- Good, because distribution latency is bounded by a KV round trip rather than
  an object-store round trip, so a rebalance is quick enough not to be noticed.
- Good, because `watch` exists, so a worker learns about a change instead of
  discovering it on its next poll.
- Bad, because a coordinated deployment now needs infrastructure it did not
  before. That is an operational cost, and the in-process store exists so a
  single-instance deployment does not pay it.
- Bad, because the trait's six primitives are a contract we now have to hold
  stable across backends, and a store whose semantics differ subtly, such as a
  compare-and-set that is not linearizable, would be unsound in a way the trait
  cannot express.

### Confirmation

Correctness does not depend on the store beyond linearizable compare-and-set;
[ADR-0026](0026-coordination-fencing.md) states what is relied on. The
in-memory store implementation doubles as the test backend, so the seam is
exercised by every coordination test.

## More information

- Landed in `d92b3d4` (#40).
- The superseded object-store design was never recorded in the decision log.
  Its pull request was closed unmerged, so there is no earlier record to
  supersede. Its reasoning is preserved in the context above rather than in a
  record of its own.
- [ADR-0025](0025-embedded-consensus-rejected.md) — why the third option stays
  rejected.
