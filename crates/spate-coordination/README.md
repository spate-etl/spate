# spate-coordination

Distributed work coordination for the
[Spate](https://github.com/spate-etl/spate) framework: how several instances
of the same pipeline divide a body of work between them without duplicating it
or dropping any of it.

A leader-elected planner enumerates weighted splits into a shared low-latency
store and publishes a desired assignment per instance. Workers lease and
heartbeat what they are assigned, cooperatively drain what they are not, and
commit progress through epoch-fenced compare-and-swap. The assignment is
sticky, so a worker keeps its splits across an unrelated membership change
rather than reshuffling everything.

Applications should depend on the [`spate`](https://crates.io/crates/spate)
facade with the `coordination` feature (in-memory store, for a single process
and for tests) or `coordination-nats` (NATS JetStream KV). The store is a
trait, so a backend is a few hundred lines rather than a fork.

## Sharp edges worth knowing

- **The fence is the correctness boundary, not the lease.** A lease that has
  expired is not proof the previous owner has stopped; the epoch fence on
  every commit is what makes a stale writer's progress unobservable. Backends
  must provide a genuine compare-and-swap for this to hold.
- **Draining is cooperative and takes as long as the work in flight.** A split
  moving between workers waits for the losing worker to finish and commit, so
  time-to-balance is dominated by the drain rather than by the protocol.
- The `testing` feature exposes a controllable clock. It is off by default and
  the facade must never enable it — a test clock in production stops the
  control loop dead.

The algorithm is normative and documented in the repository's
`docs/user-guide/02-concepts/08-work-assignment.mdx`, whose numbered
invariants name the property tests that enforce them.
