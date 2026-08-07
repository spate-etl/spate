# ADR-0003 — A poll-based source API, split into a control plane and a data plane

- **Status:** accepted
- **Date:** 2026-07-05 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A source has to do two unrelated jobs: service lifecycle events — assignment,
revocation, watermark commits, pause and resume — and hand records to the
pipeline as fast as the pipeline can take them. The second job is on the hot
path and the first is not, and the shape chosen for the second decides whether
records can be handed over as **borrowed** bytes or have to be copied.

The idiomatic Rust answer for a byte producer is `futures::Stream`. Applied here
it forces `'static` on the item type, because a stream's item cannot borrow from
the stream across a poll. That would mean a copy per record, at the one point in
the pipeline where a per-record allocation is least affordable.

## Considered options

- One `Stream` per source, with owned items
- A poll-based API split in two: `Source` for the control plane, `SourceLane`
  for the data plane, with lanes yielding borrowed payloads
- A single poll-based trait covering both jobs

## Decision outcome

Chosen option: "A poll-based API split in two", because it is what allows a
payload to be handed to the operator chain as a borrow of the source's own
buffer, and because the two jobs have genuinely different shapes — the control
plane is called from one thread and returns events, while a lane is pinned to a
pipeline thread and returns batches.

The split has a second consequence that matters more than it first appears:
because a lane is pinned to one thread and its payloads never cross a thread
boundary, **deserialization has to happen on the pipeline thread**. That is not
a separate decision so much as the same one seen from the other end — the borrow
lifetimes force it. It also happens to be where the CPU work belongs, since
those threads are the ones that can be pinned to cores.

A single combined trait was rejected because a rebalance callback and a hot poll
loop have different callers, different frequencies and different failure modes,
and merging them makes every source implement locking it does not need.

### Consequences

- Good, because a record can borrow from the source's buffer for its whole life,
  so the chain allocates nothing per record.
- Good, because CPU work — decode, operators, encode — lands on threads that can
  be pinned, and the async runtime is left to do I/O.
- Bad, because a connector author implements two traits instead of one, and has
  to understand which of their methods runs on which thread.
- Bad, because the ecosystem's `Stream` combinators are unavailable, so anything
  a source wants from them has to be written by hand.

### Confirmation

Structural: `SourceLane::poll` returns `Option` of an associated batch type
parameterised by the borrow, so an implementation that copied would have to
change the signature to do it. INV-2 separately holds the control plane to
never blocking on a channel send.

## Evidence

The borrowed arm of the production chain emits a record roughly every 10 ns and
allocates a fixed handful per *batch* — none per record — where the owned
equivalent allocates once per record. Measured by
`crates/spate-core/benches/chain_wall.rs`, with
`crates/spate-core/tests/chain_alloc.rs` holding it to absolute bounds under a
counting allocator.

That rig establishes the **allocation** contrast. Its two arms fan out
differently, so a wall-clock ratio between them is not a quantity it measures.
A separate seam prototype shaped to compare put that ratio at 3.7×;
spike-measured and hand-recorded, with no committed rig.

## More information

- Landed in `c8973e6`; the traits were frozen in `93229b3`.
- [ADR-0013](0013-zero-copy-seam.md) — the erasure boundary that lets these
  borrowed types cross into a `dyn` object.
- [Writing a source](../user-guide/06-extending/custom-source.mdx) — what
  implementing the two traits actually involves.
