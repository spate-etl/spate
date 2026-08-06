# ADR-0013 — Untyped payload batches cross the erasure boundary, with a lifetime-to-type family

- **Status:** accepted
- **Date:** 2026-07-05 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

[ADR-0004](0004-static-operator-chain.md) puts exactly one erasure boundary in
the pipeline, so the chain can be stored as a trait object. That runs into a
Rust limitation: a trait object cannot be parameterised by a lifetime that
varies per call, and the record type here *is* lifetime-parameterised, because
records borrow from the source's buffers ([ADR-0003](0003-poll-based-source-api.md)).

Storing `Box` of a chain whose record type is `Record<'buf, T>` is not
expressible. The naive fix — make records owned at the boundary — is a copy per
record, which is the thing the borrowing design exists to avoid.

## Considered options

- Own the records at the boundary, accepting one copy per record
- Pass untyped payload batches across the boundary, and have records be born and
  die inside a single call
- Make the whole pipeline generic over the chain, with no erasure at all

## Decision outcome

Chosen option: "Pass untyped payload batches across the boundary", because it
sidesteps the limitation instead of paying for it. The boundary trait takes a
`PayloadBatch` and returns an outcome; **no lifetime-parameterised type is ever
stored across the call.** Records are deserialised, transformed and serialised
into shard frames entirely inside one `push_batch`, so by the time the call
returns there is nothing borrowed left to name. That makes `Box` of the boundary
trait legal.

The remaining problem is that a chain still needs to *name* its record type in
its own generic parameters. That is solved by a lifetime-to-type family: a
`'static` marker type with an associated type parameterised by the borrow. The
family crosses generic and `dyn` boundaries because the marker is `'static`;
the borrowed type is recovered by projection at each use. An `Owned` family is
provided for records that do not borrow.

A fully generic pipeline was rejected because the chain's type depends on what
the user composed, so every containing structure would have to be generic over
it, and nothing could be stored in a field or returned from a function without
naming it.

### Consequences

- Good, because records live and die inside one call, so the borrowed design
  survives contact with a trait object.
- Good, because the boundary costs one virtual call per batch, amortised over
  every record in it.
- Bad, because the family indirection is genuinely hard to read: a connector
  author writes a marker type whose only job is to carry an associated type, and
  the error messages when it is wrong are poor.
- Bad, because anything that would need to *hold* a record beyond the call —
  windowing, sorting, stateful aggregation — is structurally impossible. That is
  consistent with the v1 non-goals, but it is a ceiling rather than a
  preference.

### Confirmation

Structural: `RunnableChain::push_batch` takes the batch by reference and returns
a resume cursor, so there is nowhere to store a borrowed record even if an
implementation tried.

## Evidence

The borrowed arm emits a record roughly every 10 ns and allocates a fixed handful
per *batch* — none per record — where the owned equivalent allocates once per
record. Measured by `crates/spate-core/benches/chain_wall.rs`, held to absolute
bounds by `crates/spate-core/tests/chain_alloc.rs` under a counting allocator.

The allocation contrast is what that rig establishes. **Its two arms fan out
differently, so a wall-clock ratio between them is not a quantity it measures** —
a distinction worth keeping, because the ratio is the number people want to
quote. A separate seam prototype shaped to compare put that ratio at 3.7×:
spike-measured and hand-recorded, with no committed rig.

## More information

- Landed in `93229b3`; the contracts were frozen in `7d00ac5` with the spike
  evidence recorded alongside them.
- Deltas applied at freeze time: a blocked push carries a reason distinguishing
  sink capacity (which engages backpressure) from an upstream dependency such as
  a schema fetch (retried without pausing the source, counted on
  `spate_deser_not_ready_total`); the boundary returns a resume cursor so
  already-pushed records are never re-run through the operators; the trait gained
  flush and drain hooks; and a raw payload carries the message key for shard
  routing.
- [ADR-0004](0004-static-operator-chain.md) — the chain this is the boundary of.
