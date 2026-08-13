# ADR-0033 — Listing-order first-fit packing with a bounded lookback and a per-object open cost

- **Status:** accepted
- **Date:** 2026-07-18 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The planner has to group a bucket listing into splits. Object sizes in a real
bucket vary by orders of magnitude, and the number of objects can be very large,
so the packing algorithm decides both how even the work is and how much memory
the planner needs.

The obvious quality-maximizing approach is to sort by size and pack
largest-first. That requires the whole listing in memory before packing can
start, which makes planner memory a function of object count, and it destroys
**prefix locality**. Objects that share a key prefix, and therefore usually
share physical locality, get scattered across splits.

## Considered options

- Sort by size, then pack largest-first for the most even bins
- Listing-order first-fit with a bounded lookback window, charging each object a
  minimum open cost
- One object per split

## Decision outcome

Chosen option: "Listing-order first-fit with a bounded lookback", because it is a
pure function of the listing order: packing holds only a bounded window of open
bins, and objects that listed together stay together.

Three parameters carry the design. A lookback of ten bins is enough to place a
small object well without unbounded search. Each object is charged
`max(size, target/16)` as its **open cost**, so a split of many tiny objects is
bounded by request overhead rather than by bytes. The cost of a split is
dominated by opening objects, not by reading them. And an object at or above the
target gets its own split.

That cost floor has a second effect, and it is what makes
[ADR-0030](0030-split-record-layout.md) work: it structurally caps a split at
about sixteen members, which keeps a descriptor comfortably under store value
limits. The bound is a consequence of the cost model rather than a separate
check.

One object per split was rejected because it makes split count equal object
count, and every split carries coordination overhead.

### Consequences

- Good, because prefix locality survives, so a split's reads are usually
  physically close.
- Neutral, because the lookback bounds only the open-bin window. The planner
  collects and sorts the full listing by key before packing sees it, so planner
  memory is a function of object count either way, the same cost the sorted
  pack was charged with above. Packing adds a bounded window on top of it.
- Bad, because bin evenness is worse than a sorted pack would achieve, so some
  splits are meaningfully larger than others.
- Bad, because the target size and the divisor in the open cost are tuned
  constants, and their interaction, which is what caps descriptor size, is not
  obvious from either one alone.

### Confirmation

The packing is a pure function of the listing, so it is property-tested directly
against generated listings, including the degenerate cases of all-tiny and
all-huge objects.

## More information

- Landed in `84e1583` (#49).
- Follows established practice for object-storage table formats, which pack in
  listing order for the same locality and streaming reasons.
- [ADR-0030](0030-split-record-layout.md) — the descriptor size bound this
  provides.
