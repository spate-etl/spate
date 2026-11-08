---
description: "How connector code keeps per-record work from allocating, and how to show it with the allocation snapshots and the counted tier."
---

# The per-record path

The per-record path is the code a connector runs once for every record or
every message: a deserializer, an encoder, the operator chain, and a sink
writer preparing each message of a batch. The
[user guide](../user-guide/README.mdx) promises zero per-record allocations on
the pipeline threads; hold a sink writer's per-message work to the same rule.
Code that runs once per batch, poll or object may allocate a bounded amount.
The count that grows with the records has to stay flat.

## Shapes that keep it flat

**Reuse a buffer across calls.** Clear it and fill it again for the next
record. The Kafka sink's encoder keeps its message accumulator and each header
slot between records
([`spate-kafka/src/sink/encoder.rs`](repo:crates/spate-kafka/src/sink/encoder.rs)),
so its allocation snapshot reads zero for every case that encodes.

**Borrow from the bytes you were given.** A sink batch arrives as `Bytes`
frames and a source batch lends its payloads. A slice into either costs
nothing, and one slice can stand for a variable-length part of a record. A
collection built per record to hold borrowed slices is still an allocation per
record.

**Size a collection from a count you already hold.** A batch knows its row
count before parsing starts, so a list of its messages is allocated once at
that size. When the count comes from the input, cap the capacity at what the
remaining bytes could hold, since a corrupt count near `u32::MAX` otherwise
asks the allocator for gigabytes and aborts the process.

**Resolve metrics handles at build time**, as [INV-8](../INVARIANTS.md)
requires.

## Measuring a change

[Allocation snapshots](04-benchmarks-and-fuzzing.md#allocation-snapshots)
report allocator calls and requested bytes for each case, and a change to this
path reads both. They move independently: one buffer that grows by doubling
can request more bytes in total than many small allocations of exact size, so
a change that cuts the calls can still raise the bytes.

Read [the counted tier](04-benchmarks-and-fuzzing.md#the-counted-tier) as
well. It shows the instructions a change adds, such as a second pass over
bytes it no longer copies, and its DHAT rows see allocations the snapshots
miss.

Give a case for each shape whose cost scales with a count, such as records
with and without headers, so a regression in one shape is not averaged away by
the others.

Code a bench cannot reach through the public API is reached through a seam
behind the crate's `testing` feature, which
[Anatomy of a connector crate](03-connector-crate.md#seams-behind-testing)
describes. Land the seam and its benches in a pull request before the change
they measure, so the change's pull request shows the counts moving against a
committed baseline.

## Related

- [Benchmarks and fuzzing](04-benchmarks-and-fuzzing.md) — how to add a
  snapshot target or a counted bench, and what each tier gates.
- [Anatomy of a connector crate](03-connector-crate.md) — the `testing`
  feature and the rules a bench seam follows.
- [Spate invariants](../INVARIANTS.md) — INV-8 and the other properties the
  hot path holds to.
