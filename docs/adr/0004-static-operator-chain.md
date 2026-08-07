# ADR-0004 — A statically composed operator chain with one dynamic call per batch

- **Status:** accepted
- **Date:** 2026-07-05 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The operator chain — `map`, `filter`, `flat_map` and the terminal sink stage —
runs on every record. Whatever dispatch it uses is paid per record per operator,
so a chain of five operators over a million records a second is five million
dispatch decisions a second. At the same time the chain has to be *storable*:
the pipeline holds one, and its concrete type depends on how many operators the
user composed and what types flow between them, which is not a type the runtime
can name.

Those two requirements pull opposite ways. Naming the type keeps dispatch static
and makes the chain unstorable; erasing it makes the chain storable and makes
dispatch dynamic.

## Considered options

- Per-operator dynamic dispatch: each stage a `Box` of a trait object
- Pull-based iterator composition
- Push-based composition, monomorphised, with type erasure at the chain boundary
  only — one dynamic call per *batch*

## Decision outcome

Chosen option: "Push-based composition with erasure at the chain boundary",
because it puts the erasure where its cost amortises over a whole batch instead
of over one record, and because push composition is what makes `flat_map`
allocation-free.

Each operator implements a push interface and calls its downstream inline, so
the whole chain compiles to one loop and the optimiser can inline and vectorise
across operator boundaries. `flat_map` emits through a stack-borrowed emitter,
so fan-out allocates nothing. Erasure happens exactly once, at the boundary,
where a single virtual call delivers an entire batch.

Per-operator dynamic dispatch was rejected because it defeats cross-operator
inlining, which is worth more than the dispatch itself. Pull-based iterators were
rejected because `flat_map` in a pull model has to buffer its children, which
reintroduces the per-record allocation the design exists to avoid.

### Consequences

- Good, because adding a stage costs an inlined function call rather than a
  dispatch, so chain length is close to free.
- Good, because fan-out allocates nothing — the emitter lives on the stack.
- Bad, because the chain is monomorphised, so compile time and binary size grow
  with the number of distinct chains in a program.
- Bad, because higher-ranked lifetimes hit a language limit for borrowing
  families: `map` and `try_map` closures cannot be inferred (E0582 — the
  higher-ranked lifetime must appear in an associated-type projection), so those
  two combinators need a `map_rec`/`try_map_rec` tier taking plain `fn` items.
  `filter`, `inspect` and `flat_map` are unaffected.

### Confirmation

`crates/spate-core/tests/chain_alloc.rs` holds the chain to absolute allocation
bounds under a counting allocator, which is what would catch an operator
regressing into per-record allocation.

## Evidence

Roughly 10 ns per record and zero allocations per record on the borrowed arm,
measured by `crates/spate-core/benches/chain_wall.rs`.

## More information

- Landed in `c8973e6`.
- [ADR-0013](0013-zero-copy-seam.md) — the boundary this erases at, and why a
  lifetime-parameterised record can cross it.
- [Writing operators](../user-guide/06-extending/custom-operators.mdx) —
  including the `map_rec` tier and when it is needed.
