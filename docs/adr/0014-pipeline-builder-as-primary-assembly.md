# ADR-0014 — A non-generic `Pipeline` builder that owns initialization

- **Status:** accepted
- **Date:** 2026-07-07 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Assembling a pipeline by hand meant installing telemetry, installing the metrics
exporter, building an I/O runtime, constructing a source and a sink, wiring
queues, and running the whole thing, in an order where several steps are
silently wrong if taken out of sequence. The sharpest was metrics: a handle binds
to whichever recorder exists when it is constructed, so anything built before
`metrics::install` records into the void, with no error and no missing series to
notice. The ordering was documented, which is another way of saying it had to be
remembered.

## Considered options

- Keep manual assembly as the only path, and document the ordering
- A builder using typestate generics, with each stage a distinct type parameter
- A non-generic builder whose constructor performs initialization, with the
  source supplied at the terminal call
- A configuration-driven registry mapping tags to constructors

## Decision outcome

Chosen option: "A non-generic builder whose constructor performs initialization",
because it converts the ordering hazards from documented into
**unconstructible**. `Pipeline::from_config` installs telemetry and the exporter
and builds the I/O runtime before it returns, so a builder cannot be held without
a live recorder. It refuses to be constructed inside an async runtime, because it
owns a blocking one.

Four design points, each earned by a specific footgun:

- **Coarse typestate.** One session-like state object rather than
  state-parameter generics, so the builder stays nameable and storable. Typestate
  generics would make `Pipeline` un-nameable in a struct field, which is where
  people put it.
- **One I/O runtime.** The builder's runtime moves into the pipeline runtime
  rather than `run()` building a second one, which had been silently doubling
  `io_threads` behind the thread-reservation arithmetic.
- **A connector-agnostic sink slot.** `SinkBundle`, destructured into a
  `#[non_exhaustive]` `SinkParts`, is the seam between connector factories and
  the builder. Its only bound is `ShardWriter`, which preserves
  [ADR-0011](0011-msrv-and-dependency-policy.md)'s dependency policy, and the
  fields can grow additively.
- **The source stays a terminal generic**, minted at `run`. Connector
  construction remains one explicit line.

The registry was rejected on principle: topology is code-defined, and a
tag-to-constructor registry would reintroduce it as data, the thing
[ADR-0009](0009-yaml-configuration-with-opaque-passthrough.md) deliberately keeps
YAML out of.

Nothing in the builder touches the data path. It assembles the cold path and
passes the user's chain factory through unchanged, so the chain stays fully
monomorphized behind the same one-call-per-batch boundary.

### Consequences

- Good, because the exporter-ordering bug cannot be written, rather than being
  caught in review.
- Good, because drop ordering is structural: the sink drains only when every
  queue clone is gone, and the builder lends queues per chain-factory call by
  value rather than exposing them.
- Bad, because two assembly paths now exist. The manual primitives stay public
  and semver-committed, so both have to keep working and the convenience layer
  has to document its exact desugaring.
- Bad, because connector-typed flows the framework cannot name, such as the
  ClickHouse schema validation path producing an encoder, stay concrete
  pre-steps outside the trait, so the seam is not quite uniform.

### Confirmation

Structural for the ordering bug: `from_config` returns an initialized builder or
nothing. The desugaring is held to the manual primitives by the rustdoc on
`Pipeline`, which shows the exact equivalent sequence.

## More information

- Landed in `affb949` (#3).
- [Assembling a pipeline](../user-guide/03-guides/assembling-a-pipeline.mdx) —
  the builder in use.
- [Manual assembly](../user-guide/03-guides/manual-assembly.mdx) — the
  lower-level path and the desugaring table.
