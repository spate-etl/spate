# ADR-0042 — A first-party load generator shipping one named dataset, not a schema language

- **Status:** accepted
- **Date:** 2026-08-08
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Every source this workspace ships needs infrastructure standing before it
produces a record: `spate-kafka` needs a broker, `spate-s3` needs a bucket, and
a multi-instance run of either needs a coordination store. That prerequisite
falls on the examples under `crates/spate/examples/`, on the quickstart, and on
anybody evaluating the framework — the first thing between a reader and a
running pipeline is a `docker compose up`.

A synthetic source removes it. What is not obvious is what such a source should
generate, because the examples that most need one are the ones that demonstrate
routing, joins and aggregation — and those need records that *reference each
other*, not independent rows. The question this record settles is what shape
that generator takes and where it lives.

## Considered options

- A published `spate-datagen` crate generating one built-in, named dataset.
- A published crate driven by a configured **schema**: a `fields:` map naming
  types, ranges and cardinalities per column.
- A module inside `crates/spate/examples/`, shared by the examples and
  published to nobody.

## Decision outcome

Chosen option: "a published `spate-datagen` crate generating one built-in,
named dataset", because the property that makes a generated stream worth
demonstrating on cannot be expressed field-wise, and the audience that most
needs it is outside this repository.

The dataset is a storefront: orders over a small catalog, payments against
those orders, and refunds against those payments. Each lane owns a disjoint
slice of the order-id space and a bounded ring of the orders it has placed, so
a payment names an order that was really placed, for an amount that matches its
lines, on the same partition, at a strictly greater offset — with no shared
state on the record path and therefore no coordination to survive the
CPU-pinned fan-out.

A **schema DSL was rejected on the same point.** A `fields:` map can say "a
`u64` here"; it cannot say "this `u64`, drawn from the ids the same lane minted
earlier and not yet drawn". Every generator that has tried to express
referential consistency field-wise has grown expressions, then references, then
scope — a small programming language, which is a different product with a
different maintenance cost, and one whose surface would be locked by semver
before anybody had asked for it.

An **examples-local module was rejected** because the users who need it are not
reading this repository. Somebody evaluating the framework, writing a
connector, or reproducing a bug report needs a source they can depend on by
name; a module under `examples/` is not one. It would also sit outside the
crate gates — no semver surface, no feature matrix, no per-crate coverage
component — which is the wrong bargain for code that examples and tests both
run.

### Consequences

- Good, because an example, a quickstart and a bug reproduction all run with
  nothing installed, which is the shortest path from reading about the
  framework to watching it move records.
- Good, because the referential structure gives the examples something real to
  do — a key-routed join, a sum to check, a late reference to handle — instead
  of demonstrating operators on independent rows.
- Good, because the crate adds no third-party package: the PRNG is a
  hand-rolled SplitMix64, so `deny.toml`, `about.toml` and `THIRD-PARTY.md` are
  untouched by a crate whose whole job is removing prerequisites.
- Bad, because a second dataset is a code change here rather than a
  configuration change by the person who wants it. That is the cost of refusing
  the DSL, and it is paid by us rather than by them.
- Bad, because the crate is published and therefore versioned: the event model
  and the encoded field names are a semver surface, and a dataset that turns
  out to be wrong cannot simply be reshaped.
- Neutral, because the generator keeps no durable progress. A restart
  regenerates the whole stream, which is strictly more duplication than a real
  at-least-once source — safe under INV-1, and stated in the crate docs, the
  README and a startup `WARN` so that nobody mistakes it for a resumable
  source. A `resume_from:` file is declined for the same reason.

### Confirmation

Structural, in three places. `DatagenSourceConfig` carries
`#[serde(deny_unknown_fields)]`, so a `fields:` map is a load error rather than
an ignored key. The referential property is a test: generated events are
checked to reference only orders placed earlier on the same lane, and a lane's
stream is asserted to replay identically from the same seed. And the absence of
a durable progress store is what makes the no-resumability claim checkable —
`commit` writes to a `HashMap` and nothing else.

## More information

- Landed in the pull request adding `crates/spate-datagen`.
- [ADR-0002](0002-at-least-once-delivery.md) — the delivery model this source
  is deliberately weaker than, and says so.
- [ADR-0003](0003-poll-based-source-api.md) — the `Source`/`SourceLane` shape
  the generator implements.
