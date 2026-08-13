# ADR-0011 — A rolling N-2 MSRV, and no 0.x types in the public API

- **Status:** accepted
- **Date:** 2026-07-05 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Two questions with the same shape: how much of somebody else's release cadence
is the framework willing to inherit?

The first is the Rust version. Pinning an old one keeps the crates usable in
conservative environments but blocks language features and, more importantly,
makes every dependency that raises its own floor a problem. Tracking stable
makes the crates unusable for anyone who cannot move immediately.

The second is dependency types. The connector crates wrap the 0.x libraries
rdkafka, clickhouse and apache-avro. Under Cargo's semver rules a 0.x crate's
minor release is a breaking release. If those types appear in our public API,
every one of their breaking releases becomes one of ours, on a cadence we do not
control.

## Considered options

- Pin an old MSRV and hold it; re-export dependency types freely
- Rolling N-2 MSRV, edition 2024; no 0.x types in `spate-core`'s public API or
  in any public trait bound
- Track stable, and treat dependency types as our own surface

## Decision outcome

Chosen option: "Rolling N-2 MSRV, and no 0.x types in the public API", because
both halves buy the same thing: our version number means what it says.

N-2 (currently 1.94, edition 2024) is enough reach for library consumers while
absorbing dependency MSRV ratchets without an emergency. Dependencies raising
their floors is what forces the issue, more than any language feature.

For the API rule, the boundary is `spate-core` and any public trait bound.
Connector crates may re-export their underlying crate for advanced use, clearly
documented as exempt from our stability promises. That is an escape hatch a
consumer opts into, not a type they receive by accident from a framework
signature.

The `metrics` facade is the **one sanctioned exception**, for the reasons in
[ADR-0008](0008-metrics-facade.md): the instrumentation API *is* that facade, and
wrapping it would create a second vocabulary for the same concepts.

### Consequences

- Good, because an rdkafka or clickhouse major bump is not a breaking change for
  anyone depending on `spate`.
- Good, because a third-party connector can implement our traits without naming
  any of our dependencies.
- Bad, because the sink seam has to be expressed in framework types, so
  connector-typed flows the framework cannot name, such as the ClickHouse schema
  validation path, stay concrete pre-steps outside the trait rather than being
  unified into it.
- Bad, because N-2 still moves, so a consumer pinned to an older toolchain will
  eventually be left behind; the policy makes that predictable, not absent.

### Confirmation

`make check-features` and the feature matrix in CI compile the crates without
default features, which is what catches a dependency type leaking into a
signature through a feature-gated path. The rule itself is INV-6.

## More information

- Landed in `c8973e6`.
- [ADR-0008](0008-metrics-facade.md) — the sanctioned exception and its
  mitigation.
