---
description: "Adopts the metrics crate itself as the instrumentation API, re-exported from spate-core, accepting its 0.x types as the one exception to the dependency policy."
---

# ADR-0008 — The `metrics` facade as the instrumentation API, with a Prometheus exporter

- **Status:** accepted
- **Date:** 2026-07-05 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The framework needs instrumentation that a Kubernetes deployment can scrape, and
connector and pipeline authors need to register families of their own. The
question is whether that type is a framework-owned abstraction wrapping some
backend, or the ecosystem's own facade.

There is a complication. The `metrics` crate is 0.x, and
[ADR-0011](0011-msrv-and-dependency-policy.md)'s dependency policy says 0.x types
must not appear in our public API, because their breaking releases would become
ours.

## Considered options

- A framework-owned metrics trait, with the facade hidden behind it
- The `metrics` facade directly, as a sanctioned exception to the dependency
  policy, re-exported from `spate-core`
- A concrete Prometheus client, with no facade

## Decision outcome

Chosen option: "The `metrics` facade directly, as a sanctioned exception,
re-exported from `spate-core`", because the framework's instrumentation API *is*
that facade. A wrapper would be a second vocabulary for the same concepts, and
anyone registering a metric would have to learn ours instead of the one they
already know.

The exception is mitigated rather than accepted: `spate-core` re-exports
the handle types, and connectors never take a direct `metrics` dependency. That
keeps exactly one facade version in the tree, so a breaking `metrics` release is
one coordinated edit rather than per-crate drift.

A concrete Prometheus client was rejected because it forecloses any other
backend for no gain. The facade already resolves to Prometheus at the exporter.

Two rules follow from the hot path rather than from the choice of facade. Metric
handles are **pre-registered at build time**, because resolving a name or a
label per record is both an allocation and a lookup in the one place neither is
affordable. And counting happens at batch boundaries, not per record.

### Consequences

- Good, because a connector author registering a family writes ordinary
  `metrics` code with no framework-specific wrapper.
- Good, because the backend stays pluggable. The facade is the registry
  abstraction, and the Prometheus exporter is one implementation mounted on the
  admin server.
- Bad, because a 0.x crate is in our public API, so a breaking `metrics` release
  is a breaking release for us. This is the one place that is true, and it is
  named in the dependency policy rather than discovered.
- Bad, because assembly order decides where a handle records. A handle binds to
  whichever recorder exists when it is constructed, so one built before
  `metrics::install` records into the void.

### Confirmation

INV-8 (handles pre-registered at build time) and INV-9 (every family under the
`spate_` umbrella, with connector families prefixed through a `Meter` that
rejects a namespace shadowing a reserved root).

The ordering hazard is made unconstructible rather than documented:
`Pipeline::from_config` installs the exporter before it returns, so a builder
cannot be held without a live recorder.

## More information

- Landed in `c8973e6`.
- [ADR-0011](0011-msrv-and-dependency-policy.md) — the dependency policy this is
  the exception to.
- [`docs/METRICS.md`](../METRICS.md) — the taxonomy and the reserved roots.
- [Instrumenting connectors](../user-guide/06-extending/instrumenting-connectors.mdx)
  — registering a family through a `Meter`.
