# ADR-0036 — The coordination backend is injected at assembly, never configured per connector

- **Status:** accepted
- **Date:** 2026-07-18 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A coordinated source needs a coordination backend. The natural-looking place to
say which one is the source's own configuration section, the same place its
bucket and credentials live.

That does not scale. Every coordinated source would need configuration for every
backend, and every backend would need a cargo feature on every source crate. The
surface grows as connectors times backends, in two dimensions at once, and adding
a backend means touching every connector.

## Considered options

- Per-connector backend configuration and cargo features
- Assembly-only injection: sources accept a boxed coordinator and carry no
  backend configuration or features at all
- A single global backend chosen by framework configuration

## Decision outcome

Chosen option: "Assembly-only injection", because the deployer's binary is the
right owner of "which store". It is where the dependency is already declared,
and it is the one place that knows what infrastructure the deployment has.

A source accepts a boxed coordinator and keeps in its YAML only what shapes the
work itself, planner knobs like the split target size and whether to refresh the
listing. Nothing about *where coordination lives* appears in a connector's
configuration or features. A new backend is then a new crate implementing the
store trait, with **zero connector changes**.

A single framework-level backend was rejected as less flexible for no
simplification: it would still need the same trait, and it would prevent a
deployment from using different backends for different sources.

This is the same shape as the framer seam
([ADR-0009](0009-yaml-configuration-with-opaque-passthrough.md)'s principle that
a component's configuration belongs to whoever understands it): the framework
owns the seam, the deployer supplies the implementation.

### Consequences

- Good, because adding a backend touches no connector, and adding a connector
  supports every backend automatically.
- Good, because connector cargo features stay about the connector rather than
  about infrastructure.
- Bad, because coordination cannot be configured from YAML. Switching backends
  is a code change and a redeploy, not a configuration change.
- Bad, because assembly is now the only place the wiring is visible, so a reader
  of the configuration file cannot tell how the source coordinates.

### Confirmation

Structural: no coordination backend appears in the object-storage source's
public API or its cargo features, which is checkable by reading either.

## More information

- Landed in `84e1583` (#49).
- [ADR-0029](0029-framework-owned-coordination-driver.md) — the seam this injects
  into.
- [ADR-0032](0032-s3-always-coordinated.md) — why the in-process store is linked
  unconditionally, so a solo run needs no injection at all.
