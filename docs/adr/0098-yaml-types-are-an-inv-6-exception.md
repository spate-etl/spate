---
description: "spate-core's public API carries the YAML crate's value and parse error as a second exception to the 0.x rule, so a breaking YAML release breaks spate-core."
---

# ADR-0098 — The YAML crate's value and parse error are a second exception to the 0.x rule

- **Status:** accepted
- **Date:** 2026-11-11
- **Supersedes:** [ADR-0011](0011-msrv-and-dependency-policy.md) (in part:
  naming the `metrics` facade as the only exception)
- **Superseded by:** —

## Context and problem statement

[ADR-0011](0011-msrv-and-dependency-policy.md) keeps 0.x dependency types out of
`spate-core`'s public API and out of any public trait bound. It names the
`metrics` facade as the one sanctioned exception.

Two types from `yaml_serde`, a 0.x crate, appear in `spate-core`'s public
API. The `YamlValue` re-export is its `Value`, and `ComponentConfig::new`
takes one as a component body. The `source` field of `ConfigError::Parse`
holds its `Error`. The `spate` crate re-exports both under `spate::config`.
Neither type sits in a trait bound. The alias's rustdoc called it an exemption,
but INV-6 and ADR-0011 recorded no such exception.

[ADR-0009](0009-yaml-configuration-with-opaque-passthrough.md) made the
configuration YAML, with an opaque section per component that each connector
deserializes into its own type. A component body is therefore a YAML document.
Code that builds a config without a file passes the same type the loader parses
into.

## Considered options

- Record the YAML value and parse error as a second sanctioned exception
- Replace them with a `spate-core` body type and a parse error that `spate-core`
  owns
- Read the rule as covering trait bounds only, so the YAML types need no
  exception

## Decision outcome

Chosen option: "Record the YAML value and parse error as a second sanctioned
exception", because a component body is a YAML document under ADR-0009. A
`spate-core` wrapper would be a second vocabulary for the same tree, which is
the argument [ADR-0008](0008-metrics-facade.md) makes for `metrics`.

Replacing the types would change `ComponentConfig::new` and `ConfigError::Parse`
for every caller. It would also need a new way to build a body that holds
settings. At this record's date, `yaml_serde` has published 0.10 releases only,
so the change would guard against a breaking release that has not happened.

Reading the rule as covering trait bounds only would loosen the rule that
ADR-0008 and [ADR-0089](0089-std-maps-take-seeded-fast-or-quality-aliases.md)
were written to meet. ADR-0089 wraps `foldhash` in newtypes to keep it out of
`spate-core`'s public signatures.

### Consequences

- Good, because no public signature changes, and a config built in code keeps
  the type the YAML loader produces.
- Bad, because a breaking `yaml_serde` release is a breaking release of
  `spate-core` and `spate`. Before 1.0 that costs a minor release, as any other
  break does. After 1.0 it would cost a major release, so this record is to be
  revisited before 1.0.
- Neutral, because INV-6 covers documented items, so an item marked
  `#[doc(hidden)]` is outside it. The developer guide hides the items behind
  the `testing` feature to keep them outside the semver surface, and INV-6
  gives every hidden item the same scope. `AckRef::test_pair` is a hidden test
  constructor outside `testing` that returns a `crossbeam_channel` receiver, so
  it needs no exception.

### Confirmation

Nothing yet. [INV-6](../INVARIANTS.md#inv-6) states the rule and both
exceptions. #2062 asks for a check that fails when `spate-core`'s public API
names a 0.x crate outside them.

## Evidence

At `86e1ee47`, nightly rustdoc JSON for `spate-core` listed the external crates
that public items reference or re-export with `use`. They were `bytes`,
`bytesize`, `tokio`, `url`, `serde_core`, `metrics` and `yaml_serde`, and only
`metrics` and `yaml_serde` were 0.x. With hidden items included, the list added
`crossbeam_channel`, through `AckRef::test_pair`. Hand-recorded; no committed
rig.

## More information

- Landed with the pull request that closes #1903.
- [INV-6](../INVARIANTS.md#inv-6) — the rule and its exceptions, stated once.
- [ADR-0011](0011-msrv-and-dependency-policy.md) — the dependency policy this
  record adds an exception to.
- [ADR-0008](0008-metrics-facade.md) — the first exception, for the `metrics`
  facade.
- [ADR-0009](0009-yaml-configuration-with-opaque-passthrough.md) — YAML
  configuration with opaque component sections.
