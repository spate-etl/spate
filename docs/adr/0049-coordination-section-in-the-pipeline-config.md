---
description: "A top-level coordination: section names the store and the coordinator's tuning, and a coordinated source builds its coordinator from it. Supersedes ADR-0036."
---

# ADR-0049 — The coordination store is configured in a top-level pipeline section, and the source builds its coordinator from it

- **Status:** accepted
- **Date:** 2026-09-27
- **Supersedes:** [ADR-0036](0036-coordinator-wiring-at-assembly.md)
- **Superseded by:** —

## Context and problem statement

[ADR-0036](0036-coordinator-wiring-at-assembly.md) kept the coordination
backend out of every connector's configuration and cargo features, and in
doing so made coordination code-only. The store's servers, credentials and job,
and the coordinator's lease and identity, were built in the pipeline's `main`.
A deployment could not change any of them without a rebuild, which is the cost
[ADR-0009](0009-yaml-configuration-with-opaque-passthrough.md) rejects for a
broker address. [ADR-0032](0032-s3-always-coordinated.md) states that scaling
from one instance to several is a configuration change.

The components involved are `PipelineConfig` and the `Source` trait in
`spate-core`, the coordinator in `spate-coordination`, and the object-storage
source in `spate-s3`.

## Considered options

- Keep coordination code-only, as ADR-0036 decided
- A top-level `coordination:` section that the pipeline's `main` decodes into a
  coordinator and injects with `with_coordinator`
- A top-level `coordination:` section handed to the source through a `Source`
  hook, from which the source builds its own coordinator

## Decision outcome

Chosen option: "a top-level section handed to the source through a `Source`
hook", because it moves every deployment setting into the file without
reintroducing per-connector backend configuration or features.

The section sits beside `source:`. Its `store:` key is a single-key component
selecting the backend, and every other key is `CoordinationConfig` tuning.
`spate-core` holds it opaque, as it holds a source body. The runtime calls
`Source::configure_coordination` with it before the source opens and before
any thread starts. A coordinated source decodes it there with
`spate-coordination`'s `CoordinatorSpec`, which does no I/O, and builds the
coordinator in `open` with the metrics scope the source receives there. The
default hook rejects the section, so a pipeline whose source does not
coordinate fails at startup.

Connectors still carry no backend configuration and no backend feature. Which
stores a build has is decided by `spate-coordination`'s features, and cargo
feature unification carries them to every source that calls it. A new store is
a new arm in `CoordinatorSpec` and a new crate feature; no connector changes.

ADR-0036 rejected a single framework-level backend because a deployment could
not use different backends for different sources. A pipeline has exactly one
source, so a per-file section loses nothing there.

The `main`-decodes option was rejected because every `main` would need the
lines, the coordinator would get no metrics unless that code built them, and
the framework could not tell a section that nothing read from one that was
used.

### Consequences

- Good, because a deployment changes its store, credentials, identity and
  tuning in the file, and an existing `main` needs no change to pick them up.
- Good, because the store's lease TTL and the coordinator's `lease_duration`
  are one value, so they cannot diverge.
- Good, because a coordinator built from the section gets the coordination
  metric families with the source's labels.
- Bad, because the `coordination:` keys are checked when the pipeline starts.
  `spate-core` cannot decode them, so the loader cannot check them.
- Bad, because the `Source` trait grows a method, and a coordinated custom
  source must override it to accept the section.
- Neutral, because a coordinator built in code still reaches a source through
  its own builder, and a source rejects having both.

### Confirmation

`PipelineRuntime::run` calls the hook before the admin server binds or any
thread starts, and `crates/spate-core/src/pipeline/tests.rs` pins that an
uncoordinated source fails startup with a section set. `crates/spate-s3`'s
`coordinated_pipeline` tests pin that the section builds the coordinator and
that a section plus `with_coordinator` fails startup.

## More information

- [ADR-0036](0036-coordinator-wiring-at-assembly.md) is the record this
  replaces. [ADR-0029](0029-framework-owned-coordination-driver.md)'s
  statement that backends are injected at assembly holds only for coordinators
  built in code.
- [Configuration reference](../user-guide/07-reference/configuration.mdx#coordination)
  lists the section's keys.
