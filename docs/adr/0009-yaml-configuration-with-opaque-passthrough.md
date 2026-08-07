# ADR-0009 — YAML configuration with opaque per-component sections

- **Status:** accepted
- **Date:** 2026-07-05 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A deployment needs to change broker addresses, batch sizes and credentials
without recompiling. But the framework cannot know the shape of a connector's
configuration — that is the connector's business, and a framework-owned schema
covering every connector would have to change whenever any of them did.

Separately, the YAML library situation in Rust was unsettled: `serde_yaml` is
archived, and `serde-yml`, the obvious successor by name, carries a RUSTSEC
advisory.

## Considered options

- A single typed schema covering the framework and every connector
- A typed top-level section plus **opaque** per-component sections, deserialised
  by each connector itself
- TOML or JSON instead of YAML
- Configuration in code only, with no file

## Decision outcome

Chosen option: "A typed top-level section plus opaque per-component sections",
because it puts each schema in the crate that owns it. The framework
deserialises threads, checkpointing, backpressure and metrics with
`deny_unknown_fields`, humantime durations and byte sizes; everything under a
component's key is passed through untouched to that component's factory, which
deserialises its own type. `serde_path_to_error` wraps the result so a typo
reports the offending YAML path rather than a position.

YAML over TOML because deeply nested configuration is what this is, and TOML's
table syntax makes nesting painful. Configuration-in-code-only was rejected for
the operational reason: a rebuild to change a broker address is not viable in a
container image.

The topology stays in code regardless — YAML configures connectors and tuning,
never the operator graph. That boundary is what keeps the chain monomorphised.

Library choice: **`yaml_serde`**, whose provenance was verified rather than
assumed. It is the YAML organisation's own successor, at
`github.com/yaml/yaml-serde`, published by the maintainer who co-created YAML.
Given that both obvious candidates were unusable — one archived, one with an
advisory — checking who actually publishes the replacement was the minimum due
diligence.

### Consequences

- Good, because adding a connector adds no framework configuration code, and its
  keys are validated by the crate that understands them.
- Good, because unknown keys are rejected everywhere rather than silently
  ignored, so a typo fails at startup instead of at 3am.
- Bad, because there is no single schema to validate a whole file against ahead
  of time — validation happens when each component is constructed.
- Bad, because a raw passthrough (the librdkafka property map) can express
  settings that break framework guarantees, which then needs a denylist rather
  than being impossible.

### Confirmation

`deny_unknown_fields` at every level of the typed section, and a validation
denylist for passthrough properties that would break framework guarantees — for
example `enable.auto.offset.store`, which would let the client commit offsets the
checkpointer has not authorised.

## More information

- Landed in `c8973e6`; the `yaml_serde` provenance was recorded in `2008877`
  after the library question was investigated.
- [Configuration reference](../user-guide/07-reference/configuration.mdx) — every
  framework-owned key.
