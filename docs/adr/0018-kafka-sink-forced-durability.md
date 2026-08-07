# ADR-0018 — `acks=all` and idempotence are forced, and denied in the passthrough

- **Status:** accepted
- **Date:** 2026-07-13 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

[ADR-0016](0016-kafka-sink-ack-wiring.md) resolves acknowledgments from delivery
reports, and the framework commits source offsets once acknowledgments resolve.
That is only sound if a delivery report means the data is durable.

With `acks=1` a report means the leader wrote it, and a leader failover before
replication loses the data — after the framework has already committed the
offset behind it. That is silent data loss presented as at-least-once delivery.
And the Kafka connector accepts a raw librdkafka property map
([ADR-0009](0009-yaml-configuration-with-opaque-passthrough.md)), so an operator
can set `acks` themselves.

## Considered options

- Default to `acks=all` and idempotence, and let the passthrough override them
- Force both, and reject them in the passthrough including aliases
- Leave both to the operator and document the requirement

## Decision outcome

Chosen option: "Force both, and reject them in the passthrough including
aliases", because the failure mode is silent and the correct setting is not a
tuning preference — it is what makes the framework's delivery promise true.

A default that can be overridden is not enough here. The override does not fail;
it produces a pipeline that appears to work and loses data on a leader failover,
and nothing in the metrics distinguishes it from a healthy one. So the
passthrough denies `acks` and `enable.idempotence` outright, along with their
librdkafka aliases — an alias that slipped through would be an override with a
different spelling.

Documenting the requirement was rejected for the same reason the metrics
ordering hazard was made unconstructible in
[ADR-0014](0014-pipeline-builder-as-primary-assembly.md): a documented
requirement is one somebody has to have read.

### Consequences

- Good, because a delivery report means a durable write, so committing the
  offset behind it is sound.
- Good, because the denial is explicit and fails at startup with the offending
  key, rather than silently ignoring the setting.
- Bad, because an operator who genuinely wants weaker durability — a
  best-effort side-channel topic — cannot have it through this sink, and there
  is no escape hatch by design.
- Bad, because forcing idempotence constrains other producer settings
  librdkafka couples to it, so some tuning combinations become unavailable
  without an obvious explanation.

### Confirmation

The denylist is enforced at configuration load, so a denied property fails the
build with the key named. INV-1 is what it protects.

## More information

- Landed in `6af6861` (#24).
- [ADR-0016](0016-kafka-sink-ack-wiring.md) — the acknowledgment path this makes
  sound.
- [Kafka sink](../user-guide/04-connectors/sinks/kafka/README.mdx) — the
  passthrough denylist table.
