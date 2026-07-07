# Getting started

Three on-ramps, in increasing order of infrastructure. Pick the one that
matches what you have in front of you right now.

## [1. Installation](01-installation.md)

Add the `etl` facade crate to your project, choose connector features, and
check the toolchain requirements (MSRV 1.94, edition 2024). Two minutes.

## [2. Quickstart](02-quickstart.md)

Run a complete pipeline — source, deserializer, operator chain, sharded
sink, checkpointed commits — entirely in memory. No Kafka, no ClickHouse,
no Docker. This is the fastest way to see the assembly pattern every real
pipeline follows, and it doubles as the pattern for
[testing your own pipelines](../03-guides/testing-pipelines.md).

## [3. Your first pipeline](03-first-pipeline.md)

The flagship shape end to end: Kafka → Confluent-framed Avro → an operator
chain → sharded ClickHouse, configured by YAML, drained gracefully on
SIGTERM. Requires Kafka, a schema registry, and ClickHouse (or Docker to
run them).

## After this section

- The mental model behind what you just ran: [Concepts](../02-concepts/README.md),
  starting with [Architecture](../02-concepts/01-architecture.md).
- Every builder step in depth:
  [Assembling a pipeline](../03-guides/assembling-a-pipeline.md).
- Connector-specific configuration: [Connectors](../04-connectors/README.md).
