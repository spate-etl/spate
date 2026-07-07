# Extending

This section is for library users building their own components: connector
authors and anyone whose pipeline needs more than the shipped Kafka, Avro,
and ClickHouse pieces. The framework's abstractions were sized for exactly
this — small, stable traits at the edges, with batching, retries,
acknowledgements, checkpointing, and backpressure all owned by the engine so
your component doesn't have to be.

The frozen v1 contracts these pages implement are specified in
[docs/DESIGN.md](../../DESIGN.md) (§ Frozen v1 contracts); exact signatures
live on [docs.rs](../07-reference/README.md). Every page mirrors a runnable
example under `crates/etl/examples/`.

## [Custom sources](custom-source.md)

Implement `Source` (the control plane: lifecycle, lane assignment events,
commits, pause/resume) and `SourceLane` (the data plane: poll borrowed
payload batches, one `AckRef` per batch). Covers the contracts that keep
sources live under backpressure and correct across rebalances.

## [Custom sinks](custom-sink.md)

Implement `RowEncoder` (the CPU half: record to wire bytes, on pipeline
threads) and `ShardWriter` (the I/O half: sealed batch to replica endpoint,
on sink workers), then hand them to the builder as `SinkParts` — no trait
impl required for hand-rolled sinks.

## [Custom operators](custom-operators.md)

Operators are stateful closures over `map`/`filter`/`try_map`/`flat_map` —
no trait to implement, and the whole chain still compiles to one
monomorphized loop. Covers per-stage error policies, zero-allocation
fan-out, and when to drop down to the `Collector`/`StageLifecycle` traits.

## Prerequisites

You'll want the [Concepts](../02-concepts/README.md) section first —
especially [Architecture](../02-concepts/01-architecture.md) (which half of
the process your code runs on) and
[Delivery guarantees](../02-concepts/02-delivery-guarantees.md) (what the
ack machinery expects of you). For wiring a finished component into a
pipeline, see [Assembling a pipeline](../03-guides/assembling-a-pipeline.md)
and [Manual assembly](../03-guides/manual-assembly.md); for testing it,
[Testing pipelines](../03-guides/testing-pipelines.md).
