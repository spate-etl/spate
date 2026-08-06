# spate-core

The engine of the [Spate](https://github.com/spate-etl/spate)
framework: records and acknowledgement handles, the operator-chain builder,
the source/sink abstractions, checkpointing (per-partition contiguity
watermarks), backpressure, YAML configuration with opaque per-connector
sections, metrics/admin/telemetry, and the pipeline runtime (pinned driver
threads, controller, graceful drain).

Applications should depend on the [`spate`](https://crates.io/crates/spate)
facade crate instead; connector authors implement
`source::{Source, SourceLane}` and `sink::{RowEncoder, ShardWriter}` from
here. The architecture and its invariants are documented in the
repository's `docs/INVARIANTS.md` and `docs/adr/`.
