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
here. The properties the engine is arranged around are documented under
[Invariants](https://spate.kainth.dev/docs/INVARIANTS), and the decisions
behind them under [Decisions](https://spate.kainth.dev/docs/adr/).
