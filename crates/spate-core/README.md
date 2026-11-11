# spate-core

The engine of the [Spate](https://spate.kainth.dev/) framework: the pipeline
runtime and every abstraction that is not tied to one technology. Connector
authors build against it. Applications should depend on the
[`spate`](https://crates.io/crates/spate) facade instead, which re-exports this
crate at its root.

## Overview

- **Records and acknowledgement handles.** A `Record` carries a payload that may
  borrow the source's buffers, its metadata, and a clone of its source batch's
  `AckRef`. The batch resolves when the last clone drops.
- **Operator chains.** `ChainBuilder` composes `map`, `filter`, `try_map` and
  `flat_map` stages that compile to one loop per chain. A fallible stage takes a
  Skip or Fail `ErrorPolicy`.
- **Source, sink and format contracts.** The traits a connector implements,
  listed under [Writing a connector](#writing-a-connector).
- **Work feed.** `Feed` carries work units to a source that reads its work from
  a queue. `MemoryFeed` is the in-process feed for a single instance.
- **Checkpointing.** The `Checkpointer` advances a contiguous watermark per
  partition and hands it to `Source::commit` on an interval. A watermark never
  moves past an unacknowledged or failed batch.
- **Backpressure.** A global in-flight byte budget and a pause and resume
  controller with hysteresis. A pipeline thread asks the controller to pause its
  lanes and never blocks on a send. The controller pauses them and keeps polling
  the source.
- **Configuration.** Loads the pipeline's YAML with `${VAR}` environment
  interpolation. The framework's own sections are typed and reject unknown
  keys. Each connector's section reaches the connector as an opaque
  `ComponentConfig`.
- **Metrics and health.** Installs the exporter and registers each stage's
  metric handles before the record loop starts. A `Meter` registers a
  connector's own families with the standard labels. The admin server answers
  `/healthz` and `/readyz`, and `/metrics` when the pipeline exports metrics.
- **Pipeline runtime.** `Pipeline` assembles a process from a configuration, a
  source, chains and sinks. Driver threads run the lanes and chains, and a
  controller thread owns the source and the checkpointer. On shutdown the
  runtime drains and makes a final commit within a deadline.

## Writing a connector

A source implements `source::{Source, SourceLane}`. `Source` is the control
plane: lane assignment and revocation, commits, and pause and resume. Each
`SourceLane` runs on one pipeline thread and polls batches of payloads. A
sink implements `sink::{RowEncoder, ShardWriter}`. `RowEncoder` encodes records
on the pipeline threads, and `ShardWriter` writes one sealed batch to one
endpoint. The engine owns batching, retries, acknowledgements and backpressure
between them.

A format implements `deser::Deserializer`. A format that a streaming source
reads also implements `framing::RecordFramer`, which cuts a byte stream into
records. A feed backend implements `feed::Feed`.

[Extending](https://spate.kainth.dev/docs/user-guide/extending) walks through
each contract, and the [API reference on docs.rs](https://docs.rs/spate-core)
has the signatures.
[`custom_source_sink.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/custom_source_sink.rs)
is a compiled example of a source and a sink, and needs no infrastructure.

## Invariants

The properties the engine holds to are numbered in
[Spate invariants](https://spate.kainth.dev/docs/INVARIANTS). A change that
touches one of them cites its number and says how the property still holds.

## Stability

- Every publishable Spate crate releases at one version. The `spate` facade
  depends on exactly that version of `spate-core`.
- Before 1.0, a breaking change ships in a minor release, such as 0.4 to 0.5. A
  patch release, such as 0.4.0 to 0.4.1, does not break. Raising the minimum
  supported Rust version is a minor release too, and so is a new feature or a
  deprecation, so a patch release carries fixes. Only the newest minor release
  is supported.
- CI compares the public API with the last published release. A break fails that
  check until a changelog entry marked `**Breaking:**` announces a break for the
  coming release, and that release is then a minor one.
- [ADR-0011](https://spate.kainth.dev/docs/adr/msrv-and-dependency-policy) and
  [INV-6](https://spate.kainth.dev/docs/INVARIANTS#inv-6) limit which
  dependency types the public API can carry.
- The `testing` feature is for tests only, and it is off by default. The semver
  check does not exempt its items, so a break in them needs a `**Breaking:**`
  entry too.
- The minimum supported Rust version is 1.96.
  [Installation](https://spate.kainth.dev/docs/user-guide/getting-started/installation#toolchain-requirements)
  states the policy.
