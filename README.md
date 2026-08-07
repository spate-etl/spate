<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)"
          srcset="https://raw.githubusercontent.com/spate-etl/spate/main/website/static/img/brand/lockup-dark.png">
  <source media="(prefers-color-scheme: light)"
          srcset="https://raw.githubusercontent.com/spate-etl/spate/main/website/static/img/brand/lockup-light.png">
  <img alt="spate"
       src="https://raw.githubusercontent.com/spate-etl/spate/main/website/static/img/brand/lockup-light.png"
       width="380">
</picture>

**A high-performance, at-least-once ETL pipeline framework for Rust.**

*spate* /speɪt/ — a river in sudden flood.

[![crates.io](https://img.shields.io/crates/v/spate.svg)](https://crates.io/crates/spate)
[![CI](https://img.shields.io/github/actions/workflow/status/spate-etl/spate/ci.yml?branch=main&label=CI)](https://github.com/spate-etl/spate/actions/workflows/ci.yml?query=branch%3Amain)
[![coverage](https://img.shields.io/codecov/c/github/spate-etl/spate?branch=main)](https://app.codecov.io/gh/spate-etl/spate)
[![docs.rs](https://img.shields.io/docsrs/spate)](https://docs.rs/spate)
[![MSRV](https://img.shields.io/crates/msrv/spate)](https://blog.rust-lang.org/)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/spate-etl/spate/badge)](https://scorecard.dev/viewer/?uri=github.com/spate-etl/spate)

[Documentation](https://spate.kainth.dev/) ·
[Quickstart](https://spate.kainth.dev/docs/user-guide/getting-started/quickstart) ·
[Examples](crates/spate/examples) ·
[Changelog](CHANGELOG.md)

</div>

---

## Why Spate

Moving a stream into a warehouse usually means choosing between two shapes.
Take a general-purpose stream processor and you inherit its delivery
guarantees and its operational maturity, but your transformations are written
in whatever language that runtime accepts, and the runtime is not yours to
profile. Write the consumer loop yourself and you get the opposite trade: your
language, your allocator, your profile — and every guarantee is now your
problem, including the ones you find out about in production.

Spate is the third shape. Transformations are ordinary Rust functions,
monomorphized into the pipeline rather than interpreted by it. Delivery,
backpressure, checkpointing, rebalancing and drain-on-shutdown belong to the
framework, and the properties they hold to are written down, numbered, and
tested rather than described.

The name is the workload: more water arriving than the channel was built for.

## How it works

One process runs one pipeline, in four stages. The property each stage holds
to is stated and numbered in [docs/INVARIANTS.md](docs/INVARIANTS.md), so a claim
below is something you can go and check.

**Extract** — one consumer per process. Partitions fan out across CPU-pinned
threads as zero-copy lanes, so a record is read from the source buffer and
never copied on the way in. A thread that cannot keep up pauses its lanes and
keeps polling; it never blocks on a channel send, because a blocked poll loop
is how a consumer gets evicted from its group.

**Transform** — operators are stateful closures chained in Rust. A chain
compiles to a single loop over borrowed records with no per-record
allocation. Record-level failure is `Skip` or `Fail`, never a silent drop:
both are surfaced through metrics.

**Load** — sinks are sharded and replicated, running asynchronously on a
shared I/O runtime. The chain routes rows into bounded per-shard queues;
workers merge chunks, seal batches, rotate replicas and retry. The queue
bound is the backpressure signal that reaches all the way back to Extract.

**Observe** — a source watermark advances only behind data the sink has
acknowledged as durable, so commits trail delivery rather than leading it.
Instrumentation is built on the [`metrics`](https://crates.io/crates/metrics)
facade, so any recorder in that ecosystem works; a Prometheus scrape endpoint
and health probes ship on the admin server.

## Install

```toml
[dependencies]
spate = { version = "0.1", features = ["kafka", "clickhouse", "avro"] }
```

Nothing is enabled by default. A pipeline that only writes to ClickHouse never
compiles the Kafka tree and never resolves `rdkafka` into its lockfile.

## A taste

Operators are stateful closures composed into one monomorphized loop; YAML
carries the tuning and connector configuration. This is a whole program —
against in-memory mocks, so it needs no infrastructure to build or run:

```rust,no_run
use spate::prelude::*;
use spate_test::{TestDeserializer, TestEncoder, capture_sink, memory_source};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = PipelineConfig::from_str(
        "pipeline: { name: demo, threads: 1 }\n\
         checkpoint: { interval: 100ms }\n\
         source: { memory: {} }\n\
         sink: { capture: {} }",
    )?;
    let (source, _handle) = memory_source();
    let (sink, _script) = capture_sink(1, 1);

    let report = Pipeline::from_config(config)?
        .sink(sink)?
        .chains(|ctx| {
            // The sink's YAML `chunk:` block, bound before `with_metrics`
            // takes ownership of `ctx.pipeline`.
            let chunk_cfg = ctx.chunk();
            chain_owned::<Vec<u8>, _>(TestDeserializer::split_on(b','))
                .with_metrics(ctx.pipeline, "main")
                .filter(|word: &Vec<u8>| !word.is_empty())
                .map(|word: Vec<u8>| word.to_ascii_uppercase())
                .sink(TestEncoder, KeyHashRouter, chunk_cfg, ctx.queues, ctx.budget)
                .build()
        })
        .run(source)?;

    report.log();
    std::process::exit(report.exit_code());
}
```

Swap `memory_source()` for `KafkaSource::from_component_config` and the
capture sink for a ClickHouse one, and the chain in the middle does not
change. `run` installs signal handling and blocks until the pipeline has
drained; tests use `into_runtime` instead, which hands back a shutdown handle
so they can drive it. A version that scripts records through and asserts on
what the sink captured runs from the repository:

```sh
cargo run -p spate --example memory_pipeline
```

Start at [`crates/spate/examples`](crates/spate/examples):
`kafka_avro_to_clickhouse` is the fully-commented production assembly,
`custom_source_sink` is the connector-author tutorial, and
`s3_coordinated_backfill` runs two instances sharing one bounded backfill
without either duplicating it.
[`examples/docker`](examples/docker) covers containers and Kubernetes —
probes, drain timeouts, sizing.

## Delivery semantics, honestly

At-least-once. A batch's offsets commit only after every record derived
from it is durably written (or intentionally dropped by `filter`/`Skip`
policies) — enforced across rebalances, shutdown, and failure, where the
watermark stalls rather than ever committing past unacknowledged data.
Duplicates remain possible: in-session retries are idempotent where sinks
support it (ClickHouse deduplication tokens), but **crash replay re-batches
with new boundaries and will land rows twice** — design target tables to
tolerate that (`ReplacingMergeTree` with a version column is the sanctioned
ClickHouse pattern).

## Connectors

| Crate | Feature | Role |
|---|---|---|
| [`spate-kafka`](https://crates.io/crates/spate-kafka) | `kafka` | Kafka source and sink on `rdkafka`: one consumer per process, partitions fanned across pipeline threads as zero-copy lanes. |
| [`spate-clickhouse`](https://crates.io/crates/spate-clickhouse) | `clickhouse` | ClickHouse sink: Native or RowBinary encoded on pipeline threads, one deduplication-tokened `INSERT` per batch, replica rotation. |
| [`spate-s3`](https://crates.io/crates/spate-s3) | `s3` | Coordinated object-storage backfill source: a leader plans a prefix into splits, workers lease them with fenced progress. |
| [`spate-avro`](https://crates.io/crates/spate-avro) | `avro` | Avro deserialization: Confluent wire format, async schema-registry fetching that never blocks a pipeline thread. |
| [`spate-json`](https://crates.io/crates/spate-json) | `json` | JSON deserialization: single, NDJSON and array framings, with an optional SIMD backend. |
| [`spate-coordination`](https://crates.io/crates/spate-coordination) | `coordination` | Multi-instance work assignment: leader-computed sticky assignment over a pluggable store. |

And the framework itself:

| Crate | Role |
|---|---|
| [`spate`](https://crates.io/crates/spate) | The facade — the only crate applications depend on. |
| [`spate-core`](https://crates.io/crates/spate-core) | The engine: operator chains, source and sink abstractions, checkpointing, backpressure, config, metrics, the runtime. |
| [`spate-test`](https://crates.io/crates/spate-test) | In-memory sources and sinks with scripting handles — test your pipelines without infrastructure. |

Each connector feature turns on one crate. Finer knobs — a SIMD JSON backend,
TLS and SASL for Kafka, `chrono`/`time`/`uuid`/`rust_decimal` column types for
ClickHouse, a NATS JetStream store for coordination — are separate features,
listed with what they pull in on [docs.rs](https://docs.rs/spate). Writing your
own connector is a supported path, not a fork: see
[`custom_source_sink`](crates/spate/examples/custom_source_sink.rs).

## Performance

Single-node throughput is the point of the design, so it is measured rather
than asserted. A change that reaches Rust runs allocation assertions and
request-shape assertions, and one whose blast radius reaches a benched crate
runs instruction-count benches too. What they compare are counts rather than
elapsed time, so a regression they report is a property of the change and not
of how busy the runner was.

Wall-clock benches sit beside them as `cargo bench` targets, and nothing
gates on one: a wall-clock figure is only worth reading against another taken
on the same quiet hardware, which a shared CI runner is not.

## Testing

The guarantees above are claims, so they are tested rather than asserted.
proptest covers the checkpoint tracker, the codecs and the assignment
protocol across seven crates. loom models the tracker's concurrency
directly, which is why that module stays synchronous and free of async
runtime types. Kafka runs against librdkafka's `MockCluster` on every pull
request; brokers, ClickHouse and object stores run against real containers
whenever a change reaches them, and on a schedule regardless. The
work-assignment invariants each
[name the property test that enforces them](docs/user-guide/02-concepts/08-work-assignment.mdx).

The most useful contribution is one that proves a delivery guarantee wrong.

## Documentation

The full documentation site — the user guide plus the generated API reference —
is published at **<https://spate.kainth.dev/>** (source in
[`website/`](website), content in [`docs/`](docs)).

- [docs/INVARIANTS.md](docs/INVARIANTS.md) — the numbered properties the
  engine is arranged around.
- [docs/adr/](docs/adr/README.mdx) — one record per architectural decision,
  with the alternatives that were rejected and why.
- [docs/METRICS.md](docs/METRICS.md) — every metric, its labels, and
  alerting starting points.
- [examples/docker](examples/docker) — containers and Kubernetes.

## Status

Under active initial development — APIs are not yet stable (0.x). Breaking
changes ship in a minor bump and are called out in
[CHANGELOG.md](CHANGELOG.md). The newest `0.x` minor is the supported one.

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) has the invariants, the gates, and how
changes land; the [Code of Conduct](CODE_OF_CONDUCT.md) applies throughout.
[AI_POLICY.md](AI_POLICY.md) covers what a contribution has to withstand,
whatever wrote it.

Vulnerabilities go through
[GitHub's private advisory flow](https://github.com/spate-etl/spate/security/advisories/new),
never a public issue — see [SECURITY.md](SECURITY.md).

## License

Copyright 2026 Marcus Kainth.

Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE).

Dependency licenses are inventoried in [THIRD-PARTY.md](THIRD-PARTY.md); the full
texts are published at
[spate.kainth.dev/licenses](https://spate.kainth.dev/licenses/).

Contributions are accepted under the same terms, per Apache-2.0 §5 — there is no
CLA to sign.
