# Changelog

All notable changes are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/2.0.0/), and versions follow
[semantic versioning](https://semver.org/) — with the pre-1.0 caveat that a
breaking change ships in a **minor** bump, not a major one.

All crate versions in lockstep, so one entry covers the whole release.
There is a single supported version at a time: the newest `0.x` minor.

Entries are written by hand, one per change, as fragments under
[`changelog.d/`](changelog.d) and assembled here at release time. Commit
subjects say what changed; a release note says what it means for somebody
upgrading, and the second is what belongs here.

## [Unreleased]

## [0.1.0] — 2026-07-27

First public release.

### Added

- **The engine** (`spate-core`) — an operator chain you write in Rust that
  compiles to a single monomorphized loop over zero-copy borrowed records, on
  CPU-pinned processing threads. Checkpoint-driven source commits with
  per-partition contiguity watermarks, sharded and replicated asynchronous
  sinks, backpressure that never blocks a source poll, YAML configuration with
  opaque per-connector sections, and a Prometheus exposition with an admin
  endpoint.
- **Kafka** (`spate-kafka`) — source and producer sink on `rdkafka`. One
  consumer per process, partitions fanned across pipeline threads as zero-copy
  lanes; offsets stored as watermarks advance and committed on an interval.
  Optional TLS and SASL behind a feature that vendors OpenSSL.
- **ClickHouse** (`spate-clickhouse`) — sink with both Native and RowBinary
  encoders, one deduplication-tokened `INSERT` per batch, replica rotation, and
  shard affinity for `Distributed` tables.
- **Object storage** (`spate-s3`) — coordinated backfill source. A leader plans
  a prefix into weighted splits; workers lease them with fenced per-split
  progress, resume is drift-checked against ETag pins, and the job terminates
  itself when the plan completes.
- **Formats** — Avro (`spate-avro`, Confluent wire format with a
  non-blocking schema-registry client) and JSON (`spate-json`, single, NDJSON
  and array framings, with an optional SIMD backend).
- **Coordination** (`spate-coordination`) — leader-computed sticky assignment
  over a pluggable store, with an in-memory backend and a NATS JetStream KV
  one. Cooperative drain on revocation, forced release behind a deadline so one
  wedged drain cannot pin a rebalance open.
- **Testing** (`spate-test`) — in-memory sources and sinks with scripting
  handles, so a pipeline can be tested with no infrastructure at all.

### Delivery semantics

At-least-once. A source watermark is never committed past unacknowledged data,
including across rebalances and shutdown. Records that fail are skipped or fail
the pipeline — there is no policy that drops one without counting it.

Duplicates remain possible after a crash: replay re-batches with new
boundaries, so rows can land twice. Design target tables to tolerate that.

### Known limitations

- Pre-1.0. The API will change, and a breaking change ships in a minor bump.
- Object-storage enumeration is a single listing pass; very large prefixes
  without an inventory feed are the case to watch.
- Connector configuration structs are not yet `#[non_exhaustive]`, so adding a
  field is a breaking change until they are.

[Unreleased]: https://github.com/spate-etl/spate/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/spate-etl/spate/releases/tag/v0.1.0
