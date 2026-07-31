# Changelog

All notable changes are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/2.0.0/), and versions follow
[semantic versioning](https://semver.org/) — with the pre-1.0 caveat that a
breaking change ships in a **minor** bump, not a major one.

All nine crates version in lockstep, so one entry covers the whole release.
There is a single supported version at a time: the newest `0.x` minor.

Entries are written by hand, one per change, as fragments under
[`changelog.d/`](changelog.d) and assembled here at release time. Commit
subjects say what changed; a release note says what it means for somebody
upgrading, and the second is what belongs here.

## [Unreleased]

## [0.2.0] — 2026-07-31

### Added

- **Typed Avro datum decoding** (`spate-avro`) — `AvroDeserializerBuilder::build_datum`
  and `build_serde_datum` return an `AvroDatumDeserializer` that decodes a datum
  straight into a typed record in a single pass, rather than materialising a
  dynamic `Value` first. Reach for it when the record shape is known at compile
  time; the `Value`-based path is unchanged and remains the one to use when it is
  not. ([#31])

### Changed

- **Breaking:** **`breaker.open_for` is validated at load** (`spate-core`,
  `spate-kafka`, `spate-clickhouse`). `breaker.open_for: 0s`, and any value above a
  year, now fail startup. Both previously loaded and ran degenerately — a zero
  quarantine re-probed on the very next check — because the connectors validated
  only `failure_threshold` and `half_open_probes`. A pipeline relying on either has
  to set a real duration.

  `half_open_probes: 0` is unaffected in practice: both connectors already rejected
  it, and what changes is the framework's own normalisation of it. The rules now
  live on `BreakerConfig::validate` beside `RetryConfig::validate`, with
  `BreakerConfig::MAX_OPEN_FOR` as the bound and `BreakerConfigError` naming the
  failure — `open_for` is stamped into a deadline, and `Instant + Duration` panics
  rather than saturating, so it needed a load-time limit it had nowhere. ([#35])

### Fixed

- **A sink shard can no longer be left unwritable by a writer that panicked
  mid-probe** (`spate-core`). A replica's half-open probe budget was returned only
  by leaving `HalfOpen`, so a panic during a probe consumed the slot permanently
  and pinned the replica half-open for the life of the process — every later batch
  for that shard had nowhere to go. Picks now report whether they spent a slot, and
  the write task holds a guard that returns it if no outcome is ever reported. A
  related case let a slot released against an already-ended half-open run credit
  the current one, admitting `half_open_probes + 1` concurrent writes to the
  endpoint the breaker exists to shield; a release naming a run that has ended is
  now discarded. Both are reachable on the default `inflight.max_per_shard: 2`.
  ([#34])
- **A sink shard waiting for a circuit-breaker probe no longer consumes its retry
  ladder** (`spate-core`). With every replica half-open and its probe budget spent,
  the write loop fell back to the retry backoff — advancing the ladder while
  publishing no backoff gauge and incrementing no `spate_sink_retries_total`, so
  two batches contending for one probe ended up on different steps for reasons no
  metric explained. The wait now selects on a breaker wake alongside any real probe
  deadline. Reachable on the defaults, where `half_open_probes: 1` and
  `inflight.max_per_shard: 2` put two batches in exactly that contention. ([#34])

### Contributors

- Marcus Kainth

[#31]: https://github.com/spate-etl/spate/pull/31
[#34]: https://github.com/spate-etl/spate/pull/34
[#35]: https://github.com/spate-etl/spate/pull/35

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

[Unreleased]: https://github.com/spate-etl/spate/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/spate-etl/spate/releases/tag/v0.2.0
[0.1.0]: https://github.com/spate-etl/spate/releases/tag/v0.1.0
