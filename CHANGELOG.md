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

## [0.2.0] — 2026-08-22

### Added

- **A deadline for a member that never gets its partitions back**
  (`spate-kafka`) — the Kafka source's `assignment_timeout` (default `5m`, `0s`
  disables it) reports a fatal error once the member has held no partitions for
  that long, so a pipeline whose group stops re-forming exits and is restarted
  with a fresh consumer instead of idling at zero ingest while lag climbs. The
  deadline counts from the moment ownership is released, and any accepted
  assignment clears it, an empty one included, so a member in a group with more
  members than partitions keeps running. `startup_timeout` still covers the
  window before the first assignment.
  ([#291])
- **Typed Avro datum decoding** (`spate-avro`) — `AvroDeserializerBuilder::build_datum`
  and `build_serde_datum` return an `AvroDatumDeserializer` that decodes a datum
  straight into a typed record in a single pass, rather than materializing a
  dynamic `Value` first. Reach for it when the record shape is known at compile
  time; the `Value`-based path is unchanged and remains the one to use when it is
  not. ([#31])
- **Synthetic data source** (`spate-datagen`, `spate` feature `datagen`) — a
  built-in storefront dataset of orders, payments and refunds, so a pipeline runs
  with no broker, bucket or coordination store. Each lane owns a disjoint slice
  of the order-id space and its own bounded ring of open orders, so a payment
  always references an order on the same partition at a lower offset with no
  coordination on the record path. `count` drains the pipeline to a clean exit;
  `tick_interval: 0s` runs unthrottled. It keeps no durable progress — a restart
  regenerates the whole stream — and is a demo and test source, not a production
  one.
  ([#146])
- **Nameable `map_rec` bounds** (`spate-core`) — `MapFn` and `TryMapFn`, the two
  traits that carry the bound on `ChainBuilder::map_rec` and `try_map_rec`, are
  exported from `spate_core::ops` (`spate::ops` on the facade). A record family
  that borrows the source buffer transforms through those two methods. Naming
  the bound is what a helper generic over one stage needs.
  ([#272])

### Changed

- **Breaking:** **The admin server has its own `admin:` section** (`spate-core`)
  — `metrics.listen` moves to `admin.listen`, and `MetricsSettings::listen` is
  gone. One server carries `/metrics`, `/healthz` and `/readyz`, so its address
  belongs beside neither the exporter that supplies one of them nor the probes
  that need no exporter at all. `admin: { listen: none }` is new and runs no
  server, which is what lets two pipelines share a host without one of them
  naming a port it never wanted. A file still carrying `metrics.listen` is
  rejected at load, naming the key.

  Three further breaks for anyone below the YAML layer: `PipelineConfig` gains a
  public `admin` field, so an exhaustive struct literal needs `..Default::default()`
  or the new field; `AdminServer::bind` takes `Option<RenderFn>`, where `None`
  serves the probes without `/metrics`; and `MetricsHandle::exports()` is new,
  reporting whether a handle has an exposition to render.
  ([#191])
- **Breaking:** **`apache-avro` moves to 0.22** (`spate-avro`, `spate-datagen`).
  The schema parser is stricter — duplicate union members, duplicate field names,
  duplicate enum symbols, a `record`/`enum`/`fixed` used as a type name, and an
  out-of-range decimal precision are all rejected where 0.21 accepted them — so a
  schema running today can stop compiling on upgrade, surfacing as
  `SchemaUnavailable` per record for a registry id and at build time for a fixed
  schema.

  A `uuid` logical type now carries its backing. A 16-byte `fixed` backing
  decoded to nothing usable and decodes correctly now; a `bytes` backing was not
  recognized as a `uuid` at all, so such a field moves from `AvroValue::Bytes` to
  `AvroValue::Uuid` and a borrowed `&[u8]` target for it stops decoding.
  `AvroValue` is a re-export of `apache_avro::types::Value`, so the
  dynamically-typed path takes that crate's own breaking changes with this bump;
  the typed paths do not.
  ([#248])
- **Writer-schema resolution held across payloads** (`spate-avro`) — `build_value()`
  and `build_serde::<T>()` rebuild the schema's named-type lookup once per writer
  schema id instead of once per record, keeping a reader per id and reusing it. On
  a cross-framework rig whose payloads each carry one record, that is worth 6.9% of
  end-to-end throughput and 4.8% of per-row CPU. A payload carrying a batch
  amortizes the work across its rows already and does not move. Each chain lane
  keeps up to 64 readers and displaces one to admit another, so a stream carrying
  more schema ids than that still decodes and still costs bounded memory.

  `build_datum()` and `build_serde_datum::<T>()` never went through that decoder
  and are unchanged — they remain the throughput path by a wide margin, and are
  still the answer if Avro decode is what bounds your pipeline.
  ([#253])
- **Breaking:** **`breaker.open_for` is validated at load** (`spate-core`,
  `spate-kafka`, `spate-clickhouse`). `breaker.open_for: 0s`, and any value above a
  year, now fail startup. Both previously loaded and ran degenerately — a zero
  quarantine re-probed on the very next check — because the connectors validated
  only `failure_threshold` and `half_open_probes`. A pipeline relying on either has
  to set a real duration.

  `half_open_probes: 0` is unaffected in practice: both connectors already rejected
  it, and what changes is the framework's own normalization of it. The rules now
  live on `BreakerConfig::validate` beside `RetryConfig::validate`, with
  `BreakerConfig::MAX_OPEN_FOR` as the bound and `BreakerConfigError` naming the
  failure — `open_for` is stamped into a deadline, and `Instant + Duration` panics
  rather than saturating, so it needed a load-time limit it had nowhere. ([#35])
- **Breaking:** **The coordination-seam example is `custom_coordinated_source`**
  (`spate`) — the `coordinated_pipeline` example target is gone; run it as
  `cargo run -p spate --features coordination --example custom_coordinated_source`.
  The name says what the example is for: writing a coordination-aware source from
  scratch, as distinct from `s3_coordinated_backfill`, which consumes coordination
  through a connector that already implements it.
  ([`d3eb4a9`](https://github.com/spate-etl/spate/commit/d3eb4a9b76950ccd5c61b12bc6cf3a83bd7a793c))
- **Breaking:** **The JSON example is `json_skip_bad_records`** (`spate`) — the
  `json_ndjson_memory` example target is gone; run it as
  `cargo run -p spate --features json --example json_skip_bad_records`. The name
  says what the example teaches — a malformed record is skipped and counted in
  `spate_json_deser_records_dropped_total` rather than stopping the pipeline —
  instead of the NDJSON-in-memory plumbing it happens to use to show it.
  ([`4338fc5`](https://github.com/spate-etl/spate/commit/4338fc507ed235228d9f3fc517a50caa326f6769))
- **Breaking:** **Connector, sink-pool and coordination configuration structs are
  `#[non_exhaustive]`** (`spate-core`, `spate-kafka`, `spate-clickhouse`,
  `spate-s3`, `spate-coordination`) — from outside the defining crate a struct
  literal, a functional update (`..Default::default()`) and an exhaustive pattern
  all stop compiling. Programmatic assembly starts from `new` where a field has no
  default (`KafkaSourceConfig`, `KafkaSinkConfig`, `ClickHouseSinkConfig`,
  `ShardConfig`, `DistributedCheckSection`, `S3SourceConfig`, and `SinkPoolConfig`
  over its four sections) and from `default()` where every field has one
  (`BatchConfig`, `InflightConfig`, `RetryConfig`, `BreakerConfig`,
  `TimeoutSection`, `CoordinationConfig`), then assigns the fields it wants, which
  stay public. Both sink `Compression` enums, and ClickHouse's `Format` and
  `SchemaValidation`, are sealed too, so a `match` over one needs a wildcard arm.
  Loading a pipeline from YAML is unaffected, and a knob or a codec added after
  this ships in an additive release.
  ([#266])
- **Breaking:** **The framework configuration sections are `#[non_exhaustive]`**
  (`spate-core`) — `PipelineConfig`, `PipelineSection`, `CheckpointSection`,
  `BackpressureSection`, `AdminSection` and `MetricsSection` join the connector
  configs sealed before them, so from outside the crate a struct literal, a
  functional update (`..Default::default()`) and an exhaustive pattern all stop
  compiling. A config built in code starts from `PipelineConfig::new(pipeline,
  source, sink)`, or `PipelineConfig::new_multi_sink(pipeline, source, sinks)`
  for a `sinks:` map, with the `pipeline` section from
  `PipelineSection::new(name)`, the four optional sections from `default()` and a
  deserializer from `PipelineConfig::with_deserializer`, then assigns the fields
  it wants, which stay public. Each of those entry points tags the component it
  takes with its section, so a bad connector body reports the same
  `source.<type>.<key>` error path it reports when the config was loaded from
  YAML. Loading a pipeline from YAML is unaffected, and a section or a key added
  after this ships arrives in an additive release.
  ([#284])
- **Breaking:** **`startup_timeout: 0s` disables the startup deadline**
  (`spate-kafka`) — a zero there leaves the Kafka source waiting for its first
  partition assignment for as long as the group takes, which is what
  `assignment_timeout: 0s` already means for a member that loses one. It
  previously failed the pipeline on the first poll, before any broker could
  answer. The two deadlines are one mechanism with two windows: the wait runs
  from `open` under `startup_timeout`, and from every later loss of the last
  partitions under `assignment_timeout`.
  ([#292])

### Fixed

- **`metrics.exporter: none` no longer takes a port it cannot use**
  (`spate-core`) — the admin server was bound unconditionally, so a pipeline
  configured for no metrics still occupied `0.0.0.0:9090`, and a second one on
  the same host failed to start with an I/O error rather than a message about
  the address. The bind follows `admin.listen`, and a failure to take an address
  reports that address as `StartError::AdminBind`.

  `/metrics` answers 404 where the pipeline has no exposition of its own to
  render, rather than 200 with an empty body — which reads to a scraper as a
  healthy target delivering nothing. That covers `exporter: none` and equally a
  recorder another library installed first, where the pipeline records into that
  recorder and cannot render it.
  ([#191])
- **An out-of-range narrowing Avro promotion is rejected instead of truncating
  silently** (`spate-avro`). A `reader_schema` declaring `int` for a field a
  writer wrote as `long` used to wrap the value: a quantity of `5_000_000_000`
  was delivered as `705_032_704`, with no error, nothing logged and no metric
  moved. The record now fails to decode and takes the deserializer's
  `ErrorPolicy` — dropped and counted on
  `spate_deser_records_dropped_total{reason="skip_policy"}` under `Skip`, fatal
  under `Fail`. A pipeline quietly writing wrong numbers starts reporting them
  instead.

  Only the range is checked, not the direction: a `long` that fits the reader's
  `int` still resolves, `double`→`float` is not checked at all and saturates to
  infinity, and `long`→`float`/`double` loses precision. The Avro connector page
  carries the table.
  ([#248])
- **A sink shard waiting for a circuit-breaker probe no longer consumes its retry
  ladder** (`spate-core`). With every replica half-open and its probe budget spent,
  the write loop fell back to the retry backoff — advancing the ladder while
  publishing no backoff gauge and incrementing no `spate_sink_retries_total`, so
  two batches contending for one probe ended up on different steps for reasons no
  metric explained. The wait now selects on a breaker wake alongside any real probe
  deadline. Reachable on the defaults, where `half_open_probes: 1` and
  `inflight.max_per_shard: 2` put two batches in exactly that contention. ([#34])
- **Coordinator links count toward broker health** (`spate-kafka`) — the
  `spate_kafka_source_broker_up` and `spate_kafka_sink_broker_up` gauges report a
  broker as up while any connection to it is up, including a logical coordinator
  link. A broker serving only as group coordinator previously rendered 0 for the
  life of the process — even while commits flowed through it — because the client
  never reopens a regular connection it has no fetch-reason for, so brokers-up
  panels undercounted after any coordinator-broker outage. ([#197])
- **The `clickhouse_aggregating_mv` example builds on default features**
  (`spate`) — it needs the `clickhouse` feature but declared no
  `required-features`, so `cargo build --examples` against a default-feature
  checkout failed to resolve `spate::clickhouse`. The example now carries a
  `[[example]]` entry gating it, and is built when the feature is enabled rather
  than always attempted.
  ([#137])
- **A peer joining says so, rather than reading as a fault**
  (`spate-coordination`) — an instance's startup probe writes and deletes a key
  in the durable keyspace every peer is already watching, and each of those
  deletes was reported as `durable record deleted externally`, the wording
  reserved for a record vanishing from under the coordinator. The probe key is
  recognized for what it is; a durable record deleted by something outside the
  process still warns.

  Fleet membership is on the log as well as in
  `spate_coordination_live_workers`: `peer joined` and `peer left` on every
  worker with the resulting live count, and `assignment published` on whichever
  worker holds leadership, once per rebalance rather than once per completed
  split. `split claimed`, `drain started` and `drain finished` sit a level below
  at `RUST_LOG=info,spate_coordination=debug`, and `spate_core::telemetry`
  states the level convention the rest of the framework is written to.
  ([#192])
- **A refused resume hands the split back** (`spate-core`) — a coordinated
  source that rejected carried progress from `SplitSource::validate_resume` had
  its gain discarded while the backend kept the lease renewed, so the split was
  held by an instance that never read it and never handed it back. A bounded job
  missing a split that way reaches neither `AllComplete` nor `Stalled`: it waits
  on work nobody is doing. The driver reports a rejected gain to the coordinator
  as poison, which consumes one delivery attempt, releases the lease for a peer,
  and quarantines the split at the attempt cap; a report the backend refuses is
  re-offered until it lands, which `spate_coordination_split_failures_total`
  counts. A rejection also stops discarding the coordination events polled
  alongside it, and a fatal rejection is no longer masked by a retryable one
  raised earlier in the same batch.

  Class a rejection `ErrorClass::Fatal` to end the run, as before. Any other
  class leaves the split to the coordinator and the pipeline keeps going, which
  is what that class always claimed to do.
  ([#193])
- **`validate_schema: full` checks a decimal wrapper's scale** (`spate-clickhouse`)
  — a `Decimal64<4>` field against a `Decimal(18, 2)` column was accepted, because
  the check compared only the wrapper's integer width against the column's
  precision. The widths agree, the insert succeeds, and every row lands 100× too
  large. The decimal wrappers now serialize under a name carrying their scale, so
  a disagreement fails fatally on the first record, before anything is inserted. A
  plain integer field declares no scale and still passes any decimal column of the
  matching width.
  ([#277])
- **`cargo test` compiles without `--all-features`** (`spate`) — the
  `e2e_drain_outage` target was undeclared, so it carried no `required-features`
  and failed on `spate::avro`, `spate::clickhouse` and `spate::kafka` under any
  smaller feature set. Its stanza declares `full`.
  ([#236])
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
- **A flattened `Nested` column can be written** (`spate-clickhouse`) — under
  ClickHouse's default `flatten_nested = 1` a `Nested` column is stored as
  parallel `outer.inner` columns, and naming one in `columns:` failed the sink's
  identifier check, which rejected any name containing a dot. There was no way
  around it, so a table with a `Nested` column could not be written in either
  format. A column name may now be a dotted path of identifiers. The check is
  otherwise unchanged: a backtick, a space and everything else are still
  rejected, so the name cannot escape its backtick quoting.
  ([#278])
- **`checkpoint.max_pending_batches` is a hard bound** (`spate-core`) — the
  per-partition pending-batch limit is enforced at the poll boundary: a
  partition at the ceiling has its lanes skipped until acknowledgments retire
  batches, so `spate_checkpoint_pending_batches` can no longer overshoot the
  configured value. Enforcement previously ran only on the commit tick, letting
  pending exceed the limit by fetch-rate × `checkpoint.interval` per partition —
  orders of magnitude under a post-rebalance replay. The ceiling is now
  per-partition in effect as well as in name: one partition at its limit no
  longer pauses its siblings, and a partition stalled behind a failed batch
  accumulates at most the configured replay before `stalled_fail_after` fails
  the pipeline. ([#200])
- **Rebalance errors no longer wedge the consumer** (`spate-kafka`) — a rebalance
  event that is neither an assignment nor a revocation is answered with
  `unassign`, as the client library's callback contract requires, instead of only
  being recorded. A member that received one previously stayed mid-rebalance for
  the life of the process — no rejoin, no fresh assignment, commits fenced —
  until a restart. The error is now classified like every other consumer error,
  so an authorization failure fails the pipeline fast while transient rebalance
  codes retry, and the affected lanes are drained through the ordinary
  revocation choreography first; their uncommitted work replays. A warning is
  also logged when rebalance events queue faster than they complete. ([#198])

### Contributors

- Marcus Kainth

[#31]: https://github.com/spate-etl/spate/pull/31
[#34]: https://github.com/spate-etl/spate/pull/34
[#35]: https://github.com/spate-etl/spate/pull/35
[#137]: https://github.com/spate-etl/spate/pull/137
[#146]: https://github.com/spate-etl/spate/pull/146
[#191]: https://github.com/spate-etl/spate/pull/191
[#192]: https://github.com/spate-etl/spate/pull/192
[#193]: https://github.com/spate-etl/spate/pull/193
[#197]: https://github.com/spate-etl/spate/pull/197
[#198]: https://github.com/spate-etl/spate/pull/198
[#200]: https://github.com/spate-etl/spate/pull/200
[#236]: https://github.com/spate-etl/spate/pull/236
[#248]: https://github.com/spate-etl/spate/pull/248
[#253]: https://github.com/spate-etl/spate/pull/253
[#266]: https://github.com/spate-etl/spate/pull/266
[#272]: https://github.com/spate-etl/spate/pull/272
[#277]: https://github.com/spate-etl/spate/pull/277
[#278]: https://github.com/spate-etl/spate/pull/278
[#284]: https://github.com/spate-etl/spate/pull/284
[#291]: https://github.com/spate-etl/spate/pull/291
[#292]: https://github.com/spate-etl/spate/pull/292

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
