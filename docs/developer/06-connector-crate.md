---
description: "Conventions for a shipped connector's public surface, and what its crate carries: manifest, tests, container suites, benches, fuzz targets and docs page."
---

# Adding a connector crate

A connector crate ships a source, a sink, a format or a feed backend, or several of them, behind a feature of the `spate` crate.
The first part of this page gives the conventions its public surface follows, so a user meets the same shapes in every connector.
The second part covers what the crate carries beyond its `src/`.

Each convention states what to do, then why, then code that follows it where some does, then what fails when a change breaks it.
"Review" there means no test or lint checks it, so a reviewer does.

Some shipped code departs from a convention.
The convention then lists the departures under **Departs:**, with the issue that brings them in line.
New code follows the convention, never the departure.

These conventions are for the connectors this repository ships.
A user writing a connector in their own application needs only the contracts in [Extending](../user-guide/06-extending/README.mdx).

## Shape its public surface

### Name the role and its config for the connector

Name the role type `<Vendor><Role>` and its config `<Vendor><Role>Config`, such as `KafkaSource` and `KafkaSourceConfig`, or `ClickHouseSink` and `ClickHouseSinkConfig`.

**Why:** an application imports from several connectors, and the vendor in each name keeps the imports apart.
The name also stays clear of the core trait it implements, such as `Source` or `Feed`.

**Follow:** [`crates/spate-kafka/src/lib.rs`](repo:crates/spate-kafka/src/lib.rs).

**Departs:** the feed backends' config types are private ([#2319](https://github.com/spate-etl/spate/issues/2319)).

**Checked by:** review.

### Name every exported config type for the connector

Start the name of every public config type a connector exports with the connector's name, nested types included, such as `JsonFraming` and `AvroMode`.
The rule covers the types a connector declares.

**Why:** two connectors can then both export a `Compression` or a `Format` and an application that imports from both needs no aliases.
A datagen `Clock` would otherwise share its name with core's `Clock` trait.

**Follow:** `JsonFraming` in [`crates/spate-json/src/config.rs`](repo:crates/spate-json/src/config.rs).

**Departs:** `Format`, `SchemaSource`, `Dataset`, `Encoding`, `Clock`, the three `Compression` enums, `RegistrySection`, `DistributedCheckSection`, `ShardConfig` and `TimeoutSection` ([#2330](https://github.com/spate-etl/spate/issues/2330)).

**Checked by:** review.

### Name each type a role uses for the trait it implements

Name an encoder, writer, lane, batch, framer or router `<Vendor><Variant><Noun>`, where the noun is the last word of the trait it implements, such as `ClickHouseFormatEncoder` for a `RowEncoder` or `S3Lane` for a `SourceLane`.
The variant names what sets the type apart, such as its format or its shard placement, as in `ClickHouseDistributedRouter`.
A connector with one implementor of a trait may leave the variant out, as `KafkaWriter` does.
A role type keeps its `<Vendor><Role>` name, so `ClickHouseSink` stays as it is although it implements `SinkBundle`.

**Why:** the vendor keeps one connector's types apart from another's in an application's imports.
The variant tells two implementors of one trait apart, as `KafkaJsonEncoder` and `KafkaBytesEncoder` are told apart.
The trait's last word keeps the name short, and it matches how the workspace already names these types.

**Follow:** [`crates/spate-kafka/src/sink/encoder.rs`](repo:crates/spate-kafka/src/sink/encoder.rs).

**Checked by:** review.

### Suffix a config `Config` and a nested block `Section`

Name the type a component is built from `*Config`, and a block nested inside a YAML section `*Section`, such as core's `CheckpointSection`.

**Why:** a reader then knows from the name whether a type builds a component or is one part of another config.

**Follow:** `CheckpointSection` in [`crates/spate-core/src/config/mod.rs`](repo:crates/spate-core/src/config/mod.rs).

**Departs:** nested blocks named `*Config`, such as ClickHouse's `ShardConfig` ([#2330](https://github.com/spate-etl/spate/issues/2330), [#1604](https://github.com/spate-etl/spate/issues/1604)).

**Checked by:** review.

### Export every type a role's user names from the crate root

Export from the crate root every public type a user of a role names: the role type, its config, every nested config type, and any type a constructor returns.
The spate-s3 crate exports `S3Source`, `S3SourceConfig`, `S3Lane` and `S3Batch` from its root.

**Why:** a user then reaches every type a connector needs through one path, such as `spate::s3::`.
The connector's name on each nested type keeps two roots from colliding.

**Follow:** [`crates/spate-s3/src/lib.rs`](repo:crates/spate-s3/src/lib.rs).

**Departs:** `KafkaFeed`, `KafkaFeedPublisher`, `KafkaEndpoint`, Kafka's sink `Compression`, and ClickHouse's `ShardConfig` and `TimeoutSection`, each reachable only through a submodule ([#1603](https://github.com/spate-etl/spate/issues/1603)).

**Checked by:** review.

### Build each role through a fallible `new` and `from_component_config`

Give each role type two constructors.
The `new` constructor takes its config, calls the config's public `validate`, and returns `Result<Self, ConfigError>`.
The `from_component_config` constructor takes the role's YAML section, parses the config and goes through the same checks.
Both also take any runtime handle the role needs, as `S3Source::new` takes the I/O runtime's handle.

A role that needs I/O before it can run, such as the ClickHouse sink reading its table schema, builds in two steps.
Its two constructors return a builder, and the builder's `connect` does the I/O and returns the role, as `ClickHouseSinkBuilder::connect` does.

**Why:** an application that builds a pipeline in code and one that reads YAML reach the same type and the same checks.
An invalid config fails at construction, as the `ConfigError` the rest of the configuration returns, and never later as text inside a start error.

**Follow:** [`crates/spate-kafka/src/sink/config.rs`](repo:crates/spate-kafka/src/sink/config.rs).

**Departs:** the feed backends have no public config type ([#2319](https://github.com/spate-etl/spate/issues/2319)).

**Checked by:** review.

### Name a method `with_*` only when it returns `Self`

Name a method `with_<noun>` only when it returns or builds `Self`.
Give a step that does I/O or returns another type a verb, such as `connect`.
Pass a value the role cannot run without to `new`, never to a `with_*` setter.

**Why:** a reader then knows from the name what comes back, and a role cannot be built without a part it needs.

**Follow:** `PipelineRuntime::with_feed` in [`crates/spate-core/src/pipeline/runtime.rs`](repo:crates/spate-core/src/pipeline/runtime.rs).

**Departs:** `with_*` constructors that take no `self` ([#1607](https://github.com/spate-etl/spate/issues/1607)).

**Checked by:** review.

### Give each role's errors one shape

Report a role's run-time failure through that role's error type, and give every role's type the same methods: `kind()`, `class()`, `reason()`, its constructors, and `source()` holding the client's error.
Mark every public error type `#[non_exhaustive]`.

A constructor that does no I/O returns `ConfigError`.
One that does I/O, such as a feed's `connect`, returns core's `ConnectError`.

**Why:** a caller then asks "retry or stop?" the same way of every role, and the client's error survives for logging.
A meaning that belongs to one role, such as `NotReady` on a deserializer, stays on that role.

**Departs:** core's role error types have other shapes, and the ClickHouse sink returns its own `SchemaError` and `DistributedCheckError` ([#2318](https://github.com/spate-etl/spate/issues/2318)).
The `FatalError` type is not `#[non_exhaustive]` ([#1605](https://github.com/spate-etl/spate/issues/1605)).
Until core's types change, a connector uses them as they are.

**Checked by:** review.

### Refuse unknown YAML keys

Put `#[serde(deny_unknown_fields)]` on every config struct and every nested section.

**Why:** a misspelt key then fails at startup and names itself.
Without the attribute, serde drops the key and the default applies with no warning.

**Follow:** [`crates/spate-kafka/src/config.rs`](repo:crates/spate-kafka/src/config.rs).

**Checked by:** review.

### Mark public config types `#[non_exhaustive]`

Mark every public config struct and enum `#[non_exhaustive]`.

**Why:** a later release can then add a key or a variant without breaking an application, because the application builds a config through `new` or `Default` and cannot match every variant.

**Follow:** [`crates/spate-datagen/src/config.rs`](repo:crates/spate-datagen/src/config.rs).

**Checked by:** review.
The `cargo xtask semver-checks` command fails the later change that adds a field to a struct without the attribute.

### Read durations with `humantime_serde`, in flat `_timeout` keys

Deserialize every `Duration` field with `humantime_serde`, so the YAML value reads `30s` or `500ms`.
Name a timeout `<what>_timeout` at the level of the section it belongs to.
When a timeout can be turned off, let `0s` turn it off.

Some timeouts must stay bounded, such as the Kafka sink's `delivery_timeout`, which refuses `0s` because librdkafka reads zero as no timeout.

**Why:** every duration key in every connector then takes the same syntax, and a timeout that can be off is turned off the same way everywhere.

**Follow:** `startup_timeout` in [`crates/spate-kafka/src/config.rs`](repo:crates/spate-kafka/src/config.rs).

**Departs:** the ClickHouse sink's nested `timeouts:` block, where null turns a timeout off ([#1604](https://github.com/spate-etl/spate/issues/1604)).

**Checked by:** review.

### Hold byte sizes in `ByteSize`

Give every byte-size field the type `ByteSize`, so the YAML value reads `1MiB` and the Rust field keeps its unit.

**Why:** a byte size then has one type beside its neighbors in every config.

**Follow:** `max_message_bytes` in [`crates/spate-kafka/src/sink/config.rs`](repo:crates/spate-kafka/src/sink/config.rs).

**Departs:** `BatchConfig::max_bytes`, a `u64` ([#1604](https://github.com/spate-etl/spate/issues/1604)).

**Checked by:** review.

### Spell YAML enum values in snake_case, and the off value `none`

Put `#[serde(rename_all = "snake_case")]` on every config enum.
Spell the value that turns a feature off `none`.

The ClickHouse format value `rowbinary` is the one exception.
Under `snake_case` it would read `row_binary`, so give that variant an explicit rename that keeps `rowbinary`.

**Why:** a key and its value then read in one case, and a user turns any feature off with the same word.

**Follow:** `PinningMode` in [`crates/spate-core/src/config/mod.rs`](repo:crates/spate-core/src/config/mod.rs) for the case, and spate-s3's `Compression` in [`crates/spate-s3/src/config.rs`](repo:crates/spate-s3/src/config.rs) for `none`.

**Departs:** enums renamed `lowercase` or `kebab-case` in spate-clickhouse, spate-datagen, spate-kafka, spate-s3 and core's chunk config, and the off values `pinning: off` and the ClickHouse `compression` key's `off` alias ([#2324](https://github.com/spate-etl/spate/issues/2324)).

**Checked by:** review.

### Name the client passthrough map for the system that defines its keys

Name a map of client settings passed through unread for the system whose documentation defines its keys: `rdkafka` for librdkafka properties, `store` for object_store options, `settings` for ClickHouse query settings.

**Why:** the key name then tells a user where to look up what goes in the map.

**Follow:** [`crates/spate-kafka/src/config.rs`](repo:crates/spate-kafka/src/config.rs).

**Checked by:** review.

### Take trust roots in core's typed `tls:` block

Give a connector that opens TLS connections core's typed `tls:` section, and map it onto the client.
When the connector also has a passthrough map, refuse a passthrough key that sets the same thing.

**Why:** a user then sets a private root certificate the same way in every connector, documented once.

**Follow:** [`crates/spate-sqs/src/config.rs`](repo:crates/spate-sqs/src/config.rs).

**Departs:** the Kafka connectors, which take trust roots only through `ssl.*` keys in the `rdkafka` map ([#2322](https://github.com/spate-etl/spate/issues/2322)).

**Checked by:** review.

### Leave the framework's keys to core

Declare in a connector's config only the keys its own code reads.
Keys the framework reads for every backend of a role sit beside the backend's key in the role's section, as `units_held` does in a `feed:` section.
A sink leaves out `batch`, `inflight`, `retry` and `breaker` and passes a default pool in its `SinkParts`, which the sink section's blocks overlay.

**Why:** core then parses each framework key once, and every backend accepts the same ones.

**Follow:** [`crates/spate-core/src/config/feed.rs`](repo:crates/spate-core/src/config/feed.rs), and [`crates/spate-core/src/config/sink.rs`](repo:crates/spate-core/src/config/sink.rs) for a sink section.

**Checked by:** review.

### Name a feature for the dependency it enables

Name a crate feature that exists to turn on one optional dependency after that dependency, such as `rust_decimal`.
Name any other feature in kebab-case.
The `spate` crate exposes a connector's feature as `<connector>-<name>`, such as `clickhouse-rust-decimal`.

A feature makes an optional backend available.
When the backend is faster for some data and slower for other data, each component chooses it, as the Kafka encoder does with `with_simd_json()`.

**Why:** a user turning on a dependency already knows its name, and a choice that depends on the data sits where the data is known.

**Follow:** [`crates/spate-clickhouse/Cargo.toml`](repo:crates/spate-clickhouse/Cargo.toml).

**Departs:** spate-json's `simd` feature, which switches the backend for the whole build, and spate-kafka's `json-simd` ([#2321](https://github.com/spate-etl/spate/issues/2321)), and spate-datagen's `avro`, which turns on `apache-avro` ([#1603](https://github.com/spate-etl/spate/issues/1603)).

**Checked by:** review.

### Name its metrics

Name each metric family and label key the crate registers by [Instrumenting connectors § Contracts](../user-guide/06-extending/instrumenting-connectors.mdx#contracts).
The framework's own names follow the same rules.

**Why:** a dashboard or alert written against one connector's families then reads the same way against another's.

**Follow:** [`crates/spate-json/src/metrics.rs`](repo:crates/spate-json/src/metrics.rs).

**Checked by:** review.
The test `names_follow_prometheus_conventions` in [`crates/spate-core/src/metrics/names.rs`](repo:crates/spate-core/src/metrics/names.rs) holds the framework's own names to the suffix rules, and reads no connector's names.

### Resolve metric handles in `attach_metrics`

Resolve a component's metric handles in its `attach_metrics` hook, from the context the framework passes.

**Why:** every role then receives its `Meter` the same way.

**Follow:** [`crates/spate-json/src/deser.rs`](repo:crates/spate-json/src/deser.rs).

**Departs:** a source reads its `Meter` from `SourceCtx` in `open`, because `Source` has no `attach_metrics` ([#2320](https://github.com/spate-etl/spate/issues/2320)).
Until it does, a new source reads it from `SourceCtx`.

**Checked by:** review.

### Hold its metric handles in one struct

Hold the handles each component registers in one struct, shaped like its peers:

- Declare each metric name and each label key the crate adds as a constant in the struct's module.
- Keep the handle fields private.
  Callers outside that module record through one function per event, such as `JsonDeserMetrics::dropped_malformed`.
- When another task records into some of the handles, give it a struct of just those handles with the same shape, such as `LaneCounters` in spate-datagen.
- Derive `Debug`.
- Name the struct for its connector and role, such as `KafkaFeedMetrics` or `JsonDeserMetrics`.
  When one struct serves two roles, name it for the client it reads, such as `KafkaConsumerStatsMetrics`.

**Why:** a reader then finds every family a component registers, and every place it records, in one module of each crate.

**Follow:** [`crates/spate-json/src/metrics.rs`](repo:crates/spate-json/src/metrics.rs).

**Checked by:** review.

## Package the crate

### Write the manifest

Write the crate's `description` as one paragraph that says what the crate is, and end it by naming the `spate` feature an application turns on.
**Why:** a reader who finds the crate on docs.rs or crates.io is then sent to `spate`, the only crate an application depends on.

For a filled-in example, see [`crates/spate-kafka/Cargo.toml`](repo:crates/spate-kafka/Cargo.toml).

### Put test-only access behind `testing`

Declare a `testing` feature when the crate's tests, benches or fuzz targets need more than its public API:

```toml
# Test-only. Off by default, not enabled by the `spate` facade, and outside
# the semver surface.
testing = []
```

The crate then dev-depends on itself with that feature on, so its own tests reach what sits behind it.
A sibling crate that needs one of those items turns the feature on in its dev-dependency, with a comment that names the item.

Mark each item behind `testing` `#[doc(hidden)]`.
**Why:** no user of the `spate` crate then sees it, and it stays outside the semver surface.

Give a seam the smallest shape that reaches the work.
It can be a constructor that injects a clock or skips a startup check, a module that hands a mock server the rows it answers with, or a function over one whole unit of a stage's work.
The S3 crate's [`bench_seams.rs`](repo:crates/spate-s3/src/bench_seams.rs) states the rules a seam of that last kind follows.

The test doubles that no shipped path reaches are listed in `.cargo/mutants.toml`, so cargo-mutants leaves them out.

### Declare each test target and its features

Give a test file that needs an optional feature a `[[test]]` stanza in the crate's `Cargo.toml`, with the `required-features` it needs.

**Why:** `autotests` is left at its default, so cargo collects an undeclared test file anyway, with no `required-features`.
Every build then compiles it, including a build where the items it imports do not exist.

The test file [`crates/spate/tests/manifest.rs`](repo:crates/spate/tests/manifest.rs) checks each of `spate`'s test targets against its stanza.
No connector crate has that check, so review checks a connector's stanzas.

The `cargo xtask hack` command runs `cargo check --workspace --all-targets`, which type-checks every test target on the default feature set.
The `cargo hack --no-dev-deps` step in the same command removes the dev-dependencies, so it checks no test target.

A test that branches on `cfg!(feature = …)` runs its feature-on arm under `cargo xtask test`, which passes `--all-features`.
It runs its feature-off arm under `cargo xtask hack`, which runs the suite on default features.
The feature-off arm of a default feature, or of a feature a dev-dependency turns on, runs in neither.

### Add a container suite and its lanes

Mark a test that needs the real server `#[ignore = "requires Docker"]`.
Read its image through `spate_test_support::container_image`, which takes the image pinned under `ci/<service>/`.

Pass `--pull` to `container_image`, so the call fetches the pinned image by its digest.
**Why:** without `--pull`, the call resolves the tag alone, and the suite runs whatever image the host holds under that tag.

A service's lanes are directories under `ci/<service>/`.
See [`ci/README.md`](repo:ci/README.md) for how the lanes are laid out and how to add a service.
Put the helpers a suite's tests share in its `tests/support/` module.

CI selects a suite by path, through [`xtask/src/ci/classify.rs`](repo:xtask/src/ci/classify.rs).
A connector's suite runs when the connector changes, when a crate it depends on changes, or when its server's pinned image changes.
[Run the container suites](03-commands.md#run-the-container-suites) says how to run them on your machine.

### Add bench targets

Name a connector's benches and write their `[[bench]]` stanzas by the rules [Performance](07-performance.md) gives for each tier.
That page also says how to add a target.

### Add fuzz targets

Give each parser of outside bytes a target in `fuzz/`, one `[[bin]]` per boundary in [`fuzz/Cargo.toml`](repo:fuzz/Cargo.toml).
[Fuzz a parser](04-writing-a-test.md#fuzz-a-parser) says how a target reaches a decoder that the crate keeps private, and what CI runs.

### Add its docs page

A connector's page is the `README.mdx` of a folder under `docs/user-guide/04-connectors/<role>/`.
Copy it from `docs/user-guide/04-connectors/_template.mdx`, and keep the heading order that [`docs/STYLE.md`](repo:docs/STYLE.md) § 3 fixes.
Add more pages to the folder when one page is not enough.

When the page has a support table, name the page in `ci/<service>/DOCS`.
Then `cargo xtask tidy supported-versions` holds the table to the lanes that pin a release line.

### Add its changelog fragment

A change to the crate's `src/`, `build.rs` or `Cargo.toml` changes what the crate ships, so it carries a fragment under `changelog.d/`.
When nobody upgrading would notice the change, write `Changelog: none` in the pull request body instead.
A change to the crate's `tests/`, `benches/` or docs page ships nothing.
See [`changelog.d/README.md`](repo:changelog.d/README.md) for how to write a fragment.

## Related

- [Custom sources](../user-guide/06-extending/custom-source.mdx) — the contracts a connector implements, for a user writing one outside this repository.
- [Instrumenting connectors](../user-guide/06-extending/instrumenting-connectors.mdx) — the metric contracts any connector meets, which the metric sections above build on.
- [Commands and CI](03-commands.md) — the commands that build, test and select the suites named here.
- [Performance](07-performance.md) — the bench targets a connector adds.
