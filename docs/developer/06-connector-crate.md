---
description: "What a connector crate carries: manifest, constructor shape, testing feature, tests, container suites, bench and fuzz targets, metric handles, docs page and changelog fragment."
---

# Adding a connector crate

A connector crate ships a source, a sink, a format or a feed backend, or several of them, behind a feature of the `spate` crate.
Each section below covers one thing the crate carries, in its `src/` or beside it.

## Write the manifest

Write the crate's `description` as one paragraph that says what the crate is, and end it by naming the `spate` feature an application turns on.
**Why:** a reader who finds the crate on docs.rs or crates.io is then sent to `spate`, the only crate an application depends on.

For a filled-in example, see [`crates/spate-kafka/Cargo.toml`](repo:crates/spate-kafka/Cargo.toml).

## Give each component a config type and two constructors

Give each source, sink and deserializer a config type, and put its two constructors on the type they return.
**Why:** every connector then builds the same way, so a user who has wired one connector can wire the next from its type names alone.

The config type follows four rules:

- It is a `#[non_exhaustive]` struct named `<Vendor><Role>Config`, such as `KafkaSinkConfig`.
- Its `from_component_config(section)` deserializes the pipeline's section and validates it.
- Its public `validate(&self)` checks the configuration rules, and creates no client and reads no file.
- It refuses with `ConfigError`, built through a `SectionPath`, so each error starts with the key it refuses.

`validate` reports at the role's own path, such as `sink.kafka.shards`.
A source or deserializer config's `from_component_config` reports at the same path, because a pipeline holds one section of each.
A sink config's `from_component_config` reports at the section's own path, because a pipeline can hold several sinks under `sinks:`.
An error in a `sinks:` entry named `eu` then starts at `sinks.eu.kafka`.

The type the config turns into follows three rules:

- It has `new(config)` for a config built in code, and `from_component_config(section)` for the pipeline's section.
  `new` also takes any runtime handle it needs, as `S3Source::new` takes the I/O runtime's handle.
- Its `new` returns `Result<_, ConfigError>` only when it can fail, such as when it creates a client or loads a file.
  `KafkaSink::new` creates the sink's producers, so it is fallible, and `KafkaSource::new` creates nothing, so it returns the source.
- A `new` that can fail also runs the config's `validate` rules, so a config built in code meets the same rules as one read from YAML.
  An infallible `new` takes a config that is already valid, so a caller who builds the config in code calls `validate` first.

The Kafka sink in [`spate-kafka/src/sink/config.rs`](repo:crates/spate-kafka/src/sink/config.rs) has this shape.
Its signatures, with the bodies left out, are:

```rust
impl KafkaSinkConfig {
    pub fn from_component_config(section: &ComponentConfig) -> Result<Self, ConfigError>;
    pub fn validate(&self) -> Result<(), ConfigError>;
}

impl KafkaSink {
    pub fn new(cfg: KafkaSinkConfig) -> Result<KafkaSink, ConfigError>;
    pub fn from_component_config(section: &ComponentConfig) -> Result<KafkaSink, ConfigError>;
}
```

Make the constructors return a builder only when an input must come after the config: a type parameter, or a startup step that does I/O.
Name the builder `<Vendor><Role>Builder`, and finish it with `build` or `build_<output>`.
`ClickHouseSinkBuilder::build::<F>()` in [`spate-clickhouse/src/config.rs`](repo:crates/spate-clickhouse/src/config.rs) takes the row type and reads the table's schema from every replica.
`JsonDeserializerBuilder` in [`spate-json/src/config.rs`](repo:crates/spate-json/src/config.rs) finishes with `build_serde::<T>()` or `build_value()`.

Without such an input, the constructors return the role type itself.
[`KafkaSource`](repo:crates/spate-kafka/src/source.rs) needs no later input.
`KafkaSink` takes its encoder's record type on the built sink, through `encoder_json::<F>()`, so it needs no builder either.

Feed backends and sink encoders have constructor shapes of their own, which this section does not cover.

## Put test-only access behind `testing`

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

## Declare each test target and its features

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

## Add a container suite and its lanes

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

## Add bench targets

Name a connector's benches and write their `[[bench]]` stanzas by the rules [Performance](07-performance.md) gives for each tier.
That page also says how to add a target.

## Add fuzz targets

Give each parser of outside bytes a target in `fuzz/`, one `[[bin]]` per boundary in [`fuzz/Cargo.toml`](repo:fuzz/Cargo.toml).
[Fuzz a parser](04-writing-a-test.md#fuzz-a-parser) says how a target reaches a decoder that the crate keeps private, and what CI runs.

## Name its metrics

Name each metric family and label key the crate registers by [Instrumenting connectors § Contracts](../user-guide/06-extending/instrumenting-connectors.mdx#contracts).
The framework's own names follow the same rules.

## Hold its metric handles in one struct

Hold the handles each component registers in one struct, shaped like its peers:

- Declare each metric name and each label key the crate adds as a constant in the struct's module.
- Keep the handle fields private.
  Callers outside that module record through one function per event, such as `JsonDeserMetrics::dropped_malformed`.
- When another task records into some of the handles, give it a struct of just those handles with the same shape, such as `LaneCounters` in spate-datagen.
- Derive `Debug`.
- Name the struct for its connector and role, such as `KafkaFeedMetrics` or `JsonDeserMetrics`.
  When one struct serves two roles, name it for the client it reads, such as `KafkaConsumerStatsMetrics`.

## Add its docs page

A connector's page is the `README.mdx` of a folder under `docs/user-guide/04-connectors/<role>/`.
Copy it from `docs/user-guide/04-connectors/_template.mdx`, and keep the heading order that [`docs/STYLE.md`](repo:docs/STYLE.md) § 3 fixes.
Add more pages to the folder when one page is not enough.

When the page has a support table, name the page in `ci/<service>/DOCS`.
Then `cargo xtask tidy supported-versions` holds the table to the lanes that pin a release line.

## Add its changelog fragment

A change to the crate's `src/`, `build.rs` or `Cargo.toml` changes what the crate ships, so it carries a fragment under `changelog.d/`.
When nobody upgrading would notice the change, write `Changelog: none` in the pull request body instead.
A change to the crate's `tests/`, `benches/` or docs page ships nothing.
See [`changelog.d/README.md`](repo:changelog.d/README.md) for how to write a fragment.

## Related

- [Custom sources](../user-guide/06-extending/custom-source.mdx) — the contracts a connector implements, for a user writing one outside this repository.
- [Commands and CI](03-commands.md) — the commands that build, test and select the suites named here.
- [Performance](07-performance.md) — the bench targets a connector adds.
