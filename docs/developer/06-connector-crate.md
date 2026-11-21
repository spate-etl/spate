---
description: "Conventions for a shipped connector's public surface, and what its crate carries: manifest, tests, container suites, benches, fuzz targets and docs page."
---

# Adding a connector crate

A connector crate ships a source, a sink, a format or a feed backend, or several of them, behind a feature of the `spate` crate.
The first part of this page gives the conventions its public surface follows, so a user meets the same shapes in every connector.
The second part covers what the crate carries beyond its `src/`.

Each convention states what to do, then why, then a connector that follows it, then what fails when a change breaks it.
"Review" there means no test or lint checks it, so a reviewer does.

These conventions are for the connectors this repository ships.
A user writing a connector in their own application needs only the contracts in [Extending](../user-guide/06-extending/README.mdx).

## Shape its public surface

### Name the role and its config for the connector

Name the role type `<Vendor><Role>` and its config `<Vendor><Role>Config`, such as `KafkaSource` and `KafkaSourceConfig`, or `ClickHouseSink` and `ClickHouseSinkConfig`.

**Why:** an application imports from several connectors, and the vendor in each name keeps the imports apart.

**Follow:** [`crates/spate-kafka/src/lib.rs`](repo:crates/spate-kafka/src/lib.rs).

**Checked by:** review.

### Export a source's types from the crate root

Export a source's role type, its config, its lane and its batch from the crate root, as spate-s3 exports `S3Source`, `S3SourceConfig`, `S3Lane` and `S3Batch`.

**Why:** a user then reaches every type a source needs through one path, `spate::s3::`, whichever source they use.

**Follow:** [`crates/spate-s3/src/lib.rs`](repo:crates/spate-s3/src/lib.rs).

**Checked by:** review.

### Build a source through `new` and `from_component_config`

Give a source's role type two constructors: `new`, which takes its config, and `from_component_config`, which takes the source's YAML section and returns `Result<Self, ConfigError>`.
`new` also takes any runtime handle the source needs, as `S3Source::new` takes the I/O runtime's handle.

**Why:** an application that builds a pipeline in code and one that reads YAML reach the same type, and a YAML error comes back as the `ConfigError` the rest of the configuration returns.

**Follow:** [`crates/spate-datagen/src/source.rs`](repo:crates/spate-datagen/src/source.rs).

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
`cargo xtask semver-checks` fails the later change that adds a field to a struct without the attribute.

### Read durations with `humantime_serde`

Deserialize every `Duration` field with `humantime_serde`, so the YAML value reads `30s` or `500ms`.

**Why:** every duration key in every connector then takes the same syntax.

**Follow:** [`crates/spate-kafka/src/config.rs`](repo:crates/spate-kafka/src/config.rs).

**Checked by:** review.

### Name its metrics

Name each metric family and label key the crate registers by [Instrumenting connectors § Contracts](../user-guide/06-extending/instrumenting-connectors.mdx#contracts).
The framework's own names follow the same rules.

**Why:** a dashboard or alert written against one connector's families then reads the same way against another's.

**Follow:** [`crates/spate-json/src/metrics.rs`](repo:crates/spate-json/src/metrics.rs).

**Checked by:** review.
The test `names_follow_prometheus_conventions` in [`crates/spate-core/src/metrics/names.rs`](repo:crates/spate-core/src/metrics/names.rs) holds the framework's own names to the suffix rules, and reads no connector's names.

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
