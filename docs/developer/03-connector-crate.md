---
description: "What a crate under crates/ carries beyond its source: the testing feature and its seams, test targets, container suites and lanes, bench and fuzz targets, its docs page and its changelog fragment."
---

# Anatomy of a connector crate

A connector crate ships one source, sink, format or feed backend behind a
feature on the `spate` facade. Its `src/` is the smaller part of what it
carries. The rest is below, section by section, with a pointer to where each
part is documented in full.

## The manifest

The crate's `description` says what it is in one paragraph and ends by naming
the facade feature an application depends on, so a reader on docs.rs or
crates.io is sent to `spate`. A `testing` feature follows:

```toml
# Test-only. Off by default, not enabled by the `spate` facade, and outside
# the semver surface.
testing = []
```

The crate dev-depends on itself with that feature on, so its own tests reach
what sits behind it. A sibling crate that needs a seam enables the feature on
its dev-dependency and says which seam in a comment beside it.
[`crates/spate-kafka/Cargo.toml`](repo:crates/spate-kafka/Cargo.toml) is a
filled-in example.

## Seams behind `testing`

What a test, a bench or a fuzz target cannot reach through the public API is
exported behind that feature and marked `#[doc(hidden)]`, so no user of the
facade can see it and it sits outside the semver surface. The rules a seam
honors are stated in the module that holds the S3 crate's bench seams,
[`crates/spate-s3/src/bench_seams.rs`](repo:crates/spate-s3/src/bench_seams.rs):
it exports functions and the aliases a caller needs to name, while the crate's
own types stay private, and each function is one whole unit of the work a
stage does. Other seams in the tree are an injected clock, such as
`KafkaSource::with_clock`, the S3 crate's `fuzz_seams`, and the ClickHouse
crate's `testing` module, which hands the frame codec to its mock server.

cargo-mutants excludes the seam modules in `.cargo/mutants.toml`, since no
production path reaches them.

## Test targets and features

A test file reaching an optional feature is declared in its crate's
`Cargo.toml` with the `required-features` it needs. `autotests` is left at its
default, so an undeclared file is collected regardless, carrying no
`required-features`, and every build compiles it — including the ones its
imports do not exist under. `crates/spate/tests/manifest.rs` holds each test
target to its stanza, and `cargo check --workspace --all-targets` in
`cargo xtask hack` is what builds them on the default feature set;
`cargo hack --no-dev-deps` strips dev-dependencies and reaches no test target
at all.

A test that branches on `cfg!(feature = …)` runs its feature-on arm under
`cargo xtask test`, which passes `--all-features`, and its feature-off arm under
`cargo xtask hack`, which runs the suite on default features. The feature-off
arm of a default feature, or of one a dev-dependency enables, runs in neither.

## Container suites and lanes

A test that needs the real server carries `#[ignore = "requires Docker"]` and
starts the image through `spate_test_support::container_image`, which pulls it
by digest through the task runner. The suite's `tests/support/` module holds
the start and the helpers the suite's tests share. The image, and the lanes a
service has, come from the service's `ci/<service>/` tree;
[`ci/README.md`](repo:ci/README.md) is the account.

CI selects a suite by path. The classifier in
[`xtask/src/ci/classify.rs`](repo:xtask/src/ci/classify.rs) maps a changed
file to the container suites that can reach it over the crate graph, and an
image bump to the suites that boot that image, so a connector's suite runs on
a change to the connector, to a crate it depends on, or to its server's pin.
[Container suites](01-commands.md#container-suites) says how to run them
yourself.

## Bench targets

A connector measures itself in the benchmark tiers, each through a `benches/`
file named for its tier, such as `<name>_gungraun.rs`, `<name>_alloc.rs` or
`<name>_wall.rs`, and declared as a `[[bench]]` with `harness = false`. The
commands discover a target by that name, so there is no list to join.
[Benchmarks and fuzzing](04-benchmarks-and-fuzzing.md) says what each tier
measures and how to add a target to it.

## Fuzz targets

A parser over outside bytes has a target in `fuzz/`: a `[[bin]]` per boundary
in [`fuzz/Cargo.toml`](repo:fuzz/Cargo.toml), reaching a decoder the crate
keeps private through the crate's `testing` feature.
[Fuzzing](04-benchmarks-and-fuzzing.md#fuzzing) has the commands and what CI
runs.

## Its docs page

A connector has one page,
`docs/user-guide/04-connectors/<role>/<name>/README.mdx`, copied from the
template beside the role folders and kept to the heading order
[`docs/STYLE.md`](repo:docs/STYLE.md) § 3 fixes. Its `## Metrics` section is the
one home for the families the crate registers. A support claim the page makes is
backed by a lane: the service's `ci/<service>/DOCS` names the page, and
`cargo xtask tidy supported-versions` holds its table to the lanes that pin a
release line.

## Its changelog fragment

A change under the crate's `src/`, `build.rs` or `Cargo.toml` ships, and
carries a fragment under `changelog.d/`;
[`changelog.d/README.md`](repo:changelog.d/README.md) says when and how. Its
`tests/`, `benches/` and docs page ship nothing.

## Related

- [Custom sources](../user-guide/06-extending/custom-source.mdx) — the
  contracts a connector implements, for a user writing one outside this
  repository.
- [Commands and gates](01-commands.md) — the commands that build, test and
  select the suites named here.
- [Benchmarks and fuzzing](04-benchmarks-and-fuzzing.md) — the targets a
  connector adds beyond its tests.
