---
description: "What a crate under crates/ carries beyond its source: the testing feature and its seams, test targets, container suites and lanes, bench and fuzz targets, its docs page and its changelog fragment."
---

# Anatomy of a connector crate

A connector crate ships a source, a sink, a format or a feed backend, or
several of them, behind a feature on the `spate` facade. Its `src/` is the
smaller part of what it carries. The rest is below, section by section, with a
pointer to where each part is documented in full.

## The manifest

Write the crate's `description` as one paragraph that says what the crate is,
and end it by naming the facade feature an application depends on, so a
reader on docs.rs or crates.io is sent to `spate`.
[`crates/spate-kafka/Cargo.toml`](repo:crates/spate-kafka/Cargo.toml) is a
filled-in example.

A crate whose tests, benches or fuzz targets reach past its public API
declares a `testing` feature:

```toml
# Test-only. Off by default, not enabled by the `spate` facade, and outside
# the semver surface.
testing = []
```

The crate then dev-depends on itself with that feature on, so its own tests
reach what sits behind it. A sibling crate that needs one of those seams
enables the feature on its dev-dependency and says which seam in a comment
beside it.

## Seams behind `testing`

What a test, a bench or a fuzz target cannot reach through the public API is
exported behind that feature and marked `#[doc(hidden)]`, so no user of the
facade sees it and it sits outside the semver surface. A seam takes the
smallest shape that reaches the work: a constructor that injects a clock or
skips a startup check, a module that hands a mock server the rows it has to
answer with, or a function over one whole unit of a stage's work. The S3
crate's bench seams, in
[`crates/spate-s3/src/bench_seams.rs`](repo:crates/spate-s3/src/bench_seams.rs),
state the rules a seam of that last kind honors.

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
reads its image through `spate_test_support::container_image`, passing
`--pull` so the image is fetched by digest on every run; without `--pull` the
call resolves the tag alone and the suite runs whatever the host holds under
it. The image, and the lanes a service has, come from the service's
`ci/<service>/` tree; [`ci/README.md`](repo:ci/README.md) is the account.
Helpers a suite's tests share live in its `tests/support/` module where it has
one.

CI selects a suite by path. The classifier in
[`xtask/src/ci/classify.rs`](repo:xtask/src/ci/classify.rs) maps a changed
file to the container suites that can reach it over the crate graph, and an
image bump to the suites that boot that image, so a connector's suite runs on
a change to the connector, to a crate it depends on, or to its server's pin.
[Container suites](01-commands.md#container-suites) says how to run them
yourself.

## Bench targets

A connector's `benches/` follows the naming and the `[[bench]]` stanza that
[Benchmarks and fuzzing](04-benchmarks-and-fuzzing.md) states for each tier,
and that page says how to add a target.

## Fuzz targets

A parser over outside bytes has a target in `fuzz/`, one `[[bin]]` per
boundary in [`fuzz/Cargo.toml`](repo:fuzz/Cargo.toml).
[Fuzzing](04-benchmarks-and-fuzzing.md#fuzzing) says how a target reaches a
decoder the crate keeps private and what CI runs.

## Its docs page

A connector has a folder under `docs/user-guide/04-connectors/<role>/`, whose
`README.mdx` is copied from the template beside the role folders and kept to
the heading order [`docs/STYLE.md`](repo:docs/STYLE.md) § 3 fixes; the folder
holds further pages where one page is not enough. A support table on the page
is held to the lanes that pin a release line by
`cargo xtask tidy supported-versions`, once the service's `ci/<service>/DOCS`
names the page.

## Its changelog fragment

A change under the crate's `src/`, `build.rs` or `Cargo.toml` touches what the
crate ships, so it carries a fragment under `changelog.d/`, or a
`Changelog: none` line in the pull request body when nobody upgrading would
notice; [`changelog.d/README.md`](repo:changelog.d/README.md) says which. Its
`tests/`, `benches/` and docs page ship nothing.

## Related

- [Custom sources](../user-guide/06-extending/custom-source.mdx) — the
  contracts a connector implements, for a user writing one outside this
  repository.
- [Commands and gates](01-commands.md) — the commands that build, test and
  select the suites named here.
- [Benchmarks and fuzzing](04-benchmarks-and-fuzzing.md) — the targets a
  connector adds beyond its tests.
