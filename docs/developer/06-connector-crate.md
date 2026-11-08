---
description: "What a connector crate carries beyond its source: the manifest, the testing feature and its seams, test targets, container suites and lanes, bench and fuzz targets, its docs page and its changelog fragment."
---

# Adding a connector crate

A connector crate ships a source, a sink, a format or a feed backend, or several of them, behind a feature of the `spate` crate.
Its `src/` is the smaller part of what it carries.
Each section below covers one of the other parts.

## Write the manifest

End the crate's `description` by naming the `spate` feature an application turns on.
**Why:** a reader who finds the crate on docs.rs or crates.io is then sent to `spate`, the only crate an application depends on.

[`crates/spate-kafka/Cargo.toml`](repo:crates/spate-kafka/Cargo.toml) is a filled-in example.

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

`crates/spate/tests/manifest.rs` checks each of `spate`'s test targets against its stanza.
No connector crate has that check, so review checks a connector's stanzas.

`cargo xtask hack` runs `cargo check --workspace --all-targets`, which builds every test target on the default feature set.
The `cargo hack --no-dev-deps` step in the same command removes the dev-dependencies, so it builds no test target.

A test that branches on `cfg!(feature = …)` runs its feature-on arm under `cargo xtask test`, which passes `--all-features`.
It runs its feature-off arm under `cargo xtask hack`, which runs the suite on default features.
The feature-off arm of a default feature, or of a feature a dev-dependency turns on, runs in neither.

## Add a container suite and its lanes

Mark a test that needs the real server `#[ignore = "requires Docker"]`.
Read its image through `spate_test_support::container_image`, which takes the image pinned under `ci/<service>/`.

Pass `--pull` to `container_image`, so the call fetches the pinned image by its digest.
**Why:** without `--pull`, the call resolves the tag alone, and the suite runs whatever image the host holds under that tag.
`cargo xtask integration-test` and CI pull every pinned image first, so a suite that runs only through them gets the pinned image either way.

A service's lanes are directories under `ci/<service>/`.
[`ci/README.md`](repo:ci/README.md) says how the lanes are laid out and how to add a service.
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

## Add its docs page

A connector's page is the `README.mdx` of a folder under `docs/user-guide/04-connectors/<role>/`.
Copy it from `docs/user-guide/04-connectors/_template.mdx`, and keep the heading order that [`docs/STYLE.md`](repo:docs/STYLE.md) § 3 fixes.
Add more pages to the folder when one page is not enough.

When the page has a support table, name the page in `ci/<service>/DOCS`.
`cargo xtask tidy supported-versions` then holds the table to the lanes that pin a release line.

## Add its changelog fragment

A change to the crate's `src/`, `build.rs` or `Cargo.toml` changes what the crate ships, so it carries a fragment under `changelog.d/`.
When nobody upgrading would notice the change, write `Changelog: none` in the pull request body instead.
A change to the crate's `tests/`, `benches/` or docs page ships nothing.
[`changelog.d/README.md`](repo:changelog.d/README.md) says how to write a fragment.

## Related

- [Custom sources](../user-guide/06-extending/custom-source.mdx) — the contracts a connector implements, for a user writing one outside this repository.
- [Commands and CI](03-commands.md) — the commands that build, test and select the suites named here.
- [Performance](07-performance.md) — the bench targets a connector adds.
