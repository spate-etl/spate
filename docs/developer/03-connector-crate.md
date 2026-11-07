---
description: "What a crate under crates/ carries beyond its source, starting with the test targets and features it declares."
---

# Anatomy of a connector crate

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
