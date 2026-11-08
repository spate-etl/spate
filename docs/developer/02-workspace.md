---
description: "The published crates and the unpublished members, what belongs in spate-core, where the engine's properties are written down, and where the rest of the repository lives."
---

# The workspace

The workspace's members are listed in the root `Cargo.toml`, and every one of them shares its version and its minimum Rust version.

## The published crates

The Connectors section of the root [`README.md`](repo:README.md) lists the published crates in two tables.
The first holds the connectors, each turned on by a feature of the `spate` crate.
The second holds the framework: `spate`, `spate-core` and `spate-test`.

One more crate is published and appears in neither table.
The proc macro behind `#[derive(ClickHouseRow)]` in `spate-clickhouse` is `spate-clickhouse-derive`.

Each published crate lives under `crates/<name>/`.
An application depends on `spate` alone, and turns a connector on by its feature.

## The unpublished members

Four members set `publish = false`:

| Path | Package | What it holds |
| --- | --- | --- |
| `xtask/` | `spate-xtask` | The repository automation that `cargo xtask` runs: the gates, CI's change detection and the release tooling |
| `bench/` | `spate-bench` | The wall-clock A/B benchmark harness |
| `fuzz/` | `spate-fuzz` | The libFuzzer targets, which build on nightly |
| `test-support/` | `spate-test-support` | Helpers that only this repository's tests and benches use, such as reading a pinned container image |

## Put shared code in `spate-core`, unless its API names a connector type

When two crates need the same mechanism, it lives in `spate-core`, within the limits [INV-6](../INVARIANTS.md) sets on its public API.
A helper that only this repository's tests and benches need goes in `test-support/`.

**Why:** `spate-core`'s public API is part of Spate's semver promise.
A 0.x dependency can break its own API in a minor release, and a connector type changes with its connector, so INV-6 limits where either can appear in that API.

Do not copy code between crates.
A change that would add a second copy moves the first one to `spate-core` or `test-support/`.
[Reuse before you add](08-code-and-comments.md#reuse-before-you-add) says how to find the first copy.

## Where the engine's properties are written down

- [`docs/INVARIANTS.md`](../INVARIANTS.md) numbers the properties the engine holds to, and is the only place they are stated in full.
  Cite a property by its number, such as INV-6.
  The pull request template asks which numbers a change touches.
- [`docs/adr/`](../adr/README.mdx) holds the architecture decision records, one file per decision.
  An accepted record is never rewritten.
  A later record supersedes it, and both stay.
  `cargo xtask adr new <slug>` starts a record from `docs/adr/_template.md`, which states when a decision needs one.
- [Architecture](../user-guide/02-concepts/01-architecture.mdx) in the user guide describes how the engine is put together today.
- [`docs/METRICS.md`](../METRICS.md) lists every metric family and its labels.

## Where the rest of the repository lives

| Path | What it holds |
| --- | --- |
| `crates/spate/examples/` | The runnable examples, indexed in their `README.md` |
| `examples/docker/` | The container image a deployment builds from |
| `ci/` | The pinned service images the container suites run against, one directory per service |
| `changelog.d/` | One changelog fragment per pull request, assembled into `CHANGELOG.md` at release |
| `docs/` | The user guide, this guide, the decision records, the invariants, the metrics and `STYLE.md` |
| `website/` | The Docusaurus site that renders `docs/` |
| `handbook/` | The rules for writing outside the code, such as a blog post |
| `.github/` | The workflows, the shared actions, the issue forms and the pull request template |
| `.githooks/` | The `commit-msg` hook |
| `.config/nextest.toml` | The test profiles |
| `.cargo/` | The `cargo xtask` alias and the cargo-mutants settings |

## Related

- [`ci/README.md`](repo:ci/README.md) — the services, their lanes and what CI runs of each.
- [`changelog.d/README.md`](repo:changelog.d/README.md) — when a change needs a fragment, and how to write one.
- [Code and comments](08-code-and-comments.md) — the reuse rule in full.
