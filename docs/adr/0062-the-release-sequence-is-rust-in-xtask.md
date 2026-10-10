---
description: "The release scripts move into xtask behind traits a test can fail, so the release path is tested like any other task. Supersedes part of ADR-0045."
---

# ADR-0062 — The release sequence moves from Bash into xtask, behind traits a test can fail

- **Status:** accepted
- **Date:** 2026-10-10
- **Supersedes:** [ADR-0045](0045-rust-task-runner.md), the paragraph keeping `release.sh` and `release-version.sh` in `scripts/`
- **Superseded by:** —

## Context and problem statement

ADR-0045 moved the repository's tasks into `xtask` and kept two release
scripts in Bash, `scripts/release.sh` and `scripts/release-version.sh`, on the
grounds that they manipulate git refs, tags, throwaway worktrees and a minted
registry token, that a failed rewrite means a half-published release, and that
the sequence runs a few times a year.

Those scripts carry the behavior with the widest blast radius in the
repository, and the least test coverage. They have inline self-tests over pure
helpers, `shellcheck`, and a dry run, which cannot reach registry acceptance,
the OIDC exchange, the consumer smoke test, or any failure between a tag push
and an upload. A contributor changing them cannot reuse the `xtask` helpers or
run them under `cargo xtask test`. The question is whether the release stays in
Bash and, if not, how it moves without risking the release it describes.

## Considered options

- Port both scripts to `xtask`, putting git, `gh` and registry access behind
  small traits with fakes, and land the port one script at a time with a
  parity run against the Bash version before each script is deleted.
- Port only `release-version.sh`, which has no side effects outside the
  working tree, and keep `release.sh` in Bash.
- Keep both scripts, and grow their self-tests.

## Decision outcome

Chosen option: "Port both scripts to `xtask`", because a release step that
fails partway (a tag pushed and its push reported failed, an upload that stops
after some crates) can then be reproduced in a test against a fake, which no
amount of Bash self-test reaches.

The commands sit under `cargo xtask release`: `release version` for the
version arithmetic and the literals that carry it, then the sequence itself.
Git, `gh` and `curl` stay child processes, so the port adds no HTTP or TLS
dependency to `xtask`. The sequence reaches them through four traits, `Git`,
`Forge`, `Registry` and `Workspace`, each with a process-backed
implementation, and takes its retry pauses as a function, so a test drives
every step against fakes without waiting.

A script is deleted only after its Rust replacement gives the same result on
the same commit. A step that acts only on a real release cannot be run that
way: `finish`, and the pushes and pull request writes `assemble` makes. Its
failure-injection tests stand in for the parity run, and the first release the
Rust sequence runs exercises the rest.

`scripts/transclude.sh` is untouched by this record; ADR-0045's reasoning for
it still holds.

### Consequences

- Good, because the version derivation, the rewriters and the snippet scan are
  `#[test]` functions over fixture text and throwaway repositories, and the
  publish sequence runs under failure-injection tests against fakes of the
  four traits.
- Good, because a change to the release reuses the `xtask` helpers for
  processes, the sparse index and the changelog instead of reimplementing them
  in shell.
- Bad, because a release now depends on `xtask` compiling, and `xtask` is a
  workspace member: a manifest whose `spate-*` pins disagree with the
  workspace fails to resolve before the version check can report it. Cargo's
  own resolution error names the pin instead.
- Bad, because the port is a rewrite of the path whose failure mode is a
  half-published release. The parity runs, the fakes and the dry run are what
  stand between the two, and `finish` meets a real registry for the first time
  on a real release.
- Neutral, because `shellcheck` stays, for `transclude.sh` and the git hooks.

### Confirmation

`cargo xtask test` runs the release module's tests, and
`the_workflows_name_only_commands_that_exist` holds every `cargo xtask release`
invocation in `.github/workflows/` to a declared command. A real release
exercises what the fakes stand in for.

## Evidence

- `scripts/release.sh` was 704 lines with a 41-line self-test;
  `scripts/release-version.sh` was 617 lines with a 160-line self-test. Counted
  with `wc -l` and the span of each `self_test()` function.
- On the same commit, the Bash and Rust bumps to 0.3.0 produced byte-identical
  `Cargo.toml`, `Cargo.lock` and install snippet files. The derivation, the
  literal check and the metadata check gave the same answers, and a tree with
  five injected disagreements drew the same five diagnostics from both. The
  tree held no Rust test fixtures, which the Rust check leaves out of its scan
  and the Bash one would have flagged.
- On the same commit, `prepare --dry-run` against the live registry printed
  the same selection, trust check and packaging output from both, and both
  refused on the same two conditions in the registry's state at the time. The
  ten SBOMs both generated were byte-identical, and so were the release commit
  body and pull request body `assemble` generates.
- Both runs were made by hand in throwaway worktrees; no committed rig.

## More information

- Landed in #1059, the `release.sh` port. `release-version.sh` was ported in
  #1058. Both are under #931.
- [ADR-0045](0045-rust-task-runner.md) is the decision this one narrows, and
  [ADR-0048](0048-changelog-fragments-are-the-release-signal.md) defines the
  bump the derivation computes.
- [`RELEASING.md`](repo:RELEASING.md) describes the release these commands run.
