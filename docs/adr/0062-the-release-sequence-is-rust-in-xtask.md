---
description: "The release scripts move into xtask behind traits a test can fail, so the release path is tested like any other task. Supersedes part of ADR-0045."
---

# ADR-0062 — The release sequence moves from Bash into xtask, behind traits a test can fail

- **Status:** accepted
- **Date:** 2026-10-10 (the record precedes the `release.sh` port it describes)
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
dependency to `xtask`, and the `release.sh` port is to put a trait boundary
around those calls. A script is deleted only after its Rust replacement gives
the same result on the same commit.

`scripts/transclude.sh` is untouched by this record; ADR-0045's reasoning for
it still holds.

### Consequences

- Good, because the version derivation, the rewriters and the snippet scan are
  `#[test]` functions over fixture text and throwaway repositories, and the
  publish sequence becomes reachable by failure-injection tests once its
  external calls sit behind a trait.
- Good, because a change to the release reuses the `xtask` helpers for
  processes, the sparse index and the changelog instead of reimplementing them
  in shell.
- Bad, because a release now depends on `xtask` compiling, and `xtask` is a
  workspace member: a manifest whose `spate-*` pins disagree with the
  workspace fails to resolve before the version check can report it. Cargo's
  own resolution error names the pin instead.
- Bad, because the port is a rewrite of the path whose failure mode is a
  half-published release. The parity runs and the dry run are what stand
  between the two.
- Neutral, because `shellcheck` stays, for `transclude.sh` and the git hooks once
  `release.sh` is gone.

### Confirmation

`cargo xtask test` runs the release module's tests, and
`the_workflows_name_only_commands_that_exist` holds every `cargo xtask release`
invocation in `.github/workflows/` to a declared command. A real release
exercises what the fakes stand in for.

## Evidence

- `scripts/release.sh` is 704 lines with a 41-line self-test;
  `scripts/release-version.sh` is 617 lines with a 160-line self-test. Counted
  with `wc -l` and the span of each `self_test()` function.
- On the same commit, the Bash and Rust bumps to 0.3.0 produced byte-identical
  `Cargo.toml`, `Cargo.lock` and install snippet files. The derivation, the
  literal check and the metadata check gave the same answers, and a tree with
  five injected disagreements drew the same five diagnostics from both. The
  tree held no Rust test fixtures, which the Rust check leaves out of its scan
  and the Bash one would have flagged.
  Run by hand in two throwaway worktrees; no committed rig.

## More information

- Landed in #1058, the first of the ports under #931. The record precedes the
  `release.sh` port, as ADR-0045 preceded its implementation.
- [ADR-0045](0045-rust-task-runner.md) is the decision this one narrows, and
  [ADR-0048](0048-changelog-fragments-are-the-release-signal.md) defines the
  bump the derivation computes.
- [`RELEASING.md`](repo:RELEASING.md) describes the release these commands run.
