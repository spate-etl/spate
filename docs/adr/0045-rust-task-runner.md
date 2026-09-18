---
description: "The Makefile and most of scripts/ are replaced by cargo xtask, so a CI step and the local command that reproduces it are the same code."
---

# ADR-0045 — One Rust task runner replaces the Makefile and most of the shell

- **Status:** accepted
- **Date:** 2026-09-18 (the record precedes the implementation it describes)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The `Makefile` declares 50 targets, 45 of them carrying a recipe. Twenty of
those invoke a script under `scripts/`, twelve are a single `cargo` line, and
the longest recipe is ten lines. The file is a dispatch table.

The logic sits in `scripts/`, 6,278 lines across 16 files. Of those, 1,497 sit
inside a `self_test()` function, because testing a shell function means writing
a probe file to a `mktemp` directory and re-executing the script against it.
`gungraun-collected-region.sh` is 752 lines, 444 of them self-test.

The workflows carry 104 `run:` steps. Twenty-five invoke a `make` target.
Twenty-four invoke a script directly, and most of those pass flags no target
passes: `semver-checks.sh --against-registry --packages …`,
`gungraun-benches.sh --features … --run …`,
`gungraun-report.sh --regressions-out …`. Reproducing one of those locally
means reading the workflow and retyping the invocation, and nothing detects it
when the two fall out of step. A few of the twenty-four do have a target,
`site-meta.sh --check` among them, which is the shape the rest could have had.

Two vocabularies therefore describe the same work, and a third already exists
in Rust. `xtask/` decides which CI jobs a change needs, and two of its
functions shell back out to `scripts/` for the crates owning a counted bench
and for the ClickHouse lane names.

## Considered options

- One Rust runner in `xtask`, covering every CI step and every local command.
- Keep the `Makefile` as the dispatch table and move only the script bodies
  into `xtask`.
- Replace the `Makefile` with a `justfile`.
- Declare the single-line commands as `[alias]` entries in
  `.cargo/config.toml` and keep `xtask` for the checks.
- Do nothing.

## Decision outcome

Chosen option: "One Rust runner in `xtask`", because it is the only option
under which a CI step and the command that reproduces it locally are the same
code. The others leave the twenty-four direct script invocations outside any
runner, so the surface that drifts today keeps drifting.

Commands are declared as a `clap` enum. That declaration is readable at run
time, so a test can walk `.github/workflows/` and assert every
`cargo xtask <name>` it finds resolves to a command that exists.

`scripts/` keeps three files. `release.sh` and `release-version.sh` manipulate
git refs, tags, throwaway worktrees and a minted registry token; the failure
mode of a rewrite is a half-published release, and the sequence is rehearsed a
few times a year. `transclude.sh` resolves documentation fences and belongs to
the Node site toolchain.

### Consequences

- Good, because a CI step names a command a contributor can run, and a test
  holds the workflows to the command list.
- Good, because 1,148 of the 1,497 lines of shell self-test become ordinary
  `#[test]` functions with committed fixtures. The remaining 349 sit in the
  three scripts that stay.
- Good, because the two spawns inside `xtask` become function calls, leaving
  one reader for the counted-bench set and one for the lane names.
- Bad, because the first check a contributor runs now needs a Rust toolchain
  and a 2.06-second build. Several `make` targets needed neither.
- Bad, because five CI jobs that install no toolchain today need one: the
  workflow lint, the changelog gate, the instruction-count report, the site
  build and the label sync. Four of them run checks that read files and call
  no compiler.
- Neutral, because three shell scripts remain, so `shellcheck` stays.

### Confirmation

A `#[test]` in `xtask` asserts that every `cargo xtask <name>` appearing in
`.github/workflows/` resolves to a declared command. `cargo xtask ci` is the
gate that runs the rest.

## Evidence

- 50 `Makefile` targets, 45 with a recipe, 20 invoking a script, 12 a single
  `cargo` line, longest recipe 10 lines. Counted by an `awk` pass over the
  target lines and their tab-indented bodies.
- 6,278 lines under `scripts/` across 16 files, 1,497 of them within a
  `self_test()` function, of which 349 are in the three files that stay.
  Counted with `wc -l` and an `awk` span per file.
- 104 `run:` steps across `.github/workflows/`, 25 invoking a `make` target and
  24 invoking a script. Counted by a parser over each step block, since a step
  writes `run:` either as its first key or as a later one and a line-oriented
  grep sees only one of the two forms.
- Five workflow jobs run absorbed work with no toolchain step. Counted by
  checking each job body for `./.github/actions/setup-rust`.
- A cold `xtask` build takes 2.06 seconds, measured with an empty
  `CARGO_TARGET_DIR` on an arm64 macOS laptop in the dev profile. A CI runner
  starts from a restored cache, so the figure does not hold there.

## More information

- Landed in the pull request replacing the `Makefile` with `cargo xtask`. The
  record precedes the implementation, as ADR-0043 did.
- The approach was proven first by #545, which moved the CI change classifier
  out of a 1,478-line script and was accepted on a replay of 150 merged pull
  requests through both implementations.
- [`CONTRIBUTING.md`](https://github.com/spate-etl/spate/blob/main/CONTRIBUTING.md)
  and
  [`DEVELOPING.md`](https://github.com/spate-etl/spate/blob/main/DEVELOPING.md)
  name the commands this decision replaces.
