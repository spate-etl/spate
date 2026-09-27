---
description: "A changelog fragment opening with **Breaking:** is what derives a minor release and excuses an API break; commit subjects carry no release signal."
---

# ADR-0048 — Changelog fragments carry the release signal, so subjects carry none

- **Status:** accepted
- **Date:** 2026-09-27
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Three pieces of release tooling read a breaking-change signal:
`scripts/release-version.sh --derive`, which picks a minor or a patch bump; the
semver gate in `xtask/src/checks/semver_checks.rs`, which lets an API break
through once one is announced; and the changelog gate in
`xtask/src/checks/changelog.rs`, which decides whether a pull request needs a
fragment. All three read the Conventional Commits type and `!` from commit
subjects, and the first two also scan squash bodies for constituent subjects.

The repository moves to `area: description` subjects, which carry no type and
no `!`, and to squash merges with a blank body. Neither signal survives that.
The fragments already say the same thing in their own words: every `!` commit
since v0.2.0 added a fragment that opens with `**Breaking:**`.

## Considered options

- Fragments carry the signal. A fragment opening with `**Breaking:**` derives a
  minor bump and excuses a semver finding, and a fragment is required by the
  paths a change touches.
- Keep a marker in the subject, such as `kafka!: …`, beside the area.
- A `Breaking-Change:` trailer in the pull request body.
- A `breaking` label on the pull request.

## Decision outcome

Chosen option: "Fragments carry the signal", because the fragment is already
the release note a breaking change has to carry, so one file states the break
for the reader and for the tooling. A subject marker and a trailer are a second
statement of the same fact that can disagree with the first, and a blank squash
body leaves no trailer on `main`. A label lives outside git, so the release
derivation could not read it from the tree it releases.

Between a release merge and its tag the fragments are consumed while the tag
still names the previous release. For that window the marker is read from the
new version's section of `CHANGELOG.md` instead.

### Consequences

- Good, because the tooling and the release notes read one source, so a break
  announced to tooling is always announced to users.
- Good, because a subject is free to say what the change does, and nothing
  parses it except the title gate.
- Bad, because the fragment requirement moves from the subject to the paths a
  change touches, and a change under a crate's `src/` that nobody upgrading
  would notice now says `Changelog: none` in its body.
- Bad, because outside `crates/` only a `rust-version` move is asked for a
  fragment, so a lockfile bump that changes behavior relies on its author. A
  changed requirement under the root `[workspace.dependencies]` gets its entry
  from `cargo xtask changelog build` at release instead.

### Confirmation

`breaking_announced` in `xtask/src/checks/changelog.rs` is the one reader. The
semver gate calls it, and `release-version.sh --derive` reaches it through
`cargo xtask changelog breaking`. The
changelog gate runs as `cargo xtask tidy changelog` in the `changelog` CI job,
and `cargo xtask tidy title` rejects a subject carrying a type or `!`.

## More information

- Landed in [#730](https://github.com/spate-etl/spate/pull/730).
- The release-time entry for root requirements landed in [#745](https://github.com/spate-etl/spate/pull/745).
- [ADR-0045](0045-rust-task-runner.md) is the runner these checks live in.
- `changelog.d/README.md` states when a fragment is required and how the marker
  is written.
