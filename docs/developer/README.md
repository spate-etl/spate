---
description: "How this repository is built, tested and reviewed, for a Rust engineer opening a first pull request here, with the documents that own the rest."
---

# Developer guide

How this repository is built, tested and reviewed. It is written for a Rust
engineer about to open a first pull request here: comfortable with async Rust,
cargo and git, and new to this repository's tooling and test harnesses.
[`CONTRIBUTING.md`](repo:CONTRIBUTING.md) is the entry point and says how a
change is proposed and lands. This guide says how the work is done.

## The pages

- [Commands and gates](01-commands.md) — the task runner, what `cargo xtask ci`
  covers and what CI adds, the test runner and its profiles, the container
  suites, and which checks a change selects.
- [Anatomy of a connector crate](03-connector-crate.md) — what a crate under
  `crates/` carries beyond its source.
- [Benchmarks](04-benchmarks-and-fuzzing.md) — the benchmark tiers outside
  `ci`, and how to add a target to each.
- [Writing a test](04-writing-a-test.md) — where a test lives, which harness to
  choose, what a test binary shares, and the tools that check a test reaches
  the code.
- [Code and comments](05-code-style.md) — the comment and rustdoc contract,
  and the reuse rule.
- [Tests that do not flake](05-flaky-tests.md) — the rules that keep a test
  passing on every CI runner, and what to do when one fails there.
- [Reading a review](06-reading-a-review.md) — what a review checks, so you
  can run it first.
- [The per-record path](07-per-record-path.md) — keeping per-record work from
  allocating, and measuring a change to it.

## Where the rest lives

A fact has one home. A page here links the document that owns a fact and does
not restate it.

| Document | What it owns |
|---|---|
| [`CONTRIBUTING.md`](repo:CONTRIBUTING.md) | Issue assignment, the pull request title rule, the body template, and the invariants cited by number |
| [`AI_POLICY.md`](repo:AI_POLICY.md) | What a contribution has to withstand, however it was produced |
| [`RELEASING.md`](repo:RELEASING.md) | Milestones, priorities and the release process |
| [`SECURITY.md`](repo:SECURITY.md) | The threat model and the disclosure route |
| [`ci/README.md`](repo:ci/README.md) | The pinned service images and their lanes |
| [`changelog.d/README.md`](repo:changelog.d/README.md) | When a change needs a changelog fragment and how to write one |
| [Documentation standards](repo:docs/STYLE.md) | The rules for every page under `docs/`, this guide included |
| [Spate invariants](../INVARIANTS.md) | The numbered properties the engine holds to |
