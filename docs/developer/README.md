---
description: "How this repository is built, tested and reviewed, for a Rust engineer opening a first pull request here, with the documents that own the rest."
---

# Developer guide

This guide says how this repository is built, tested and reviewed.

## Who this guide is for

It is written for a Rust engineer about to open a first pull request here.
It assumes you know async Rust, threads, cargo and git, and that this repository's tooling and test harnesses are new to you.

[`CONTRIBUTING.md`](repo:CONTRIBUTING.md) says how a change is proposed and how it lands.
This guide says how the work is done.

## Read in this order

1. [Getting started](01-getting-started.md) — install the toolchain and the tools the gates call, and run the gates once.
2. [The workspace](02-workspace.md) — the crates, what belongs in `spate-core`, and where the rest of the repository lives.
3. [Commands and CI](03-commands.md) — the working loop, what `cargo xtask ci` runs, what CI adds, and how CI picks what to run.
4. [Writing a test](04-writing-a-test.md) — where a test lives, which harness to use, and the tools that check a test reaches the code.
5. [Tests that do not flake](05-flaky-tests.md) — the rules that keep a test passing on every CI runner, and what to do when one fails there.
6. [Adding a connector crate](06-connector-crate.md) — what a connector crate carries beyond its source.
7. [Performance](07-performance.md) — the allocation-free per-record path, and the benchmark tiers that measure a change.
8. [Code and comments](08-code-and-comments.md) — what rustdoc and a comment state, and the reuse rule.
9. [Before you open a pull request](09-before-review.md) — the checks a review runs, so you can run them first.

## Where the rest lives

A fact has one home.
A page here links the document that owns a fact, and does not restate it.

| Document | What it owns |
| --- | --- |
| [`CONTRIBUTING.md`](repo:CONTRIBUTING.md) | Issue assignment, the pull request title rule, the body template, and the invariants cited by number |
| [`AI_POLICY.md`](repo:AI_POLICY.md) | The bar for a contribution an agent did most of |
| [`RELEASING.md`](repo:RELEASING.md) | Milestones, priorities and the release process |
| [`SECURITY.md`](repo:SECURITY.md) | The threat model and the disclosure route |
| [`ci/README.md`](repo:ci/README.md) | The pinned service images and their lanes |
| [`changelog.d/README.md`](repo:changelog.d/README.md) | When a change needs a changelog fragment, and how to write one |
| [Documentation standards](repo:docs/STYLE.md) | The rules for every page under `docs/`, this guide included |
| [Spate invariants](../INVARIANTS.md) | The numbered properties the engine holds to |
