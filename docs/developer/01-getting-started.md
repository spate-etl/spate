---
description: "Install the Rust toolchain and the tools the gates call, exempt your terminal from Gatekeeper on macOS, install the commit hook, and run the gates once."
---

# Getting started

Set up a clone once, in the order below.
Each step names the command that needs it, so you can skip a tool whose command you never run.

## Install the toolchain

Install the current stable Rust through [rustup](https://rustup.rs), with the `rustfmt` and `clippy` components.
CI builds and tests on the current stable release.

The workspace's minimum supported Rust version is the `rust-version` in the root `Cargo.toml`, currently 1.96.
CI's `Check (MSRV)` job type-checks the workspace on that version, so code you write must build there.

The repository has no `rust-toolchain.toml` at its root.
The pins under `.github/toolchains/` are for CI jobs that need one particular release.

## Install the tools the gates call

`cargo xtask ci` runs these tools, and fails naming the first one it cannot find:

| Tool | Called by | Version |
| --- | --- | --- |
| [cargo-nextest](https://nexte.st) | the test suite | 0.9.89 or later; 0.9.148 recommended |
| [cargo-hack](https://github.com/taiki-e/cargo-hack) | the feature matrix | any |
| [cargo-deny](https://github.com/EmbarkStudios/cargo-deny) | licenses and advisories | any |
| [rumdl](https://github.com/rvben/rumdl) | `cargo xtask tidy markdown` | 0.3.0, as CI pins |
| [zizmor](https://github.com/zizmorcore/zizmor) | `cargo xtask tidy zizmor` | 1.28.0, as CI pins |
| [shellcheck](https://www.shellcheck.net) | `cargo xtask tidy shellcheck` | any |

Install the same rumdl and zizmor versions as CI.
**Why:** a newer rumdl can reflow pages differently, and a newer zizmor can change which audits it runs, so your local result would differ from CI's.

Nextest checks its own version against `.config/nextest.toml`.
A version below the required one stops the run.

These commands need more, and `cargo xtask ci` runs none of them:

| Command | Needs |
| --- | --- |
| `cargo xtask integration-test` | Docker |
| `cargo xtask docs` | Node, at the version in `.node-version` |
| `cargo xtask fuzz`, `cargo xtask docsrs` | A nightly toolchain; `--nightly <channel>` selects the dated one that `.github/toolchains/nightly/` pins for CI |
| `cargo xtask bench counted` | Linux and valgrind |
| `cargo xtask mutants` | [cargo-mutants](https://mutants.rs) |

## Exempt your terminal from Gatekeeper on macOS

On macOS, add your terminal to *System Settings → Privacy & Security → Developer Tools*.

**Why:** Gatekeeper scans each freshly linked binary on its first run, and the binary waits for tens of seconds at 0% CPU while it does.
Across this workspace that costs about half an hour per edit-and-test cycle.

## Install the commit hook

Run this once per clone:

```sh
cargo xtask hooks install
```

It points `core.hooksPath` at `.githooks/`, and every worktree of the clone shares the setting.
The `commit-msg` hook checks each commit's subject against the rule in [`CONTRIBUTING.md`](repo:CONTRIBUTING.md).

The hook builds xtask first.
When cargo is missing or xtask does not build, it prints a warning and lets the commit through.
CI checks the pull request title either way.

The command refuses to replace a `core.hooksPath` that points somewhere else.
It also names any hook in the default hook directory that will stop running.

## Run the gates once

Run the full gate before your first change:

```sh
cargo xtask ci
```

It checks that every tool is installed, and it warms the build cache for the working loop.
[Commands and CI](03-commands.md) says what it runs and what CI adds.

## Related

- [`CONTRIBUTING.md`](repo:CONTRIBUTING.md) — how to find an issue, and how to open a pull request.
- [Commands and CI](03-commands.md) — the working loop and every gate.
- [Writing a test](04-writing-a-test.md) — where a test lives and which harness to use.
