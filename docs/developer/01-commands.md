---
description: "The task runner and its gates: what cargo xtask ci covers, what CI adds, the test runner and its profiles, the container suites, and which checks a change selects."
---

# Commands and gates

## Commands

`cargo xtask tidy --list` names the checks. `cargo xtask ci` is the pull request
bar and covers formatting, clippy, the type check, the loom models, the test
suite, doctests, rustdoc, the feature matrix, licenses and advisories, and
`cargo xtask tidy`, the repository consistency checks. An advisory is listed
without failing; CI fails on one only when a pull request changes the
dependency inputs.

`cargo xtask tidy markdown` runs `rumdl check` and needs `rumdl` on `PATH`. It
fails when a page under `docs/` runs past the width `rumdl.toml` sets, and
`rumdl fmt` rewraps the page.

`cargo xtask ci --since [REF]` narrows the Rust gates to the packages the diff
against REF (default `origin/main`) can affect: the changed crates and
everything that depends on them, over normal and dev edges. The diff covers
commits, staged and unstaged edits, and untracked files. It runs formatting,
clippy, the tests, doctests, rustdoc and the feature matrix on that set, the
loom models when `spate-core` is in it, and every `tidy` member. It skips
`cargo deny`, the fuzz harness and the packages outside the set, which CI still
runs. A change to a manifest, `Cargo.lock`, `test-support/`, `xtask/`, tooling
or Rust outside `crates/` runs the full `ci`. The root `README.md` counts as a
change to `spate`, which compiles it as a doctest. Passing `--since` is a
pre-push check; `cargo xtask ci` stays the pull request bar.

Verify a gate by its **exit code**. Piped `grep` and `tail` chains report the
status of the last command in the pipeline and have masked failures here. No
command contains a pipe.

CI runs the same commands for lint, type check, doctests, the feature matrix,
licenses and every `tidy` member. Other jobs spell out invocations of their
own for a coverage run, a container image, Node or a pinned tool, so a green
`cargo xtask ci` locally does not mean CI has nothing left to say.

These sit outside `ci`, by cost or by dependency:

| Command | Why it is outside |
| --- | --- |
| `cargo xtask integration-test` | Needs Docker and pulls real images |
| `cargo xtask docs` | Needs Node; runs nightly and on documentation changes |
| `cargo xtask bench check` | Builds the whole tree again in the release profile |
| `cargo xtask bench counted` | Needs Linux and valgrind |
| `cargo xtask bench alloc` | Builds each snapshotted crate in the release profile; its committed counts are for Linux x86_64 and aarch64 |
| `cargo xtask bench gungraun --check` | Proves only that the benches build, not what they count |
| `cargo xtask bench ab`, `cargo xtask bench arms`, `cargo xtask bench list`, `cargo xtask bench compare` | Wall clock; never a gate |
| `cargo xtask attribution` | `THIRD-PARTY.md` is regenerated at release; the nightly tier checks that it still generates |
| `cargo xtask fuzz build`, `cargo xtask fuzz run` | Needs a nightly toolchain; the nightly tier fuzzes |
| `cargo xtask mutants` | Runs a package's suite once per mutant; CI runs it on each pull request, advisory |

Some commands omit `--locked`, which everything else passes because CI does.
`cargo hack --no-dev-deps` rewrites each `Cargo.toml` as it runs and a locked
build refuses; `cargo fmt` resolves nothing, reading only `.rs` files;
`cargo fuzz`, `cargo semver-checks` and `cargo cyclonedx` accept no `--locked`;
and `cargo update` and `cargo generate-lockfile` write the lockfile on purpose.

## Git hooks

```sh
cargo xtask hooks install   # once per clone; every worktree shares it
```

This points `core.hooksPath` at `.githooks/`, whose commit-msg hook checks each
subject against the rule in [`CONTRIBUTING.md`](repo:CONTRIBUTING.md). The hook
builds xtask first. When cargo is missing or xtask does not build, it prints a
warning and lets the commit through, so a broken tree can still be committed;
the title gate in CI applies the rule regardless. The command refuses to
replace a `core.hooksPath` that points elsewhere, and names any hook in the
default directory that stops running.

## The test runner and its profiles

Tests run under [cargo-nextest](https://nexte.st), one process per test
concurrently, where `cargo test` runs one binary at a time. Plain
`cargo test --workspace` still works and is many times slower. nextest does not
run doctests; `cargo xtask doctest` does.

`cargo xtask doc` builds the API reference with `RUSTDOCFLAGS="-D warnings"`, so
a broken intra-doc link fails the build instead of rendering as dead text on
docs.rs. It does not reach bench support modules, which `cargo doc` never
builds.

The profiles in `.config/nextest.toml`:

- **`default`** — `fail-fast = false`, and a 30-second slow warning that
  terminates after four periods, so a hard kill at 120s. The container suites
  are excluded here and nothing left should take that long.
- **`ci`** — what `Test and coverage`, `Test (arm64)` and `Test (macOS)` run.
  60 seconds, terminating after four, with no retry and a JUnit report.
- **`docker`** — warns at 120 seconds and **never terminates**: a cold image
  pull can exceed any figure worth setting, and a SIGKILL reports as a timeout
  indistinguishable from a hang. A JUnit report, and no retry except on the S3
  job's RGW lane.

:::warning
On macOS every freshly linked binary stalls for tens of seconds at 0% CPU on
its first exec while Gatekeeper scans it. Across this workspace that alone
costs about half an hour per edit-test cycle. Add your terminal to *System
Settings → Privacy & Security → Developer Tools* to exempt it.
:::

## Container suites

Container-backed tests use testcontainers and are `#[ignore]`d, so a normal run
skips them. `cargo xtask integration-test` is what selects them.

A suite that boots a real server runs it at a pinned version, in a lane selected
by a `SPATE_<SERVICE>_LANE` variable. [`ci/README.md`](repo:ci/README.md) is the
account of the services, their lanes and what CI runs of them.

One suite sits outside even that. `spate`'s `e2e_examples` drives the shipped
example binaries, those whose stanza carries no `test = true`, against real
servers, stopping the ones with no stop condition with `SIGTERM` and asserting
the drain. It costs minutes and reports nightly, so the `docker` profile's
`default-filter` holds it back from every other invocation. Selecting it:

```sh
cargo nextest run --profile docker -p spate --all-features --locked \
  --run-ignored ignored-only -E 'binary(e2e_examples)' \
  --ignore-default-filter --test-threads 1
```

`--all-features` turns on `spate-kafka/tls`, which compiles OpenSSL from source.
When you are not touching the TLS surface, replace it with the features the
`e2e_examples` stanza in `crates/spate/Cargo.toml` requires.

## Mutants

`cargo xtask mutants [--since REF]` runs cargo-mutants over the diff from the
merge base of REF (default `origin/main`) and `HEAD` to the working tree, with
the settings in `.cargo/mutants.toml` and the `mutants` nextest profile.
Untracked files are not in the diff, and a renamed file brings only its edited
lines. It exits 2 when a mutant survives. The container suites do not run under
it, so a mutant only they would catch reports as missed. Its summary, printed
at the end of a local run and written to the job summary in CI, names the
mutant forms the tool does not generate, which stay a review step.

## What CI selects

CI picks the container suites from the paths a pull request changed. Which
counted benches run is derived the same way, from the benches themselves: a
change to a crate selects the benched crates that depend on it, its own
included, over normal and dev edges, and a change to the root `Cargo.toml`,
which holds the build profiles, selects every one. The semver
gate follows a third closure over the same paths, this one over the non-dev
dependency edges: a connector change checks that connector and the facade, and
a `spate-core` change checks every published crate. `cargo xtask tidy self-test`
checks each classifier against the crate graph.
