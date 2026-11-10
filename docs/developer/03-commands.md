---
description: "The working loop, what cargo xtask ci runs and what CI adds, the test profiles, the container suites, the mutants run, and how CI picks what to run."
---

# Commands and CI

Each command has a `--help` of its own, and `cargo xtask --help` lists them all.

## The working loop

Run these after each edit, narrowest first:

```sh
cargo xtask clippy
cargo nextest run -p spate-s3 --all-features --locked   # the crate you touched
cargo xtask ci --since
cargo xtask mutants --since origin/main
```

The first two answer fastest for the crate you are changing.
Run the last two before you push.
A pull request must pass `cargo xtask ci` with no argument.

### What `ci --since` covers

With `--since [REF]`, the command reads the diff from the merge base of REF and `HEAD`, where REF defaults to `origin/main`.
The diff holds commits, staged and unstaged edits, and untracked files.
It selects each crate the diff changes and every package that depends on that crate, over normal and dev edges.

On that set it runs clippy, the tests, doctests, rustdoc and the feature matrix.
It checks formatting across the whole workspace.
It runs the loom models when `spate-core` is in the set.
It runs every `tidy` member and the changelog check whatever the set holds.

It skips the type check, `cargo deny`, the fuzz harness and the packages outside the set.
CI still runs all of them.

A diff runs the full `ci` when it touches a manifest, `Cargo.lock`, or any file under `xtask/`, `bench/`, `fuzz/` or `scripts/`.
So does any other file outside `crates/`, apart from Markdown, `docs/`, `website/`, and a few files that reach no Rust job, such as `.gitignore`, `LICENSE`, `.node-version` and the issue templates under `.github/`.
So does a diff that git cannot produce.
A change to the root `README.md` selects `spate`, which compiles it as a doctest.

## Verify every gate by its exit code

Check a command's exit code, never the last lines of its output.

**Why:** a pipeline such as `cargo test | tail` exits with the status of its last command, and pipelines like it have hidden failures in this repository.
No `xtask` command contains a pipe.

## What `cargo xtask ci` runs

It runs these in order, and stops at the first failure:

1. Formatting and clippy
2. The type check
3. The loom models
4. The test suite
5. Doctests
6. Rustdoc
7. The feature matrix
8. `cargo deny`
9. Every `tidy` member, then the changelog check

The test suite runs under nextest and leaves out the container suites.
Nextest does not run doctests, so `cargo xtask doctest` runs them.

Rustdoc runs as `cargo xtask doc`, with warnings denied, so a broken intra-doc link fails the build.
It does not reach bench support modules, because `cargo doc` never builds them.

The feature matrix runs as `cargo xtask hack`, which checks every feature alone and the feature-off combinations, type-checks every target, and runs the tests on default features.
Its checks deny warnings, so a warning that appears on one feature set alone fails the command.

The `cargo deny` step fails on a license, ban or source finding.
It reports an advisory without failing.
CI fails on an advisory only when a pull request changes the dependency inputs.

The repository checks run under `cargo xtask tidy`.
Its `--list` flag names them, and `cargo xtask tidy <name>` runs one.

The `markdown` check runs `rumdl check`, so it needs `rumdl` on your `PATH`.
It fails a page under `docs/` that runs past the width `rumdl.toml` sets, and `rumdl fmt` rewraps the page.
Under `docs/developer/` each sentence sits on a line of its own, and `rumdl fmt` splits a line that holds two.

## What CI runs beyond it

CI runs the same commands for lint, the type check, doctests, the feature matrix, licenses and every `tidy` member.
Its other jobs run commands of their own, so a green `cargo xtask ci` on your machine does not mean CI will pass.

| Job | What it runs |
| --- | --- |
| `Test and coverage` | The suite under `cargo llvm-cov` on Linux x86_64 |
| `Test (arm64)`, `Test (macOS)` | The suite on Linux arm64, and on macOS when a build input changes |
| `Rustdoc as docs.rs builds it` | Rustdoc per crate on nightly |
| `Check (MSRV)` | The type check on the declared minimum Rust version |
| `Allocation assertions`, `Allocation snapshots` | The per-record allocation tests and the committed counts |
| `Container suites`, `ClickHouse <lane>`, `S3 <lane>`, `Kafka <lane>` | The container suites the change can reach |
| `Instruction counts` | The counted benches the change can reach |
| `Semver` | `cargo semver-checks` on the crates whose public API the change can move |
| `Declared version floors build` | The type check with each direct dependency at its lowest declared version, when a manifest changes |
| `Fuzz targets build`, `Build site` | The fuzz harness and the website |
| `Release dry run` | A rehearsal of the release, of up to an hour, when a pull request changes release tooling |
| `Analyze (<language>)` | CodeQL's analysis, on every pull request |

[How CI differs from your machine](05-flaky-tests.md#how-ci-differs-from-your-machine) says what changes for a test on those runners.

`Mutants over the diff` runs in a workflow of its own on each pull request.
It is advisory, and `CI gate` does not wait for it.

## Commands outside `ci`

These sit outside `ci`, by cost or by what they need:

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

## Pass `--locked`, except where cargo cannot take it

Pass `--locked` on every cargo command you run by hand, as CI does.

**Why:** without it, cargo can resolve a different dependency graph from the one CI builds, and hide a failure that CI then finds.

These commands cannot take it:

- Running `cargo hack --no-dev-deps` rewrites each `Cargo.toml` as it runs, and a locked build refuses the rewrite.
- The formatter, `cargo fmt`, resolves nothing, because it reads only `.rs` files.
- The subcommands `cargo fuzz`, `cargo semver-checks` and `cargo cyclonedx` accept no `--locked`.
- Both `cargo update` and `cargo generate-lockfile` write the lockfile on purpose.

## The test profiles

Tests run under [cargo-nextest](https://nexte.st), which runs each test in a process of its own and runs them concurrently.
A plain `cargo test --workspace` also works, and is many times slower.

The profiles live in `.config/nextest.toml`:

| Profile | Used by | Slow warning | Killed after |
| --- | --- | --- | --- |
| `default` | `cargo xtask test` and a plain `cargo nextest run` | 30 s | 120 s |
| `ci` | `Test and coverage`, `Test (arm64)`, `Test (macOS)` | 60 s | 240 s |
| `docker` | The container suites | 120 s | Never |
| `mutants` | cargo-mutants | 30 s | 120 s |

No profile retries a failed test.

The `docker` profile never kills a test.

**Why:** a cold image pull can take longer than any limit worth setting, and a killed test reports as a timeout, which looks the same as a hang.

The `mutants` profile stops at the first failing test, because one failure decides a mutant.
The other profiles run every test after one fails.
The `ci` and `docker` profiles write a JUnit report to `junit.xml`.

## Run the container suites

The container suites need Docker.
Their tests are marked `#[ignore]`, so a normal run skips them.
Run them with `cargo xtask integration-test`, which pulls each service's selected lane and then runs them under the `docker` profile.

A suite that boots a real server runs it at a pinned version.
A `SPATE_<SERVICE>_LANE` variable selects the lane:

```sh
SPATE_CLICKHOUSE_LANE=stable cargo xtask integration-test
```

When the selected S3 store declares no notifications, the command leaves out the notification suite.
When the selected Kafka broker is not the primary one, it leaves out the suites that run only on the primary broker.
See [`ci/README.md`](repo:ci/README.md) for the services, their lanes and what CI runs of each.

The `e2e_examples` suite in `spate` runs each example whose stanza carries no `test = true`, as the shipped binary against real servers.
It stops an example that has no stop condition with `SIGTERM`, and requires it to exit 0 within the drain deadline.
It takes minutes and reports nightly, so the `docker` profile's `default-filter` leaves it out of every other run.
Select it by name:

```sh
cargo nextest run --profile docker -p spate --all-features --locked \
  --run-ignored ignored-only -E 'binary(e2e_examples)' \
  --ignore-default-filter --test-threads 1
```

The `--all-features` flag turns on `spate-kafka/tls`, which compiles OpenSSL from source.
When your change does not touch TLS, pass the features that the `e2e_examples` stanza in `crates/spate/Cargo.toml` requires.

## Run the mutants

The command `cargo xtask mutants --since REF` runs cargo-mutants over the diff from the merge base of REF and `HEAD` to your working tree.
REF defaults to `origin/main`.
The command takes its settings from `.cargo/mutants.toml`, and the tests run under the `mutants` profile.

The diff leaves out untracked files, so commit a new file before the run.
A renamed file brings only its edited lines.

The command exits 2 when a mutant survives.
The container suites do not run under it, so a mutant that only they would catch reports as missed.
Mutate that guard by hand and run its container suite.

The `--budget MINUTES` flag stops cargo-mutants after that many minutes, and the command exits non-zero.
The summary then lists the survivors and timeouts among the mutants it tested, and names each mutant it did not reach.

CI runs the command on each pull request with a budget below the job's timeout.
It caps the writable memory of each process the run starts, so a mutant that loops while allocating fails its test.

## How CI picks what to run

CI's `Detect changes` job reads the paths a pull request changed, and selects the jobs and suites those paths can reach.

- **Container suites.**
  A change to a crate selects the container suites of that crate and of every crate that depends on it, over normal and dev edges.
- **Counted benches.**
  The same closure selects the crates that own a counted bench.
  A change to the root `Cargo.toml`, which holds the build profiles, selects every one.
- **Semver.**
  A change to a crate checks the published crates that depend on it over normal edges, because a dev-dependency reaches no public signature.
  A connector change checks that connector and `spate`, and a `spate-core` change checks every published crate.
- **macOS tests.**
  A pull request runs them when a build input changes, and every merge-queue entry that builds runs them.

The tests of xtask itself run under `cargo xtask tidy self-test`, and check each classifier against the crate graph.

## Related

- [Writing a test](04-writing-a-test.md) — the harnesses and tools these commands run.
- [Tests that do not flake](05-flaky-tests.md) — what a test needs to pass on every CI runner.
- [`ci/README.md`](repo:ci/README.md) — the pinned services and their lanes.
