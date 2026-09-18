# Developing

Maintainer and returning-contributor reference: the build, test and benchmark
mechanics.

## Commands

`cargo xtask --help` lists the commands, each with its own `--help`, and
`cargo xtask tidy --list` names the checks. `cargo xtask ci` is the pull request
bar and covers formatting, clippy, the type check, the test suite, doctests,
rustdoc, the feature matrix, licenses and advisories, and `cargo xtask tidy`,
the repository consistency checks.

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
| `cargo xtask loom` | Exhaustive interleaving; minutes, not seconds |
| `cargo xtask docs` | Needs Node; runs nightly and on documentation changes |
| `cargo xtask bench check` | Builds the whole tree again in the release profile |
| `cargo xtask bench counted` | Needs Linux and valgrind |
| `cargo xtask bench gungraun --check` | Proves only that the benches build, not what they count |
| `cargo xtask bench ab`, `cargo xtask bench arms`, `cargo xtask bench list`, `cargo xtask bench compare` | Wall clock; never a gate |
| `cargo xtask attribution` | `THIRD-PARTY.md` is checked nightly and regenerated at release |
| `cargo xtask fuzz build`, `cargo xtask fuzz run` | Needs a nightly toolchain; the nightly tier fuzzes |

Three commands omit `--locked`, which everything else passes because CI does.
`cargo hack --no-dev-deps` rewrites each `Cargo.toml` as it runs and a locked
build refuses; `cargo fmt` resolves nothing, reading only `.rs` files; and
`cargo fuzz` accepts no `--locked` passthrough.

`cargo xtask docs` sets `CI=true`. The client-redirects plugin only registers
under it, so a plain `npm run build` skips redirect validation, and a redirect
pointing at a page you deleted is a hard failure.

## The test suite

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
  terminates after four periods, so a hard kill at 120s. The container suites are
  excluded here and nothing left should take that long.
- **`ci`** — what runs on a pull request. 60 seconds, terminating after four,
  plus one retry and a JUnit report, so a retried test surfaces as a flaky
  annotation rather than a green run.
- **`docker`** — warns at 120 seconds and **never terminates**: a cold image pull
  can exceed any figure worth setting, and a SIGKILL reports as a timeout
  indistinguishable from a hang. One retry, JUnit report.

Container-backed tests use testcontainers and are `#[ignore]`d, so a normal run
skips them. `cargo xtask integration-test` is what selects them.

A suite that boots a real server runs it at a pinned version, one lane per
release line the vendor still supports, selected by a `SPATE_<SERVICE>_LANE`
variable and pinned by tag and digest under `ci/<service>/`. CI runs the primary
lane for the whole tier and gives every other lane a job of its own.
[`ci/`](ci/README.md) is the account, and the services it covers are listed
there rather than here.

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
Drop it when you are not touching the TLS surface.

**On macOS every freshly linked binary stalls for tens of seconds at 0% CPU on
its first exec** while Gatekeeper scans it. Across this workspace that alone
costs about half an hour per edit-test cycle. Add your terminal to *System
Settings → Privacy & Security → Developer Tools* to exempt it.

### What CI selects, and how to widen it

CI picks the container suites from the paths a pull request changed. Which
counted benches run is derived the same way, from the benches themselves: a crate
with a bench selects its own, `spate-core` selects every benched crate because
everything depends on it, and a crate without one selects nothing. The semver
gate follows a third closure over the same paths, this one over the non-dev
dependency edges: a connector change checks that connector and the facade, and
a `spate-core` change checks every published crate.

For a change whose reach those paths do not show, such as a refactor moving code
between crates or a dependency swap, a maintainer can label the pull request
`ci: docker`, `ci: loom` or `ci: bench`. The label takes effect on the branch's
next push: `ci.yml` classifies each run from the event that triggered it, so a
re-run of an already-started run carries the labels that run started with. They
only ever add work; none can switch a suite off, and none reaches the semver
gate, which follows the crate graph alone. `cargo xtask tidy self-test` checks each
classifier against that graph.

## Testing conventions

Unit tests inline in a `#[cfg(test)]` module, integration tests in each crate's
`tests/`, doc tests on public APIs. proptest for tracker and codec invariants,
loom for the synchronisation primitives, rdkafka's `MockCluster` and the
ClickHouse mocks for connector behavior that does not need a container.

Framework users test their pipelines with `spate-test`'s in-memory source and
capture sink. Keep those first-class, and prefer them for reproductions. A test
written against them needs no infrastructure and runs in milliseconds.

One trap worth knowing: `cargo test` runs a binary's tests in one process, and
metric series ownership is process-wide (INV-10). Fixtures therefore need
per-test `pipeline` and `component` labels; a local recorder does not isolate the
claim.

A test file reaching an optional feature is declared in its crate's
`Cargo.toml` with the `required-features` it needs. `autotests` is left at its
default, so an undeclared file is collected regardless, carrying no
`required-features`, and every build compiles it — including the ones its
imports do not exist under. `crates/spate/tests/manifest.rs` holds each test
target to its stanza, and `cargo check --workspace --all-targets` in
`cargo xtask hack` is what builds them on the default feature set;
`cargo hack --no-dev-deps` strips dev-dependencies and reaches no test target
at all.

## Fuzzing

`fuzz/` is a [cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz.html)
crate with one libFuzzer target per boundary where the pipeline turns bytes
into records or records into bytes. Each target asserts a property rather
than only the absence of a panic. The crate sits outside the workspace, so no
`--workspace` target compiles it.

A decoder its crate keeps private is reached through that crate's
off-by-default `testing` feature, in a `fuzz_seams` module. `fuzz/Cargo.toml`
enables the feature on the crates whose targets need it.

```sh
cargo xtask fuzz install
cargo xtask fuzz build
cargo xtask fuzz run avro_wire_confluent --secs 60
```

`cargo +nightly fuzz list` prints the set. libFuzzer's instrumentation is
nightly-only, which is why every target carries `+nightly`.

A pull request touching `fuzz/`, `ci.yml`, `.github/actions/` or
`xtask/` builds every target and runs none of them. The nightly
tier in `scheduled.yml` fuzzes each for five minutes, set by `MAX_TOTAL_TIME`,
and carries `fuzz/corpus` between nights in a cache entry. A crash uploads the
input as the `fuzz-artifacts` artifact and opens an issue titled
`A fuzz target found a crashing input`.

## Benchmarks

The tiers below answer different questions. Only the counted one gates a pull
request.

None of them sweeps this framework's own settings against each other end to end,
and neither does the
[benchmark repository](https://github.com/spate-etl/benchmark), which runs one
fixed pipeline across several frameworks and publishes the
[results](https://spate.kainth.dev/benchmarks). The tiers here measure inside a
single component. A claim no tier here can measure is stated as unmeasured.

### The counted tier

`cargo xtask bench counted` counts instructions under valgrind rather than measuring
wall time, so its numbers are comparable across machines. It needs Linux,
valgrind, and a `gungraun-runner` at the version `Cargo.lock` pins for
`gungraun`; a mismatch is a hard error. On macOS the most you can check is that
the benches build, with `cargo xtask bench gungraun --check`.

Adding one means naming the file `benches/<something>_gungraun.rs` and declaring
it in the crate's `Cargo.toml` as a `[[bench]]` with `harness = false`.
Nothing else registers it. `cargo xtask bench gungraun` discovers it by that
name, and the bench commands, both CI legs and the CI selector all read from
that one place, so there is no list to add yourself to. Running that command
bare prints what would run. Without the `harness = false` stanza, cargo
auto-discovers the file under the default libtest harness, so the bench compiles
cleanly and fails at run time complaining about arguments.
`cargo xtask tidy gungraun-benches` catches it.

**Put the measured work in a named `#[inline(never)]` function and have the
benchmark function call it.** Getting this wrong produces a number rather than an
error. Collection is bounded by a callgrind toggle on the module the
`#[library_benchmark]` macro wraps the function in, and a toggle *flips*
collection rather than forcing it on. Work written inline in that function can
therefore be reshaped by the optimizer until it falls outside the collected
region, and whatever runs while collection happens to be on is counted instead.
Here that was glibc tearing down the corpus the fixture built: one bench reported
858,925 instructions, every one of them in `malloc_consolidate` and
`unlink_chunk`, with no application frame at all and the same total whether its
corpus held 400 documents or 6,400. Moving the loop behind a named callee took it
to 30,086,540.

`cargo xtask bench region` enforces it from the callgrind profile
rather than from the source: a case must attribute at least 10% of its collected
instructions to the binary under measurement, and must collect at least 1,000 of
them, since a region can also be lost by leaving almost nothing rather than by
leaving the allocator. Observed cases bottom out at 33.35% on the runner
architecture and 28.67% on arm64. `cargo xtask bench counted`
runs it after the benches, and CI runs it per shard as a *gate*: the counts are
advisory, a bench measuring the allocator is not. `cargo xtask tidy self-test`
holds the guard itself to captured profiles of both shapes, and needs no
valgrind.

Measuring a crate under more than one compiled feature arm *is* a second edit:
CI runs one job per (package, arm), and the arm table is `feature_arms_for` in
`xtask/src/ci/classify.rs`. Add an arm when a feature swaps an implementation
the benches execute, not for every feature key; each arm is another pair of
builds and valgrind runs. `cargo xtask tidy self-test` holds every arm to a feature its
package declares.

### The wall-clock tier

`spate-bench`, in [`bench/`](bench/README.md), plus `*_wall.rs` targets in the
crates themselves. Nothing it produces is stored and nothing here is a gate. A
wall-clock number answers "did this change move it" for a specific change on a
machine you control.

```sh
cargo xtask bench list                 # every case, with its flags
cargo xtask bench ab --ref main --replicates 20  # this tree against a reference
cargo xtask bench ab --ref main --package spate-avro       # only one crate's targets
cargo xtask bench arms --head-features spate-json/simd   # two feature arms of this tree
```

`--package` narrows the build as well as the report. Use it while writing or
debugging one crate's cases, and leave it off for the run a change is accepted
on.

Targets follow the same rule as the counted tier: `benches/<name>_wall.rs` plus
a `[[bench]]` with `harness = false`. Without the stanza cargo builds the target
under libtest, which rejects the runner protocol's arguments, and the driver says
so with the stanza to add. Expect that minutes in rather than at the start, since
it builds both legs before it starts either.

When you measure two arms by hand, **interleave them**: every arm once per
repetition, rather than one arm finished before the next starts. Throw the first
pass away. Anything that drifts over a run otherwise lands entirely on whichever
arm goes last, and the first repetition hands one arm the cold-start cost, which
has been large enough here to decide which arm looked faster. Report an interval
and the repetition count beside the value, so a reader can tell a difference from
a spread. `cargo xtask bench ab` and `cargo xtask bench arms` do all of that.

Which of the two you want depends on what the arms are. `bench ab` varies the
tree; `bench arms` varies the Cargo features and holds the tree still, building
each arm into its own directory. **Two `bench run`s and a `bench compare` are
not a substitute for either**: a lone leg calibrates its own iteration count, so
two of them pin two different counts for the same case, and every case that
happens to is dropped. Nothing interleaves them either.

### Criterion

`crates/spate-clickhouse/benches/encode.rs` is a criterion target, outside both
conventions above. `cargo xtask bench check` compiles it and the weekly job runs it.
