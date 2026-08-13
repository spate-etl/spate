# Developing

Maintainer and returning-contributor reference: the build, test and benchmark
mechanics.

## Targets

`make help` lists every target, grouped. `make gates` is the pull request bar and
covers formatting, clippy, the type check, the test suite, doctests, the feature
matrix, licences and advisories, and `make ci-lint`, the repository-metadata
checks that read files and need no toolchain.

Verify a gate by its **exit code**. Piped `grep` and `tail` chains report the
status of the last command in the pipeline and have masked failures here. No
target contains a pipe.

CI calls the same targets for lint, type check, doctests, the feature matrix,
licences and every `ci-lint` member. Other jobs spell out invocations of their
own for a coverage run, a container image, Node or a pinned tool, so a green
`make gates` locally does not mean CI has nothing left to say.

These sit outside `gates`, by cost or by dependency:

| Target | Why it is outside |
| --- | --- |
| `make test-docker` | Needs Docker and pulls real images |
| `make loom` | Exhaustive interleaving; minutes, not seconds |
| `make docs` | Needs Node; runs nightly and on documentation changes |
| `make bench-check` | Builds the whole tree again in the release profile |
| `make bench-gungraun` | Needs Linux and valgrind |
| `make bench-gungraun-check` | Proves only that the benches build, not what they count |
| `make bench-ab`, `make bench-arms`, `make bench-list`, `make bench-compare` | Wall clock; never a gate |
| `make attribution` | `THIRD-PARTY.md` is checked nightly and regenerated at release |

Two commands omit `--locked`, which everything else passes because CI does.
`cargo hack --no-dev-deps` rewrites each `Cargo.toml` as it runs and a locked
build refuses; `cargo fmt` resolves nothing, reading only `.rs` files.

`make docs` sets `CI=true`. The client-redirects plugin only registers under it,
so a plain `npm run build` skips redirect validation, and a redirect pointing at
a page you deleted is a hard failure.

## The test suite

Tests run under [cargo-nextest](https://nexte.st), one process per test
concurrently, where `cargo test` runs one binary at a time. Plain
`cargo test --workspace` still works and is many times slower. nextest does not
run doctests; `make doctest` does.

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
skips them. `make test-docker` is what selects them.

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
everything depends on it, and a crate without one selects nothing.

For a change whose reach those paths do not show, such as a refactor moving code
between crates or a dependency swap, a maintainer can label the pull request
`ci: docker`, `ci: loom` or `ci: bench`. They only ever add work; none can
switch a suite off. `make self-test` checks the classifier against the crate
graph.

## Testing conventions

Unit tests inline in a `#[cfg(test)]` module, integration tests in each crate's
`tests/`, doc tests on public APIs. proptest for tracker and codec invariants,
loom for the synchronisation primitives, rdkafka's `MockCluster` and the
ClickHouse mocks for connector behaviour that does not need a container.

Framework users test their pipelines with `spate-test`'s in-memory source and
capture sink. Keep those first-class, and prefer them for reproductions. A test
written against them needs no infrastructure and runs in milliseconds.

One trap worth knowing: `cargo test` runs a binary's tests in one process, and
metric series ownership is process-wide (INV-10). Fixtures therefore need
per-test `pipeline` and `component` labels; a local recorder does not isolate the
claim.

## Benchmarks

The tiers below answer different questions. Only the counted one gates a pull
request.

None of them sweeps this framework's own settings against each other end to end,
and neither does the
[benchmark repository](https://github.com/spate-etl/benchmark), which runs one
fixed pipeline across several frameworks. The tiers here measure inside a single
component. A claim no tier here can measure is stated as unmeasured.

### The counted tier

`make bench-gungraun` counts instructions under valgrind rather than measuring
wall time, so its numbers are comparable across machines. It needs Linux,
valgrind, and a `gungraun-runner` at the version `Cargo.lock` pins for
`gungraun`; a mismatch is a hard error. On macOS the most you can check is that
the benches build, with `make bench-gungraun-check`.

Adding one means naming the file `benches/<something>_gungraun.rs` and declaring
it in the crate's `Cargo.toml` as a `[[bench]]` with `harness = false`.
Nothing else registers it. `scripts/gungraun-benches.sh` discovers it by that
name, and the Makefile target, both CI legs and `scripts/ci-changes.sh` all read
from that one place, so there is no list to add yourself to. Running that script
bare prints what would run. Without the `harness = false` stanza, cargo
auto-discovers the file under the default libtest harness, so the bench compiles
cleanly and fails at run time complaining about arguments.
`make check-gungraun-benches` catches it.

**Put the measured work in a named `#[inline(never)]` function and have the
benchmark function call it.** Getting this wrong produces a number rather than an
error. Collection is bounded by a callgrind toggle on the module the
`#[library_benchmark]` macro wraps the function in, and a toggle *flips*
collection rather than forcing it on. Work written inline in that function can
therefore be reshaped by the optimiser until it falls outside the collected
region, and whatever runs while collection happens to be on is counted instead.
Here that was glibc tearing down the corpus the fixture built: one bench reported
858,925 instructions, every one of them in `malloc_consolidate` and
`unlink_chunk`, with no application frame at all and the same total whether its
corpus held 400 documents or 6,400. Moving the loop behind a named callee took it
to 30,086,540.

`scripts/gungraun-collected-region.sh` enforces it from the callgrind profile
rather than from the source: a case must attribute at least 10% of its collected
instructions to the binary under measurement, and must collect at least 1,000 of
them, since a region can also be lost by leaving almost nothing rather than by
leaving the allocator. Observed cases bottom out at 33.35% on the runner
architecture and 28.67% on arm64. `make bench-gungraun`
runs it after the benches, and CI runs it per shard as a *gate*: the counts are
advisory, a bench measuring the allocator is not. `make check-collected-region`
checks the guard itself against captured profiles of both shapes, and needs no
valgrind.

Measuring a crate under more than one compiled feature arm *is* a second edit:
CI runs one job per (package, arm), and the arm table is `feature_arms_for` in
`scripts/ci-changes.sh`. Add an arm when a feature swaps an implementation the
benches execute, not for every feature key; each arm is another pair of builds
and valgrind runs. `make self-test` checks the table against `cargo metadata`.

### The wall-clock tier

`spate-bench`, in [`bench/`](bench/README.md), plus `*_wall.rs` targets in the
crates themselves. Nothing it produces is stored and nothing here is a gate. A
wall-clock number answers "did this change move it" for a specific change on a
machine you control.

```sh
make bench-list                 # every case, with its flags
make bench-ab REF=main REPS=10  # this tree against a reference
make bench-arms HEAD_FEATURES=spate-json/simd   # two feature arms of this tree
```

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
a spread. `make bench-ab` and `make bench-arms` do all of that.

Which of the two you want depends on what the arms are. `bench-ab` varies the
tree; `bench-arms` varies the Cargo features and holds the tree still, building
each arm into its own directory. **Two `bench run`s and a `bench compare` are
not a substitute for either**: a lone leg calibrates its own iteration count, so
two of them pin two different counts for the same case, and every case that
happens to is dropped. Nothing interleaves them either.

### Criterion

`crates/spate-avro/benches/decode.rs` and
`crates/spate-clickhouse/benches/encode.rs` are criterion targets, outside both
conventions above. `make bench-check` compiles them and the nightly job runs
them.
