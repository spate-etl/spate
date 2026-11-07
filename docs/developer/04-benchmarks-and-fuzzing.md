---
description: "The instruments outside cargo xtask ci: the fuzz harness and the counted, allocation and wall-clock benchmark tiers, and how to add a target to each."
---

# Benchmarks and fuzzing

## Fuzzing

`fuzz/` is a [cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz.html)
crate with one libFuzzer target per boundary where the pipeline turns bytes
into records or records into bytes. Each target asserts a property rather
than only the absence of a panic. It is a workspace member, so `--workspace`
compiles it and the root lockfile pins what it builds against.

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

A pull request touching `fuzz/`, `ci.yml`, `fuzz.yml`, `.github/actions/`, the
nightly pin or `xtask/` builds every target with instrumentation and runs none
of them, and so does one touching a workspace crate the harness depends on
directly.
`nightly.yml` fuzzes each for five minutes, set by
`MAX_TOTAL_TIME`, and carries `fuzz/corpus` between nights in a cache entry.
A crash uploads the input as the `fuzz-artifacts` artifact and opens an issue
titled `[bug] A fuzz target found a crashing input`.

## Benchmarks

The tiers below answer different questions. Two of them gate a pull request: the
counted tier and the allocation snapshots.

None of them sweeps this framework's own settings against each other end to end,
and neither does the
[benchmark repository](https://github.com/spate-etl/benchmark), which runs one
fixed pipeline across several frameworks and publishes the
[results](https://spate.kainth.dev/benchmarks). The tiers here measure inside a
single component. A claim no tier here can measure is stated as unmeasured.

### The counted tier

`cargo xtask bench counted` counts instructions under valgrind rather than
measuring wall time, so its numbers are comparable across machines of one
architecture. In CI, each architecture's rows compare its own merge base and
head. It needs Linux, valgrind 3.24 or later, and a `gungraun-runner` at the
version `Cargo.lock` pins for `gungraun`; a mismatch is a hard error. On macOS
the most you can check is that the benches build, with
`cargo xtask bench gungraun --check`.

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
benchmark function call it.** Getting this wrong produces a number rather than
an error. Collection is bounded by a callgrind toggle on the module the
`#[library_benchmark]` macro wraps the function in, and a toggle *flips*
collection rather than forcing it on. Work written inline in that function can
therefore be reshaped by the optimizer until it falls outside the collected
region, and whatever runs while collection happens to be on is counted instead.

`cargo xtask bench region` enforces it from the callgrind profile
rather than from the source: a case must attribute at least 10% of its collected
instructions to the binary under measurement, and must collect at least 1,000 of
them, since a region can also be lost by leaving almost nothing rather than by
leaving the allocator. Observed cases bottom out at 33.35% on x86_64 and
28.67% on arm64. `cargo xtask bench counted`
runs it after the benches, and CI runs it per shard as a *gate*: the counts are
advisory, a bench measuring the allocator is not. `cargo xtask tidy self-test`
holds the guard itself to captured profiles of both shapes, and needs no
valgrind.

Measuring a crate under more than one compiled feature arm *is* a second edit:
CI runs one job per (package, arm, architecture), on Linux x86_64 and aarch64,
and the arm table is `feature_arms_for` in `xtask/src/ci/classify.rs`. Add an
arm when a feature swaps an implementation the benches execute, not for every
feature key; each arm is another pair of builds and valgrind runs on each
architecture. `cargo xtask tidy self-test` holds every arm to a feature its
package declares.

### Allocation snapshots

`cargo xtask bench alloc` runs each `benches/<name>_alloc.rs` target in the
release profile, on the stable release pinned in `.github/toolchains/stable/`,
once per feature arm. It compares what each prints with
`benches/alloc/<name>_alloc.<arm>.<arch>.tsv`, and any difference fails CI.
`--bless` rewrites the files for the host's architecture. It runs on Linux
x86_64 and aarch64 only, and refuses any other host.

[`spate_test_support::alloc`](repo:test-support/src/alloc.rs) says what each
figure counts. Allocations C code makes through `malloc`, as libzstd does, are
not in them; the counted tier's DHAT heap rows include them.

A failing run uploads the files it would have written as the
`alloc-snapshots-<arch>` artifact. A change that moves a count, including a
dependency or toolchain bump, is accepted by committing those files from both
legs on its pull request. Dependabot stops rebasing a pull request once someone
else has pushed a commit to it, so a bump that needed new files is rebased by
hand from then on.

Adding one means naming the file `benches/<name>_alloc.rs`, declaring it as a
`[[bench]]` with `harness = false`, and calling
`spate_test_support::alloc::main_with` from its `main`. The command discovers it
by that name. A case whose pass reads the clock is left out.

### The wall-clock tier

`spate-bench`, in [`bench/README.md`](repo:bench/README.md), plus `*_wall.rs`
targets in the crates themselves. Nothing it produces is stored and nothing here
is a gate. A wall-clock number answers "did this change move it" for a specific
change on a machine you control. CI runs the same comparison weekly on one
hosted runner, `main` against `main` a week earlier, advisory, with its report
in the job summary.

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
under libtest, which rejects the runner protocol's arguments, and the driver
says so with the stanza to add. Expect that minutes in rather than at the start,
since it builds both legs before it starts either.

When you measure two arms by hand, **interleave them**: every arm once per
repetition, rather than one arm finished before the next starts. Throw the first
pass away. Anything that drifts over a run otherwise lands entirely on whichever
arm goes last, and the first repetition hands one arm the cold-start cost, which
has been large enough here to decide which arm looked faster. Report an interval
and the repetition count beside the value, so a reader can tell a difference
from a spread. `cargo xtask bench ab` and `cargo xtask bench arms` do all of
that.

Which of the two you want depends on what the arms are. `bench ab` varies the
tree; `bench arms` varies the Cargo features and holds the tree still, building
each arm into its own directory. **Two `bench run`s and a `bench compare` are
not a substitute for either**: a lone leg calibrates its own iteration count, so
two of them pin two different counts for the same case, and every case that
happens to is dropped. Nothing interleaves them either.
