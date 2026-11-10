---
description: "Keep the per-record path allocation-free, choose a benchmark tier, add a counted bench, an allocation snapshot or a wall-clock bench, and land the benchmark before the change."
---

# Performance

This page answers one question about a change: did it cost anything?

## Keep the per-record path allocation-free

Do not allocate on the per-record path.
The per-record path is the code a connector runs once for every record or message: a deserializer, an encoder, the operator chain, and a sink writer preparing each message of a batch.

**Why:** the [user guide](../user-guide/README.mdx) promises zero per-record allocations on the pipeline threads.
Hold a sink writer's per-message work to the same rule.

Code that runs once per batch, poll or object may allocate a bounded amount.
The allocation count that grows with the records stays flat.

### Reuse a buffer across calls

Clear a buffer and fill it again for the next record.
The Kafka sink's encoder in [`encoder.rs`](repo:crates/spate-kafka/src/sink/encoder.rs) keeps its message buffer and each header slot between records.
Its allocation snapshot reads zero for every case that encodes a record.

### Borrow from the bytes you were given

A sink batch arrives as `Bytes` frames, and a source batch lends its payloads.
A slice into either costs nothing, and one slice can stand for a variable-length part of a record.
A collection built per record to hold borrowed slices is still an allocation per record.

### Size a collection from a count you already hold

A batch knows its row count before parsing starts, so allocate a list of its messages once, at that size.
When the count comes from the input, cap the capacity at what the remaining bytes could hold.

**Why:** a corrupt count near `u32::MAX` would otherwise ask the allocator for gigabytes and abort the process.

### Resolve metrics handles at build time

Register every metrics handle when the pipeline is built, as [INV-8](../INVARIANTS.md) requires.

## Choose the benchmark tier

Each tier answers a different question:

| Tier | Command | What it answers | Runs on | Gates a pull request |
| --- | --- | --- | --- | --- |
| Counted | `cargo xtask bench counted` | How many instructions a case executes | Linux with valgrind | The region guard gates; the counts are advisory |
| Allocation snapshots | `cargo xtask bench alloc` | How many allocations and bytes a case requests | Linux x86_64 and aarch64 | Yes: any difference fails |
| Wall clock | `cargo xtask bench ab`, `cargo xtask bench arms` | Whether a change moved the time on a machine you control | Unix | No |

Every tier measures inside one component.
None of them sweeps Spate's own settings against each other end to end.
The [benchmark repository](https://github.com/spate-etl/benchmark) runs one fixed pipeline across several frameworks, and publishes the [results](https://spate.kainth.dev/benchmarks).
State a claim that no tier can measure as unmeasured.

## Run the counted tier

The counted tier, `cargo xtask bench counted`, counts instructions under valgrind instead of measuring time.
Its numbers are comparable across machines of one architecture.
In CI, each architecture compares its own merge base and head.

It needs Linux, valgrind 3.24 or later, and a `gungraun-runner` at the version that `Cargo.lock` pins for `gungraun`.
A version mismatch is a hard error.
On macOS, `cargo xtask bench gungraun --check` checks only that the benches build.

### Add a counted bench

Name the file `benches/<something>_gungraun.rs`, and declare it in the crate's `Cargo.toml` as a `[[bench]]` with `harness = false`.
Nothing else registers it.
The bench commands, both CI legs and the CI selector all find it by that name.
Run `cargo xtask bench gungraun` with no argument to print what would run.

Without the `harness = false` stanza, cargo builds the file under the default libtest harness.
The bench then runs no case and exits 0.
The `gungraun-benches` tidy check catches the missing stanza.

### Put the measured work in a named `#[inline(never)]` function

Put the work you measure in a named `#[inline(never)]` function, and have the benchmark function call it.

**Why:** a callgrind toggle bounds collection, on the module that the `#[library_benchmark]` macro wraps the function in.
The toggle flips collection instead of forcing it on.
The optimizer can reshape work written inline in that function until it falls outside the collected region, and then whatever runs while collection is on is counted instead.
The result is a wrong number, never an error.

### Pass the region guard

The region guard, `cargo xtask bench region`, checks each case's callgrind profile.
A case must attribute at least 10% of its collected instructions to the binary under measurement, and must collect at least 1,000 instructions.
A region can be lost by leaving almost nothing, as well as by leaving the allocator, so the guard checks both.
When the guard was added, all forty cases were measured.
The lowest share was 33.35% on x86_64 and 28.67% on aarch64, and a reproduced lost region held 0.47%.

The `bench counted` command runs the guard after the benches.
CI runs it on each shard as a gate, while the counts themselves stay advisory.
The guard's tests run under `cargo xtask tidy self-test`, against captured profiles of both shapes, and need no valgrind.

### Rerun a moved case at one codegen unit

When a counted case moves on a change that did not touch its code, run that case at base and at head again with one codegen unit:

```sh
CARGO_PROFILE_BENCH_CODEGEN_UNITS=1 cargo bench -p spate-core --locked --bench checkpoint_gungraun
```

If base and head then agree, the case did not regress.
The change moved how rustc split the crate.

**Why:** the bench profile builds each crate as 16 codegen units, and whether a call is inlined can depend on which unit each function lands in.
Adding functions anywhere in a crate can move that split.
A change that added functions to `spate-core`'s `feed` and `pipeline` modules moved the `narrow_ticks` case of `checkpoint_gungraun` by 5.2% on aarch64 in CI, because two `crossbeam-channel` `send` calls on the ack path stopped being inlined.
Built at one codegen unit in an aarch64 container, base and head counted within 0.06% of each other.

Leave the bench profile at the default number of codegen units.

**Why:** a downstream build uses the default unless its own manifest sets one, so a bench at one codegen unit would count code that users do not run.

### Add a feature arm only when a feature swaps an implementation

CI runs one counted job per package, feature arm and architecture, on Linux x86_64 and aarch64.
The arms are listed in `feature_arms_for` in [`xtask/src/ci/classify.rs`](repo:xtask/src/ci/classify.rs).
Add an arm when a feature swaps an implementation the benches execute, and not for every feature.

**Why:** each arm adds a pair of builds and valgrind runs on each architecture.

A test under `cargo xtask tidy self-test` checks that every arm names a feature its package declares.

## Run the allocation snapshots

The command `cargo xtask bench alloc` runs each `benches/<name>_alloc.rs` target in the release profile, once per feature arm.
It builds them on the stable release pinned in [`.github/toolchains/stable/`](repo:.github/toolchains/stable/).
It compares each target's output with `benches/alloc/<name>_alloc.<arm>.<arch>.tsv`, and any difference fails CI.
The `--bless` flag rewrites the files for the host's architecture.
The command runs on Linux x86_64 and aarch64 only, and refuses any other host.

Each figure is defined in [`spate_test_support::alloc`](repo:test-support/src/alloc.rs).
The figures leave out allocations that C code makes through `malloc`, as libzstd and librdkafka do.
The counted tier's DHAT rows include them.

### Accept a count that moved

A failing run uploads the files it would have written, as the `alloc-snapshots-<arch>` artifact.
Accept a change that moves a count by committing those files from both architectures to its pull request.
That includes a dependency or toolchain bump.

Dependabot stops rebasing a pull request once someone else has pushed to it.
A bump that needed new files is rebased by hand from then on.

### Add a snapshot

Name the file `benches/<name>_alloc.rs`, declare it as a `[[bench]]` with `harness = false`, and call `spate_test_support::alloc::main_with` from its `main`.
The command finds it by that name.
Leave out a case whose pass reads the clock.

## Run the wall-clock tier

The wall-clock tier is the `spate-bench` harness in [`bench/`](repo:bench/README.md), plus `*_wall.rs` targets in the crates.
It stores nothing and gates nothing.
A wall-clock number answers whether a change moved the time, on a machine you control.
CI's weekly workflow runs the same comparison on one hosted runner, `main` against the `main` of a week earlier, and writes the report to the job summary.

```sh
cargo xtask bench list                                    # every case, with its flags
cargo xtask bench ab --ref main --replicates 20           # this tree against a reference
cargo xtask bench ab --ref main --package spate-avro      # only one crate's targets
cargo xtask bench arms --head-features spate-json/simd    # two feature arms of this tree
```

The `--package` flag narrows the build as well as the report.
Use it while you write or debug one crate's cases, and leave it off for the run a change is accepted on.

### Add a wall-clock bench

Name the file `benches/<name>_wall.rs`, and declare it as a `[[bench]]` with `harness = false`.
Without the stanza, cargo builds the target under libtest, which rejects the harness's arguments.
The driver then names the stanza to add.
Expect that error minutes into a run, because the driver builds both legs before it starts either.

### Interleave the arms you measure by hand

Run every arm once per repetition, and throw the first pass away.

**Why:** a drift over the run otherwise lands on whichever arm goes last.
The first repetition also hands one arm the cold-start cost, which has been large enough here to decide which arm looked faster.

Report an interval and the repetition count beside the value, so a reader can tell a difference from a spread.
Both `cargo xtask bench ab` and `cargo xtask bench arms` do all of this.

### Use `bench ab` for a tree and `bench arms` for features

The `ab` command varies the tree.
The `arms` command varies the Cargo features and holds the tree still, building each arm into a directory of its own.

Two legs measured apart and joined with `cargo xtask bench compare` are no substitute for either.

**Why:** a lone leg calibrates its own iteration count, so two legs can settle on two different counts for one case, and every such case is dropped.
Nothing interleaves the two legs either.

## Measure a change to the per-record path

Read both figures an allocation snapshot reports: allocator calls and requested bytes.
They move independently.
One buffer that grows by doubling can request more bytes than many small allocations of exact size, so a change that cuts the calls can still raise the bytes.

Read the counted tier as well.
It shows the instructions a change adds, such as a second pass over bytes it no longer copies.
Its DHAT rows also see the allocations the snapshots leave out.

Give each shape whose cost scales with a count a case of its own, such as records with and without headers.
A regression in one shape is then not averaged away by the others.

## Land the benchmark before the change

Land a bench, and any seam it needs, in a pull request before the change it measures.

**Why:** the change's pull request then shows the counts moving against a committed baseline.

A bench reaches code outside the public API through a seam behind the crate's `testing` feature.
[Put test-only access behind `testing`](06-connector-crate.md#put-test-only-access-behind-testing) gives the rules a seam follows.

## Related

- [Adding a connector crate](06-connector-crate.md) — the `testing` feature and the bench targets a connector adds.
- [`bench/README.md`](repo:bench/README.md) — the wall-clock harness in full.
- [Spate invariants](../INVARIANTS.md) — INV-8 and the other properties the hot path holds to.
