# spate-bench

The wall-clock A/B benchmark harness for the
[Spate](https://github.com/spate-etl/spate) framework. **Not published** — it
exists inside the repository to measure it, and is `publish = false`.

Benchmarks here answer one question — *did this change move it* — by measuring
two builds against each other in one sitting, on one machine. Nothing is stored:
a comparison is produced, read, and thrown away. Nothing here is a gate, and no
CI job runs it; the instruction-count tier is what gates a pull request, because
counting is comparable across shared machines and timing is not.

```sh
make bench-list                              # every case, with its flags
make bench-ab REF=main REPS=10               # compare this tree against a ref
make bench-ab REF=main FILTER=decode         # only cases whose id contains this
make bench-arms HEAD_FEATURES=spate-json/simd   # compare two feature arms
make bench-compare BASE=dir HEAD=dir FORMAT=markdown
```

`bench-ab` builds the reference in a detached worktree, builds this tree, and
interleaves the two — expect two full bench-profile builds before the first
measurement. `bench-arms` is the same comparison over the other axis: one tree,
two feature sets, each arm in its own build directory. Both legs are written
under `$TMPDIR/spate-bench`, or `SPATE_BENCH_CACHE` when that is set, never
inside the repository.

Two `bench run`s and a `bench compare` are not a substitute for either. A single
leg has no partner to inherit an iteration count from, so it calibrates its own;
two of them pin two counts for the same case, and the comparator drops every
case that happens to — leaving an empty table and a non-zero exit. Nor does
anything interleave them.

Targets live at `crates/<pkg>/benches/<name>_wall.rs`, and `make bench-list` is
what says which exist — the case list comes from the compiled target rather than
from a manifest, which is what stops the list and the run ever disagreeing.

## Where things are written down

The crate docs are the account of how the tier works — what is measured, what
makes two legs comparable, how a difference is decided, and the rules a case has
to satisfy. Each rule sits with the code that enforces it:

```sh
cargo doc -p spate-bench --all-features --no-deps --open
```

Each bench target's own `//!` header is the account of what *that* target
measures and the traps particular to it. `benches/selftest_wall.rs` carries every
shape the case builder supports, and is what the A/A acceptance run drives:
`make bench-ab REF=HEAD REPS=6` compares it against itself and must flag nothing.
