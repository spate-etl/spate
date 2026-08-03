# Every gate the project has. The workflows call these targets rather than
# spelling out cargo invocations of their own, so a command that runs here is
# the command CI runs.
#
# Three rules, all load-bearing:
#
#   1. **No `@` prefixes on anything that runs a gate.** Make echoes each recipe
#      line, so a CI log shows the real cargo invocation rather than a target
#      name. `help` is the single exception; it checks nothing.
#   2. **No pipes.** Make aborts on the first non-zero exit, which is this
#      repo's "verify by explicit exit code" rule mechanised. A `| grep` or
#      `| tail` reports the exit status of the last command in the pipeline.
#   3. **`--locked` on every target that resolves a dependency graph.** Two do
#      not take it: `cargo fmt`, which resolves nothing and reads only `.rs`
#      files, and `cargo hack --no-dev-deps`, which rewrites each manifest as it
#      runs and fails outright with the flag. Both are called out where they
#      happen.
#
# `make help` lists everything. `make gates` is what a pull request must pass.

# Print the help when invoked bare: `make` should tell you what it can do rather
# than guess which of twenty targets you meant.
.DEFAULT_GOAL := help

# Every target is a verb, not a file. Without this a target would be skipped if
# a same-named file ever appeared in the tree.
.PHONY: help fmt fmt-check clippy lint check test doctest test-docker \
        check-features check-examples bench-check bench bench-ab bench-compare bench-gungraun \
        bench-gungraun-check loom \
        deny attribution \
        supply-chain zizmor shellcheck self-test check-labels check-perf-report \
        check-gungraun-benches check-collected-region check-invariants \
        check-results check-bench-compare check-changelog changelog-new \
        ci-lint docs docs-serve gates

##@ Help

help: ## List every target
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage: make <target>\n"} \
		/^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)
	@printf "\n"

##@ Develop

fmt: ## Format the workspace
	cargo fmt --all

fmt-check: ## Check formatting without writing
	cargo fmt --all --check

# `--all-targets` compiles every lib, bin, test, bench and example in the
# workspace, which makes `check-examples` and `bench-check` subsets of it in the
# dev profile. Neither appears in `gates` for that reason.
clippy: ## Lint, warnings denied
	cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

lint: fmt-check clippy ## Formatting and clippy together

check: ## Type-check the workspace
	cargo check --workspace --all-features --locked

##@ Test

test: ## Unit and integration tests, no containers
	cargo nextest run --workspace --all-features --locked

doctest: ## Doc tests — nextest does not run these
	cargo test --workspace --all-features --locked --doc

# Container suites are `#[ignore]`d so a normal run skips them; this is the only
# way to select them. Needs Docker running.
test-docker: ## Container-backed suites (needs Docker)
	cargo nextest run --profile docker --workspace --all-features --locked \
		--run-ignored ignored-only

# Loom explores thread interleavings exhaustively, so it is far too slow for the
# default suite and lives behind a cfg. `--lib` matters: the models are unit
# tests inside the crate, and a `--test` run builds integration targets that the
# cfg leaves empty.
loom: ## Loom concurrency models (slow)
	RUSTFLAGS="--cfg loom" cargo test -p spate-core --release --lib --locked

##@ Matrix

check-features: ## Every feature alone, and the feature-off combinations
	# The one deliberate `--locked` omission in this file. `cargo hack
	# --no-dev-deps` rewrites each Cargo.toml as it runs, which a locked build
	# refuses. Do not "fix" this by adding the flag; it will fail.
	cargo hack check --workspace --each-feature --no-dev-deps --exclude-features full
	# Feature-off combinations that an --all-features build cannot reach: with
	# every feature on, a crate that fails to compile without one still passes.
	cargo check -p spate-coordination --no-default-features --tests --locked
	cargo check -p spate --no-default-features --features s3 --locked

# Narrower and much faster than `clippy`, which already covers it. Useful when
# you are iterating on an example and do not want to lint the workspace.
check-examples: ## The examples still compile (subset of clippy)
	cargo check -p spate --examples --all-features --locked

##@ Bench

# Not in `gates`, and not on the pull request path in CI. It builds the whole
# dependency tree a second time in the bench (release) profile, so it runs
# nightly, where a release build already exists. What that defers is
# release-only breakage — a link error, or a `cfg(not(debug_assertions))` path.
bench-check: ## Every benchmark rig still compiles (release profile, slow)
	cargo bench --no-run --workspace --all-features --locked

# Names the bench target explicitly, as the scheduled job does. Without
# `--bench chain`, cargo also runs the lib's default libtest harness, which
# rejects any forwarded criterion argument.
bench: ## Criterion and divan micro benches
	cargo bench -p spate-core --locked --bench chain

# Instruction counts, not wall time. Runs only on a platform valgrind supports
# — Linux here — with valgrind on PATH and `gungraun-runner` installed at the
# version Cargo.lock pins for the `gungraun` dependency, which
#   cargo metadata --format-version 1 --locked \
#     | jq -r '.packages[] | select(.name == "gungraun") | .version'
# prints and CI derives the same way. A mismatched runner is a hard error,
# not a warning.
#
# The bench list is discovered rather than written here: both CI legs run the
# same script, so a bench cannot be measured on one side of a comparison and
# not the other. `./scripts/gungraun-benches.sh` on its own prints what would
# run. On a machine without valgrind, `make bench-gungraun-check` builds each
# bench without running it — cheaper and more targeted than `bench-check`,
# which builds every rig in the workspace.
#
# The guard runs straight after, over the profiles the run just wrote. A bench
# whose collected region measured the allocator instead of its own code still
# reports a plausible number, so the machine that can run the benches at all is
# the right place to find that out — CI runs the same check per shard.
bench-gungraun: ## Instruction-count benches (needs Linux, valgrind, gungraun-runner)
	./scripts/gungraun-benches.sh --run
	./scripts/gungraun-collected-region.sh

bench-gungraun-check: ## Every instruction-count bench still builds (no valgrind needed)
	./scripts/gungraun-benches.sh --check

# An A/B sweep on the machine in front of you: two or more arms of the
# object-storage backfill, interleaved, with a discarded priming pass. See
# `docs/benchmarks/methodology.mdx` for why all three matter and what skipping
# them costs.
#
# Defaults are plain variables rather than `$(or ...)`, which cannot express a
# comma-separated default at all: make reads `$(or $(ARMS),none,gzip,zstd)` as
# four arguments and returns `none`, so the target silently measured one arm
# and called it an A/B. A command-line `ARMS=…` still wins over `?=`.
ARMS ?= none,gzip,zstd
REPS ?= 5
BENCH ?= s3_backfill_ab

# Only `s3_backfill` is driven here, and the parameter it used to take was a
# lie: driving another rig needs that rig to read `CODECS`, `REPS`, `BENCH` and
# `RESULTS`, and no other bin in the crate reads any of them —
# `s3_backfill_coordinated`, the nearest neighbour, takes `CODEC` singular and
# hardcodes its own report name. Add a rig here when it grows that contract,
# not before.
#
# `cargo run` rather than a hand-built path: `./target/release/…` is wrong the
# moment `CARGO_TARGET_DIR` is set, and the failure is silent — an older binary
# at that path runs and is stamped with today's commit, which is exactly the
# misattribution this whole tier is trying to avoid.
#
# GIT_COMMIT is deliberately *not* set here. The report layer already derives
# the same value the same way when it is unset, so exporting it would add
# nothing — and it would clobber a caller's value, which is the one case that
# matters: a comparison of two builds has to name each build's commit itself.
#
#   make bench-ab ARMS=none,gzip REPS=5
# BENCH_TRIGGER=dispatched is what makes the output of this target
# unpublishable, and it is set here rather than left to the caller because this
# is the one target that produces that shape. An A/B sweep answers "did this
# change move it"; it is not a recording, and the arms are not the configuration
# anything ships at. Writing to gitignored `tuning/` keeps it out of the tree by
# path; the trigger keeps it out by rule, so copying a line into `results/` by
# hand fails `make check-results` rather than quietly publishing a laptop
# number.
bench-ab: ## Interleaved A/B of the s3 backfill: make bench-ab ARMS=… REPS=…
	mkdir -p benchmarks/tuning
	BENCH_TRIGGER=dispatched \
	  CODECS="$(ARMS)" REPS="$(REPS)" BENCH="$(BENCH)" \
	  RESULTS=benchmarks/tuning/s3_backfill-ab.jsonl \
	  cargo run --release -p benchmarks --bin s3_backfill --locked
	echo "wrote benchmarks/tuning/s3_backfill-ab.jsonl (gitignored, never publishable)"

# Renders two result files as Markdown: what moved, what did not, what is
# reported but never judged, and what could not be paired. BASE and HEAD are the
# two legs' JSONL.
#
#   make bench-compare BASE=before.jsonl HEAD=after.jsonl
#
# Both are required. The script names which of the two is unusable rather than
# failing on an empty path, so no `@`-prefixed guard is needed here — the rule
# at the top of this file keeps `help` as the only recipe that hides its
# commands.
bench-compare: ## Compare two benchmark result files: make bench-compare BASE=… HEAD=…
	./scripts/bench-compare.sh "$(BASE)" "$(HEAD)" "$(BASE_LABEL)" "$(HEAD_LABEL)"

##@ Supply chain

deny: ## Licences, advisories, bans, sources
	cargo deny --all-features --locked check all

attribution: ## Regenerate THIRD-PARTY.md
	./scripts/attribution.sh

supply-chain: deny attribution ## Both of the above

##@ Repository

zizmor: ## Audit the workflows
	# Needs GH_TOKEN, or the online audits are skipped silently rather than
	# failing — a clean run without it does not mean what it looks like.
	zizmor --persona=regular .github/

shellcheck: ## Lint the shell scripts
	shellcheck scripts/*.sh

self-test: ## The CI change classifier still matches the crate graph
	./scripts/ci-changes.sh --self-test

check-labels: ## Every referenced label is a defined label
	./scripts/check-labels.sh

# The report script and perf-label.yml share a one-file contract that no pull
# request can execute end to end (`workflow_run` runs the default branch's
# definition), so the write side runs here instead.
check-perf-report: ## The perf report's flag file stays parseable by perf-label.yml
	./scripts/gungraun-report.sh --self-test

check-invariants: ## The invariant lists still agree
	./scripts/check-invariants.sh

# A `*_gungraun.rs` without a `[[bench]] harness = false` stanza is still
# auto-discovered by cargo — under the default libtest harness, which rejects
# gungraun's arguments. It compiles and dies at run time complaining about the
# wrong thing, so the manifest is checked here rather than found in a CI log.
check-gungraun-benches: ## Every gungraun bench declares a harness-free target
	./scripts/gungraun-benches.sh --self-test

# The guard that rejects a bench measuring the C runtime instead of itself.
# Its rule is only exercised where valgrind runs, so the fixtures — real
# profiles, captured under valgrind on Linux — are what holds it to its word
# everywhere else.
check-collected-region: ## The degenerate-region guard still recognises both shapes
	./scripts/gungraun-collected-region.sh --self-test

# The site draws every record under `benchmarks/results/` without knowing which
# machine was busy, so a number measured in CI renders identically to one taken
# on a quiet host. The record's trigger is what bars the first from being
# committed. CI runs this same target on every pull request — a bar that held
# only for whoever remembered to run the gates would not be one.
check-results: ## No committed benchmark record carries a trigger that bars publication
	./scripts/check-results-publishable.sh
	./scripts/check-results-publishable.sh --self-test

# The comparator renders a table off a pairing, and its worst failure is a
# well-formed table built on the wrong one — an answer that looks right. No
# amount of reading catches that, so the fixtures assert the wrong answers
# rather than the happy path.
check-bench-compare: ## The A/B comparator still reports every unpairable input
	./scripts/bench-compare.sh --self-test

check-changelog: ## A user-visible change carries a changelog fragment
	./scripts/changelog.sh --check

# TYPE and SLUG rather than positional arguments, because a bare `make
# changelog-new fixed retry-ladder` reads them as two more targets to build.
changelog-new: ## Scaffold a fragment: make changelog-new TYPE=fixed SLUG=short-description
	./scripts/changelog.sh --new "$(TYPE)" "$(SLUG)"

ci-lint: zizmor shellcheck self-test check-labels check-perf-report check-gungraun-benches check-collected-region check-invariants check-results check-bench-compare check-changelog ## Every repository-metadata check

##@ Docs

# `CI=true` is load-bearing: the client-redirects plugin only registers when it
# is set, so a build without it silently skips redirect validation and a broken
# redirect reaches the deployed site. GitHub Actions sets CI itself, so this
# matters locally rather than in the workflow.
docs: ## Build the documentation site the way CI does
	cd website && npm ci
	cd website && npm run typecheck
	cd website && CI=true npm run build

docs-serve: ## Serve the site locally with hot reload
	cd website && npm start

##@ Gates

# What a pull request has to pass.
#
# Absent on purpose: container suites and loom need Docker or minutes and CI
# selects them from the changed paths; the benchmark rigs cost a second
# release-profile build and run nightly; `check-examples` is a subset of
# `clippy --all-targets`.
gates: lint check test doctest check-features deny ci-lint ## Everything a pull request must pass
