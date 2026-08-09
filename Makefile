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
        test-examples \
        check-features check-examples bench-check bench-gungraun \
        bench-gungraun-check bench-list bench-ab bench-compare loom \
        deny attribution \
        supply-chain zizmor shellcheck self-test check-perf-report \
        check-gungraun-benches check-collected-region \
        check-transclusions \
        check-adr adr-new \
        check-changelog changelog-new \
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

# Narrower than `test`, which already collects these: `[[example]] test = true`
# makes every infrastructure-free example a one-test binary whose `main` the
# runner calls, so the assertions each example already carried become a gate.
# Useful while iterating on one example; `test` is the gate.
test-examples: ## Just the examples, run as tests (subset of test)
	cargo nextest run -p spate --all-features --locked -E 'kind(example)'

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
bench-check: ## Every bench target still compiles (release profile, slow)
	cargo bench --no-run --workspace --all-features --locked

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
# which builds every bench target in the workspace.
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

# The wall-clock tier: `spate-bench`, in `bench/`. Nothing here is a gate and
# nothing is stored. A wall-clock number answers "did this change move it",
# which is a question somebody asks about a specific change on a machine they
# control — not something a pull request passes.
# `bench/README.md` and the crate's own docs are the full account.
#
# Plain variables rather than `$(or ...)`, so a command-line `REF=…` still wins
# and a value containing a comma is not read as more arguments. Both legs of a
# comparison are written under the bench cache — `$TMPDIR/spate-bench`, or
# `SPATE_BENCH_CACHE` — never inside the repository, where cargo and git would
# both find them.
REF ?= main
REPS ?= 10
FILTER ?=
FORMAT ?= table

bench-list: ## Every wall-clock bench case, with its flags (FILTER=substr narrows it)
	cargo run -p spate-bench --features driver --locked --bin bench -- list --cases \
		$(if $(FILTER),--filter "$(FILTER)")

# Builds the reference in a detached worktree, builds this tree, and interleaves
# the two. Expect it to take a while: it is two full bench-profile builds before
# the first measurement.
bench-ab: ## Compare this tree against a ref: make bench-ab REF=main REPS=10 FILTER=substr
	cargo run -p spate-bench --features driver --locked --bin bench -- ab "$(REF)" \
		--replicates "$(REPS)" --format "$(FORMAT)" $(if $(FILTER),--filter "$(FILTER)")

# Re-renders two legs a previous run left behind — in another format, say —
# without measuring anything again. `bench-ab` prints the two paths when it
# finishes.
bench-compare: ## Re-render two legs: make bench-compare BASE=dir HEAD=dir FORMAT=markdown
	cargo run -p spate-bench --features driver --locked --bin bench -- compare \
		"$(BASE)" "$(HEAD)" --format "$(FORMAT)"

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

# The report script and perf-label.yml share a one-file contract that no pull
# request can execute end to end (`workflow_run` runs the default branch's
# definition), so the write side runs here instead.
check-perf-report: ## The perf report's flag file stays parseable by perf-label.yml
	./scripts/gungraun-report.sh --self-test

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

# The remark plugin resolves every `file=`/`region=` fence when the site builds
# and throws on a miss. This runs the same resolution straight off disk, which
# matters twice: the site build carries a persistent bundler cache, and the
# `site` job is path-filtered, so "the site built" is a weaker statement than it
# looks. Needs neither cargo nor node.
check-transclusions: ## Every transcluded region a docs page names exists
	./scripts/transclude.sh --check

check-adr: ## The decision records stay consistent with their index
	./scripts/adr.sh --check

# SLUG rather than a positional argument, for the same reason changelog-new
# takes named variables: a bare `make adr-new leader-assignment` reads the slug
# as another target to build.
adr-new: ## Scaffold a decision record: make adr-new SLUG=short-description
	./scripts/adr.sh --new "$(SLUG)"

check-changelog: ## A user-visible change carries a changelog fragment
	./scripts/changelog.sh --check

# TYPE and SLUG rather than positional arguments, because a bare `make
# changelog-new fixed retry-ladder` reads them as two more targets to build.
changelog-new: ## Scaffold a fragment: make changelog-new TYPE=fixed SLUG=short-description
	./scripts/changelog.sh --new "$(TYPE)" "$(SLUG)"

# The examples answer to `crates/spate/tests/examples_{manifest,index}.rs`,
# which the `test` target already runs. Accept an index change with:
#
#     UPDATE_EXAMPLES_INDEX=1 cargo test -p spate --test examples_index

ci-lint: zizmor shellcheck self-test check-perf-report check-gungraun-benches check-collected-region check-adr check-changelog check-transclusions ## Every repository-metadata check

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
# selects them from the changed paths; the bench targets cost a second
# release-profile build and run nightly; `check-examples` is a subset of
# `clippy --all-targets`.
gates: lint check test doctest check-features deny ci-lint ## Everything a pull request must pass
