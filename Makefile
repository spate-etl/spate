# Editing rules: no `@` prefix on a gate, so the log shows the real invocation
# (`help` is the exception). No pipes; a pipeline reports the last command's
# exit status and masks a failure. `--locked` wherever a dependency graph is
# resolved.

.DEFAULT_GOAL := help

.PHONY: help fmt fmt-check clippy lint check test doctest doc test-docker \
        test-examples \
        check-features check-examples bench-check bench-gungraun \
        bench-gungraun-check bench-list bench-ab bench-arms bench-compare loom \
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

clippy: ## Lint, warnings denied
	cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

lint: fmt-check clippy ## Formatting and clippy together

check: ## Type-check the workspace
	cargo check --workspace --all-features --locked

##@ Test

test: ## Unit and integration tests, no containers
	cargo nextest run --workspace --all-features --locked

doctest: ## Doc tests, which nextest does not run
	cargo test --workspace --all-features --locked --doc

doc: ## Rustdoc as the site builds it, warnings denied
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features --locked

test-examples: ## Just the examples, run as tests (subset of test)
	cargo nextest run -p spate --all-features --locked -E 'kind(example)'

test-docker: ## Container-backed suites (needs Docker)
	cargo nextest run --profile docker --workspace --all-features --locked \
		--run-ignored ignored-only

# `--lib` matters: the models are unit tests inside the crate, and a `--test`
# run builds integration targets the cfg leaves empty.
loom: ## Loom concurrency models (slow)
	RUSTFLAGS="--cfg loom" cargo test -p spate-core --release --lib --locked

##@ Matrix

check-features: ## Every feature alone, the feature-off combinations, and every target on the default set
	# `cargo hack --no-dev-deps` rewrites each Cargo.toml as it runs, which a
	# locked build refuses. Do not add `--locked`; it fails.
	cargo hack check --workspace --each-feature --no-dev-deps --exclude-features full
	# Stripping dev-dependencies drops test and bench targets, so the run above
	# reaches no test target in any crate. These two build them, on the axes it
	# covers for the library: features off, then the default set.
	cargo check -p spate-coordination --no-default-features --tests --locked
	# Last, because `--no-dev-deps` restores each Cargo.toml only when it is
	# finished and a locked build reads what is on disk.
	cargo check --workspace --all-targets --locked

check-examples: ## The examples still compile (subset of clippy)
	cargo check -p spate --examples --all-features --locked

##@ Bench

bench-check: ## Every bench target still compiles (release profile, slow)
	cargo bench --no-run --workspace --all-features --locked

bench-gungraun: ## Instruction-count benches (needs Linux, valgrind, gungraun-runner)
	./scripts/gungraun-benches.sh --run
	./scripts/gungraun-collected-region.sh

bench-gungraun-check: ## Every instruction-count bench still builds (no valgrind needed)
	./scripts/gungraun-benches.sh --check

# The wall-clock tier. `bench/README.md` is the account.
#
# Plain variables, not `$(or ...)`, so a command-line `REF=` still wins and a
# value containing a comma is not read as more arguments.
REF ?= main
REPS ?= 20
# A space-separated list, one `--package` per name: the driver's flag repeats
# rather than splitting a value.
PACKAGE ?=
FILTER ?=
FORMAT ?= table
BASE_FEATURES ?=
HEAD_FEATURES ?=

bench-list: ## Every wall-clock bench case, with its flags (PACKAGE=crate, FILTER=substr narrow it)
	cargo run -p spate-bench --features driver --locked --bin bench -- list --cases \
		$(foreach p,$(PACKAGE),--package "$(p)") $(if $(FILTER),--filter "$(FILTER)")

bench-ab: ## Compare this tree against a ref: make bench-ab REF=main REPS=20 PACKAGE=crate FILTER=substr
	cargo run -p spate-bench --features driver --locked --bin bench -- ab "$(REF)" \
		--replicates "$(REPS)" --format "$(FORMAT)" \
		$(foreach p,$(PACKAGE),--package "$(p)") $(if $(FILTER),--filter "$(FILTER)")

bench-arms: ## Compare two feature arms: make bench-arms HEAD_FEATURES=pkg/feat PACKAGE=crate FILTER=substr
	cargo run -p spate-bench --features driver --locked --bin bench -- arms \
		--base-features "$(BASE_FEATURES)" --head-features "$(HEAD_FEATURES)" \
		--replicates "$(REPS)" --format "$(FORMAT)" \
		$(foreach p,$(PACKAGE),--package "$(p)") $(if $(FILTER),--filter "$(FILTER)")

bench-compare: ## Re-render two legs: make bench-compare BASE=dir HEAD=dir FORMAT=markdown
	cargo run -p spate-bench --features driver --locked --bin bench -- compare \
		"$(BASE)" "$(HEAD)" --format "$(FORMAT)"

##@ Supply chain

deny: ## Licenses, advisories, bans, sources
	cargo deny --all-features --locked check all

attribution: ## Regenerate THIRD-PARTY.md
	./scripts/attribution.sh

supply-chain: deny attribution ## Both of the above

##@ Repository

zizmor: ## Audit the workflows
	# Needs GH_TOKEN. Without it the online audits are skipped instead of
	# failing, so a clean run does not mean what it looks like.
	zizmor --persona=regular .github/

shellcheck: ## Lint the shell scripts
	shellcheck scripts/*.sh

self-test: ## The CI change classifier still matches the crate graph
	./scripts/ci-changes.sh --self-test

check-perf-report: ## The perf report's flag file stays parseable by perf-label.yml
	./scripts/gungraun-report.sh --self-test

check-gungraun-benches: ## Every gungraun bench declares a harness-free target
	./scripts/gungraun-benches.sh --self-test

check-collected-region: ## The degenerate-region guard still recognizes both shapes
	./scripts/gungraun-collected-region.sh --self-test

# The site build resolves every `file=`/`region=` fence behind a persistent
# bundler cache, so a green build can miss one. This resolves them off disk.
check-transclusions: ## Every transcluded region a docs page names exists
	./scripts/transclude.sh --check

check-adr: ## The decision records stay consistent with their index
	./scripts/adr.sh --check

adr-new: ## Scaffold a decision record: make adr-new SLUG=short-description
	./scripts/adr.sh --new "$(SLUG)"

check-changelog: ## A user-visible change carries a changelog fragment
	./scripts/changelog.sh --check

changelog-new: ## Scaffold a fragment: make changelog-new TYPE=fixed SLUG=short-description
	./scripts/changelog.sh --new "$(TYPE)" "$(SLUG)"

# Accept an examples-index change (the `test` target checks it):
#
#     UPDATE_EXAMPLES_INDEX=1 cargo test -p spate --test examples_index --locked

ci-lint: zizmor shellcheck self-test check-perf-report check-gungraun-benches check-collected-region check-adr check-changelog check-transclusions ## Every repository-metadata check

##@ Docs

# `CI=true` is required: the client-redirects plugin only registers when it is
# set, so a build without it skips redirect validation and a broken redirect
# reaches the deployed site.
docs: ## Build the documentation site the way CI does
	cd website && npm ci
	cd website && npm run typecheck
	cd website && CI=true npm run build

docs-serve: ## Serve the site locally with hot reload
	cd website && npm start

##@ Gates

gates: lint check test doctest doc check-features deny ci-lint ## Everything a pull request must pass
