# Spate

High-performance, at-least-once ETL pipeline framework in Rust. Workspace of
publishable crates under `crates/` plus an unpublished `benchmarks/` crate.
The full architecture and its rationale live in `docs/DESIGN.md`; the metric
taxonomy in `docs/METRICS.md`; the coordinated-work distribution algorithm in
`docs/user-guide/02-concepts/08-work-assignment.mdx`, which is normative — its
numbered invariants name the property tests that enforce them, so changing the
balancer means changing that page in the same commit. Read those before
changing engine behavior.

`CONTRIBUTING.md` is the contributor-facing statement of the same gates,
invariants and conventions repeated below. The overlap is deliberate — one
audience needs them in every session, the other needs them once with the
reasoning attached — but the two must not drift, so a change to either belongs
in the same commit as the other.

## Commands

**Pass `--locked` — CI does.** Every dependency-resolving cargo call in CI runs
`--locked`, so a command run without it here can silently resolve a different
graph and hide a failure that CI will then find. The one deliberate exception is
`cargo hack --no-dev-deps`, which rewrites each `Cargo.toml` as it runs and so
cannot take the flag.

```sh
cargo check --workspace --all-features --locked
cargo nextest run --workspace --all-features --locked   # unit + integration (no docker)
cargo nextest run -p spate-s3 --all-features --locked     # between edits; --workspace is the final gate
cargo test --workspace --all-features --locked --doc    # nextest does not run doctests
cargo nextest run --profile docker --workspace --all-features --locked --run-ignored ignored-only  # container suites
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo fmt --all
cargo deny --all-features --locked check all   # licences, advisories, bans, sources
cargo bench --no-run --workspace --all-features --locked # the 11 benchmark rigs still compile
cargo bench -p spate-core --locked               # criterion + divan micro benches
RUSTFLAGS="--cfg loom" cargo test -p spate-core --release --lib --locked   # loom models
cargo check -p spate --examples --all-features --locked   # all examples compile
cargo check -p spate-coordination --no-default-features --tests --locked  # feature-off matrix (CI runs --all-features and misses it)
cargo check -p spate --no-default-features --features s3 --locked     # facade s3 without coordination-nats never links async-nats
cargo hack check --workspace --each-feature --no-dev-deps --exclude-features full  # no --locked; see above
./scripts/attribution.sh                       # regenerates THIRD-PARTY.md; nightly + release check it
./scripts/ci-changes.sh --self-test            # the container-suite map still matches the crate graph
zizmor .github/                                # workflow lint; needs GH_TOKEN or it silently skips audits
shellcheck scripts/*.sh                        # CI runs this one
actionlint .github/workflows/*.yml             # LOCAL ONLY — see below. Run it before pushing workflow edits.
cargo run -p spate --example memory_pipeline     # runnable without infrastructure
docker build -f examples/docker/Dockerfile -t spate-pipeline .    # flagship image
```

## CI layout

Two workflows: `ci.yml` gates the pull request, `scheduled.yml` detects the
world changing underneath a static tree. `ci-gate` is the only job that should
ever be a required status check, `scripts/ci-changes.sh` decides which expensive
jobs run, and every action is pinned to a full commit SHA.

The full account — the gate's `always()` trap, the merge-base diff rules, the
Docusaurus build and its `CI=true` requirement, the nextest profiles, and the
coverage split — is
[docs/user-guide/07-reference/ci.mdx](docs/user-guide/07-reference/ci.mdx).
Read it before changing a workflow.

Note `actionlint` runs **locally only**; run it before pushing workflow edits.
Verify gates by **explicit exit code** — piped `grep`/`tail` chains have masked
real failures here.

## Invariants (do not break)

- **Source threads never block on a channel send.** Backpressure is
  `try_send` + `Source::pause` + keep polling. A blocked poll loop gets the
  consumer evicted from its group (`max.poll.interval.ms`).
- **The checkpoint tracker stays synchronous and tokio-free** (`spate-core`'s
  `checkpoint` module) — it is loom-tested and must remain so.
- **No connector-crate types in `spate-core` public APIs**, and no
  rdkafka/clickhouse/apache-avro types in any public trait bounds — those are
  0.x dependencies and must not leak into our semver surface. The **one
  sanctioned exception is the `metrics` facade**: `Meter`, `ComponentLabels`,
  and the re-exported `Counter`/`Gauge`/`Histogram`/`SharedString` are public
  because the framework's instrumentation API *is* that facade. It is
  re-exported from `spate-core` (never depended on directly by connectors) so a
  breaking `metrics` bump is one coordinated change, not per-crate drift.
- **Acks can never block behind data.** The ack path is unbounded/atomic.
- **The sink worker's intake path never awaits outside a `select!` arm.**
  Anything it blocks on must sit in a branch position alongside the
  drain-deadline branch, or the deadline is not polled while it waits and
  shutdown deadlocks. `ShardWorker::dispatch` is deliberately not `async`:
  it parks a sealed batch for a permit instead of awaiting one. `SinkPool::drain`
  force-aborts a worker 2s past the deadline as a backstop, but that loses the
  worker's drain report — it is not a licence to add a blocking await.
- Record error policies are **Skip or Fail only**, always surfaced through
  metrics (`*_dropped_total{reason}` / `*_errors_total{error_type}`).
- All metrics handles are **pre-registered at build time** — never resolve
  metric names/labels on the per-record path.
- **A gauge series has exactly one live owner per process.** Handle structs
  claim their series at construction (`metrics::ownership`); a duplicate handle
  set on the same key becomes a *shadow* that still counts (counters sum) but
  publishes no gauge. Assembly fails on a collision (`BuildError`/`StartError`);
  direct construction logs and shadows. See "Series ownership" in
  `docs/METRICS.md`. A corollary for tests: `cargo test` runs a binary's tests
  in one process, so fixtures must carry per-test `pipeline`/`component` labels
  — a local recorder does not isolate the process-wide claim.
- **All metrics live under the `spate_` umbrella.** The framework owns the
  reserved stage roots (`spate_source_`, `spate_sink_`, …); connector/user families
  register through a `Meter`, which auto-prefixes `spate_<namespace>_` (default
  `custom`) and rejects a namespace that shadows a reserved root — so custom
  series never collide with the taxonomy. Raw-facade metrics are the opt-out for
  names deliberately outside `spate_`.
- Delivery is at-least-once: never commit a source watermark past
  unacknowledged data, including across rebalances and shutdown.

## Commit conventions

Conventional Commits. Scope = crate touched (`spate-core`, `spate-kafka`, ...),
comma-separated for several (`feat(spate-kafka,spate-core): ...`); use
`workspace`, `ci`, `docs`, `examples`, `benchmarks` for non-crate areas.
Messages must make sense to outsiders — no plan/phase references.

Dependabot raises dependency bumps as `chore`, scoped by area —
`chore(workspace)` for Cargo, `chore(ci)` for Actions, `chore(docs)` for the
website's npm tree, `chore(examples)` for the Docker base images. Prefer `fix`
or `feat` for your own commits, so `chore` reads as "a bot bumped a version".

## Testing layout

Unit tests inline (`#[cfg(test)]`), integration tests per-crate in `tests/`,
doc tests on public APIs. proptest for tracker/codec invariants, loom for the
sync concurrency primitives, rdkafka MockCluster and clickhouse mocks in
default CI, testcontainers behind the Docker job. Framework users test with
`spate-test` mocks — keep those first-class.
