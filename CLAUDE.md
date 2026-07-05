# etl-rs

High-performance, at-least-once ETL pipeline framework in Rust. Workspace of
publishable crates under `crates/` plus an unpublished `benchmarks/` crate.
The full architecture and its rationale live in `docs/DESIGN.md`; the metric
taxonomy in `docs/METRICS.md`. Read those before changing engine behavior.

## Commands

```sh
cargo check --workspace --all-features
cargo test --workspace --all-features          # unit + integration (no docker)
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all
cargo bench -p etl-core                        # criterion + divan micro benches
RUSTFLAGS="--cfg loom" cargo test -p etl-core --release --lib   # loom models
```

The loom invocation must stay `--lib`: doc tests compiled under `--cfg loom`
construct loom-typed primitives outside a model and abort. The same applies
to any test or module touching loom-aware dependencies outside a model —
tokio (net disappears under the cfg), quanta via the prometheus exporter,
and `AckRef` internals — gate those with `#[cfg(not(loom))]`.

Docker-backed integration tests (testcontainers) are `#[ignore]`d by default;
run them explicitly. MSRV is 1.94 — CI checks it; don't use newer features.

## Invariants (do not break)

- **Source threads never block on a channel send.** Backpressure is
  `try_send` + `Source::pause` + keep polling. A blocked poll loop gets the
  consumer evicted from its group (`max.poll.interval.ms`).
- **The checkpoint tracker stays synchronous and tokio-free** (`etl-core`'s
  `checkpoint` module) — it is loom-tested and must remain so.
- **No connector-crate types in `etl-core` public APIs**, and no
  rdkafka/clickhouse/apache-avro types in any public trait bounds — those are
  0.x dependencies and must not leak into our semver surface.
- **Acks can never block behind data.** The ack path is unbounded/atomic.
- Record error policies are **Skip or Fail only**, always surfaced through
  metrics (`*_dropped_total{reason}` / `*_errors_total{error_type}`).
- All metrics handles are **pre-registered at build time** — never resolve
  metric names/labels on the per-record path.
- Delivery is at-least-once: never commit a source watermark past
  unacknowledged data, including across rebalances and shutdown.

## Commit conventions

Conventional Commits. Scope = crate touched (`etl-core`, `etl-kafka`, ...),
comma-separated for several (`feat(etl-kafka,etl-core): ...`); use
`workspace`, `ci`, `docs`, `examples`, `benchmarks` for non-crate areas.
Messages must make sense to outsiders — no plan/phase references.

## Testing layout

Unit tests inline (`#[cfg(test)]`), integration tests per-crate in `tests/`,
doc tests on public APIs. proptest for tracker/codec invariants, loom for the
sync concurrency primitives, rdkafka MockCluster and clickhouse mocks in
default CI, testcontainers behind the Docker job. Framework users test with
`etl-test` mocks — keep those first-class.
