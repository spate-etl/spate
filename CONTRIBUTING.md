# Contributing

The most useful contribution this project can receive is one that proves a
delivery guarantee wrong. At-least-once is the promise everything else is
arranged around, and a framework that quietly loses records is worth less than
no framework at all — so a report or a failing test that shows the watermark
advancing past unacknowledged data goes to the front of the queue, ahead of
any feature.

After that: connectors, because the abstractions exist so third parties can
write them; and anything that makes the engine measurably faster without
weakening what it promises.

## How changes land

Fork the repository and open a pull request against `main`. That is the only
route — nobody pushes to `main` directly, including the maintainers, and the
branch rules enforce it.

CI on a pull request from a fork waits for an explicit approval before it runs.
That is deliberate: a workflow runs as it exists in the pull request, so an
unreviewed run is an unreviewed change to what CI proves. It costs you one
round-trip and it is not a comment on your change.

Commits follow [Conventional Commits](https://www.conventionalcommits.org),
scoped to the crate touched — `fix(spate-kafka): …`, comma-separated for
several, and `workspace`, `ci`, `docs`, `examples` or `benchmarks` for the
areas that are not crates. Breaking changes carry `!`. Messages should make
sense to somebody who was not in the conversation: say what changed and why,
not which iteration of a plan it belongs to.

Contributions are accepted under Apache-2.0 §5, inbound under the same terms as
outbound. There is no CLA and nothing to sign.

## The invariants

These are the properties the engine is built around. Most changes touch none of
them. A change that does touch one is not automatically wrong — but it needs to
say how the property still holds, and that is the conversation. The reasoning
behind each lives in [`docs/DESIGN.md`](docs/DESIGN.md).

- **Delivery is at-least-once.** A source watermark is never committed past
  unacknowledged data, including across rebalances and shutdown.
- **Source threads never block on a channel send.** Backpressure is `try_send`
  plus `Source::pause` plus continuing to poll. A blocked poll loop gets the
  consumer evicted from its group, which is a worse failure than the one it was
  avoiding.
- **The checkpoint tracker stays synchronous and free of async runtimes.** It is
  loom-tested, and it must stay something loom can model.
- **Acks can never block behind data.** The ack path is unbounded and atomic.
- **The sink worker's intake path never awaits outside its `select!`.** Anything
  it blocks on has to sit in a branch alongside the drain-deadline branch, or
  the deadline is not polled while it waits and shutdown deadlocks.
- **No connector types in `spate-core`'s public API**, and no 0.x dependency
  types in any public trait bound — those cannot be allowed into our semver
  surface. The `metrics` facade is the one sanctioned exception, because the
  instrumentation API *is* that facade.
- **Record error policies are Skip or Fail only**, and both are surfaced through
  metrics rather than only logged. There is deliberately no third policy that
  drops a record without counting it.
- **Metrics handles are pre-registered at build time.** Never resolve a metric
  name or label on the per-record path.
- **Every metric lives under the `spate_` umbrella**, and a gauge series has
  exactly one live owner per process.

One documentation page is normative rather than descriptive:
[`docs/user-guide/02-concepts/08-work-assignment.mdx`](docs/user-guide/02-concepts/08-work-assignment.mdx).
Its numbered invariants name the property tests that enforce them, so a change
to the balancer means a change to that page in the same commit.

## Running the gates

Everything CI runs, you can run. **Pass `--locked` — CI does.** A command
without it can resolve a different dependency graph and hide a failure that CI
will then find.

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo nextest run --workspace --all-features --locked    # unit + integration
cargo test --workspace --all-features --locked --doc     # nextest skips doctests
cargo check -p spate --examples --all-features --locked
```

If you changed dependencies:

```sh
cargo deny --all-features --locked check all   # licences, advisories, bans, sources
./scripts/attribution.sh                       # regenerates THIRD-PARTY.md; nightly + release check it
```

If you changed the docs site or any page under `docs/`:

```sh
cd website && npm ci && CI=true npm run build
```

**The `CI=true` matters.** The client-redirects plugin is only registered when
it is set, so a plain build silently skips redirect validation — and a redirect
pointing at a page you deleted is a hard failure that would otherwise surface
only on the pull request.

A few more, for the changes that need them:

```sh
cargo nextest run --profile docker --workspace --all-features --locked --run-ignored ignored-only
RUSTFLAGS="--cfg loom" cargo test -p spate-core --release --lib --locked
cargo hack check --workspace --each-feature --no-dev-deps --exclude-features full
```

The loom invocation must stay `--lib`: doc tests compiled under `--cfg loom`
construct loom-typed primitives outside a model and abort. `cargo hack
--no-dev-deps` is the one command that cannot take `--locked`, because it
rewrites each `Cargo.toml` as it runs.

Verify a gate by its **exit code**. Piped `grep` and `tail` chains have masked
real failures in this repository more than once.

The MSRV is **1.94** and CI checks it, so nothing newer than that.

### Two things about the test suite

Tests run under [cargo-nextest](https://nexte.st), which runs one process per
test concurrently where `cargo test` runs one binary at a time. Plain `cargo
test --workspace` still works and is many times slower.

`--all-features` turns on `spate-kafka/tls`, which compiles OpenSSL from source.
Drop it when you are not touching the TLS surface.

**On macOS, every freshly linked binary stalls for tens of seconds at 0% CPU on
its first exec** while Gatekeeper scans it. Across this workspace that alone
costs about half an hour per edit-test cycle. Add your terminal to *System
Settings → Privacy & Security → Developer Tools* to exempt it.

Docker-backed tests use testcontainers and are `#[ignore]`d by default. Run them
with `--profile docker`; the default profile hard-kills a test at 120s, which a
cold image pull can exceed, and the kill reports as a timeout indistinguishable
from a real hang.

## Testing conventions

Unit tests inline in a `#[cfg(test)]` module, integration tests in each crate's
`tests/`, doc tests on public APIs. proptest for tracker and codec invariants,
loom for the synchronisation primitives, rdkafka's `MockCluster` and the
ClickHouse mocks for connector behaviour that does not need a container.

Framework users test their pipelines with `spate-test`'s in-memory source and
capture sink — keep those first-class, and prefer them for reproductions. A
test written against them needs no infrastructure and runs in milliseconds.

One trap worth knowing: `cargo test` runs a binary's tests in one process, and
metric series ownership is process-wide. Test fixtures therefore need per-test
`pipeline` and `component` labels; a local recorder does not isolate the claim.

## Documentation

Documentation lives in `docs/` and the site renders that tree in place, so a
docs change is a change to the published site. The structure, prose and voice
rules are in [`docs/STYLE.md`](docs/STYLE.md).

**The one rule to know before writing a line:** framework pages
(`docs/user-guide/`, everything outside `04-connectors/`) are vendor-neutral
prose. A connector name belongs in a link, a `## Related` entry, or a
`:::note Connector specifics` pointer block — never in the explanation itself,
which is stated in framework vocabulary and belongs to every connector equally.
Vendor mechanisms, setting keys and tuning numbers live on the connector's own
page, once. Code and YAML blocks are exempt: a config example has to name a
real tag. `docs/STYLE.md` § 1 has the full rule and its exemptions.

The boundary is enforced in review — the rule has judgement at its edges, so
there is no lint gate for it. Before you push, run
`cd website && npm run build`: that one *is* gated, and it is what catches a
link you broke by moving a page.

**Benchmark numbers are never hand-written into the docs.** They come from a
versioned record emitted by the rigs in `benchmarks/`, and the site reads those
records. If a change makes something faster, say so in the pull request and it
gets measured — on reference hardware, under the published protocol, because a
number from a busy laptop is not comparable to the ones already published. The
methodology lives in
[`docs/benchmarks/methodology.mdx`](docs/benchmarks/methodology.mdx) and the
[benchmark repository](https://github.com/spate-etl/benchmark).

## Releases

Maintainers cut releases as described in [`RELEASING.md`](RELEASING.md) —
contributors never need it, but that is where the version, tag and changelog
mechanics live.

## Reporting a vulnerability

Privately, through
[GitHub's advisory flow](https://github.com/spate-etl/spate/security/advisories/new),
never as a public issue. [`SECURITY.md`](SECURITY.md) has the scope and the
response times.

---

One note on history: this repository was published from a private development
history. Commits before the first release reference pull requests by number
(`(#81)`, `closes #76`) that belonged to the pre-publication repository, and
those numbers do not resolve here.
