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

## Reporting something

There are four forms, and the first one is the point of the list: a delivery
guarantee that did not hold. The others are an ordinary bug, a performance
problem, and a proposal. Blank issues are off deliberately — each form asks the
question somebody would have to ask you anyway, and asking it once beats a
round-trip.

Two things the delivery form says before you start, because they are the common
answers and neither is a bug: duplicates after a crash are expected, since replay
re-batches with new boundaries; and records dropped by `ErrorPolicy::Skip` are
counted rather than lost, so check `spate_*_dropped_total{reason}` before
concluding they vanished.

Issues are labelled at triage. `crate:` says where — the same vocabulary as the
commit scopes, so no translation — and `area:` covers what is not a crate.
`delivery-correctness` and `performance` mark the two classes with their own
priority. The whole taxonomy is defined in
[`.github/labels.yml`](.github/labels.yml) rather than in the web UI, so it is
reviewable like anything else; pull requests get their `crate:` and `area:`
labels automatically from the paths they touch.

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

If the change reaches a crate and somebody upgrading would care, it also needs a
**changelog fragment** — a short file under [`changelog.d/`](changelog.d), which
is assembled into the changelog at release time:

```sh
make changelog-new TYPE=fixed SLUG=short-description
```

In practice that means a `feat`, `fix`, `perf`, `revert` or `build` — and
anything carrying `!`, whatever its scope, since that is you declaring a breaking
change. Scoping the commit to one of the areas that is not a crate — `ci`,
`docs`, `examples`, `benchmarks`, `workspace`, `website` — is what earns an
exemption; leaving the scope off does not, and neither does a type this
repository does not recognise. [`changelog.d/README.md`](changelog.d/README.md) has the format and
the conventions, `make check-changelog` is the gate, and there is deliberately
no label to switch it off — the exemption is derived from the type and scope you
write, so the way out is a subject that is true.

Contributions are accepted under Apache-2.0 §5, inbound under the same terms as
outbound. There is no CLA and nothing to sign.

AI tools are welcome here, and [`AI_POLICY.md`](AI_POLICY.md) says what a
contribution has to withstand regardless of how it was produced. The short
version: be able to answer questions about your own change in your own words,
and back a delivery-correctness fix with a failing test rather than an
explanation.

## The invariants

These are the properties the engine is built around. Most changes touch none of
them. A change that does touch one is not automatically wrong — but it needs to
say how the property still holds, and that is the conversation.

They are numbered, and the numbers are the canonical ones from
[`docs/DESIGN.md`](docs/DESIGN.md), where the reasoning behind each lives. Cite
the number in a pull request and everyone is looking at the same property.

- **INV-1 — delivery is at-least-once.** A source watermark is never committed
  past unacknowledged data, including across rebalances and shutdown.
- **INV-2 — source threads never block on a channel send.** Backpressure is
  `try_send` plus `Source::pause` plus continuing to poll. A blocked poll loop
  gets the consumer evicted from its group, which is a worse failure than the
  one it was avoiding.
- **INV-3 — the checkpoint tracker stays synchronous and free of async
  runtimes.** It is loom-tested, and it must stay something loom can model.
- **INV-4 — acks can never block behind data.** The ack path is unbounded and
  atomic.
- **INV-5 — the sink worker's intake path never awaits outside its `select!`.**
  Anything it blocks on has to sit in a branch alongside the drain-deadline
  branch, or the deadline is not polled while it waits and shutdown deadlocks.
- **INV-6 — no connector types in `spate-core`'s public API**, and no 0.x
  dependency types in any public trait bound — those cannot be allowed into our
  semver surface. The `metrics` facade is the one sanctioned exception, because
  the instrumentation API *is* that facade.
- **INV-7 — record error policies are Skip or Fail only**, and both are
  surfaced through metrics rather than only logged. There is deliberately no
  third policy that drops a record without counting it.
- **INV-8 — metrics handles are pre-registered at build time.** Never resolve a
  metric name or label on the per-record path.
- **INV-9 — every metric lives under the `spate_` umbrella.**
- **INV-10 — a gauge series has exactly one live owner per process.**

One documentation page is normative rather than descriptive:
[`docs/user-guide/02-concepts/08-work-assignment.mdx`](docs/user-guide/02-concepts/08-work-assignment.mdx).
Its numbered invariants name the property tests that enforce them, so a change
to the balancer means a change to that page in the same commit.

## Running the gates

Everything CI runs, you can run — and it is the same command, because the
workflows call these targets rather than spelling out invocations of their own.

```sh
make gates      # everything a pull request must pass
make help       # the full list
```

`make gates` covers formatting, clippy, the type check, the test suite,
doctests, the feature matrix, licences and advisories, and the repository's own
consistency checks. A few things sit outside it because they need Docker or
minutes:

```sh
make test-docker   # container-backed suites
make loom          # the concurrency models
make docs          # the documentation site
make bench-check   # every benchmark rig still compiles, in the release profile
make bench-gungraun  # instruction counts, on Linux with valgrind installed
```

`make bench-gungraun` is the odd one out: it counts instructions under valgrind
rather than measuring wall time, which is what makes the number comparable
across machines instead of a property of the one that produced it. It needs
valgrind and a `gungraun-runner` at the same version as the pinned `gungraun`,
so it does not run on macOS at all — CI runs it, and locally the most you can
check is that the benches build.

Adding one is two steps: name the file `benches/<something>_gungraun.rs`, and
declare it in the crate's `Cargo.toml` as a `[[bench]]` with `harness = false`.
Nothing else registers it — `scripts/gungraun-benches.sh` discovers it by that
name, and the Makefile target and both CI legs all read from that one place, so
there is no list of benches to add yourself to. Whether the job runs for a
given pull request is a separate question, answered by a crate list in
`scripts/ci-changes.sh` — a bench in a crate that list does not name is built
and compared only when a maintainer applies `ci: bench`. `./scripts/gungraun-benches.sh` on its own
prints what would run, which is the quickest way to confirm a new bench is
visible. Skipping the `harness = false` stanza is the mistake worth knowing
about: cargo auto-discovers the file anyway, under the default libtest harness,
and the bench then compiles cleanly and fails at run time complaining about
arguments. `make check-gungraun-benches` (part of `make ci-lint`) catches it.

If you changed dependencies, add `make attribution` to regenerate
`THIRD-PARTY.md`. It is checked nightly and regenerated at release rather than
gated on your pull request, so it is welcome but not required.

Three things the targets encode that are worth knowing before you run a cargo
command by hand:

- **Pass `--locked` — CI does.** Without it a command can resolve a different
  dependency graph and hide a failure CI will then find. Every target that
  resolves one passes it.
- **Two commands do not take it.** `cargo hack --no-dev-deps` rewrites each
  `Cargo.toml` as it runs and a locked build refuses; `cargo fmt` resolves
  nothing at all, reading only `.rs` files.
- **The site build needs `CI=true`.** The client-redirects plugin is only
  registered when it is set, so a plain build silently skips redirect validation
  — and a redirect pointing at a page you deleted is a hard failure. `make docs`
  sets it, and also runs the typecheck that CI runs.

Verify a gate by its **exit code**. Piped `grep` and `tail` chains have masked
real failures in this repository more than once, which is why no target contains
a pipe.

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

Docker-backed tests use testcontainers and are `#[ignore]`d by default. `make
test-docker` selects them and uses the docker nextest profile; the default
profile hard-kills a test at 120s, which a cold image pull can exceed, and the
kill reports as a timeout indistinguishable from a real hang.

CI picks the container suites from the paths you changed. If your change is one
whose reach those paths do not show — a refactor moving code between crates, say
— a maintainer can label the pull request `ci: docker` to run them all,
`ci: loom` for the concurrency models, or `ci: bench` for the
instruction-count benches. All three only ever add work; none can switch a
suite off.

`ci: bench` does more than cover a blind spot, because not every crate with
benches is measured automatically. `spate-core` and `spate-avro` select
themselves from their paths; the rest — object-storage packing and framing
among them — are opt-in, since each one costs two builds and two valgrind runs
per pull request. If you are changing something whose instruction count is the
point, add the label; without it the change lands unmeasured.

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

Maintainers cut releases as described in [`RELEASING.md`](RELEASING.md), which is
where the version and tag mechanics live. The one part of it that reaches a
contributor is the changelog fragment described above: releases are assembled
from `changelog.d/`, so the release note for your change is written with the
change and not reconstructed from the log months later.

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
