# Spate

High-performance, at-least-once ETL pipeline framework in Rust. Publishable
crates under `crates/`, plus the unpublished wall-clock benchmark harness in
`bench/`.

Four documents are normative. Read the relevant one before changing what it
governs, because none of them is reconstructable from the code:

| Document | Governs |
| --- | --- |
| [`docs/DESIGN.md`](docs/DESIGN.md) | The architecture, its rationale, and the canonical invariants below |
| [`docs/METRICS.md`](docs/METRICS.md) | The metric taxonomy |
| [`docs/STYLE.md`](docs/STYLE.md) | Everything under `docs/` |
| [`docs/user-guide/02-concepts/08-work-assignment.mdx`](docs/user-guide/02-concepts/08-work-assignment.mdx) | Coordinated work distribution — its numbered invariants name the property tests that enforce them, so changing the balancer means changing that page in the same commit |

[`CONTRIBUTING.md`](CONTRIBUTING.md) states the same gates and invariants for a
human contributor, with the reasoning attached. The overlap is deliberate — one
audience needs them in every session, the other once — and `make ci-lint`
is what keeps the two honest, so prefer fixing a drift it reports over
rewording around it. [`AI_POLICY.md`](AI_POLICY.md) covers what any contribution
has to withstand; the part that most often applies here is that a
delivery-correctness change is judged on a failing test, not on reasoning that
reads well.

## Invariants (do not break)

The numbers are canonical and defined in [`docs/DESIGN.md`](docs/DESIGN.md).
Cite them: "this touches INV-5" is a reviewable claim in a way that restating
the property is not.

Most changes touch none of these. Touching one is not automatically wrong — it
means the change has to say how the property still holds.

- **INV-1 — delivery is at-least-once.** Never commit a source watermark past
  unacknowledged data, including across rebalances and shutdown.
- **INV-2 — source threads never block on a channel send.** Backpressure is
  `try_send` + `Source::pause` + keep polling. A blocked poll loop gets the
  consumer evicted from its group (`max.poll.interval.ms`).
- **INV-3 — the checkpoint tracker stays synchronous and tokio-free**
  (`spate-core`'s `checkpoint` module). It is loom-tested and must remain
  something loom can model.
- **INV-4 — acks can never block behind data.** The ack path is
  unbounded/atomic.
- **INV-5 — the sink worker's intake path never awaits outside a `select!`
  arm.** Anything it blocks on must sit in a branch position alongside the
  drain-deadline branch, or the deadline is not polled while it waits and
  shutdown deadlocks. `ShardWorker::dispatch` is deliberately not `async`: it
  parks a sealed batch for a permit instead of awaiting one. `SinkPool::drain`
  force-aborts a worker `BACKSTOP_GRACE` (1s) past the deadline as a backstop,
  but that loses the worker's drain report — it is not a licence to add a
  blocking await.
- **INV-6 — no connector-crate types in `spate-core` public APIs**, and no
  rdkafka/clickhouse/apache-avro types in any public trait bounds — those are
  0.x dependencies and must not leak into our semver surface. The **one
  sanctioned exception is the `metrics` facade**: `Meter`, `ComponentLabels`,
  and the re-exported `Counter`/`Gauge`/`Histogram`/`SharedString` are public
  because the framework's instrumentation API *is* that facade. It is
  re-exported from `spate-core` (never depended on directly by connectors) so a
  breaking `metrics` bump is one coordinated change, not per-crate drift.
- **INV-7 — record error policies are Skip or Fail only**, always surfaced
  through metrics (`*_dropped_total{reason}` / `*_errors_total{error_type}`).
- **INV-8 — all metrics handles are pre-registered at build time.** Never
  resolve metric names or labels on the per-record path.
- **INV-9 — all metrics live under the `spate_` umbrella.** The framework owns
  the reserved stage roots (`spate_source_`, `spate_sink_`, …); connector and
  user families register through a `Meter`, which auto-prefixes
  `spate_<namespace>_` (default `custom`) and rejects a namespace that shadows a
  reserved root. Raw-facade metrics are the opt-out for names deliberately
  outside `spate_`.
- **INV-10 — a gauge series has exactly one live owner per process.** Handle
  structs claim their series at construction (`metrics::ownership`); a duplicate
  handle set on the same key becomes a *shadow* that still counts (counters sum)
  but publishes no gauge. Assembly fails on a collision
  (`BuildError`/`StartError`); direct construction logs and shadows. See "Series
  ownership" in `docs/METRICS.md`.

## Working loop

After each edit, run the narrow thing — `--workspace` is the final gate, not the
between-edits one:

```sh
make clippy
cargo nextest run -p spate-s3 --all-features --locked   # the crate you touched
```

`make help` lists every target; `make gates` is what a pull request must pass,
and the workflows call the same targets, so a command that works here is the
command CI runs.

Three traps that have cost real time here:

- **Verify by explicit exit code**, everywhere — gates, checklists, all of it.
  Piped `grep`/`tail` chains report the exit status of the last command in the
  pipeline and have masked real failures in this repo more than once. The
  Makefile has no pipes for that reason.
- **Pass `--locked` on any ad-hoc cargo call — CI does.** Without it a command
  can resolve a different graph and hide a failure CI will then find. The one
  exception is `cargo hack --no-dev-deps`, which rewrites each `Cargo.toml` as
  it runs and fails outright with the flag.
- **`actionlint` runs locally only.** CI cannot catch a bad workflow edit before
  you push, so run `actionlint .github/workflows/*.yml` and `make zizmor`
  yourself after touching one.

## CI

`ci.yml` gates the pull request; `scheduled.yml` detects the world changing
underneath a static tree. The full account — the gate's `always()` trap, the
merge-base diff rules, the nextest profiles, the coverage split — is
[`docs/user-guide/07-reference/ci.mdx`](docs/user-guide/07-reference/ci.mdx).
Read it before changing a workflow.

- `ci-gate` is the only job that should ever be a required status check.
- `scripts/ci-changes.sh` picks the expensive jobs from the changed paths. It is
  an ignore-list and **fails closed** on purpose: the `ci: docker`, `ci: loom`
  and `ci: bench` labels can force a suite on, and nothing can force one off.
- Every action is pinned to a full commit SHA.

## Testing

proptest for tracker and codec invariants, loom for the sync primitives, rdkafka
MockCluster and clickhouse mocks in default CI, testcontainers behind the Docker
job. Framework users test with `spate-test` mocks — keep those first-class.

- `make test` does not run doctests; `make doctest` does.
- `cargo test` runs a binary's tests in one process, so fixtures must carry
  per-test `pipeline`/`component` labels. A local recorder does not isolate the
  process-wide gauge claim in INV-10.

## Documentation

`docs/STYLE.md` is normative; the `docs-review` skill
(`.claude/skills/docs-review/SKILL.md`) is the procedure for applying it. Read or
invoke it for any edit under `docs/`.

Two rules break by accident more than the rest:

- **Framework pages are vendor-neutral prose.** Everything under
  `docs/user-guide/` outside `04-connectors/` states its rules in framework
  vocabulary. A connector name may appear only as a link label, a `## Related`
  entry, or inside a `:::note Connector specifics` block — never carrying the
  explanation. Fenced code and YAML are exempt; the prose around them is not.
  `docs/DESIGN.md` sits outside the rule, but it should not grow connector
  *usage* guidance either.
- **Docs read as the present, never as a changelog.** No "now", "recently", "as
  of". If something changed, the page describes what is and the commit says what
  moved.

## Commits and pull requests

Conventional Commits. Scope = crate touched (`spate-core`, `spate-kafka`, …),
comma-separated for several; use `workspace`, `ci`, `docs`, `examples`,
`bench` for non-crate areas. Dependabot raises bumps as `chore` scoped by
area, so prefer `fix` or `feat` for your own commits — `chore` should read as "a
bot bumped a version".

Messages must make sense to outsiders: no plan or phase references, no issue
shorthand that only resolves in this session. For the same reason, **no AI
attribution in git** — no `Co-Authored-By` trailer for a model, no "Generated
with" footer in a pull request body. The message is about the change.

Scope discipline:

- One logical change per commit. Implement the smallest change that solves the
  problem and defer polish to a follow-up.
- Flag a diff growing past ~400 lines rather than letting it land unremarked.
- No drive-by dependency, formatting, or cleanup churn unless the task needs it.

Before opening a pull request, read
[`.github/pull_request_template.md`](.github/pull_request_template.md) and use it
as the body structure. It carries the INV checklist and the gate checklist — tick
those by exit code, not by memory.

## Filing an issue

`gh` **cannot read our issue forms** — it only sees markdown templates, and all
four of ours are `.yml` forms, so `--template` fails regardless of flags
([cli/cli#5865](https://github.com/cli/cli/issues/5865)). Read the relevant
`.github/ISSUE_TEMPLATE/*.yml`, render its fields to markdown yourself (each
field's `label` as a `###` heading, `render:` fields in a fenced block), and post
that:

```sh
gh issue create --body-file <rendered.md> --title '[bug] …' \
  --type Bug --label 'crate: spate-s3'
```

Nothing infers `--type` or `--label`, and **`--label` is silently dropped without
triage permission** ([cli/cli#13589](https://github.com/cli/cli/issues/13589),
closed unfixed) — so check the issue afterwards rather than assuming.

## Done means

- `make gates` green.
- Normative docs changed in the *same commit* as the behaviour they describe.
- A **changelog fragment** under `changelog.d/` whenever the change reaches a
  crate and somebody upgrading would care — `feat`, `fix`, `perf`, `revert` and
  `build`, plus **anything carrying `!` whatever its scope**.
  `make changelog-new TYPE=fixed SLUG=…` scaffolds one, and
  `changelog.d/README.md` has the conventions. Naming no scope is *not* an
  exemption; only naming one of the non-crate areas is. For a fix to a bug that
  was never released, a `Changelog: none` trailer is the honest way out — on the
  commit it excuses, or in the pull request body to excuse the whole thing.
- No unrun or failing tests handed over. If something is blocked, say which part
  and why, rather than narrowing the task to what passed.
