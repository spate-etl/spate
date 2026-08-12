# Spate

High-performance, at-least-once ETL pipeline framework in Rust. Publishable
crates under `crates/`, plus the unpublished wall-clock benchmark harness in
`bench/`.

[`CONTRIBUTING.md`](CONTRIBUTING.md) is the contributor-facing entry point and
[`DEVELOPING.md`](DEVELOPING.md) carries the build, test and benchmark mechanics
in full — reach for the latter when a target, a profile or a bench convention is
what you need. [`AI_POLICY.md`](AI_POLICY.md) covers what any contribution has to
withstand;
the part that most often applies here is that a delivery-correctness change is
judged on a failing test, not on reasoning that reads well.

## Invariants (do not break)

The engine's invariants are numbered and stated in full in
[`docs/INVARIANTS.md`](docs/INVARIANTS.md), which is the only place they are
stated at all. **Read it before changing engine behaviour.** It is seventy lines,
it is where any exception to a property is recorded, and nothing here substitutes
for it.

Most changes touch none of them. Touching one is not automatically wrong — it
means the change has to say how the property still holds. Cite the number when it
does: "this touches INV-5" is a reviewable claim in a way that restating the
property is not, and `.github/pull_request_template.md` asks per number.

## Working loop

After each edit, run the narrow thing — `--workspace` is the final gate, not the
between-edits one:

```sh
make clippy
cargo nextest run -p spate-s3 --all-features --locked   # the crate you touched
```

`make help` lists every target; `make gates` is what a pull request must pass.
CI calls the same targets for lint, type check, doctests, the feature matrix,
licences and every `ci-lint` member — but other jobs spell out invocations of
their own, so green gates locally is necessary and not sufficient.

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

## Testing

proptest for tracker and codec invariants, loom for the sync primitives, rdkafka
MockCluster and clickhouse mocks in default CI, testcontainers behind the Docker
job. Framework users test with `spate-test` mocks — keep those first-class.

- `make test` does not run doctests; `make doctest` does.
- `cargo test` runs a binary's tests in one process, so fixtures must carry
  per-test `pipeline`/`component` labels. A local recorder does not isolate the
  process-wide gauge claim in INV-10.

## Comments

A comment says what the code does and what a caller may rely on. Why it is this
way and not the alternative belongs in the commit message, where it is dated and
attached to the diff — in the tree it becomes an argument the reader has to
finish before reaching the description.

- **A module header names what the module provides** and its role in the crate.
  Not the dependency it replaces, not the failure that motivated it.
- **State a property as a contract, not as a verdict.** "The same seed yields the
  same stream on any build" is something a caller can use; "a pinned generator
  cannot drift" is the closing line of a decision.
- **Constraints on use stay** — not cryptographic, not cancel-safe, panics on X.
  That is the reader's business.
- **Guardrails stay**: a comment explaining why a line must not be "simplified"
  is preventing the next edit, not narrating the last one.
- **Present tense.** No "now", "previously", "used to". If it changed, the
  comment describes what is.

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
  `docs/adr/` sits outside the rule — a decision about a connector cannot be
  stated neutrally without becoming a different decision — but it should not
  grow connector *usage* guidance either.
- **Docs read as the present, never as a changelog.** No "now", "recently", "as
  of". If something changed, the page describes what is and the commit says what
  moved. The one exception is `docs/adr/`, which is a historical log by
  construction — see below.

Decision records live in `docs/adr/`, one file per decision, and are the only
place under `docs/` that reads as history. Scaffold one with
`make adr-new SLUG=…`. Two things about them differ from everything else here:
an **accepted record is immutable** — a changed decision is a *new* record that
supersedes the old one, never an edit to it — and a decision only earns a record
if it affects structure, a key quality attribute, or is hard to reverse.
`docs/adr/_template.md` states both rules in full and is normative;
`make check-adr` holds the mechanical half.

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
