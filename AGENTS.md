# Spate

High-performance, at-least-once ETL pipeline framework in Rust. Publishable
crates under `crates/`, plus the unpublished wall-clock benchmark harness in
`bench/`.

[`CONTRIBUTING.md`](CONTRIBUTING.md) is the contributor-facing entry point.
[`DEVELOPING.md`](DEVELOPING.md) carries the build, test and benchmark mechanics
in full: read it for a target, a profile or a bench convention.
[`AI_POLICY.md`](AI_POLICY.md) covers what any contribution has to withstand. The
part that most often applies here is that a delivery-correctness change is judged
on a failing test, not on reasoning that reads well.

## Invariants (do not break)

[`docs/INVARIANTS.md`](docs/INVARIANTS.md) numbers and states the engine's
invariants in full, and is the only place they are stated. **Read it before
changing engine behavior.** It records any exception to a property.

Most changes touch none of them. Touching one is not automatically wrong; the
change then has to say how the property still holds. Cite the number rather than
restating the property: "this touches INV-5" is the reviewable form, and
`.github/pull_request_template.md` asks per number.

## Working loop

After each edit, run the narrow check; `--workspace` is for the final gate:

```sh
make clippy
cargo nextest run -p spate-s3 --all-features --locked   # the crate you touched
```

`make help` lists every target; `make gates` is what a pull request must pass.
CI calls the same targets for lint, type check, doctests, the feature matrix,
licenses and every `ci-lint` member. Other jobs spell out invocations of their
own, so green gates locally is necessary and not sufficient.

Traps here:

- **Verify by explicit exit code**, everywhere: gates, checklists, all of it.
  Piped `grep`/`tail` chains report the exit status of the last command in the
  pipeline and have masked failures in this repo. The Makefile contains no pipes.
- **Pass `--locked` on any ad-hoc cargo call**, as CI does. Without it a command
  can resolve a different graph and hide a failure CI will then find. The one
  exception is `cargo hack --no-dev-deps`, which rewrites each `Cargo.toml` as
  it runs and fails outright with the flag.
- **`actionlint` runs locally only.** After touching a workflow, run
  `actionlint .github/workflows/*.yml` and `make zizmor` yourself; CI will not
  catch a bad edit before you push.

## Testing

proptest for tracker and codec invariants, loom for the sync primitives, rdkafka
MockCluster and clickhouse mocks in default CI, testcontainers behind the Docker
job. Framework users test with `spate-test` mocks; keep those first-class.

- `make test` does not run doctests; `make doctest` does.
- `cargo test` runs a binary's tests in one process, so fixtures must carry
  per-test `pipeline`/`component` labels. A local recorder does not isolate the
  process-wide gauge claim in INV-10.

## Comments

Written for a senior Rust engineer, new to this line of code but not to the
language or the domain. A comment says what the code does and what a caller may
rely on, and carries only what they cannot get from the code beside it. Why it
is this way and not the alternative belongs in the commit message. If deleting a
sentence changes nothing a reader would do differently, it goes.

- **One sentence is the default.** A second carries a constraint, a guardrail,
  or a mechanism the caller cannot see. A doc longer than its item, or any doc
  on an item whose signature says what it does, is a review finding.
- **A module header names what the module provides** and its role in the crate,
  never the dependency it replaces or the failure that motivated it.
- **State a property as a contract, not a verdict.** "The same seed yields the
  same stream on any build" is something a caller can use; "a pinned generator
  cannot drift" closes a decision.
- **Constraints on use stay in rustdoc**: not cryptographic, not cancel-safe,
  panics on X.
- **Guardrails stay.** A comment explaining why a line must not be "simplified"
  prevents the next edit.
- **No callers.** Not which, how many, or how often; `grep` finds them and the
  sentence goes stale. Describe the item's own behavior.
- **No history.** A sentence narrating the old behavior is a recap with or
  without "now". Present tense throughout.
- **No tour of visible control flow.** Restating the loop, the match arms or the
  early return drifts on the first edit.
- **Do not explain why something cannot happen.** State a fact once.

A test's doc says what the test pins, in one or two sentences, plus
`Regression for #N.` where it guards a fixed defect, and the issue holds the
account of the defect. The same rules hold in commit messages.

The em-dash used as a dramatic pause, the antithesis frame ("a bound on
patience, not a deadline"), the evaluative tail (", which is the whole point"),
the colon reveal, and intensifiers that add nothing all read as machine-written.
Replacing one with another is not a fix.

## Documentation

`docs/STYLE.md` is normative; the `docs-review` skill
(`.claude/skills/docs-review/SKILL.md`) is the procedure for applying it. Read or
invoke it for any edit under `docs/`.

The rules that break most often:

- **Framework pages are vendor-neutral prose.** Everything under
  `docs/user-guide/` outside `04-connectors/` states its rules in framework
  vocabulary. A connector name may appear only as a link label, a `## Related`
  entry, or inside a `:::note Connector specifics` block, never carrying the
  explanation. Fenced code and YAML are exempt; the prose around them is not.
  `docs/adr/` sits outside the rule, and should not grow connector *usage*
  guidance either.
- **Docs read as the present, never as a changelog.** No "now", "recently", "as
  of". If something changed, the page describes what is and the commit says what
  moved. The one exception is `docs/adr/`; see below.

Decision records live in `docs/adr/`, one file per decision, and are the only
place under `docs/` that reads as history. Scaffold one with
`make adr-new SLUG=…`. An **accepted record is immutable**: a changed decision is
a *new* record superseding the old one, never an edit to it. A decision gets a
record only if it affects structure, a key quality attribute, or is hard to
reverse. `docs/adr/_template.md` states both rules in full and is normative;
`make check-adr` holds the mechanical half.

## Commits and pull requests

Conventional Commits. Scope = crate touched (`spate-core`, `spate-kafka`, …),
comma-separated for several; use `workspace`, `ci`, `docs`, `examples`,
`bench` for non-crate areas. Dependabot raises bumps as `chore` scoped by
area, so prefer `fix` or `feat` for your own commits; `chore` should read as "a
bot bumped a version".

Messages must make sense to outsiders: no plan or phase references, no issue
shorthand that only resolves in this session. **No AI attribution in git**: no
`Co-Authored-By` trailer for a model, no "Generated with" footer in a pull
request body.

Scope discipline:

- One logical change per commit. Implement the smallest change that solves the
  problem and defer polish to a follow-up.
- Flag a diff growing past ~400 lines.
- No drive-by dependency, formatting, or cleanup churn unless the task needs it.

Before opening a pull request, read
[`.github/pull_request_template.md`](.github/pull_request_template.md) and use it
as the body structure. It carries the INV checklist and the gate checklist. Tick
those by exit code, not by memory.

## Filing an issue

`gh` **cannot read our issue forms**: it only sees markdown templates, and ours
are all `.yml` forms, so `--template` fails regardless of flags
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
closed unfixed). Check the issue afterwards.

## Done means

- `make gates` green.
- Normative docs changed in the *same commit* as the behavior they describe.
- A **changelog fragment** under `changelog.d/` whenever the change reaches a
  crate and somebody upgrading would care: `feat`, `fix`, `perf`, `revert` and
  `build`, plus **anything carrying `!` whatever its scope**.
  `make changelog-new TYPE=fixed SLUG=…` scaffolds one, and
  `changelog.d/README.md` has the conventions. Naming no scope is *not* an
  exemption; only naming one of the non-crate areas is. For a fix to a bug that
  was never released, put a `Changelog: none` trailer on the commit it excuses,
  or in the pull request body to excuse the whole thing.
- No unrun or failing tests handed over. If something is blocked, say which part
  and why, rather than narrowing the task to what passed.
