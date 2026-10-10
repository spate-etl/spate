# Spate

High-performance, at-least-once ETL pipeline framework in Rust. Publishable
crates under `crates/`, plus unpublished members at the top level such as the
wall-clock benchmark harness in `bench/` and the shared test helpers in
`test-support/`.

[`CONTRIBUTING.md`](CONTRIBUTING.md) is the contributor-facing entry point.
[`DEVELOPING.md`](DEVELOPING.md) carries the build, test and benchmark mechanics
in full: read it for a command, a profile or a bench convention.
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
cargo xtask clippy
cargo nextest run -p spate-s3 --all-features --locked   # the crate you touched
```

`cargo xtask ci --since` runs the gates over the packages the diff against
`origin/main` can affect, committed or not, and leaves `cargo deny`, the fuzz
harness and the rest of the workspace to CI. Use it between edits and before a
push. A diff that touches a manifest, `Cargo.lock`, `test-support/`, `xtask/`,
tooling, or Rust outside `crates/` runs everything.

`cargo xtask --help` lists the commands, each with its own `--help`;
`cargo xtask ci` is what a pull request must pass.
CI runs the same commands for lint, type check, doctests, the feature matrix,
licenses and every `tidy` member. Other jobs spell out invocations of their
own, so a green `cargo xtask ci` locally is necessary and not sufficient.

Traps here:

- **Verify by explicit exit code**, everywhere: gates, checklists, all of it.
  Piped `grep`/`tail` chains report the exit status of the last command in the
  pipeline and have masked failures in this repo. No command in `xtask` pipes.
- **Pass `--locked` on any ad-hoc cargo call**, as CI does. Without it a command
  can resolve a different graph and hide a failure CI will then find. The one
  exception is `cargo hack --no-dev-deps`, which rewrites each `Cargo.toml` as
  it runs and fails outright with the flag.
- **`actionlint` runs locally only.** After touching a workflow, run
  `actionlint .github/workflows/*.yml`; that is the only time it runs at all.
- **A local `zizmor` run is offline.** CI runs it through `cargo xtask tidy`
  with `GH_TOKEN` set; without a token `cargo xtask tidy zizmor` skips the API
  audits and can report no findings where the `workflows` job fails.

## Testing

proptest for tracker and codec invariants, loom for the sync primitives, rdkafka
MockCluster and clickhouse mocks in default CI, testcontainers behind the Docker
job. Framework users test with `spate-test` mocks; keep those first-class.

- `cargo xtask test` does not run doctests; `cargo xtask doctest` does.
- `cargo test` runs a binary's tests in one process, so fixtures must carry
  per-test `pipeline`/`component` labels. A local recorder does not isolate the
  process-wide gauge claim in INV-10.
- A test waits for a condition, polled with `spate_test::wait_until` or awaited
  as a signal, under a deadline; a fixed sleep belongs only where elapsed time
  is the property under test, on paused tokio time where the code allows it.

## Comments

Written for a senior Rust engineer who is new to this code. A comment carries
what the code beside it cannot show: the contract a caller relies on, or the
reason a maintainer needs before changing a line. `main` squashes with an empty
body, so a reason the next editor needs belongs in the code.

**Rustdoc states the contract.**

- Open with one sentence saying what the item does. Add more only for what a
  caller relies on and the signature does not say: units, ordering, blocking,
  cancel safety, an invariant upheld.
- Add `# Errors`, `# Panics` and `# Safety` where they apply, and `# Examples`
  on public entry points.
- A module header names what the module provides and its role in the crate.

**Inline comments state why, where the code cannot.**

- A guardrail on a line that looks removable: a deliberate re-read, an
  `#[inline(never)]`, an ordering that must hold.
- A constraint the code relies on but does not show, such as a lock held or an
  upstream behavior.
- `// SAFETY:` on every `unsafe` block, naming the invariant that makes it
  sound.
- Self-evident code takes no comment. When a change removes what a comment
  defended, the comment goes too.

The rejected alternative and how the change came about go in the pull request
body.

**Never in a comment:** history (old behavior, "now", upstream versions or
advisory IDs), callers (which, how many, how often), a tour of visible control
flow, line numbers.

A test's doc says what the test pins, plus `Regression for #N.` where it guards
a fixed defect.

These rules hold in commit messages and pull request bodies too. Avoid the
dramatic em-dash, antithesis framing, evaluative tails and empty intensifiers;
swapping one for another is no fix.

## Documentation

`docs/STYLE.md` is normative. Read it for any edit under `docs/`, and check the
claims a page makes against the source before its prose.

The rules that break most often:

- **Framework pages are vendor-neutral prose.** Everything under
  `docs/user-guide/` outside `04-connectors/` states its rules in framework
  vocabulary. `docs/STYLE.md`'s rule lists where a connector or vendor name may
  appear; nowhere else may it carry the explanation. Fenced code and YAML are
  exempt; the prose around them is not. `docs/adr/` sits outside the rule, and
  should not grow connector *usage* guidance either.
- **Docs read as the present, never as a changelog.** No "now", "recently", "as
  of". If something changed, the page describes what is and the pull request
  says what moved. The one exception is `docs/adr/`; see below.

Decision records live in `docs/adr/`, one file per decision, and are the only
place under `docs/` that reads as history. Scaffold one with
`cargo xtask adr new …`. An **accepted record is immutable**: a changed decision is
a *new* record superseding the old one, never an edit to it. A decision gets a
record only if it affects structure, a key quality attribute, or is hard to
reverse. `docs/adr/_template.md` states both rules in full and is normative;
`cargo xtask tidy adr` holds the mechanical half.

## Commits and pull requests

Subjects are `area: description` in at most 72 characters. The area is a
crate's directory name without `spate-` (`core`, `kafka`, `spate`) or one of the
areas [`CONTRIBUTING.md`](CONTRIBUTING.md) names. Name one area, the one whose
behavior the change is about; a change across crates is `workspace`. The
description starts lowercase unless it opens with a `code` reference, and has no
trailing period. Run `cargo xtask hooks install` once per clone so the
commit-msg hook checks each subject; `cargo xtask tidy title` checks the pull
request title in CI.

`main` is squash-merged with the pull request title as the subject and an empty
body, and the merge appends ` (#N)`. The title therefore gets 72 characters less
that suffix. A branch commit body is optional: one line of why where the diff
does not show it. The pull request body carries the argument, since it is the
record that outlives the branch.

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

- `cargo xtask ci` green.
- Normative docs changed in the *same commit* as the behavior they describe.
- A **changelog fragment** under `changelog.d/` whenever the change touches
  what a crate ships: a crate's `src/`, `build.rs` or `Cargo.toml`, or the
  workspace `rust-version`. `cargo xtask changelog new fixed …` scaffolds one,
  and `changelog.d/README.md` has the conventions. A breaking change is a
  fragment that opens with `**Breaking:**`, and the release derives its minor
  bump from it. When nobody upgrading would notice, as with a refactor, a test
  or a fix to a bug that was never released, put a line reading
  `Changelog: none` in the pull request body.
- No unrun or failing tests handed over. If something is blocked, say which part
  and why, rather than narrowing the task to what passed.
