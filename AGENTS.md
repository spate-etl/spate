# Spate

High-performance, at-least-once ETL pipeline framework in Rust. Publishable
crates under `crates/`, plus unpublished members at the top level such as the
wall-clock benchmark harness in `bench/` and the shared test helpers in
`test-support/`.

[`CONTRIBUTING.md`](CONTRIBUTING.md) is the contributor-facing entry point.
The [developer guide](docs/developer/README.md) carries the build, test and
benchmark mechanics in full: read it for a command, a profile or a bench
convention.
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

`cargo xtask mutants --since origin/main` runs cargo-mutants over the same diff
and exits 2 when a mutant survives. Run it before a push: a new condition,
match arm or bound comes with the test that kills its mutant
([`CONTRIBUTING.md`](CONTRIBUTING.md)). The container suites do not run under
it, so a guard only they reach reports as missed and is mutated by hand.

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
  can resolve a different graph and hide a failure CI will then find. The
  commands that cannot take it are listed in
  [Commands and gates](docs/developer/01-commands.md#commands).
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
- A test acts or asserts only on a state the code has signalled, and the
  signal is published after the state. It awaits the signal, or polls it with
  `spate_test::wait_until`.
- A test owns its environment: its names, ports, clusters and metric labels
  are its own.
- Real elapsed time decides no result. A wall-clock bound only catches a hang:
  pass `spate_test::HANG`, and the failure says the test hung. A timing
  property is tested on `TestClock` or paused tokio time where the code allows
  it, and a fixed sleep belongs only in such a test.
- A failure in an interleaving assertion is checked for a product defect
  before it is called a flake.

## Comments

How a comment and its rustdoc are written, and the reuse rule a change meets
before it adds code, is [Code and comments](docs/developer/05-code-style.md).

## Documentation

`docs/STYLE.md` is normative. Read it for any edit under `docs/`, and check the
claims a page makes against the source before its prose.

Agents alone read this file. No other file should name or link it, and a rule
a person needs lives in a document they read, which this file links to.

## Commits and pull requests

Subjects, titles, bodies and scope follow
[`CONTRIBUTING.md`](CONTRIBUTING.md#opening-a-pull-request) and
[`AI_POLICY.md`](AI_POLICY.md#agentic-contributions). Messages must make sense
to outsiders: no plan or phase references, no issue shorthand that only
resolves in this session.

## Filing an issue

`gh` **cannot read our issue forms**: it only sees markdown templates, and ours
are all `.yml` forms, so `--template` fails regardless of flags
([cli/cli#5865](https://github.com/cli/cli/issues/5865)). Read the relevant
`.github/ISSUE_TEMPLATE/*.yml`, render its fields to markdown yourself (each
field's `label` as a `###` heading, `render:` fields in a fenced block), and post
that:

```sh
gh issue create --body-file <rendered.md> --title '[bug] …' \
  --type Bug --label 'crate: spate-s3' --label 'priority: medium'
```

Every issue carries a milestone or a `priority:` label. Read
[`RELEASING.md`](RELEASING.md#milestones-and-priorities) to choose
between them, and pass `--milestone` or the priority `--label` with the rest.

Nothing infers `--type` or `--label`, and **`--label` is silently dropped without
triage permission** ([cli/cli#13589](https://github.com/cli/cli/issues/13589),
closed unfixed). Check the issue afterwards.

## Done means

- [Reading a review](docs/developer/06-reading-a-review.md) is the list a
  change is read against. Run it before handing over.
- No unrun or failing tests handed over. If something is blocked, say which part
  and why, rather than narrowing the task to what passed.
