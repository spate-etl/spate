---
description: "The checks a review of a pull request runs, so you can run them first: invariants by number, a test that fails at base, mutants, reuse, docs in the same commit, the changelog fragment and the body."
---

# Before you open a pull request

A review reads every change against the same list.
Run that list on your branch first, and most findings become a commit before anyone opens the diff.
The list also tells you what a review comment asks for.

## Cite the invariants by number

When a change touches a property in [Spate invariants](../INVARIANTS.md), cite its number in the pull request body and say how the property still holds.
The pull request template asks for each number.

Most changes touch none of the invariants, and "None" is then the answer.
The review reads that answer as a claim, and checks it against every path the diff reaches.

## Show the test fails at base

Run a fix's test with the fix reverted, and see it fail.
A test that passes without the fix pins nothing.

**Why:** a fix to delivery semantics is backed by a test that fails before it and passes after it, and an explanation of why the fix is correct does not count ([`CONTRIBUTING.md`](repo:CONTRIBUTING.md)).
A test written to pin any other fix shows itself the same way.
[`AI_POLICY.md`](repo:AI_POLICY.md) holds a contribution an agent wrote to this bar.

Revert only the source the fix changed, and keep the test.
On a clean tree, with #1826's fix to `controller.rs` and its test as the example:

```sh
base=$(git merge-base origin/main HEAD)
git checkout "$base" -- crates/spate-core/src/pipeline/controller.rs
cargo nextest run -p spate-core --locked -E 'test(a_source_that_failed_to_open_is_not_asked_to_stop_reading)'
echo "exit: $?"
git checkout HEAD -- crates/spate-core/src/pipeline/controller.rs
```

The run that counts exits 100, and its summary names the test as failed.
Exit 4 means the filter matched no test, and exit 101 means the build failed.
Neither shows anything about the fix.

A source file that is new on the branch has no copy at the base, so delete it for the run instead.

A review also reads a test that waits, sleeps or reads the clock against [Tests that do not flake](05-flaky-tests.md).

## Kill or name each mutant

Run `cargo xtask mutants --since origin/main` before you push.
A new condition, match arm or bound comes with the test that kills its mutant.

Name any survivor in the pull request body, with the reason it stands.
A guard that delivery or liveness rests on is the exception: a commit, ack, drain, retry or deadline path needs a test that kills its mutant.

cargo-mutants does not generate every form, such as a swapped match arm or a deleted early `return`.
Apply those forms to such a guard by hand.
Do the same for a guard that only the container suites reach, because they do not run under the mutants.

## Search for code that does the same job

Before the review reads your new code, it looks for code at the head that does the same job.
It searches by the trait the new code wraps, the type it holds and the words in its name.
A second mechanism beside an existing one is a finding, shown by quoting both.

The review runs the same search for the peers of each item the diff adds or renames.
A name or a signature that every peer disagrees with is a finding too.
[Reuse before you add](08-code-and-comments.md#reuse-before-you-add) gives the rule.

## Change the docs in the same commit

Change the document that describes a behavior in the same commit as the behavior.
That includes the pages under `docs/`, a crate's `README.md`, rustdoc, and the comment beside the code.

A page or a comment the diff made false is a finding, whether or not the diff touches it.
For each identifier, config key, default, metric name and error message you change, search the tree for the places that describe it.

The pages under `docs/` follow [`docs/STYLE.md`](repo:docs/STYLE.md), and `cargo xtask docs` builds them.

Edit a root document only when the change is about what that document is for.
When a change elsewhere makes a sentence there wrong, correct that sentence and add nothing.

## Add the changelog fragment

See [`changelog.d/README.md`](repo:changelog.d/README.md) for when a change carries a fragment, and when a `Changelog: none` line instead.
The review reads the fragment's scope against the diff.
A breaking change's fragment opens with `**Breaking:**`, and the pull request template's semver box has to match it.

## Write the body

The pull request body is the record that outlives the branch.
The review reads each claim in it against the diff: what changed, what was verified, each number, each "only" and each "no behavior change".

Open the body on the problem a user hits.
Give the argument for the shape you chose over the alternative.
Say where the change delivers less than the issue asked for, more, or something else.

Answer questions about your change in your own words ([`CONTRIBUTING.md`](repo:CONTRIBUTING.md)).

[`AI_POLICY.md`](repo:AI_POLICY.md) sets the bar for agentic contributions.
When an agent did most of the work, the body names the model.
No commit or body carries an AI attribution trailer, and no commit message refers to a plan or an iteration.

## Related

- [`CONTRIBUTING.md`](repo:CONTRIBUTING.md) — how a change is proposed, titled and opened.
- [Code and comments](08-code-and-comments.md) — the contract a comment states, and the reuse rule.
- [Commands and CI](03-commands.md) — the commands each check runs.
