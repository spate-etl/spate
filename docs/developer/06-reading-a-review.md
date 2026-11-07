---
description: "What a review of a pull request here checks, so the author can run the same checks first: invariants by number, a test that fails at base, mutants, reuse, docs in the same commit, the fragment and the body."
---

# Reading a review

A review here reads a change against the same list every time. Running that
list on your own branch turns most findings into a commit before anyone else
opens the diff, and it tells you what a comment on the pull request is asking
for.

## The invariants, by number

A change that touches one of the properties in
[Spate invariants](../INVARIANTS.md) says how the property still holds, and
cites it by number: "this touches INV-5" is the reviewable form. The pull
request template asks per number, and a review reads the invariant's text
against every path the diff reaches. "None" is the right answer for most
changes and is read as a claim like any other.

## A test that fails at base

A change to delivery semantics is judged on a failing test that passes
afterwards, and so is any fix: the test pins the defect, so with the fix
reverted it fails. Check it before you open the pull request:

```sh
base=$(git merge-base origin/main HEAD)
git checkout "$base" -- crates/spate-core/src/pipeline/controller.rs   # the source hunk, with the test left in place
cargo nextest run -p spate-core --locked -E 'test(a_chase_window_closes_at_its_end)'
echo "exit: $?"                                                       # non-zero, or the test pins nothing
git checkout HEAD -- crates/spate-core/src/pipeline/controller.rs
```

A test that passes with the fix reverted is a finding, shown by exactly that
run. A test that waits, sleeps or reads the clock is read against
[Writing a test](02-testing.md).

## Mutants killed, or named

A new condition, match arm or bound comes with the test that kills its mutant.
`cargo xtask mutants --since origin/main` runs them over the diff
([Mutants](01-commands.md#mutants)); a survivor on a delivery or liveness
guard needs a killing test, and any other survivor is named in the body with
why it stands. The forms the tool does not generate, such as a swapped match
arm or a deleted early `return`, are applied by hand to a new delivery or
liveness guard, and so is any guard only the container suites reach.

## Reuse

Before the new code is read, the review looks for the code at the head that
does the same job, by the shape the [reuse rule](05-code-style.md#reuse)
names: the trait it wraps, the type it holds, the terms in its name. A second
mechanism beside an existing one is a finding, shown by quoting both. The same
search runs for the peers of each item the diff adds or renames, so a name or
a signature that every peer agrees against is one too.

## Docs in the same commit

The normative document that describes a behavior changes in the same commit
as the behavior. That includes the pages under `docs/`, which
[`docs/STYLE.md`](repo:docs/STYLE.md) governs and `cargo xtask docs` gates, a
crate's `README.md`, rustdoc, and the comment beside the code. A page or a
comment the diff made false is a finding whether or not the diff touches it,
so for each changed identifier, config key, default, metric name and error
message, search the tree for the places that describe it.

A root document is edited only when the change is about what that document is
for. A change elsewhere that makes a sentence there wrong corrects that
sentence and adds nothing.

## The changelog fragment

A change to what a crate ships carries a fragment under `changelog.d/`, and
one nobody upgrading would notice carries `Changelog: none` in the body.
[`changelog.d/README.md`](repo:changelog.d/README.md) says which is which. The
review reads the fragment's scope claims against the diff, and a breaking
change's fragment opens with `**Breaking:**`, which is what the semver box
answers to.

## The body

The pull request body is the record that outlives the branch, and the review
reads every claim in it against the diff: what changed, what was verified,
each number, each "only" and each "no behavior change". It opens on the
problem a user hits, gives the argument for the shape over the alternative,
and says where it delivers less, more or something other than the issue asked
for. The checks under its own heading are ticked by exit code.

[`AI_POLICY.md`](repo:AI_POLICY.md) sets the rest: the answers to review
questions are yours in your own words, the model is named in the body when an
agent did most of the work, and no commit or body carries an attribution
trailer.

## Related

- [`CONTRIBUTING.md`](repo:CONTRIBUTING.md) — how a change is proposed, titled
  and opened.
- [Code and comments](05-code-style.md) — the contract a comment states and
  the reuse rule.
- [Commands and gates](01-commands.md) — the commands each check runs.
