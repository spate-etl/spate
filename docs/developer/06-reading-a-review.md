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
[Spate invariants](../INVARIANTS.md) cites it by number and says how it
still holds; [`CONTRIBUTING.md`](repo:CONTRIBUTING.md) states the rule and
the pull request template asks per number. The review reads the invariant's
text against every path the diff reaches, so "None", the right answer for
most changes, is read as a claim like any other.

## A test that fails at base

A change to delivery semantics is judged on a failing test that passes
afterwards ([`AI_POLICY.md`](repo:AI_POLICY.md)), and a test written to pin
any fix shows the same way: with the fix reverted, it fails. Check it on a
clean tree before you open the pull request:

```sh
base=$(git merge-base origin/main HEAD)
git checkout "$base" -- crates/spate-core/src/pipeline/controller.rs   # the source the fix changed; the test stays
cargo nextest run -p spate-core --locked -E 'test(a_chase_window_closes_at_its_end)'
echo "exit: $?"
git checkout HEAD -- crates/spate-core/src/pipeline/controller.rs
```

The run that counts exits 100 with the summary line naming the test as failed.
Exit 4 means the filter matched no test, and 101 that the build failed; neither
shows anything. A source file new on the branch has no copy at the base, so
delete it for the run instead. A test that passes with the fix reverted is a
finding, shown by exactly that run. A test that waits, sleeps or reads the clock
is read against [Tests that do not flake](05-flaky-tests.md).

## Mutants killed, or named

`cargo xtask mutants --since origin/main` runs the mutants over the diff
([Mutants](01-commands.md#mutants)), under the rule
[`CONTRIBUTING.md`](repo:CONTRIBUTING.md) states for a new condition, arm or
bound. A survivor is named in the body with why it stands, except on a guard
the engine's delivery or liveness rests on, such as a commit, ack, drain,
retry or deadline path, which needs a killing test. The forms the tool does
not generate, such as a swapped match arm or a deleted early `return`, are
applied by hand to such a guard, and so is any guard only the container
suites reach.

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

[`changelog.d/README.md`](repo:changelog.d/README.md) says when a change
carries a fragment and when a `Changelog: none` line. The review reads the
fragment's scope claims against the diff, and a breaking change's fragment
opens with `**Breaking:**`, which is what the semver box answers to.

## The body

The pull request body is the record that outlives the branch, and the review
reads every claim in it against the diff: what changed, what was verified,
each number, each "only" and each "no behavior change". It opens on the
problem a user hits, gives the argument for the shape over the alternative,
and says where it delivers less, more or something other than the issue asked
for.

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
