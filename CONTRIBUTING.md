# Contributing

The most useful contribution this project can receive is one that proves a
delivery guarantee wrong: a report, or better a failing test, showing a source
watermark advancing past unacknowledged data. At-least-once is the promise
everything else is arranged around, so that goes ahead of any feature. After it
come connectors, and anything measurably faster that does not weaken what the
engine promises. The [extending guide](docs/user-guide/06-extending/README.mdx)
covers writing a connector.

## Before you start

Outside contributors need an issue assigned to them before they open a pull
request. An outside contributor is anyone without write access to the
repository.

1. Find an open issue, or open one with an issue form.
2. Comment that you would like to work on it and how you plan to approach it.
3. A maintainer replies by assigning it to you or saying why not. Start once it
   is assigned.

The maintainer decides. An issue may be planned for a release in progress, or
may be waiting on a design decision. A pull request that is not linked to an
issue assigned to its author is closed with a pointer to this section.

Two kinds of change need no issue: a minor typo or documentation fix whose
author has checked it against the source for accuracy, and a pull request from
a maintainer or from Dependabot. A larger documentation change goes through an
issue like any other change.

A maintainer should comment on or review a pull request within a few days,
though it can take longer. Ask a question about a change in a comment on its
issue; for anything else, open the issue form closest to it.

Blank issues are off: each form asks the question somebody would have to ask
you anyway. A vulnerability never goes in an issue; see
[Security and legal](#security-and-legal).

### Keeping an assignment

You can hold at most three assigned issues at a time.

An assignment ends 14 days after it is made, or on the target date of the
issue's milestone if that comes first. It never ends sooner than 7 days after it
is made. An open pull request for the issue keeps the assignment.

When a milestone passes its target date, its open issues move on as
[`RELEASING.md`](RELEASING.md#milestones-and-priorities) describes. An
assignment with no open pull request ends then, or 7 days after it was made if
that is later. You can comment on the issue to ask for it again.

A maintainer removes the assignee and leaves a comment when an assignment ends.
You can comment on the issue to release it earlier, or to ask for more time.

## Finding something to work on

`good first issue` marks issues scoped for a first contribution, and
`priority: high` marks the backlog that matters most now. A `good first issue`
is fixed without AI tools writing the change;
[`AI_POLICY.md`](AI_POLICY.md#good-first-issues) says why. How an issue gets a
milestone or a priority is in
[`RELEASING.md`](RELEASING.md#milestones-and-priorities).

## Building and testing

[Getting started](docs/developer/01-getting-started.md) sets up a clone: the
toolchain, the tools the gates call, and the commit hook. `cargo xtask ci` runs
everything a pull request must pass.

A new condition, match arm or bound comes with the test that kills its mutant.
The [developer guide](docs/developer/README.md) has the rest of the build, test
and benchmark mechanics.

## Opening a pull request

Fork and open a pull request against `main`. Nobody pushes to `main` directly,
maintainers included, and the branch rules enforce it.

CI on a pull request from a fork waits for a maintainer's approval before it
runs, which costs you one round-trip. The title check runs without that
approval, so a title that breaks the rule below is reported as soon as the pull
request opens.

A subject names one area and says what the change does, in at most 72
characters:

```text
kafka: start a partition's fetcher before its lane is handed out
core: `Clock` moves into spate_core::clock
```

The area is a crate's directory name without the `spate-` prefix (`core`,
`kafka`, `clickhouse-derive`, and `spate` for the facade crate), or one of
`workspace`, `ci`, `docs`, `examples`, `bench` and `website`. Name the area
whose behavior the change is about; the docs and tests that come with it do not
add a second one, and a change across crates is `workspace`. The description
starts lowercase unless it opens with a `code` reference, and has no trailing
period. `release: vX.Y.Z` is reserved for the release pull request. GitHub's
Revert button titles a pull request `Revert "…"`, which needs a retitle.

The pull request title is the subject that lands. `main` is squash-merged with
the title as the subject and an empty body, and the merge appends ` (#N)`, so a
title gets 72 characters less that suffix. `cargo xtask tidy title` checks it
in CI. Run `cargo xtask hooks install` once per clone, and a commit-msg hook
applies the same rule to every commit.

A commit body is optional. Branch commits do not reach `main`, so the argument
for a change goes in the pull request body, and a reason the next editor of the
code needs goes in a comment beside it. Write both for somebody who was not in
the conversation.

In comments, commit messages and pull request bodies, avoid the dramatic
em-dash, the colon reveal, antithesis framing, evaluative tails and empty
intensifiers. Replacing one of these with another is no fix.

A change to what a crate ships needs a **changelog fragment**, and one nobody
upgrading would notice carries a line reading `Changelog: none` in the pull
request body. [`changelog.d/README.md`](changelog.d/README.md) says which is
which, and `cargo xtask changelog new fixed …` scaffolds a fragment.

[`.github/pull_request_template.md`](.github/pull_request_template.md) is the
body structure. Tick its boxes by exit code, not by memory.
[Before you open a pull request](docs/developer/09-before-review.md) says what
a review checks, so you can run the same checks first.

## The invariants

The properties the engine is arranged around are numbered in
[`docs/INVARIANTS.md`](docs/INVARIANTS.md). Most changes touch none of them. A
change that touches one says how the property still holds, and cites the
number: "this touches INV-5" is the reviewable form.

## Documentation

`docs/` is the published site, rendered in place.
[`docs/STYLE.md`](docs/STYLE.md) is normative for it, including the rule that
framework pages are vendor-neutral prose, and `cargo xtask docs` is the gate. A
pull request that adds a connector adds its page, built from the template in
`docs/STYLE.md` § 3.

## Security and legal

Vulnerabilities go privately through
[GitHub's advisory flow](https://github.com/spate-etl/spate/security/advisories/new),
never as a public issue; [`SECURITY.md`](SECURITY.md) has the scope and the
response times.

Contributions are accepted under Apache-2.0 §5, inbound under the same terms as
outbound. There is no CLA and nothing to sign, and the
[Code of Conduct](CODE_OF_CONDUCT.md) applies throughout.

AI tools are welcome here, and [`AI_POLICY.md`](AI_POLICY.md) says what a
contribution has to withstand regardless of how it was produced. The short
version: be able to answer questions about your own change in your own words, and
back a delivery-correctness fix with a failing test rather than an explanation.
