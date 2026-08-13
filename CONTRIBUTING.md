# Contributing

The most useful contribution this project can receive is one that proves a
delivery guarantee wrong: a report, or better a failing test, showing a source
watermark advancing past unacknowledged data. At-least-once is the promise
everything else is arranged around, so that goes ahead of any feature. After it
come connectors, and anything measurably faster that does not weaken what the
engine promises.

## Before you start

For anything large, and for anything breaking, open an issue first; it tells you
whether the change will be accepted before you write it. A maintainer should
comment on or review a pull request within a few days, though it can take longer.

The issue forms cover a delivery guarantee that did not hold, an ordinary bug, a
performance problem, and a proposal. Blank issues are off: each form asks the
question somebody would have to ask you anyway. A vulnerability is the exception
and never goes in an issue; see [Security and legal](#security-and-legal).

## Building and testing

The default suites need a Rust toolchain at **1.94** or newer and nothing else.
That version is the MSRV, and CI checks it.

```sh
make gates   # everything a pull request must pass
make help    # every target, grouped
```

`make gates` covers formatting, clippy, the type check, the test suite, doctests,
the feature matrix, licences and advisories, and the repository's own consistency
checks. CI calls those same targets, so a target that passes here is what runs
there. It is necessary and not sufficient: other jobs spell out invocations of
their own.

Containers, benchmarks, nextest profiles and the opt-in suites are in
[`DEVELOPING.md`](DEVELOPING.md).

## Adding a connector

The source and sink traits are public for third parties to implement, and a new
connector is a first-class contribution.
[`docs/user-guide/06-extending/`](docs/user-guide/06-extending/) is
the guide, `crates/spate-json` is the smallest complete crate to read as a shape,
and `crates/spate-test` is how you exercise one without standing up
infrastructure.

What belongs in the change rather than in a follow-up: the connector page built
from the template in [`docs/STYLE.md`](docs/STYLE.md) § 3, and keeping your own
types out of `spate-core`'s public API (INV-6).

## Opening a pull request

Fork and open a pull request against `main`. Nobody pushes to `main` directly,
maintainers included, and the branch rules enforce it.

CI on a pull request from a fork waits for an explicit approval before it runs. A
workflow runs as it exists in the pull request, so an unreviewed run is an
unreviewed change to what CI proves. It costs you one round-trip.

Commits follow [Conventional Commits](https://www.conventionalcommits.org),
scoped to the crate touched: `fix(spate-kafka): …`, comma-separated for several,
and `workspace`, `ci`, `docs`, `examples`, `bench` or `website` for the areas
that are not crates. Breaking changes carry `!`. Messages should make sense to
somebody who was not in the conversation: say what changed and why, not which
iteration of a plan it belongs to.

A change that reaches a crate and that somebody upgrading would care about also
needs a **changelog fragment**: a `feat`, `fix`, `perf`, `revert` or `build`, and
anything carrying `!` whatever its scope. Scoping to one of the areas that is not
a crate is the exemption; leaving the scope off is not.
`make changelog-new TYPE=fixed SLUG=…` scaffolds one,
[`changelog.d/README.md`](changelog.d/README.md) has the conventions, and
`make check-changelog` is the gate, so a miss fails CI.

[`.github/pull_request_template.md`](.github/pull_request_template.md) is the
body structure. Tick its boxes by exit code, not by memory.

## The invariants

The properties the engine is arranged around are numbered in
[`docs/INVARIANTS.md`](docs/INVARIANTS.md), the only place they are stated in
full. Most changes touch none of them.

A change that does touch one is not thereby wrong; it needs to say how the
property still holds, and that is the review. Cite the number rather than
restating the property: "this touches INV-5" is the reviewable form.

## Documentation

`docs/` is the published site, rendered in place, so a documentation change is a
change to what readers see. [`docs/STYLE.md`](docs/STYLE.md) is normative, and
`make docs` is the gate that catches a link you broke by moving a page.

The rule to know before writing a line: **framework pages are vendor-neutral
prose.** Everything under `docs/user-guide/` outside `04-connectors/` states its
rules in framework vocabulary. A connector name belongs in a link label, a
`## Related` entry, or a `:::note Connector specifics` block, never in the
explanation. Fenced code and YAML are exempt: a configuration example has to name
a real tag. Review enforces this boundary, not a lint.

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

Maintainers cut releases as described in [`RELEASING.md`](RELEASING.md). The part
that reaches a contributor is the changelog fragment above: releases are
assembled from `changelog.d/`, so the release note is written with the change.
