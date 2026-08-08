# Contributing

The most useful contribution this project can receive is one that proves a
delivery guarantee wrong — a report, or better a failing test, showing a source
watermark advancing past unacknowledged data. At-least-once is the promise
everything else is arranged around, so that goes ahead of any feature. After it:
connectors, because the abstractions exist so third parties can write them, and
anything measurably faster that does not weaken what the engine promises.

## Before you start

For anything large, and for anything breaking, open an issue first — it tells you
whether the change will be accepted before you write it. A maintainer should
comment on or review a pull request within a few days, though depending on
circumstances it can take longer.

There are four issue forms: a delivery guarantee that did not hold, an ordinary
bug, a performance problem, and a proposal. Blank issues are off deliberately,
because each form asks the question somebody would have to ask you anyway. A
vulnerability is the exception and never goes in an issue — see
[Security and legal](#security-and-legal).

## Building and testing

A Rust toolchain at **1.94** or newer — that is the MSRV, and CI checks it — and
nothing else for the default suites.

```sh
make gates   # everything a pull request must pass
make help    # every target, grouped
```

`make gates` covers formatting, clippy, the type check, the test suite, doctests,
the feature matrix, licences and advisories, and the repository's own consistency
checks. CI calls those same targets, so a target that passes here is what runs
there. It is necessary rather than sufficient: the test, container, site and MSRV
jobs spell out invocations of their own.

Containers, benchmarks, nextest profiles and the opt-in suites are in
[`DEVELOPING.md`](DEVELOPING.md).

## Adding a connector

The source and sink traits are public because third parties are meant to
implement them, so a new connector is a first-class contribution rather than a
special case. [`docs/user-guide/06-extending/`](docs/user-guide/06-extending/) is
the guide, `crates/spate-json` is the smallest complete crate to read as a shape,
and `crates/spate-test` is how you exercise one without standing up
infrastructure.

Two things belong in the change rather than in a follow-up: the connector page
built from the template in [`docs/STYLE.md`](docs/STYLE.md) § 3, and keeping your
own types out of `spate-core`'s public API (INV-6).

## Opening a pull request

Fork and open a pull request against `main`. That is the only route — nobody
pushes to `main` directly, maintainers included, and the branch rules enforce it.

CI on a pull request from a fork waits for an explicit approval before it runs. A
workflow runs as it exists in the pull request, so an unreviewed run is an
unreviewed change to what CI proves. It costs you one round-trip and it is not a
comment on your change.

Commits follow [Conventional Commits](https://www.conventionalcommits.org),
scoped to the crate touched — `fix(spate-kafka): …`, comma-separated for several,
and `workspace`, `ci`, `docs`, `examples`, `bench` or `website` for the areas
that are not crates. Breaking changes carry `!`. Messages should make sense to
somebody who was not in the conversation: say what changed and why, not which
iteration of a plan it belongs to.

A change that reaches a crate and that somebody upgrading would care about also
needs a **changelog fragment** — a `feat`, `fix`, `perf`, `revert` or `build`,
and anything carrying `!` whatever its scope. Scoping to one of the areas that is
not a crate is what earns an exemption; leaving the scope off does not.
`make changelog-new TYPE=fixed SLUG=…` scaffolds one,
[`changelog.d/README.md`](changelog.d/README.md) has the conventions, and
`make check-changelog` is the gate — a miss is a red CI rather than a review
comment.

[`.github/pull_request_template.md`](.github/pull_request_template.md) is the
body structure. Tick its boxes by exit code, not by memory.

## The invariants

The properties the engine is arranged around are numbered and stated in
[`docs/INVARIANTS.md`](docs/INVARIANTS.md), which is the only place they are
stated in full. Most changes touch none of them.

A change that does touch one is not thereby wrong — it needs to say how the
property still holds, and that is the review. Cite the number when you do: "this
touches INV-5" is a reviewable claim in a way that restating the property is not.

## Documentation

`docs/` is the published site, rendered in place, so a documentation change is a
change to what readers see. [`docs/STYLE.md`](docs/STYLE.md) is normative, and
`make docs` is the gate that catches a link you broke by moving a page.

The one rule to know before writing a line: **framework pages are vendor-neutral
prose.** Everything under `docs/user-guide/` outside `04-connectors/` states its
rules in framework vocabulary. A connector name belongs in a link label, a
`## Related` entry, or a `:::note Connector specifics` block — never in the
explanation, which belongs to every connector equally. Fenced code and YAML are
exempt: a configuration example has to name a real tag. The boundary has
judgement at its edges, so review enforces it rather than a lint.

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

Maintainers cut releases as described in [`RELEASING.md`](RELEASING.md). The one
part that reaches a contributor is the changelog fragment above — releases are
assembled from `changelog.d/`, so the release note is written with the change
rather than reconstructed from the log months later.
