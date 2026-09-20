# Changelog fragments

A release note for one change, written when the change is written and assembled
into [`CHANGELOG.md`](../CHANGELOG.md) at release time. One file per pull
request; the conventions follow
[towncrier](https://towncrier.readthedocs.io/en/stable/tutorial.html).

The entry lives here rather than in `CHANGELOG.md` directly. A fragment is a
**separate reviewable diff** — the wording is read on its own, the way the user
will read it, instead of being skimmed past at the top of a long file. And
checking that a file was *added* has no fail-open mode, where checking
that the `## [Unreleased]` section *grew* does: a section extractor that loses
its end boundary silently starts accepting any edit anywhere in the file.

This README is not only documentation. A release consumes every fragment, and
git does not track empty directories — this file is what keeps `changelog.d/`
present for the next change.

## When you need one

**Whenever the change reaches a crate and somebody upgrading would care.** In
practice that is a `feat`, `fix` or `perf` commit, or anything carrying `!`.

You are exempt when the scope names one of the areas that is not a crate —
`ci`, `docs`, `examples`, `bench`, `workspace`, `website` — or when the
type says nobody upgrading is affected: `docs`, `test`, `chore`, `style`, `ci`,
`refactor`.

`revert` and `build` are **not** on that list. Reverting a released feature takes
away something people are using, and a crate-scoped `build` is where an MSRV
floor moves; both are things a reader upgrading has to be told.

A `!` needs one whatever the scope and type say. It is you declaring a breaking
change, and that is the one thing a release note cannot omit.

Note which way round that is. **Naming no scope is not an exemption**, and
neither is a scope this repository does not recognize. An exemption is earned by
saying which non-crate area the change belongs to, not by leaving the scope off
— `feat: …` requires a fragment, because some of the largest changes this
project has ever shipped were written exactly that way.

`cargo xtask tidy changelog` is the gate. `cargo xtask ci` runs it, and in CI
it has a job of its own, because it reads the pull request's title and body.
There is no label and no checkbox to switch it off: the exemption is derived
from the type and scope you write, so the way out is to write a subject that is
true.

    feat(spate-core): …   ->  refactor(spate-core): …   nothing user-facing moved
    fix(spate-core): …    ->  test(spate-core): …       it only touched tests
    feat(spate-core): …   ->  feat(docs): …             it only touched docs

For the one case that leaves — **a fix to a bug that was never released** — put
a `Changelog: none` trailer on the commit. There is nothing to tell anybody
upgrading, because from outside this repository it never happened.

```
fix(spate-core): correct the probe deadline

Broken by #31 and never released.

Changelog: none
```

Because the repository squashes with the pull request title as the commit
subject, **the title is the one that has to be right.**

## Writing one

```sh
cargo xtask changelog new fixed retry-ladder
```

That writes `changelog.d/retry-ladder.fixed.md` for you to edit. The name is
`<slug>.<type>.md` — a slug, not a pull request number, because you do not know
the number until after you have opened the pull request.

The type is one of the six [Keep a
Changelog](https://keepachangelog.com/en/2.0.0/) sections, lowercased:

| Type | For |
| --- | --- |
| `added` | New capability |
| `changed` | Existing behavior that is now different |
| `deprecated` | Still works, will not for long |
| `removed` | Gone |
| `fixed` | It was wrong and now is not |
| `security` | A vulnerability closed |

There are six on purpose. A **breaking** change is not a seventh type — it is
one of these six, opened with a `**Breaking:**` marker. Pre-1.0 a breaking
change ships in a minor bump, which is easy to miss in a version number, so say
it in words.

## The conventions

A fragment tells somebody upgrading what to expect and whether they need to
take action. Write for a reader who knows the framework but has not read the
implementation, including readers whose first language is not English.

- **Start the body with what happens now.** Use present tense for current
  behavior, then past tense for what happened previously. A title describing
  current behavior does not replace that first sentence. For a new feature,
  explain a previous limitation or workaround only when it helps the reader.
- **State the consequence or required action.** Say whether records could
  replay, the process could report success incorrectly, or a dashboard query
  needs updating. For a migration, include the required steps and link to
  further instructions when available.
- **Use short sentences and familiar words.** Give each sentence one main
  point. Avoid idioms, dense clauses and implementation jargon such as
  "latched a fatal"; "recorded a fatal error" describes the behavior. Replace
  vague claims such as "improved error handling" with the actual outcome.
- **Keep exact settings, types and metric names** when a reader needs to search
  for them or change their code or configuration. Explain implementation
  details only when they help the reader understand the consequence.
- **Include useful qualifications.** Mention an unaffected behavior when
  readers might reasonably assume it changed. Keep constraints that affect
  how someone uses the feature.
- **Check both sides of the comparison.** Verify current behavior against
  source and tests, and previous behavior against history. Use "in previous
  versions" only after checking that the behavior existed in a released
  version. Describe the condition under which a failure occurred, without
  implying that every use was affected.
- **Open with a short bold title and the crate name**, then put the body in a
  separate paragraph. Keep the voice impersonal: "The pipeline now logs…"
  describes the behavior directly.
- **Usually write three to five sentences.** A simple change can take fewer;
  a breaking change may need additional paragraphs for migration details.
- No pull request number and no author. The number is derived at release time
  from the commit that added the fragment — its squash subject if that carries
  one, and the GitHub API for the commit if it does not. A commit that reached
  `main` outside a pull request links to itself. Contributors are credited in a
  section of their own, from everyone who committed in the release range rather
  than only those who left a fragment. There is nothing to type for either.

The exception is an entry for work that landed somewhere else — a note written
retroactively, or one restored after a release went out without it. Ending the
entry with an explicit `([#31])` wins over the derived link. It has to be the
**last thing in the file**: a `[#N]` mid-sentence is read as a citation, gets its
own link definition, and leaves the entry's own reference alone.

A fragment is prose, not a list item: write paragraphs, and the bullet and its
indentation are applied when the file is assembled.

For example:

```markdown
**Admin server address** (`spate-core`)

The pipeline now logs the admin server's bound address at `INFO` with the
message `admin server listening`. Previously, startup did not report this
address, so configuring `admin.listen` with port `0` left the automatically
assigned port out of the logs. You can use the logged address to reach
`/metrics`, `/healthz`, and `/readyz`.
```

## What happens at release

`cargo xtask changelog build <version>` groups the fragments by type under a
new `## [<version>] — <date>` heading, appends each entry's pull request link,
adds a `### Contributors` section from the commit range, rewrites the link
references, and deletes the fragments it consumed.
[`RELEASING.md`](../RELEASING.md) has the whole procedure.

The assembly is mechanical. The release note is not — read what it wrote before
committing it.
