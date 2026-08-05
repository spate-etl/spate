# Changelog fragments

A release note for one change, written when the change is written and assembled
into [`CHANGELOG.md`](../CHANGELOG.md) at release time. One file per pull
request; the conventions follow
[towncrier](https://towncrier.readthedocs.io/en/stable/tutorial.html).

Two reasons the entry lives here rather than in `CHANGELOG.md` directly. A
fragment is a **separate reviewable diff** — the wording is read on its own, the
way the user will read it, instead of being skimmed past at the top of a long
file. And checking that a file was *added* has no fail-open mode, where checking
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
neither is a scope this repository does not recognise. An exemption is earned by
saying which non-crate area the change belongs to, not by leaving the scope off
— `feat: …` requires a fragment, because some of the largest changes this
project has ever shipped were written exactly that way.

`make check-changelog` is the gate, and it runs as part of `make ci-lint` and in
CI. There is no label and no checkbox to switch it off: the exemption is derived
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
make changelog-new TYPE=fixed SLUG=retry-ladder
```

That writes `changelog.d/retry-ladder.fixed.md` for you to edit. The name is
`<slug>.<type>.md` — a slug, not a pull request number, because you do not know
the number until after you have opened the pull request.

The type is one of the six [Keep a
Changelog](https://keepachangelog.com/en/2.0.0/) sections, lowercased:

| Type | For |
| --- | --- |
| `added` | New capability |
| `changed` | Existing behaviour that is now different |
| `deprecated` | Still works, will not for long |
| `removed` | Gone |
| `fixed` | It was wrong and now is not |
| `security` | A vulnerability closed |

There are six on purpose. A **breaking** change is not a seventh type — it is
one of these six, opened with a `**Breaking:**` marker. Pre-1.0 a breaking
change ships in a minor bump, which is easy to miss in a version number, so say
it in words.

## The conventions

**Say what it means, not what moved.** The commit message already says what
moved. A release note answers "what does this change for me", which is a
different sentence.

- **Present tense, impersonal.** "The wait selects on a breaker wake", not "we
  changed the wait" or "the wait will now select".
- **Open with a bold lead-in naming the crate**, matching the entries already in
  `CHANGELOG.md`: `` **Typed Avro datum decoding** (`spate-avro`) — … ``
- **One to five sentences.** Long enough to say why it matters, short enough to
  scan. If it needs a migration, say so and link the page that has it.
- **Name the settings, types and metrics a reader will search for.** Somebody
  arrives at this file because a gauge moved or a config key stopped working.
- No pull request number and no author. The number is derived at release time
  from the commit that added the fragment; contributors are credited in a
  section of their own, from everyone who committed in the release range rather
  than only those who left a fragment. There is nothing to type for either.

The exception is an entry for work that landed somewhere else — a note written
retroactively, or one restored after a release went out without it. Ending the
entry with an explicit `([#31])` wins over the derived link. It has to be the
**last thing in the file**: a `[#N]` mid-sentence is read as a citation, gets its
own link definition, and leaves the entry's own reference alone.

A fragment is prose, not a list item: write paragraphs, and the bullet and its
indentation are applied when the file is assembled.

## What happens at release

`./scripts/changelog.sh --build <version>` groups the fragments by type under a
new `## [<version>] — <date>` heading, appends each entry's pull request link,
adds a `### Contributors` section from the commit range, rewrites the link
references, and deletes the fragments it consumed.
[`RELEASING.md`](../RELEASING.md) has the whole procedure.

The assembly is mechanical. The release note is not — read what it wrote before
committing it.
