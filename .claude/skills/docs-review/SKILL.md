---
name: docs-review
description: Review or author documentation under docs/ — the user guide and the decision records. Verifies claims against the source, then the framework/connector boundary, the Diátaxis quadrant and the connector page template, then how the page reads for a Rust developer. Use when writing or editing a docs page, adding a connector, touching a decision record, or auditing the tree for drift.
---

# Documentation review

**The rules live in @docs/STYLE.md. This file is the procedure for applying
them.** If the two ever disagree, STYLE.md wins and this file is the bug. Never
restate a rule here — cite its section.

Read `docs/STYLE.md` in full before step 1; the `@` above is a path, not proof
it was loaded. It is short, and it names the reader on whose behalf every
judgment below is made: a Rust developer new to Spate but not to streaming.

## 0. Route from the path

Two axes, both from the path, before reading a word. Get this wrong and every
later judgment is wrong.

| Path | What it is | Procedure |
|---|---|---|
| `docs/user-guide/04-connectors/**` | Connector page (Reference) | Steps 1–4 **plus** [references/connector-page.md](references/connector-page.md) |
| `docs/user-guide/**` — anything else | Framework page | Steps 1–4; quadrant from § 6's table |
| `docs/adr/**` | Decision record | [references/adr.md](references/adr.md) — it replaces steps 2 and 3 |
| `docs/METRICS.md`, `docs/INVARIANTS.md` | Rendered framework reference | Steps 1–4. Only `STYLE.md` is excluded from the site; these two are pages |
| The whole tree | An audit | [references/audit.md](references/audit.md) |

Two traps in this step:

- **The quadrant comes from the directory, not from how the page reads.** § 6's
  table is the authority. A page that reads like a how-to while sitting in
  `02-concepts/` is a defect to report, not a reclassification to make.
- **A page entirely about one connector is in the wrong directory** (§ 6).
  Moving it is the fix; editing it is not. A move changes the URL — step 4.

## 1. Claims — before anything else

Diátaxis treats accuracy as a precondition for everything downstream, and the
ordering here follows: a page that reads beautifully and states a false default
is worse than a clumsy one that is true. Verify first, and never trade a claim
check for a prose fix.

**Do the step 3 cold read first and hold the marks.** By the end of this step
you will have read the page four times while checking things, and the cold read
is unrecoverable after that. Read it once as a reader, note where you stall,
then start verifying.

Open the source. For each of these, if the page asserts it:

- **Config keys**, against the config struct. It is `deny_unknown_fields`, so a
  key that does not exist is not a documentation typo — it is a paste that
  fails at startup naming the offending key. Read the YAML example as if you
  were the deserializer. Check `validate()` too: a constraint *between* two
  keys fails at load and is invisible in a table of independent defaults.
- **`Default` cells**, against the `Default` impl or the serde default — not
  against the prose beside them.
- **Metric families and labels.** Framework families are constants in
  `crates/spate-core/src/metrics/names.rs`. **Connector families are not
  constants anywhere** — they are minted at runtime in the connector's own
  `src/metrics.rs` (`meter.counter("objects_listed_total", …)`), and the
  `spate_<crate>_<role>_` prefix comes from the `Meter` scope, not from any
  string you can grep for whole.
- **Rust snippets** (§ 10). A rendered fence is compiled, so the question moves:
  open the source at that anchor and ask whether the region is the code the
  sentence introducing it promises. A region that compiles is not the same as
  one that answers the paragraph above it. A hand-written fence is an implicit
  claim of exemption — name the clause; if none fits, that is the finding. The
  feature flags stay yours either way, because a region compiles under *some*
  feature set, not necessarily the one the page tells the reader to enable.
- **Performance figures.** § 7 requires provenance. A figure carrying none is a
  defect even when it happens to be true.
- **Link glosses**, against the page each one points at. `make docs` proves a
  link *resolves*; nothing proves the sentence describing it is *true*. § 7 puts
  a gloss on every cross-link in the tree, so these rot exactly like a `Default`
  cell and are just as unguarded.

A `Default` column and a YAML example are the two things in this tree that rot
fastest, and no gate covers either.

**Check the page against its siblings, not only against the code.** A formula, a
shared default or a figure that appears on three pages has three chances to be
the outlier, and a single-page read cannot see divergence — grep the tree for
the value before trusting the copy in front of you. Step 3 structurally cannot
catch this, because repetition across pages is an explicit non-defect there; it
has to be caught here or not at all.

On a connector page the claim sources are five, and only the first is obvious:
`src/config.rs` for keys, defaults and `validate()`; `src/metrics.rs` for the
families; `src/<component>.rs` for every builder method a snippet calls; the
internals a specific claim names; and the sources the page renders fences from.

That last one used to be where a page and a working program most often
disagreed. § 10 moved the disagreement rather than removing it: a rendered fence
cannot drift, because the build breaks first, so what is left unguarded is the
prose *around* it. Read the region and the paragraph introducing it as one unit,
distrust any walkthrough that counts lines instead of naming them, and treat
every fence that opted out as owing you a clause.

## 2. Structure

**The boundary (§ 1).** Read the prose; do not match strings. The defect the
rule exists to catch is a *rule stated in vendor terms* — reasoning borrowed
from one system and presented as the framework's — and that has no keyword to
search for. Ask of each paragraph on a framework page: does this hold for every
connector of this role, or only for the one its author had in mind? If only
one, restate it neutrally (§ 1 notes the neutral version is usually the *truer*
one) or move it to the connector page. Where no neutral term exists, § 1 says
add one to the glossary rather than reaching for the vendor's word. On a
connector page the direction reverses: it may name its own vendor and no other.

**Quadrant containment (§ 6).** The "Does NOT contain" column is a checklist,
and nothing else in this repository checks it — exhaustive options in a
tutorial, step-by-step in `02-concepts/`, a full key reference in `03-guides/`,
framework internals in `05-deployment/`, teaching or opinion in reference.
Diátaxis applies at sentence scale as well as page scale, and a single
explanatory paragraph inside a tutorial is the commonest form of this.

**One home (§ 1).** Every vendor fact lives on exactly one page. When the fix
for a hit is "move it", check the destination first: if the fact is already
there, delete rather than move.

**Shape.** Connector pages go to
[references/connector-page.md](references/connector-page.md). Every page: no
YAML frontmatter, sentence-case headings, relative extension-qualified links
(§ 8) — all cheap, and mostly gated by step 4 anyway. The one that needs
judgment: **a framework page matches the section-heading convention of its
directory siblings.** `## Further reading` and `## Related` are not
interchangeable, and § 3 scopes `## Related` to connector pages, so check the
neighbors before calling a heading wrong.

Site pages do not cite invariant numbers — `docs/INVARIANTS.md` is where the
properties are stated, and a concepts page restating one in its own terms is
correct. Do not raise a missing `INV-N` citation on a user-guide page.

## 3. Prose — read it as the reader

Read the page top to bottom once, checking nothing, as the reader § 0 named:
fluent in async Rust, already holding the streaming concepts, knowing nothing
Spate-specific. Note every place you would stall, re-read, or give up and open
the rustdoc instead. **That list is the finding.** The catalog only helps you
name what you already felt.

**This read happens before step 1**, even though the pass is reported third. A
page you have already verified cannot be read cold, and there is no way back to
a first impression. Take the marks early, work them here.

[references/prose.md](references/prose.md) carries the failure modes, and — just
as load-bearing — the things that are explicitly **not** defects here. Read the
non-defects before reporting, so this pass does not turn into a lint.

The highest-yield single question: **is the first screen of this page the thing
the reader came for?**

## 4. Gates

```sh
make docs; echo "EXIT=$?"
make check-adr; echo "EXIT=$?"   # whenever docs/adr/ was touched
```

**Check the exit code explicitly**, here and everywhere. Piped `grep`/`tail`
chains report the status of the last command in the pipeline and have masked
real failures in this repository more than once.

`make docs` begins with `npm ci`, which deletes and rebuilds
`website/node_modules`. In a **report-only review that must not touch the tree**,
run `cd website && CI=true npm run build; echo "EXIT=$?"` instead — same gate,
no dependency churn, and `CI=true` is what registers the redirects. If even that
is out of scope, say the gate was deferred and resolve every link and anchor on
the page by hand; do not report a gate you did not run.

- `onBrokenLinks`, `onBrokenAnchors` and `onBrokenMarkdownLinks` are all
  `'throw'`. A stale link *or* a stale `#anchor` fails the build outright;
  there is no warning tier to scan the log for.
- **Every moved page needs a `{ from, to }` entry** in the `clientRedirects`
  plugin in `website/docusaurus.config.ts`. That plugin registers only under
  `CI=true`. `make docs` sets it; a bare `npm run build` does not, so a
  redirect whose `to` no longer exists fails in CI and passes locally.
- A docs-only diff routes to the `site` job alone (`scripts/ci-changes.sh`), so
  no cargo gate runs — unless you touched rustdoc, which is a different tree
  and outside this skill.

The boundary has no lint gate, deliberately. It turns on judgment at its edges
— an index row and a pointer block are legal, the same words in a sentence are
not — so a matcher would either pass everything or block everything. This
review is the gate. A sweep script for it was tried and removed; do not
reintroduce one.

## Modes

**Reviewing a change.** Steps 0–4 over the diff. Also read each changed page
whole: a boundary or quadrant defect is a property of the page, and a diff that
adds three sound sentences to the wrong page shows nothing wrong in isolation.
If the diff touches `docs/adr/`, settle immutability
([references/adr.md](references/adr.md) § 1) *before* reading for quality —
against an accepted record most findings resolve to "this needs a new record"
rather than to an edit, and that changes how you read.

**Authoring a page.** Route first (step 0), then write, then run steps 1–4
against your own draft. For a connector page, start from
`04-connectors/_template.mdx` rather than from a sibling connector — copying a
sibling copies its drift.

**Adding a connector.** [references/connector-page.md](references/connector-page.md)
end to end. The check that rots most on this path is card-index membership
(§ 2): every connector appears in exactly one role index, and a new page that
nothing links to is invisible while looking complete.

**Auditing the tree.** [references/audit.md](references/audit.md).

## If you are contributing rather than reviewing

`CONTRIBUTING.md` § Documentation states the boundary rule in short form, and
`docs/STYLE.md` is the normative version. Neither assumes tooling beyond a
clone; the site build needs Node.
