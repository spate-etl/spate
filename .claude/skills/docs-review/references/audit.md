# Auditing the tree

For a whole-tree sweep rather than a single page or diff. Use it when checking
for drift after a restructure, before a release, or when a rule has changed.

An audit is **reading, fanned out**. There is no matcher for the defects that
matter: a rule stated in vendor terms borrows the reasoning without borrowing
the name, a quadrant leak is a paragraph in the wrong register, and a buried
lede is invisible to every tool. A sweep script for the boundary was written
once and removed; do not reintroduce one.

## Fan out by directory

The rendered tree under `docs/user-guide/`, plus the records under `docs/adr/`,
is more than one reviewer should take at a sitting, so split it. Each unit below
is independent, with no shared state and no ordering:

| Unit | Carries |
|---|---|
| `01-getting-started/` | Tutorial discipline; § 1's concrete-stack exemption |
| `02-concepts/` | Explanation discipline; the heaviest prose in the tree |
| `03-guides/` | How-to titles as goals; the security hub |
| `04-connectors/sources/`, `sinks/`, `formats/`, `coordination/` | One unit per role; template conformance |
| `05-deployment/`, `06-extending/` | § 1's worked-reference exemption in `06-` |
| `07-reference/` + `METRICS.md` + `INVARIANTS.md` | The appendix as single source of truth |
| `docs/adr/` | [adr.md](adr.md) — different rules entirely |

Give each unit the same brief: [SKILL.md](../SKILL.md) steps 1–3 over every page
in it, reporting findings rather than applying fixes. Ask for the quadrant and
layer of each page as part of the report. A unit that cannot state them has
found a filing defect.

**Do not fan out the fixes.** Findings come back to one place, get deduplicated,
and get applied in a reviewed order. Parallel agents editing overlapping pages
produce conflicts and inconsistent phrasing of the same rewrite.

## What only an audit can see

Per-page review cannot catch these, so spend the audit's advantage on them:

- **A vendor fact with two homes** (§ 1). Two connector pages, or a connector
  page and a framework page, carrying the same tuning guidance. Each reads fine
  alone; together one of them is wrong, and the pair does not say which.
- **A term defined twice, differently.** § 7 requires one definition in the
  glossary. Drift shows only when the definitions are read side by side.
- **Card-index membership** (§ 2). Every connector appears in exactly one role
  index. A connector missing from its index is invisible while its page looks
  complete; a connector in two is a filing error.
- **Appendix coverage** (§ 4). Framework-owned keys that grew onto a connector
  page, and connector sections missing from the mapping table.
- **Orphans.** A page nothing links to and no sidebar reaches. `make docs` does
  not fail on it. A page can render perfectly and be unreachable.
- **Quadrant balance.** A `02-concepts/` that has become reference, or a
  `03-guides/` where half the pages are tutorials. Visible only in aggregate.

## Close it out

Report each finding as the path, the rule number, what it says now, and what it
should say. Group by fix rather than by page. The same rewrite applied in six
places is one decision, not six.

Then run step 4's gates over whatever was changed, by explicit exit code.

If the audit finds a rule that keeps getting broken the same way, the rule is
probably unclear rather than the contributors careless. That is a `docs/STYLE.md`
change, not a bigger checklist here.
