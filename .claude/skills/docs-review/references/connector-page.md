# Connector pages

Applies to `docs/user-guide/04-connectors/**`, on top of steps 1–4. Rules are in
`docs/STYLE.md`; every check below cites the section that owns it, and `§ N`
always means STYLE.md. The numbered headings here are this file's own. If a
check here disagrees with STYLE.md, STYLE.md wins.

Work straight down when adding a connector. When reviewing an edit, run the
headings the diff touches plus Placement and Reachability. Both break without
the diff showing anything.

## 1. Placement (§ 2)

- The connector is a folder with a `README.mdx`, under the role directory that
  matches its **role**, not its crate.
- No flat file sits beside folders in a role directory. `memory.mdx` at the
  connectors root is the sole exception, and it is also exempt from the § 3
  template. Do not template-check it.
- A crate that is both source and sink is **two pages**, one per role.
  `spate-kafka` is the worked case; a single page covering both is the defect.
- A coordination store is filed under `coordination/`, not under `sources/`
  because it is what a source coordinates through.
- The new folder has a `_category_.json` (§ 8). Without it the sidebar shows a
  raw directory name and the category dead-clicks.

## 2. Heading shape (§ 3)

Diff the page's headings against the template rather than reading for them:

```sh
grep -n '^#' docs/user-guide/04-connectors/<role>/<name>/README.mdx
grep -n '^#' docs/user-guide/04-connectors/_template.mdx
```

- Order and names match. Sections may be **deleted**, never reordered or
  renamed. Behavioral sections (§ 3 item 6) are the only free slot.
- Sub-pages beside the `README.mdx` (tuning, permissions, a format note) are not
  template sections. Check them as ordinary reference pages, and check the
  `README.mdx` links each one.
- A page authored by copying a sibling inherits the sibling's drift. If the
  heading diff is clean but odd, check the sibling too.

## 3. The three canonical openings (§ 3)

Nothing else in the repository checks these, and they are what make the set of
connector pages read as one reference:

- The H1 summary paragraph in the shape § 3 item 1 fixes (role, crate, facade
  feature), before any prose about what the connector does.
- The construct-it sentence introducing the snippet (§ 3 item 2), worded as the
  template words it.
- The `## Configuration` opening sentence (§ 3 item 3), naming the config type,
  its path, and that unknown fields are rejected with the offending key.

A page that paraphrases these reads fine in isolation and wrong in sequence.
Restore the canonical wording; do not improve it here.

## 4. The configuration table (§ 3)

- **Connector-owned keys only.** Framework-owned sink-pool keys
  (`batch`/`inflight`/`retry`/`breaker`) are linked to
  `07-reference/configuration.mdx#sink-pool`, never restated. A restated
  sink-pool table is internally consistent, matches the code on the day it is
  written, and passes every other check on this list. Read the key names
  against § 4's framework-owned set explicitly.
- Every `Default` cell is one of the three forms in § 3: `required`, a
  backticked literal, or `none`. **No prose in a Default cell.** "framework
  defaults", "unset", "see below" are all the same defect. Where the default is
  owned elsewhere, the Description carries that and links `#sink-pool`.
- The YAML example deserializes: read it as the deserializer (step 1).

## 5. Passthrough (§ 3)

- A connector with a raw passthrough has a **section**, not a row. A denylist
  table appearing under `## Configuration` with no section of its own is the
  failure shape.
- **The section carries a `| Denied property | Why |` table**, one row per
  entry in the crate's `DENYLIST`. `_template.mdx` mandates it. A section that
  describes the denial in a sentence passes the bullets above and is still
  wrong: prose cannot enumerate the rejected aliases, and the reader hitting a
  load failure needs the exact key.
- Each `Why` names which framework guarantee overriding it would break, not
  that the key is denied.

## 6. Security (§ 3, § 5)

- **Any connector taking credentials has `## Security`.** Credentials include
  environment-seeded ones the page never names as configuration.
- It links back to `03-guides/securing-connections.mdx` and carries the
  connector-specific mechanism.
- Check the hub in the other direction: the hub holds
  **pointers and the framework-wide model, not mechanisms** (§ 5). A hub that has
  grown a vendor's TLS property names is the defect, even though the connector
  page it duplicates is correct.

## 7. Shared prose and partials (§ 5)

- Prose identical on two pages is an MDX partial, underscore-prefixed so
  Docusaurus does not render it as a page.
- **Relative links inside a partial resolve from the partial's own location**,
  not from the page importing it. A partial that moves, or a new importer at a
  different depth, breaks links that read as correct in the importing page's
  frame. `make docs` catches it (step 4), but only once the importer exists.
- A partial rendered by two pages sits at the **same position in both**. Drift
  here is silent: both pages are individually well-formed.

## 8. Metrics and Related (§ 3)

- `## Metrics` is the connector's families with their labels, and is their
  **only** home. `docs/METRICS.md` carries the taxonomy and the framework
  families; a connector family restated there is a second source of truth.
- Every `## Related` entry carries an em-dash gloss saying why to follow it. A
  bare link list is not a Related section. This one rots by accretion, so check
  entries added by the diff, not only the section's existence.

## 9. Reachability — indexes and the appendix (§ 2, § 4)

The add-a-connector path's highest-rot step. A new page that nothing links to is
invisible while looking complete, and no gate catches it because a page with no
inbound links breaks no link.

- **The role's `README.mdx` card index has a row**, `name · config tag ·
  one-line summary` (§ 2). Every connector in the tree appears in **exactly
  one** card index; a dual-role crate's two pages are one row each, in different
  indexes. Memory is cross-listed from `sources/` and `sinks/`, not moved.
- `04-connectors/README.mdx`'s matrix has the connector. Index pages are exempt
  from § 1 by name and one-line summary only. A row that grew behavior is a
  boundary defect on an exempt page.
- The appendix's mapping table lists the connector (§ 4). The table is under
  `## `source:`, `deserializer:`, `sink:`` with columns
  `Section | Required | Body documented in`, so it is **one row per section, not
  per connector**. The check is that the connector appears as a link inside its
  section's `Body documented in` cell. There is no `coordination:` row and there
  should not be. Coordination is assembled in code, and the appendix files it
  under its code-level knobs instead.
- Framework-owned keys were **not** duplicated onto the connector page by the
  same change (§ 4), the mirror of the § 4 check above, worth running from the
  appendix side when keys moved.
- The appendix is still **reference only** (§ 4): a key added with an
  explanation of when to reach for it has turned a row into a walkthrough. That
  belongs in `03-guides/configuring-pipelines.mdx`, and the non-exhaustive
  disclaimer stays.

## Tuning sections are not a quadrant defect

§ 6 says Reference contains no teaching or opinions, and a connector page with a
`### Sizing …` section reads like a violation. It is not. Connector tuning lives
on the connector's own page. The framework's tuning page carries framework
knobs, and a connector's guidance stays with its keys. Judge such a section on
whether it is *about this connector*: sizing its splits against a framework knob
belongs here; a general lesson on how backpressure works does not.

What is still a defect in these sections is editorializing. "losing a whole
object silently is not a policy, it's an incident" is an argument, and the
argument belongs in the decision record the page should link instead.

## What this file does not cover

Claim verification against the config struct and the `names.rs` constants
(step 1), the vendor direction on a connector page (step 2), prose (step 3), and
`make docs` (step 4). Sub-pages under a connector folder get steps 1–4 and
Placement, not the § 3 template.
