---
name: docs-review
description: Review or author documentation in docs/ — checks the framework/connector boundary, the Diátaxis quadrant, the connector page template, voice, and the build gates. Use when writing a new docs page, editing an existing one, adding a connector, or auditing the tree for drift.
---

# Documentation review

**The rules live in `docs/STYLE.md`. This file is the procedure for applying
them.** If the two ever disagree, STYLE.md wins and this file is the bug. Never
restate a rule here — link to its section.

Read `docs/STYLE.md` before doing anything else. It is short, and every step
below assumes it.

## 1. Classify the page before reading it

Two axes, both from the path. Get these wrong and every later judgement is
wrong.

**Layer** — `docs/user-guide/04-connectors/**` is a connector page; everything
else under `docs/user-guide/` is a framework page. (§1)

**Quadrant** — from the directory, per the table in §6. This determines both
what belongs on the page and its voice (§7).

If a page turns out to be entirely about one connector, it is in the wrong
directory. Moving it is the fix; editing it is not. A move changes the URL —
see step 5.

## 2. Audit the boundary

For a framework page, sweep the prose for vendor and product names:

```sh
grep -rinE 'kafka|clickhouse|avro|confluent|redpanda|\bs3\b|\baws\b|\bnats\b|jetstream|seaweedfs|minio|object_store|mergetree|rowbinary' \
  docs/user-guide --include='*.mdx' --exclude-dir=04-connectors
```

Discard hits inside fenced blocks, whole markdown links, and the files §1's
table exempts — those are the sanctioned forms. Everything left is prose that
names a vendor.

Classify each remaining hit before touching it — they need different fixes:

| Hit | Fix |
|---|---|
| A rule stated in vendor terms | Restate it in framework vocabulary. The neutral version is usually *truer*: check whether the claim actually generalises, because if it does, the vendor-specific wording was hiding that. |
| Vendor mechanism, setting key, or tuning number | Move it to the connector page. If that page already says it, **delete rather than move** — see the one-home rule. |
| An illustration that earns its place | Convert to a pointer block, or to a glossary mapping line. |
| An index row, a repo path, a marked worked reference | Already allowed (§1's sanctioned forms and exemptions) — leave it. |

For a connector page, grep for **other** vendors' names. An end-to-end example
that spans connectors is fine; describing another connector's behaviour is not.

## 3. Check the shape

Connector pages: diff the `##` headings against `04-connectors/_template.mdx`.

```sh
grep -n '^## ' docs/user-guide/04-connectors/<role>/<name>/README.mdx
```

Section order is fixed and section names are fixed (§3). Then check:

- Column header is `Description`, not `Meaning`.
- Every Default cell is one of exactly three forms — `required`, a backticked
  literal, `none`. Prose in a Default cell is a defect.
- A connector with a passthrough has a passthrough **section**, not just a
  table row.
- A connector taking credentials has a `## Security` section linking the hub.
- Every `## Related` entry has an em-dash gloss.
- Connector-owned metric families are on the page, under `## Metrics` — not in
  `docs/METRICS.md`, which carries the taxonomy and framework families only.

All pages: no YAML frontmatter (the H1 is the title), sentence-case headings,
voice matching the quadrant (§7).

## 4. Check the claims

The boundary work is textual, so it is easy to do while quietly propagating a
false statement. Anything the page asserts about behaviour, defaults, or
config keys should be checked against the source — a `Default` column and a
YAML example are the two places that rot fastest, and neither is covered by any
gate. Config examples in particular: paste-and-run them mentally against the
connector's `deny_unknown_fields` struct.

## 5. Gates

```sh
cd website && npm ci && npm run build; echo "EXIT=$?"
```

(`npm ci` only on a fresh clone or after a dependency bump; `npm run build`
alone otherwise.)

**Check the exit code explicitly.** Piped `grep`/`tail` chains have masked real
failures in this repo. `onBrokenLinks: 'throw'` makes this the correctness gate
for every retargeted link.

Then:

- Scan the log for `onBrokenAnchors` warnings — anchors only *warn*, so a stale
  `#anchor` survives a green build.
- **Every moved page needs a `{ from, to }` entry** in the `clientRedirects`
  plugin in `website/docusaurus.config.ts`. That plugin only registers under
  `CI=true`, so a local build does not exercise redirects; a redirect whose
  `to` no longer exists fails the CI build.

A docs-only diff routes to the `site` job alone (`scripts/ci-changes.sh`), so
no cargo gates apply — unless you touched rustdoc, which is a different tree.

The boundary itself has no lint gate, deliberately: it has judgement at its
edges — an index row and a pointer block are legal, the same words in a
sentence are not — so it is enforced by this review, with the grep in step 2
as the sweep.

## If you are contributing rather than reviewing

`CONTRIBUTING.md` § Documentation states the same boundary rule in short form,
and `docs/STYLE.md` is the normative version. Neither assumes any tooling
beyond a clone; the site build needs Node.
