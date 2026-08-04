# Documentation standards

How we structure `docs/` so it stays navigable as connectors grow. These rules
are enforced by review (and the Docusaurus build gate — see the last section).
They follow established developer-documentation conventions: Diátaxis for the
page taxonomy, a connector split by role, and a single reference appendix for
configuration keys.

This file is a contributor reference; it is excluded from the rendered site.

## 1. The framework/connector boundary

Two layers, and prose must not cross from the first into the second:

- **Framework pages** — everything under `docs/user-guide/` except
  `04-connectors/**`. They describe the framework, which has no vendors in it.
- **Connector pages** — `docs/user-guide/04-connectors/**`. They describe one
  connector each.

This mirrors an architectural boundary the code already enforces: no
connector-crate types in `spate-core`'s public API, and connector metrics
namespaced through a `Meter` so they cannot collide with the framework
taxonomy. The docs are the third face of the same rule.

### The rule

In framework **prose**, a connector or vendor name may appear only:

- as a link label,
- as an entry in a `## Related` list,
- inside a pointer block (below),
- inside a literal repository path (`crates/spate-kafka/src/metrics.rs`) — a
  path is a pointer to code, like a link, not prose about a vendor.

Never in framework prose: vendor setting keys (`max.poll.interval.ms`), vendor
tuning numbers, vendor troubleshooting, or vendor client-library behaviour.
State the rule in framework vocabulary and let the connector page carry the
mechanism. Usually the neutral statement is the *truer* one — "a source that
stops servicing its liveness protocol loses its work assignment" holds for a
consumer group, a lease TTL and a coordinated assignment alike, where the
vendor-specific version holds for one of them.

**Connector pages name one vendor: their own.** A connector page must not
explain another connector — link to it. An end-to-end example that necessarily
spans connectors is fine; describing another connector's behaviour is not.

**The rule governs prose, not fenced code and YAML blocks.** A config example
has to say `kafka:`; there is no fictional tag that resolves. Code blocks are
exempt, their surrounding prose is not.

**One home.** Every vendor fact lives on exactly one page and everywhere else
links to it. Without this the same paragraph regrows in four places — which is
exactly what happened to the librdkafka prefetch guidance before this rule
existed.

**The glossary is the register of framework vocabulary.** If no neutral term
exists for what you need to say, add one to
[`07-reference/glossary.mdx`](user-guide/07-reference/glossary.mdx) rather than
reaching for the vendor's word.

### The pointer block

The one sanctioned way to send a reader from a framework page to the
connector-specific detail. Pointers, not explanation:

```markdown
:::note Connector specifics
How each source expresses this: [Kafka](…) · [S3](…).
:::
```

### The glossary mapping line

A definition often needs a concrete anchor to land. In the glossary — and only
there — a term may carry one standardised trailing line:

```markdown
*Connector mapping:* [Kafka](…) — one partition queue · [S3](…) — one in-flight split.
```

One clause per connector, always linked. A new connector adds an entry; it does
not add a sentence to the definition.

### Exemptions

Each is a decision, not drift. Nothing else is exempt.

| Area | Why | Bound |
|---|---|---|
| `01-getting-started/**` | A tutorial must be concrete — a reader cannot follow an abstract pipeline. | Declares its stack in the first paragraph. |
| `07-reference/ci.mdx`, `supply-chain.mdx`, `licensing.mdx` | These document *this repository*, not framework behaviour. Real crate names are the subject matter. | Facts about the repo only; no connector usage guidance. |
| `06-extending/**` | Teaching someone to write a connector needs a real one to point at. | Shipped connectors appear as marked worked references (`worked reference: crates/spate-kafka/src/metrics.rs`), never as the normative prose. |
| Index pages — `04-connectors/README.mdx`, the role card indexes, `user-guide/README.mdx`, `07-reference/README.mdx`, the appendix's component mapping table | Indexing connectors and crates by name is their entire job. | Name and one-line summary; no behaviour. |
| `03-guides/securing-connections.mdx` | It is the security hub, and a hub's content *is* the per-connector matrix. | Matrix rows and the framework-wide model; no mechanism — that lives on each connector page. |
| `07-reference/glossary.mdx` | Definitions need anchors. | The mapping line above, nothing looser. |

`docs/DESIGN.md` sits outside `user-guide/` and outside this rule: it records
why decisions were made, including vendor-specific ones. It should not grow
connector *usage* guidance — that belongs on the connector page.

## 2. Connector layout — group by role

Connectors live under `docs/user-guide/04-connectors/`, grouped by role:

```
04-connectors/
  README.mdx            hub: the connector matrix + per-connector guarantee notes
  _template.mdx         the page scaffold for a new connector (not rendered)
  sources/              feed the pipeline        (README.mdx card index + one folder per source)
  sinks/                write records out        (README.mdx card index + one folder per sink)
  formats/              decode payloads          (README.mdx card index + one folder per format)
  coordination/         back the coordination seam (README.mdx card index + one folder per store)
  memory.mdx            dual-role test connector — the one deliberate flat-file exception
```

Rules:

- **One connector = one folder with a `README.mdx`** (the connector's overview
  page). Never mix flat files and folders in the same role directory. A
  single-page connector (e.g. the Kafka source) is still a folder with just a
  `README.mdx` — this keeps the tree uniform.
- **A crate that is both a source and a sink** is documented as two pages, one
  under `sources/` and one under `sinks/` — the reader is looking for a role,
  not a crate.
- **A coordination store is a connector.** It plugs into a trait seam, ships
  behind its own facade feature, and carries config and metrics like any other
  — so it is documented like any other, under `coordination/`.
- **Each role directory has a `README.mdx` card index**: a table of
  `name · config tag · one-line summary`, one row per connector. Every
  connector in the tree appears in exactly one card index.
- **Memory / Capture** is the sole exception: it is a dual-role test connector,
  so it is a flat `memory.mdx` at the connectors root, cross-listed (not moved)
  from the `sources/` and `sinks/` indexes. Its shape is likewise exempt from
  the § 3 template — one page documents both roles, section per role.

## 3. Per-connector page template

Copy `04-connectors/_template.mdx`. The heading order is fixed:

1. `# <Connector> (source|sink|format|coordination store)`, then a
   one-paragraph summary in the canonical shape: "The X <role> (`crate`,
   feature `f` on the `spate` facade) …".
2. A construct-it snippet, introduced with the canonical sentence
   ("Construct it from the pipeline's opaque section:").
3. `## Configuration` — the canonical sentence "deserializes into `<Config>`
   (`path`); unknown fields are rejected with the offending key", a YAML
   example, then a `| Key | Type | Default | Description |` table of
   **connector-owned keys only**. Do not restate framework-owned sink-pool keys
   — link to the [appendix](#4-configuration-appendix).
4. The passthrough section and its denylist table, if the connector has a raw
   passthrough. A passthrough that exists must have a section; a table row is
   not enough.
5. `## Security …` — **mandatory for any connector that takes credentials.**
   Connector-specific auth/TLS, linking back to the
   [securing hub](user-guide/03-guides/securing-connections.mdx). Shared prose
   goes in an MDX partial (see §5).
6. Behavioural sections (delivery semantics, backpressure, …).
7. `## Metrics` — the families this connector registers, with their labels.
   This is the only home for them; `docs/METRICS.md` carries the taxonomy and
   the framework families, not connector registries.
8. `## Related` — cross-links, each with an em-dash gloss.

**Defaults notation — exactly three forms, no others:**

| Form | Means |
|---|---|
| `required` | No default; the key must be set. |
| A backticked literal (`` `5s` ``, `` `lz4` ``) | The actual default value. |
| `none` | Optional, unset by default. |

Never put prose in the Default cell ("framework defaults", "unset"). If the
default is owned elsewhere, say so in the Description and link to
[`#sink-pool`](user-guide/07-reference/configuration.mdx#sink-pool).

## 4. Configuration appendix

`docs/user-guide/07-reference/configuration.mdx` is the appendix: one flat,
exhaustive table of every configuration key. It is **reference only** — no
walkthroughs (those go in `03-guides/configuring-pipelines.mdx`).

- **Columns:** `Key · Type · Default · Description`. Keys in backticks;
  defaults use the three forms in §3.
- **Grouped by config section** (`pipeline:`, `checkpoint:`, sink-pool, …), a
  flat table within each group, each group under an `## anchor`.
- **Single source of truth for framework-owned keys.** The `pipeline`,
  `checkpoint`, `backpressure`, `metrics`, and `SinkPoolConfig`
  (`batch`/`inflight`/`retry`/`breaker`) tables live here and nowhere else.
  Connector pages link to `#sink-pool` instead of restating those defaults.
- **Connector-owned keys stay on the connector page** and are indexed from the
  appendix's `source:/deserializer:/sink:/coordination:` mapping table. The
  appendix carries a non-exhaustive disclaimer.

## 5. Cross-cutting content — single source of truth

Content that would be identical across pages lives once and is linked or
embedded, never copy-pasted.

- **Security** is hub-and-spoke: the generic model (secrets via `${VAR}`,
  auth-failure-is-fatal, the connector matrix) lives in
  `03-guides/securing-connections.mdx`; connector specifics live on each
  connector page and link back. The hub holds pointers, not mechanisms.
- **Identical prose shared by two pages** uses an MDX partial — a file named
  `_name.mdx` (underscore-prefixed, so Docusaurus does not render it as a
  page), imported with `import X from '…/_name.mdx'` and rendered as `<X />`.
  Example: `04-connectors/_securing-kafka.mdx` is the one Kafka TLS/SASL
  matrix, imported by both Kafka pages. Relative links inside a partial resolve
  from the partial's own location. A partial rendered by two pages must sit at
  the same position in both.

## 6. Diátaxis — one page, one purpose

The `user-guide/` tree maps to Diátaxis; keep each page in exactly one quadrant:

| Directory | Quadrant | Contains | Does NOT contain |
|---|---|---|---|
| `01-getting-started/` | Tutorial | guided lessons | exhaustive options |
| `02-concepts/` | Explanation | the *why*, trade-offs | step-by-step tasks |
| `03-guides/` | How-to | goal-oriented steps | full key references |
| `05-deployment/` | How-to | running it in production | framework internals |
| `06-extending/` | How-to | writing your own components | connector tutorials |
| `04-connectors/`, `07-reference/` | Reference | neutral, exhaustive facts | teaching, opinions |

- The "why" behind a knob goes in `02-concepts/`, not in the reference table.
- Reference tables do not live in tutorials or how-tos — link to the appendix.
- How-to titles are goals ("Securing connections"); tutorial titles are lessons
  ("Your first pipeline").
- A page that turns out to be entirely about one connector belongs under
  `04-connectors/`, whatever quadrant it reads as.

## 7. Voice and prose

Voice follows the quadrant, so there is one rule to remember rather than two:

- **Second person** in Tutorial and How-to (`01-getting-started`, `03-guides`,
  `05-deployment`, `06-extending`) — the reader is doing something.
- **Impersonal** in Explanation and Reference (`02-concepts`, `04-connectors`,
  `07-reference`) — the text describes the system, not the reader. Write "the
  sink retries", not "you get a retry".

Everywhere:

- **Present-tense declarative.** "Unknown fields are rejected", not "will be".
- **Sentence case for headings**, except product nouns and identifiers
  (`ClickHouse Native format`, `` `SinkBundle` and the readiness probe ``).
- **Every `## Related` entry carries an em-dash gloss** saying why to follow it.
  A bare link list is not a Related section.
- **Define a term once**, in the glossary, and use it consistently after.

## 8. Docusaurus hygiene

- **Sidebar is autogenerated** from folder structure. Order via `NN-` numeric
  filename prefixes (stripped from the URL) and `_category_.json` `position`;
  human labels via `_category_.json` `label`. Do not hand-edit `sidebars.ts` for
  user-guide pages.
- **Every category folder has a `_category_.json`** (`label`, and `position`
  where order isn't obvious) and a `README.mdx` landing page — no dead-click
  categories.
- **Pages carry no YAML frontmatter.** The H1 is the title; a frontmatter
  `title:` alongside an H1 renders twice.
- **Internal links are relative and extension-qualified** (`../foo/bar.mdx`).
  `onBrokenLinks: 'throw'` fails the build on a stale link, so `npm run build`
  is the correctness gate for any move or rename.
- **Moving or renaming a page changes its URL** (URLs are path-derived). Add a
  `{ from, to }` entry to the `clientRedirects` plugin in
  `website/docusaurus.config.ts` for every moved page to keep old links alive.
  That plugin only registers under `CI=true`, so test redirects that way.
- **Run the gate before pushing:** `cd website && npm run build`, checking the
  **exit code explicitly** — piped `grep`/`tail` chains have masked real
  failures here. Scan the log for anchor warnings (`onBrokenAnchors: 'warn'`),
  which do not fail the build.
