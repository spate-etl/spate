# Documentation standards

How we structure `docs/` so it stays navigable as connectors grow. These rules
are enforced by review (and the Docusaurus build gate — see the last section).
They follow established developer-documentation conventions: Diátaxis for the
page taxonomy, a connector split by role, and a single reference appendix for
configuration keys.

This file is a contributor reference; it is excluded from the rendered site.

## Who this is for

The reader of `docs/user-guide/` is a **Rust developer new to Spate but not new
to streaming**. Assume they are comfortable with async Rust, and that they
already hold the concepts the field shares — at-least-once delivery, consumer
groups, backpressure, partitioning. Assume they know nothing Spate-specific.

Two consequences, because both get broken by well-meaning edits:

- **Re-explaining a concept the reader already has buries the thing they came
  for**, which is how *this* system expresses it. A paragraph defining
  backpressure in general costs the paragraph describing what our source does
  when a queue rejects.
- **A Spate term used before § 7 has defined it loses them**, and nothing later
  on the page recovers it.

**The site and the rustdoc divide the work.** The site owns tutorial, how-to
and explanation; the rustdoc owns per-item reference — signatures, trait
contracts, `Errors` / `Panics` / `Safety`. A fact belongs to exactly one of
them. Where a page needs a signature, it links to the rustdoc rather than
restating it: a restated signature is a claim that rots silently, and nothing
gates it.

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
tuning numbers, vendor troubleshooting, or vendor client-library behavior.
State the rule in framework vocabulary and let the connector page carry the
mechanism. Usually the neutral statement is the *truer* one — "a source that
stops servicing its liveness protocol loses its work assignment" holds for a
consumer group, a lease TTL and a coordinated assignment alike, where the
vendor-specific version holds for one of them.

**Connector pages name one vendor: their own.** A connector page must not
explain another connector — link to it. An end-to-end example that necessarily
spans connectors is fine; describing another connector's behavior is not.

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
there — a term may carry one standardized trailing line:

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
| `06-extending/**` | Teaching someone to write a connector needs a real one to point at. | Shipped connectors appear as marked worked references (`worked reference: crates/spate-kafka/src/metrics.rs`), never as the normative prose. |
| Index pages — `04-connectors/README.mdx`, the role card indexes, `user-guide/README.mdx`, `07-reference/README.mdx`, the appendix's component mapping table | Indexing connectors and crates by name is their entire job. | Name and one-line summary; no behavior. |
| `03-guides/securing-connections.mdx` | It is the security hub, and a hub's content *is* the per-connector matrix. | Matrix rows and the framework-wide model; no mechanism — that lives on each connector page. |
| `07-reference/glossary.mdx` | Definitions need anchors. | The mapping line above, nothing looser. |

`docs/adr/` sits outside `user-guide/` and outside this rule: it records why
decisions were made, including vendor-specific ones. A decision about a
connector cannot be stated in neutral vocabulary without becoming a different
decision. It should not grow connector *usage* guidance — that belongs on the
connector page. See § 9 for the rest of what governs `docs/adr/`.

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
6. Behavioral sections (delivery semantics, backpressure, …).
7. `## Metrics` — the families this connector registers, with their labels.
   This is the only home for them; `docs/METRICS.md` carries the taxonomy and
   the framework families, not connector registries.
8. `## Related` — cross-links, each with an em-dash gloss.

The canonical wording above must **open** its sentence, verbatim. A paraphrase
is a defect; a continuation is not — "Construct it from the pipeline's opaque
section, with the runtime handle and a framer:" is fine, because a reader
scanning the connector pages still meets the same opening clause on each. Do
not improve the canonical part; do not lose information to preserve it.

**Defaults notation — exactly three forms, no others:**

| Form | Means |
|---|---|
| `required` | No default; the key must be set. |
| A backticked literal (`` `5s` ``, `` `lz4` ``) | The actual default value. |
| `none` | Optional, unset by default. |

Never put prose in the Default cell ("framework defaults", "unset"). If the
default is owned elsewhere, say so in the Description and link to
[`#sink-pool`](user-guide/07-reference/configuration.mdx#sink-pool).

A key whose default is an **empty map or list** takes the backticked literal —
`` `{}` ``, `` `[]` `` — not `none`. Both readings are defensible; the literal
wins because it says what the deserializer produces, and `none` cannot
distinguish an absent section from an empty one.

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
- **The page reads as the present, never as a changelog.** No "now", "recently",
  "as of", "has been changed to". If something moved, the page describes what
  is and the commit message says what moved. (§ 9 suspends this for `docs/adr/`.)
- **American English** — `serialize`, `behavior`, `normalize`. The API surface
  is permanently American, because serde owns `Serializer` and `serialize`, so
  British prose mismatches the identifier in the code block beside it. The
  site's search index matches tokens literally, so a page written "behaviour"
  is invisible to a reader who searches the spelling every other Rust crate
  taught them.
  `cancelled` is the one deliberate exception: it is a metric label value
  (`outcome="cancelled"`), and the prose matches the exposition.
- **No first-person plural on a rendered page.** No "we recommend", no "our
  design" — the framework has no voice of its own, so state the thing. This
  file, `CLAUDE.md` and `CONTRIBUTING.md` are contributor files and exempt,
  which is why this one opens "How we structure `docs/`".
- **Sentence case for headings**, except product nouns and identifiers
  (`ClickHouse Native format`, `` `SinkBundle` and the readiness probe ``).
- **Conditions before the instruction they govern.** "If the sink is remote,
  raise the timeout" — not "raise the timeout if the sink is remote". A reader
  who acts on the first half of that sentence has already acted wrongly.
- **Link text names its destination.** Never "here", "this page", "see this".
  The words under the link say what is on the other side, so it still works
  read out of context — which is how a skimming reader meets it.
- **Open with the subject, not with the page.** "This page explains how
  sharding works" spends the most valuable line in the document on nothing.
  Start with sharding.
- **Every `## Related` entry carries an em-dash gloss** saying why to follow it.
  A bare link list is not a Related section.
- **Define a term once**, in the glossary, and use it consistently after.
- **Identifiers keep the spelling the compiler uses**, in backticks: a type as
  `` `SinkBundle` ``, a method as `` `Sink::write` ``, a crate as
  `` `spate-kafka` ``, a facade feature as `` feature `kafka` ``. The concept
  is unbackticked and lowercase — "the sink bundle" is the idea, `SinkBundle`
  is the type. Do not mix the two in one sentence.
- **Admonitions are `:::note`, `:::warning` and `:::danger`, and nothing else.**
  `:::note` for the pointer block above and for an aside a reader may skip;
  `:::warning` for a footgun that costs time; `:::danger` only for what costs
  *data* — a delivery caveat, a setting that drops records, an operation with
  no undo. `:::tip` and `:::caution` are unused: a tip is either worth a
  sentence of prose or is not worth the reader's eye. Never stack two, and
  never open a section with one.
- **A quantitative claim carries how it was established.** Throughput and
  latency figures, and equally any number a reader sizes infrastructure from —
  "roughly 100 bytes per key" is a memory budget somebody will provision
  against, so it needs a source as much as a benchmark does. The load-bearing
  ones sit in the `Evidence` section of the decision record they justify, each
  with a line saying what measured it — down to "measured by a rig this
  repository no longer carries", which is a real provenance and an honest one.
  Match the wording already there rather than inventing a stronger-sounding
  one: a figure nobody can place is one nobody can later check, and the same
  figure worded two ways on two pages is worse than either.

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
  `onBrokenLinks: 'throw'` fails the build on a stale link, so `make docs` is
  the correctness gate for any move or rename.
- **Moving or renaming a page changes its URL** (URLs are path-derived). Add a
  `{ from, to }` entry to the `clientRedirects` plugin in
  `website/docusaurus.config.ts` for every moved page to keep old links alive.
  That plugin only registers under `CI=true`, so test redirects that way.
- **Run the gate before pushing:** `make docs`, checking the **exit code
  explicitly** — piped `grep`/`tail` chains have masked real failures here.
  `onBrokenLinks`, `onBrokenAnchors` and `onBrokenMarkdownLinks` are all
  `'throw'`, so a stale link *or* a stale `#anchor` fails the build outright
  rather than warning.

## 9. Decision records

`docs/adr/` holds one Architecture Decision Record per decision.
**`docs/adr/_template.md` is normative for their contents** — it states the
rules inline beside the section each governs, and there is no separate how-to
page, precisely so the two cannot drift. This section covers only where they sit
relative to everything else in `docs/`.

Decision records are **not documentation, and not a Diátaxis quadrant**. A page
in `user-guide/` says how the system behaves; a record says why that was chosen
and what else was considered. A reader who needs the first should be sent to the
guide, and a record that starts explaining how to configure something has become
the wrong kind of document.

Three of the rules above are deliberately suspended here, and each is a decision
rather than an oversight:

| Rule | Status in `docs/adr/` | Why |
|---|---|---|
| § 1 vendor neutrality | Suspended | A decision about a connector cannot be restated in neutral vocabulary without becoming a different decision. |
| § 7 present-tense, "never a changelog" | Suspended | A record is a point-in-time artifact by construction. It describes the decision as it stood, and dates it. |
| § 8 no YAML frontmatter | **Applies** | Records are published pages, so a frontmatter `title:` would render twice. Status is a body line instead. |

**Accepted records are immutable.** Never rewrite one to say something
different — that is the failure this section exists to prevent, and it is not
hypothetical: the decision-log table it replaced recorded reversals by
overwriting the rows they reversed, so the superseded reasoning was lost. A
changed decision is a new record; the old one keeps its body and gains a pointer
to its replacement. Correcting a typo, a broken link or a wrong path is not
rewriting; changing what the record claims was decided is.

Everything else follows the rest of this file: sentence case, relative
extension-qualified links, and an em-dash gloss on every `## Related` entry.

`make check-adr` holds the mechanical half — numbers unique and never reused,
statuses from the permitted set, no unfilled placeholders, and every record
present in `docs/adr/README.mdx`. Whether a decision warranted a record at all
is review's job.

## 10. Code examples

A snippet is the part of the page a reader pastes, so it is the highest-rot
surface on it. Prose that goes stale reads oddly; an example that goes stale
fails to compile in someone else's editor.

- **Every fence carries a language tag** — `rust`, `yaml`, `toml`, `sh`, `text`.
  Use `text` for output, logs and trees. Never leave a fence blank to avoid
  choosing: it loses highlighting and tells the reader nothing about what they
  are looking at.
- **Shell blocks carry no `$` prompt.** The reader copies the line, and a
  prompt makes them edit it before it runs.
- **Names carry meaning** — `orders`, `order_id`, `clickhouse_sink`, never
  `foo`, `bar` or `my_thing`. A worked example doubles as a naming example
  whether it was meant to or not.
- **`?` over `unwrap()`.** Example code is copied verbatim into places where a
  panic is not acceptable, so error handling in an example is a correctness
  matter rather than a style one. Where `?` needs a signature to return into,
  show the signature.
- **A snippet names what it needs to build** — the facade features it requires,
  and the runtime if it is async. One that compiles only under a feature the
  reader has not enabled reads as a bug in the framework.

### Rust snippets come from compiled sources

**A non-trivial Rust snippet on a site page is rendered from a compiled source
under `crates/`, not hand-written into the Markdown.** Nothing compiles a fenced
block in an `.mdx` file — the site build does not type-check it and `cargo test`
never sees it — so a wrong one survives every gate this repository has. What it
is rendered from does not: `cargo clippy --workspace --all-targets` compiles
everything under `crates/`, so a region breaks the build when the API moves,
which is the whole point.

Leave the fence empty and name the source and the region on the info string:

````
```rust file=crates/spate/examples/memory_pipeline.rs region=chain
```
````

Mark the region in the source with mdBook's anchor comments. They are stripped
from what renders, so they cost the reader nothing:

```rust
// ANCHOR: chain
// ANCHOR_END: chain
```

A file that does not exist, a region with no matching pair of markers, and a
fence carrying both `file=` and hand-written content each fail the build
outright — the same tier as a stale link (§ 8), and for the same reason: a
pointer nobody checks is a pointer that rots. `make check-transclusions` holds
the same rule without a site build.

Prefer a source under `crates/spate/examples/`, because a reader can run it.
Reach past that only for something an example cannot show — a connector's own
wiring, or a test whose subject is testing.

Name a region for what the code *is*, never for where it sits on a page:
`chain`, `encoder`, `coordinator`, not `step_3`. Pages get reorganized and the
region outlives the heading above it. A region is one contiguous stretch, so a
page wanting to stitch two apart wants two fences with the sentence that belongs
between them.

Exempt, because transcluding them costs more than it protects:

- A fragment of two or three lines illustrating a single call or signature.
- A type or trait definition quoted to be read rather than run, including one
  abridged to the members under discussion.
- A snippet that deliberately does not compile — one showing what the type
  system rejects, or a skeleton with a part elided to expose a shape. Say which
  in the surrounding prose: an elision nobody announced is indistinguishable
  from a snippet that is simply wrong, and that is what this exemption most
  often decays into.

An exemption is a claim review checks, not a default.

YAML, TOML and shell blocks are unaffected; their correctness is checked
against the config structs by review (§ 3), not by a compiler.
