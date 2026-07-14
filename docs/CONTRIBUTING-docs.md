# Documentation standards

How we structure `docs/` so it stays navigable as connectors grow. These rules
are enforced by review (and the Docusaurus build gate — see the last section).
They are derived from flagship developer-docs conventions: Diátaxis, Vector.dev
and Airbyte connector taxonomy, and the Spring Boot properties appendix.

This file is a contributor reference; it is excluded from the rendered site.

## 1. Connector layout — group by role

Connectors live under `docs/user-guide/04-connectors/`, grouped by role:

```
04-connectors/
  README.mdx            hub: the connector matrix + per-connector guarantee notes
  sources/              feed the pipeline        (README.mdx card index + one folder per source)
  sinks/                write records out        (README.mdx card index + one folder per sink)
  formats/              decode payloads          (README.mdx card index + one folder per format)
  memory.mdx            dual-role test connector — the one deliberate flat-file exception
```

Rules:

- **One connector = one folder with a `README.mdx`** (the connector's overview
  page). Never mix flat files and folders in the same role directory. A
  single-page connector (e.g. Kafka source) is still a folder with just a
  `README.mdx` — this keeps the tree uniform and matches ClickHouse/Avro.
- **A crate that is both a source and a sink** (Kafka) is documented as two
  pages, one under `sources/` and one under `sinks/` — the reader is looking for
  a role, not a crate. Vector and Airbyte both split this way.
- **Each role directory has a `README.mdx` card index**: a table of
  `name · config tag · one-line summary`, one row per connector.
- **Memory / Capture** is the sole exception: it is a dual-role test connector,
  so it is a flat `memory.mdx` at the connectors root, cross-listed (not moved)
  from the `sources/` and `sinks/` indexes.

## 2. Per-connector page template

A connector `README.mdx` follows this heading order:

1. `# <Connector> (source|sink|format)` + a one-paragraph summary naming the
   crate and the `etl` feature.
2. A "construct it from the pipeline's opaque section" code snippet.
3. `## Configuration` — the `deserializes into <Config> …; unknown fields are
   rejected` sentence, a YAML example, then a `| Key | Type | Default | Description |`
   table of **connector-owned keys only**. Do not restate framework-owned
   sink-pool keys — link to the [appendix](#3-configuration-appendix). (Some
   older tables still say "Meaning"; new/edited tables use "Description".)
4. Passthrough/denylist section, if the connector has a raw passthrough.
5. `## Security …` — connector-specific auth/TLS; link back to the
   [securing hub](user-guide/03-guides/securing-connections.mdx). Shared prose
   goes in an MDX partial (see §4).
6. Behavioural sections (delivery semantics, backpressure, …).
7. `## Related` — a short cross-link list.

## 3. Configuration appendix

`docs/user-guide/07-reference/configuration.mdx` is the appendix, modelled on
Spring Boot's Common Application Properties. It is **reference only** — no
walkthroughs (those go in `03-guides/configuring-pipelines.mdx`).

- **Columns:** `Key · Type · Default · Description`. Keys in backticks; a blank
  default (`—`) means the key is required.
- **Grouped by config section** (`pipeline:`, `checkpoint:`, sink-pool, …), a
  flat table within each group, each group under an `## anchor`.
- **Single source of truth for framework-owned keys.** The `pipeline`,
  `checkpoint`, `backpressure`, `metrics`, and `SinkPoolConfig`
  (`batch`/`inflight`/`retry`/`breaker`) tables live here and nowhere else.
  Connector pages link to `#sink-pool` instead of restating those defaults.
- **Connector-owned keys stay on the connector page** and are indexed from the
  appendix's `source:/deserializer:/sink:` mapping table. The appendix carries a
  non-exhaustive disclaimer.

## 4. Cross-cutting content — single source of truth

Content that would be identical across pages lives once and is linked or
embedded, never copy-pasted.

- **Security** is hub-and-spoke: the generic model (secrets via `${VAR}`,
  auth-failure-is-fatal, the connector matrix) lives in
  `03-guides/securing-connections.mdx`; connector specifics live on each
  connector page and link back.
- **Identical prose shared by two pages** uses an MDX partial — a file named
  `_name.mdx` (underscore-prefixed, so Docusaurus does not render it as a page),
  imported with `import X from '…/_name.mdx'` and rendered as `<X />`. Example:
  `04-connectors/_securing-kafka.mdx` is the one Kafka TLS/SASL matrix, imported
  by both Kafka pages. Relative links inside a partial resolve from the
  partial's own location.

## 5. Diátaxis — one page, one purpose

The `user-guide/` tree maps to Diátaxis; keep each page in exactly one quadrant:

| Directory | Quadrant | Contains | Does NOT contain |
|---|---|---|---|
| `01-getting-started/` | Tutorial | guided lessons | exhaustive options |
| `02-concepts/` | Explanation | the *why*, trade-offs | step-by-step tasks |
| `03-guides/` | How-to | goal-oriented steps | full key references |
| `04-connectors/`, `07-reference/` | Reference | neutral, exhaustive facts | teaching, opinions |

- The "why" behind a knob goes in `02-concepts/`, not in the reference table.
- Reference tables do not live in tutorials or how-tos — link to the appendix.
- How-to titles are goals ("Securing connections"); tutorial titles are lessons
  ("Your first pipeline").

## 6. Docusaurus hygiene

- **Sidebar is autogenerated** from folder structure. Order via `NN-` numeric
  filename prefixes (stripped from the URL) and `_category_.json` `position`;
  human labels via `_category_.json` `label`. Do not hand-edit `sidebars.ts` for
  user-guide pages.
- **Every category folder has a `_category_.json`** (`label`, and `position`
  where order isn't obvious) and a `README.mdx` landing page — no dead-click
  categories.
- **Internal links are relative and extension-qualified** (`../foo/bar.mdx`).
  `onBrokenLinks: 'throw'` fails the build on a stale link, so `npm run build`
  is the correctness gate for any move or rename.
- **Moving or renaming a page changes its URL** (URLs are path-derived). Add a
  `{ from, to }` entry to the `clientRedirects` plugin in
  `website/docusaurus.config.ts` for every moved page to keep old links alive.
- **Run the gate before pushing:** `cd website && npm run build` (check the exit
  code), and scan the log for anchor warnings (`onBrokenAnchors: 'warn'`).
