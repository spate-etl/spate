# End-to-end tests

Playwright specs against the built site: accessibility today, and the place
for any future browser-driven test (navigation, search, the benchmark
explorer). Unit and data-layer tests live under `src/**/*.test.{js,ts}` and
run through `node:test`; this directory is Playwright only.

## Adding a spec

Create `specs/<name>.spec.ts` and import the extended test from `../fixtures`
rather than `@playwright/test` directly:

```ts
import {test, expect} from '../fixtures';
```

That import carries every fixture this suite defines, `colorMode` today. A
spec that needs a new fixture (a signed-in session, a seeded API response)
adds a file under `fixtures/` and re-exports it from `fixtures/index.ts`
alongside the existing ones, so the single import surface holds.

Reusable page logic belongs under `helpers/`, not inline in a spec:

- `helpers/routes.ts` — named routes and viewports. Add a route here rather
  than a literal path in a spec, and verify it against `website/build` first;
  Docusaurus strips numeric directory prefixes from the source tree.
- `helpers/navigate.ts` — `gotoRoute()`, the only sanctioned way to land on a
  page. It checks the response status and the applied colour mode before a
  test proceeds, so a redirect or a storage-key change fails loudly instead
  of quietly testing the wrong page. It then polls until no element computes
  the other colour mode's ink, so a spec reading a colour gets the settled
  value.
- `helpers/axe.ts` — `expectNoAxeViolations()` for an accessibility
  assertion, scoped to the WCAG tags the CI gate blocks on.

## Colour mode

Pin it per file or `describe` block, never per test:

```ts
test.use({colorMode: 'light'});
```

It is a test option, not a project, so it multiplies specs on request instead
of running every future spec twice by default.

## Projects and CI

`playwright.config.ts` declares `chromium`, `firefox` and `webkit`. CI runs
`--project=chromium` only and installs just that browser binary — axe-core is
DOM analysis, not rendering, so the other two engines would triple the cost
for the same result. They exist so a rendering or interaction spec has the
engines available: locally that is `npm run test:e2e` with no `--project`
flag, and in CI it is the browser added to the install step in
`.github/workflows/ci.yml` and the `--project` flag dropped from the run
beside it.

## Running locally

```sh
npm run build          # once, or after a docs/content change
npm run test:e2e        # headless, chromium + firefox + webkit
npm run test:e2e:ui     # Playwright's UI mode
npm run test:e2e:report # open the last HTML report
```

Point at a dev server instead of the static build with `PLAYWRIGHT_BASE_URL`,
which also disables the config's own `webServer`:

```sh
npm start &
PLAYWRIGHT_BASE_URL=http://localhost:3000 npm run test:e2e
```

## Growing the suite: sharding

The suite is small enough today that one CI job running `--project=chromium`
finishes in seconds. Playwright shards by test file with `--shard=<i>/<n>`,
and the runtime cost to watch for is wall-clock time in the `site` job, not
test count. When the suite is slow enough to matter, shard it across a matrix
in the `site` job rather than a new job: `strategy.matrix.shard: [1/4, 2/4,
3/4, 4/4]`, each leg passing `--shard=${{ matrix.shard }}`, with
`blob-report` merged by a final step (`npx playwright merge-reports`). A
separate job would need its own checkout, `npm ci` and access to `website/build`,
which `site` already has.
