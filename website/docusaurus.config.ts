import type { Config, PluginConfig } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
// Extensionless on purpose: `moduleResolution: bundler` rejects an explicit
// `.ts`, and Docusaurus loads this config through jiti, which transpiles
// nested TypeScript imports and resolves `.ts` itself.
import transclude from './src/remark/transclude';
import repoLinks from './src/remark/repoLinks';
import transcludeDeps from './src/plugins/transcludeDeps';
import benchData from './src/plugins/benchData';
import socialProof from './src/plugins/socialProof';
import {BENCHMARK_REPO, BENCHMARKS_BASE, NAV_ITEMS} from './src/data/nav';
import {countInvariants} from './src/data/invariants';
import prismTheme from './src/prismTheme';
// The site is deployed as a Cloudflare Worker at https://spate.kainth.dev/.
// organizationName/projectName drive the GitHub source links (githubUrl,
// editUrl, footer, and every `repo:` link on a page), not the deployed URL.
import {
  organizationName,
  projectName,
  githubUrl,
  SOURCE_REF,
} from './src/repoUrl';

// Client-side redirects that keep old published URLs alive. Registered only in
// CI (see the plugins array): plugin-client-redirects writes `<from>/index.html`
// AFTER the build, and on a case-INSENSITIVE filesystem (macOS APFS, dev
// laptops) a stub whose path differs from a real page only in case collapses
// onto that page and clobbers it. The deploy runs on ubuntu-latest, where the
// stubs and real pages coexist. Local builds drop the stubs.
//
// Entries:
//   - connectors/* -> connectors/{sources,sinks,formats}/* (connectors were
//     regrouped by role; every page below moved).
//   - guides/schema-validation -> connectors/sinks/clickhouse/schema-validation
//     (the page was entirely ClickHouse-specific, so it moved to the connector
//     it documents; see docs/STYLE.md § the framework/connector boundary).
//   - DESIGN -> user-guide/concepts (the design document was dissolved: its
//     rationale became docs/adr/, its invariants docs/INVARIANTS.md, and its
//     architecture prose the Concepts section).
//
// A redirect whose `to` names a page that no longer exists FAILS the build, so
// deleting a page means deleting any redirect aimed at it. This is only caught
// with `CI=true` (see where the plugin is registered below).
const chConnector = '/docs/user-guide/connectors';

const clientRedirects: PluginConfig = [
  '@docusaurus/plugin-client-redirects',
  {
    redirects: [
      { from: `${chConnector}/kafka`, to: `${chConnector}/sources/kafka` },
      { from: `${chConnector}/kafka-sink`, to: `${chConnector}/sinks/kafka` },
      { from: `${chConnector}/clickhouse`, to: `${chConnector}/sinks/clickhouse` },
      { from: `${chConnector}/clickhouse/aggregating-mergetree`, to: `${chConnector}/sinks/clickhouse/aggregating-mergetree` },
      { from: `${chConnector}/clickhouse/distributed-parity`, to: `${chConnector}/sinks/clickhouse/distributed-parity` },
      { from: `${chConnector}/clickhouse/multi-table`, to: `${chConnector}/sinks/clickhouse/multi-table` },
      { from: `${chConnector}/clickhouse/native-format`, to: `${chConnector}/sinks/clickhouse/native-format` },
      { from: `${chConnector}/clickhouse/performance-tuning`, to: `${chConnector}/sinks/clickhouse/performance-tuning` },
      { from: `${chConnector}/clickhouse/permissions`, to: `${chConnector}/sinks/clickhouse/permissions` },
      { from: `${chConnector}/avro`, to: `${chConnector}/formats/avro` },
      {
        from: '/docs/user-guide/guides/schema-validation',
        to: `${chConnector}/sinks/clickhouse/schema-validation`,
      },
      { from: '/docs/DESIGN', to: '/docs/user-guide/concepts' },
    ],
  },
];

const config: Config = {
  title: 'Spate',
  tagline: 'At-least-once streaming ETL for Rust.',
  favicon: 'img/favicon.svg',

  // The SVG favicon serves modern browsers; the ICO and the touch icon serve
  // the rest and the search-result thumbnails. All three come out of
  // website/tools/brand/generate.sh.
  headTags: [
    { tagName: 'link', attributes: { rel: 'icon', href: '/favicon.ico', sizes: '32x32' } },
    { tagName: 'link', attributes: { rel: 'apple-touch-icon', href: '/img/apple-touch-icon.png' } },
  ],

  url: 'https://spate.kainth.dev',
  baseUrl: '/',
  // Facts a page states that the repository, not the page, owns.
  customFields: {invariants: countInvariants()},
  organizationName,
  projectName,
  trailingSlash: false,

  // Broken internal links fail the build, and anchors are held to the same
  // standard: a link into a heading is a promise that the heading exists.
  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',
  // A page and a docs-instance page at one path would otherwise both build
  // and only one of them serve.
  onDuplicateRoutes: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  markdown: {
    mermaid: true,
    hooks: {
      // `onBrokenLinks: 'throw'` above governs links Docusaurus has resolved
      // into routes. A Markdown link such as `[x](./other.md)` that resolves
      // to no file is this separate setting, whose default is 'warn'. (The
      // top-level `onBrokenMarkdownLinks` key this replaces is deprecated as
      // of 3.9.)
      onBrokenMarkdownLinks: 'throw',
    },
  },

  future: {
    // Rspack bundler + SWC/Lightning CSS in place of webpack + Babel + Terser.
    // Stable as of 3.10 and the default in v4.
    faster: true,
    v4: {
      // Required by `faster`: its `ssgWorkerThreads` refuses to start without
      // it.
      //
      // Enabled individually rather than as `v4: true`. The other v4 flag,
      // `useCssCascadeLayers`, changes CSS precedence over this site's custom
      // styles, and takes its own change with its own visual check.
      removeLegacyPostBuildHeadAttribute: true,
    },
  },

  plugins: [
    // CI-only, as a standing precaution rather than for any entry currently
    // in the set (see clientRedirects above).
    process.env.CI === 'true' ? clientRedirects : false,
    // Not conditional. It registers which sources a page's fences are rendered
    // from, and a warm-cache local rebuild needs that as much as CI does.
    transcludeDeps,
    [benchData, {routeBasePath: BENCHMARKS_BASE, repoUrl: BENCHMARK_REPO}],
    // Stars, downloads and releases, fetched at build time with a committed
    // fallback (src/data/social-proof.json).
    socialProof,
    [
      '@docusaurus/plugin-content-docs',
      {
        id: BENCHMARKS_BASE,
        // Written by scripts/sync-benchmark-docs.mjs before every build.
        path: '.benchmarks',
        routeBasePath: BENCHMARKS_BASE,
        sidebarPath: './sidebars.benchmarks.ts',
        // The tree is generated, so git holds no history for it.
        showLastUpdateTime: false,
        // The contract pages are rendered from methodology/; the rest sit in
        // docs/ under the same name.
        editUrl: ({docPath}) => {
          const m = /^contract\/(rules|envelope|measurement|comparability)\.md$/.exec(docPath);
          const source = m ? `methodology/${m[1] === 'rules' ? 'README' : m[1]}.md` : `docs/${docPath}`;
          return `${BENCHMARK_REPO}/edit/main/${source}`;
        },
      },
    ],
  ],

  themes: [
    '@docusaurus/theme-mermaid',
    [
      '@easyops-cn/docusaurus-search-local',
      {
        hashed: true,
        indexDocs: true,
        indexPages: true,
        indexBlog: false,
        // The user guide is read in place from the repo's docs/ tree; the
        // benchmark pages are rendered into .benchmarks before the build.
        docsDir: ['../docs', '.benchmarks'],
        docsRouteBasePath: ['/docs', `/${BENCHMARKS_BASE}`],
        highlightSearchTermsOnTargetPage: true,
        explicitSearchResultPath: true,
      },
    ],
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          // Read the repo docs/ tree in place, keeping docs/INVARIANTS.md,
          // docs/METRICS.md and the rest at the paths AGENTS.md and the README
          // rely on.
          path: '../docs',
          routeBasePath: 'docs',
          sidebarPath: './sidebars.ts',
          editUrl: `${githubUrl}/edit/${SOURCE_REF}/docs/`,
          showLastUpdateTime: true,
          // Keep the Docusaurus defaults (they exclude `_*` MDX partials such as
          // 04-connectors/_securing-kafka.mdx from becoming pages) and add the
          // doc-standards file, a contributor reference rather than a site page.
          exclude: [
            '**/_*.{js,jsx,ts,tsx,md,mdx}',
            '**/_*/**',
            '**/*.test.{js,jsx,ts,tsx}',
            '**/__tests__/**',
            'STYLE.md',
          ],
          // Renders `file=`/`region=` fences from the compiled sources under
          // `crates/` (docs/STYLE.md § 10). Registered BEFORE the defaults
          // rather than after them (`remarkPlugins`) for two reasons: the
          // default list ends with plugins that mutate the tree and can throw
          // first under this site's `onBrokenMarkdownLinks: 'throw'`, so a
          // stale region should report as a stale region; and the mermaid
          // plugin *replaces* a fenced node, so a ```mermaid fence carrying
          // `file=` would never reach a plugin registered after it.
          //
          // `repoLinks` is here for the second reason as well: the default list
          // includes the plugin that resolves Markdown links, and `repo:` is a
          // scheme it does not know. Resolving first means it only ever sees a
          // finished URL.
          beforeDefaultRemarkPlugins: [
            transclude,
            [repoLinks, {githubUrl, sourceRef: SOURCE_REF}],
          ],
        },
        blog: false,
        sitemap: {
          // Docusaurus reads the date from git for docs pages; the rest carry
          // none rather than a build timestamp.
          lastmod: 'date',
          ignorePatterns: ['/search', '/blog/tags/**'],
        },
        pages: {
          // The homepage renders a region of a compiled example through the
          // same plugin the docs use (docs/STYLE.md § 10).
          beforeDefaultRemarkPlugins: [transclude],
        },
        theme: {
          customCss: ['./src/css/custom.css', './src/css/site.css', './src/css/benchmarks.css', './src/css/home.css'],
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/brand/social-spate.png',
    // The theme emits the rest of the Open Graph set itself.
    metadata: [{property: 'og:type', content: 'website'}],
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: true,
    },
    navbar: {
      logo: {
        alt: 'Spate',
        src: 'img/brand/lockup-light.svg',
        srcDark: 'img/brand/lockup-dark.svg',
      },
      items: [
        ...NAV_ITEMS.map((item) => ({to: item.to, label: item.label, position: 'left' as const})),
        {
          href: githubUrl,
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    // No footer config: src/theme/Footer renders the site footer from
    // src/data/nav.ts on every page.
    prism: {
      // One dark block on both grounds; see src/prismTheme.ts.
      theme: prismTheme,
      darkTheme: prismTheme,
      additionalLanguages: ['rust', 'toml', 'bash', 'yaml', 'docker', 'json'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
