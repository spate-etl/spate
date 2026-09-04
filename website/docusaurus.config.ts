import type { Config, PluginConfig } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import { themes as prismThemes } from 'prism-react-renderer';
// Extensionless on purpose: `moduleResolution: bundler` rejects an explicit
// `.ts`, and Docusaurus loads this config through jiti, which transpiles
// nested TypeScript imports and resolves `.ts` itself.
import transclude from './src/remark/transclude';
import repoLinks from './src/remark/repoLinks';
import transcludeDeps from './src/plugins/transcludeDeps';
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
  tagline: 'High-performance, at-least-once ETL pipelines in Rust',
  favicon: 'img/favicon.svg',

  url: 'https://spate.kainth.dev',
  baseUrl: '/',
  organizationName,
  projectName,
  trailingSlash: false,

  // Broken internal links fail the build, and anchors are held to the same
  // standard: a link into a heading is a promise that the heading exists.
  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',

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
  ],

  themes: [
    '@docusaurus/theme-mermaid',
    [
      '@easyops-cn/docusaurus-search-local',
      {
        hashed: true,
        indexDocs: true,
        indexBlog: false,
        // Source lives in the repo's ../docs tree (read in place), not website/docs.
        docsDir: '../docs',
        docsRouteBasePath: '/docs',
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
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/brand/social-spate.png',
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Spate',
      logo: {
        alt: 'Spate logo',
        src: 'img/logo.svg',
        srcDark: 'img/logo-dark.svg',
      },
      items: [
        {
          to: '/docs/user-guide/',
          label: 'Docs',
          position: 'left',
        },
        {
          to: '/docs/adr/',
          label: 'Decisions',
          position: 'left',
        },
        {
          // Generated third-party license texts, published under
          // <baseUrl>/licenses/ by CI alongside the rustdoc.
          to: 'pathname:///licenses/',
          label: 'Licenses',
          position: 'right',
        },
        {
          // Static rustdoc output published under <baseUrl>/api/ by CI.
          // `pathname://` prefixes baseUrl and bypasses the SPA router.
          to: 'pathname:///api/',
          label: 'API (rustdoc)',
          position: 'left',
        },
        {
          href: githubUrl,
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            { label: 'User Guide', to: '/docs/user-guide/' },
            { label: 'Decisions', to: '/docs/adr/' },
            { label: 'Invariants', to: '/docs/INVARIANTS' },
            { label: 'Metrics', to: '/docs/METRICS' },
          ],
        },
        {
          title: 'Reference',
          items: [
            { label: 'API (rustdoc)', to: 'pathname:///api/' },
            { label: 'docs.rs', href: 'https://docs.rs/spate' },
            { label: 'crates.io', href: 'https://crates.io/crates/spate' },
          ],
        },
        {
          title: 'More',
          items: [
            { label: 'GitHub', href: githubUrl },
            { label: 'Issues', href: `${githubUrl}/issues` },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Marcus Kainth. Spate is licensed under Apache-2.0.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['rust', 'toml', 'bash', 'yaml', 'docker', 'json'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
