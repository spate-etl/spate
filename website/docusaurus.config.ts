import type { Config, PluginConfig } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import { themes as prismThemes } from 'prism-react-renderer';

// The site is deployed to Cloudflare Pages on a dedicated subdomain:
// https://etl-rs.pages.kainth.net/ (Direct Upload from CI — see the nightly
// tier of scheduled.yml; pull requests build the site but never publish it).
// organizationName/projectName below still drive the GitHub source links
// (githubUrl, editUrl, footer), not the deployed URL.
const organizationName = 'MarcusKainth';
const projectName = 'etl-rs';
const githubUrl = `https://github.com/${organizationName}/${projectName}`;

// Client-side redirects that keep old published URLs alive. Registered only in
// CI (see the plugins array): plugin-client-redirects writes `<from>/index.html`
// AFTER the build, and on a case-INSENSITIVE filesystem (macOS APFS, dev laptops)
// the BENCHMARKS stub path collapses onto build/docs/benchmarks/index.html and
// clobbers the real page. The deploy runs on ubuntu-latest (case-sensitive) where
// the stubs and real pages coexist, so gate the whole set to CI. Local builds drop
// the stubs — fine, they exist solely to preserve old links.
//
// Entries:
//   - /docs/BENCHMARKS -> /docs/benchmarks (folder was renamed).
//   - connectors/* -> connectors/{sources,sinks,formats}/* (connectors were
//     regrouped by role; every page below moved).
//
// A redirect whose `to` names a page that no longer exists FAILS the build.
// Deleting a page therefore means deleting any redirect aimed at it — the
// avro `fast-backend` entry went when that backend was removed. Note this is
// only caught with `CI=true` (see where the plugin is registered below).
const chConnector = '/docs/user-guide/connectors';
const clientRedirects: PluginConfig = [
  '@docusaurus/plugin-client-redirects',
  {
    redirects: [
      { from: '/docs/BENCHMARKS', to: '/docs/benchmarks' },
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
    ],
  },
];

const config: Config = {
  title: 'etl-rs',
  tagline: 'High-performance, at-least-once ETL pipelines in Rust',
  favicon: 'img/favicon.svg',

  url: 'https://etl-rs.pages.kainth.net',
  baseUrl: '/',
  organizationName,
  projectName,
  trailingSlash: false,

  // Broken internal links fail the build — a content-hygiene gate that keeps
  // CI honest as docs change. Anchors are held to the same standard: a link
  // into a specific heading is a promise that the heading exists, and a
  // rename breaking it silently is exactly the drift the rest of these gates
  // exist to stop.
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
      // into routes. A *Markdown* link — `[x](./other.md)` — that resolves to
      // no file is a separate setting, and its default is only 'warn', so
      // those were passing CI while rendered broken links failed it. Same
      // standard for both. (The top-level `onBrokenMarkdownLinks` key this
      // replaces is deprecated as of 3.9.)
      onBrokenMarkdownLinks: 'throw',
    },
  },

  future: {
    // Rspack bundler + SWC/Lightning CSS in place of webpack + Babel + Terser.
    // Stable as of 3.10 and the default in v4; upstream measures 3-4x on cold
    // builds and, with the Rspack persistent cache preserved between runs,
    // 6-7x on rebuilds. The site build is on the critical path of every docs
    // pull request, so this is the largest single saving available here.
    faster: true,
    v4: {
      // Required by `faster`: its `ssgWorkerThreads` refuses to start without
      // it, because rendering pages off the main thread cannot support the
      // legacy post-build head attribute.
      //
      // Enabled individually rather than as `v4: true`. The other v4 flag,
      // `useCssCascadeLayers`, changes CSS precedence, and this site carries
      // custom styles — that one deserves its own change with its own visual
      // check, not a free ride on a build-speed commit.
      removeLegacyPostBuildHeadAttribute: true,
    },
  },

  plugins: [
    // Loads benchmarks/results/*.jsonl into global data for the chart
    // components. Reads BENCH_RESULTS_DIR when set (fixtures during dev).
    './plugins/benchmark-results',
    // CI-only: on case-insensitive dev filesystems the redirect stubs clobber
    // real pages (see clientRedirects above).
    process.env.CI === 'true' ? clientRedirects : false,
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
          // Read the existing repo docs/ tree in place — keeps docs/DESIGN.md,
          // docs/METRICS.md, etc. at the paths CLAUDE.md and the README rely on.
          path: '../docs',
          routeBasePath: 'docs',
          sidebarPath: './sidebars.ts',
          editUrl: `${githubUrl}/edit/main/docs/`,
          showLastUpdateTime: true,
          // Keep the Docusaurus defaults (they exclude `_*` MDX partials such as
          // 04-connectors/_securing-kafka.mdx from becoming pages) and add the
          // doc-standards file, which is a contributor reference, not a site page.
          exclude: [
            '**/_*.{js,jsx,ts,tsx,md,mdx}',
            '**/_*/**',
            '**/*.test.{js,jsx,ts,tsx}',
            '**/__tests__/**',
            'CONTRIBUTING-docs.md',
          ],
          // `docs/benchmarks/` is an autogenerated sidebar *root*, so Docusaurus
          // never turns it into a category and never hoists its index page —
          // that only happens for subdirectories. sidebars.ts declares the
          // category and links it to `benchmarks/index`, so drop that page from
          // the generated children or it renders twice.
          async sidebarItemsGenerator({defaultSidebarItemsGenerator, ...args}) {
            const items = await defaultSidebarItemsGenerator(args);
            if (args.item.dirName !== 'benchmarks') {
              return items;
            }
            return items.filter(
              (item) => !(item.type === 'doc' && item.id === 'benchmarks/index'),
            );
          },
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/favicon.svg',
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'etl-rs',
      logo: {
        alt: 'etl-rs logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          to: '/docs/user-guide/',
          label: 'Docs',
          position: 'left',
        },
        {
          to: '/docs/DESIGN',
          label: 'Design',
          position: 'left',
        },
        {
          to: '/docs/benchmarks',
          label: 'Benchmarks',
          position: 'left',
        },
        {
          // Generated third-party licence texts, published under
          // <baseUrl>/licenses/ by CI alongside the rustdoc.
          to: 'pathname:///licenses/',
          label: 'Licences',
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
            { label: 'Architecture (Design)', to: '/docs/DESIGN' },
            { label: 'Metrics', to: '/docs/METRICS' },
            { label: 'Benchmarks', to: '/docs/benchmarks' },
          ],
        },
        {
          title: 'Reference',
          items: [
            { label: 'API (rustdoc)', to: 'pathname:///api/' },
            { label: 'docs.rs', href: 'https://docs.rs/etl' },
            { label: 'crates.io', href: 'https://crates.io/crates/etl' },
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
      copyright: `Copyright © ${new Date().getFullYear()} Marcus Kainth. etl-rs is dual-licensed MIT OR Apache-2.0.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['rust', 'toml', 'bash', 'yaml', 'docker', 'json'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
