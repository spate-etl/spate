import type { Config } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import { themes as prismThemes } from 'prism-react-renderer';

// The repo is a personal-account project, so this publishes to a *project*
// GitHub Pages site: https://marcuskainth.github.io/etl-rs/.
const organizationName = 'MarcusKainth';
const projectName = 'etl-rs';
const githubUrl = `https://github.com/${organizationName}/${projectName}`;

const config: Config = {
  title: 'etl-rs',
  tagline: 'High-performance, at-least-once ETL pipelines in Rust',
  favicon: 'img/favicon.svg',

  url: `https://${organizationName.toLowerCase()}.github.io`,
  baseUrl: `/${projectName}/`,
  organizationName,
  projectName,
  trailingSlash: false,

  // Broken internal links fail the build — a content-hygiene gate that keeps
  // CI honest as docs change. Anchors stay lenient (cross-doc heading links
  // are easy to trip on and low-risk).
  onBrokenLinks: 'throw',
  onBrokenAnchors: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  markdown: {
    mermaid: true,
  },

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
            { label: 'Benchmarks', to: '/docs/BENCHMARKS' },
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
