/** The primary navigation, shared by the marketing navbar and the docs navbar. */
export type NavItem = {label: string; to: string};

export const NAV_ITEMS: NavItem[] = [
  {label: 'Docs', to: '/docs/user-guide/'},
  {label: 'Benchmarks', to: '/benchmarks/'},
  {label: 'Decisions', to: '/docs/adr/'},
];

export type FooterColumn = {title: string; items: Array<{label: string; to?: string; href?: string}>};

/** Footer columns. `to` is a site route, `href` an external address. */
export const FOOTER_COLUMNS = (githubUrl: string): FooterColumn[] => [
  {
    title: 'Docs',
    items: [
      {label: 'User guide', to: '/docs/user-guide/'},
      {label: 'Getting started', to: '/docs/user-guide/getting-started/'},
      {label: 'Concepts', to: '/docs/user-guide/concepts/'},
      {label: 'Decisions', to: '/docs/adr/'},
      {label: 'Invariants', to: '/docs/INVARIANTS'},
      {label: 'Metrics', to: '/docs/METRICS'},
    ],
  },
  {
    title: 'Benchmarks',
    items: [
      {label: 'Results', to: '/benchmarks/'},
      {label: 'The fairness contract', to: '/benchmarks/contract/rules'},
      {label: 'Environments', to: '/benchmarks/environments'},
      {label: 'Reproducing this', to: '/benchmarks/reproduce'},
    ],
  },
  {
    title: 'Reference',
    items: [
      {label: 'docs.rs', href: 'https://docs.rs/spate'},
      {label: 'crates.io', href: 'https://crates.io/crates/spate'},
      {label: 'Changelog', href: `${githubUrl}/blob/main/CHANGELOG.md`},
      {label: 'Licenses', to: 'pathname:///licenses/'},
      {label: 'Brand', to: '/brand'},
    ],
  },
  {
    title: 'Community',
    items: [
      {label: 'GitHub', href: githubUrl},
      {label: 'Issues', href: `${githubUrl}/issues`},
      {label: 'Security policy', href: `${githubUrl}/blob/main/SECURITY.md`},
      {label: 'Code of conduct', href: `${githubUrl}/blob/main/CODE_OF_CONDUCT.md`},
    ],
  },
];
