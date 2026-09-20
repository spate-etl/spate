/**
 * Routes the end-to-end suite visits. `home`, `quickstart` and `kafkaSource`
 * are the accessibility sweep's set; `installation` is a `docs-table.spec.ts`
 * fixture for a table that fits its column and carries no axe run of its
 * own; `clickdoomPost` is the one built page carrying an `<hr>`. Paths are
 * checked against the built output, not derived from the source tree —
 * Docusaurus strips numeric directory prefixes.
 */
export const ROUTES = {
  home: '/',
  quickstart: '/docs/user-guide/getting-started/quickstart',
  kafkaSource: '/docs/user-guide/connectors/sources/kafka',
  installation: '/docs/user-guide/getting-started/installation',
  clickdoomPost: '/blog/2026/09/12/running-doom-inside-clickhouse',
} as const;

export type RouteName = keyof typeof ROUTES;

/** The viewport #396 measured the defect at: a 1505px window over an 823px docs column. */
export const VIEWPORTS = {
  desktop: {width: 1505, height: 900},
} as const;
