/**
 * Routes the end-to-end suite visits. `home`, `quickstart`, `kafkaSource` and
 * `brand` are the accessibility sweep's set; `installation` is a `docs-table.spec.ts`
 * fixture for a table that fits its column and carries no axe run of its
 * own; `architecture` has a code block directly below its page
 * carrier; `clickdoomPost` is the one built page carrying an `<hr>`. Paths are
 * checked against the built output, not derived from the source tree —
 * Docusaurus strips numeric directory prefixes.
 */
export const ROUTES = {
  home: '/',
  quickstart: '/docs/user-guide/getting-started/quickstart',
  kafkaSource: '/docs/user-guide/connectors/sources/kafka',
  installation: '/docs/user-guide/getting-started/installation',
  architecture: '/docs/user-guide/concepts/architecture',
  clickdoomPost: '/blog/2026/09/12/running-doom-inside-clickhouse',
  brand: '/brand',
} as const;

export type RouteName = keyof typeof ROUTES;

/** A path the build has no page for, which the server answers with the 404 page. */
export const MISSING_ROUTE = '/no-page-at-this-address';

/**
 * `desktop` is the viewport #396 measured the defect at: a 1505px window over
 * an 823px docs column. `phone` is a 390px window, narrow enough that the brand
 * page's widest specimen is capped by its frame.
 */
export const VIEWPORTS = {
  desktop: {width: 1505, height: 900},
  phone: {width: 390, height: 844},
} as const;
