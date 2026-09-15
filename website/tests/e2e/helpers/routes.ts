/**
 * Routes the accessibility sweep covers: a hand-written landing page, a docs
 * page with no table, and the docs page a wide table overflows on (#396).
 * Paths are checked against the built output, not derived from the source
 * tree — Docusaurus strips numeric directory prefixes.
 */
export const ROUTES = {
  home: '/',
  quickstart: '/docs/user-guide/getting-started/quickstart',
  kafkaSource: '/docs/user-guide/connectors/sources/kafka',
} as const;

export type RouteName = keyof typeof ROUTES;

/** The viewport #396 measured the defect at: a 1505px window over an 823px docs column. */
export const VIEWPORTS = {
  desktop: {width: 1505, height: 900},
} as const;
