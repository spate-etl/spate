// Types, contract rules and scale maths for the results surface.
//
// Everything here is pure and runs at prerender time. It is separated from the
// components so that the three decisions the fairness contract actually turns on
// — may this arm be RANKED, may its number be POSITIONED on a shared axis, and
// which records may share a scale at all — are stated once, in one place, in
// terms a reader can check against methodology/.
//
// NOTHING IN THIS FILE MAY BRANCH ON AN ENTRANT ID
//
// Ordering, emphasis and eligibility derive from descriptor fields and from the
// measurement itself. `website/plugins/neutrality.test.js` enforces that, and it
// is the mechanism by which a vendor-run benchmark's neutrality claim becomes
// checkable rather than asserted.

// Imported WITH the extension, and it has to stay that way: `node --test` strips
// types but does not resolve the bundler's extensionless specifiers, so a test
// that reaches the contract predicates below cannot load this file without them.
// `tsconfig.json` sets `allowImportingTsExtensions` for exactly this.
import {PRIMARY_COLUMN} from './columns.ts';
import {niceCeil} from './filter.ts';

export {niceCeil};

export type Metric = {
  value: number;
  unit: string;
  higher_is_better: boolean;
  n: number;
  /** Smallest repetition. */
  lo: number;
  /** Largest repetition. */
  hi: number;
  /** Every repetition, ascending. At n=3 this is exactly [lo, value, hi]. */
  values: number[];
  /** (hi - lo) / median, or null where the median is zero and it has no meaning. */
  spread: number | null;
};

export type Row = {
  key: string;
  /** The sweep this reading came from. Only an arm's newest one is published. */
  sitting: string;
  group: string;
  entrant: string;
  variant_id: string;
  version: string | null;
  commit: string | null;
  env_id: string;
  harness_version: number;
  dataset_version: string;
  ts_ms: number;
  status: string;
  approach: string;
  wire_format: string | null;
  reps_counted: number;
  flags: string[];
  /**
   * The harness's own account of the repetition that produced `status`.
   *
   * This is where an `infra_bound` arm's REASON lives — which ceiling it was
   * measured against and what share of it the arm reached. Optional because a
   * record may carry no note at all.
   */
  note?: string | null;
  /**
   * The A/A spread this sweep's own control measured, when it ran one.
   *
   * The difference between two measurements of the same arm under two labels:
   * what the rig does when the system under test does not change. `null` for a
   * sweep that ran no control.
   */
  aa_spread?: number | null;
  metrics: Record<string, Metric>;
  image_digest?: string;
  mode?: string;
};

/**
 * A sweep that happened and produced no publishable number.
 *
 * One per sitting rather than per repetition: a three-repetition failure is one
 * thing that went wrong. The comparability fields are carried so a group with
 * nothing but attempts can still be named on the page.
 */
export type Attempt = {
  group: string;
  entrant: string;
  variant_id: string;
  sitting: string;
  status: string;
  note: string | null;
  ts_ms: number;
  reps_counted: number;
  env_id: string;
  harness_version: number;
  dataset_version: string;
};

export type Variant = {
  id: string;
  label?: string;
  approach?: string;
  default?: boolean;
  unshipped?: string[];
  /** The resolved knob values this arm ran with. */
  knobs?: Record<string, string | number | boolean>;
  env?: Record<string, string>;
  reports?: {wire_format?: string};
  /**
   * Rule 6 — "why was throughput X and not 2X" — which the fairness contract
   * requires in the published results table.
   *
   * NO DESCRIPTOR CARRIES THIS YET. `harness/src/entrant.rs` does not validate
   * it and no `entrant.toml` sets it, so today it is always undefined and the
   * arm's disclosure says so in as many words. That is deliberate: a contract
   * clause that is not implemented should be visibly not implemented, because
   * silently omitting it makes the page look compliant when it is not.
   */
  binding_constraint?: string;
};

/**
 * A parsed `entrant.toml`, in full.
 *
 * The build plugin hands the site whatever the descriptor parsed to, unmodified,
 * so every field a system declares about itself is reachable here — and the
 * system profile page renders most of them. Declaring the shape beats reaching
 * for `any`, and it is also the list of what a new entrant may say for itself.
 */
export type Entrant = {
  entrant: {
    id: string;
    name: string;
    status: string;
    runtime: string;
    licence: string;
    vendor: string;
    language?: string[];
    kind?: string;
    homepage?: string;
    repo?: string;
    docs?: string;
  };
  display?: {short?: string; order?: number; hue?: number};
  maintainer?: {who?: string; reviewed_upstream?: boolean; review_url?: string};
  variants?: Variant[];
  planned?: {blockers?: string; tracking?: string; licence_gate?: string};
  guarantees?: {delivery?: string; durability?: string; interval_ms?: number};
  constraints?: {knob: string; exceeds: string; why: string}[];
  deviations?: {what?: string; why?: string; affects?: string[]}[];
  envelope?: {
    cpus?: string;
    memory?: string;
    container?: {role?: string; name?: string; cpus?: string; memory?: string}[];
  };
};

export type Env = {
  id: string;
  class: string;
  host?: {description?: string; cpu?: string; cores?: number};
};

export type Group = {
  key: string;
  env_id: string;
  harness_version: number;
  dataset_version?: string;
};

export type Data = {
  entrants: Entrant[];
  environments: Env[];
  rows: Row[];
  attempts: Attempt[];
  groups: Group[];
  counts: {files: number; lines: number; kept: number};
  generatedAt: string | null;
  /** Site path the results pages live under, with the site's base URL and a trailing slash. */
  basePath: string;
  /** The benchmark repository, for source links. */
  repoUrl: string;
};

export const EMPTY: Data = {
  entrants: [],
  environments: [],
  rows: [],
  attempts: [],
  groups: [],
  counts: {files: 0, lines: 0, kept: 0},
  generatedAt: null,
  basePath: '/benchmarks/',
  repoUrl: 'https://github.com/spate-etl/benchmark',
};

/**
 * The metric the page ranks by, and the one the default order means.
 *
 * Taken from the column catalogue so there is exactly one answer to "which
 * metric leads" — a second literal here could drift from the table's first
 * column and nothing would catch it.
 */
export const PRIMARY = PRIMARY_COLUMN;

// ---------------------------------------------------------------------------
// The contract, as three predicates
// ---------------------------------------------------------------------------

/**
 * Why an arm is shown but not ranked. Empty string means headline-eligible.
 *
 * Ordered by severity, so an `infra_bound` `stripped` arm says it was
 * infra-bound: the disowned number is the stronger statement.
 */
export function unrankedBecause(r: Row): string {
  if (r.status === 'infra_bound') return 'infra-bound';
  if (r.approach !== 'realistic') return r.approach;
  return '';
}

export const isRanked = (r: Row) => unrankedBecause(r) === '';

/**
 * May this arm's number be drawn as a POSITION on the shared axis?
 *
 * Ranking and positioning are different permissions and conflating them costs
 * the page something either way.
 *
 * A `tuned` or `stripped` arm's number is sound; what is wrong with it is the
 * configuration, not the measurement. Rule 3 bars it from the headline, and
 * METHODOLOGY's stated purpose for the category is that "the delta quantifies"
 * a specific effect — which a reader cannot see if the arm is exiled to a list
 * below the chart. So it is positioned on the same axis, in an outline mark,
 * with no rank ordinal, and it never sets the scale.
 *
 * `infra_bound` is the opposite case: the NUMBER is disowned. An infra-bound
 * figure describes what ClickHouse could absorb rather than what the system
 * could do, and the distortion is arm-dependent — so it changes the differences
 * and not merely the levels, and a distorted difference is exactly what a
 * position on a shared axis encodes. Such an arm keeps its number and its
 * reason; it does not keep its position.
 */
export function isPlotted(r: Row): boolean {
  return r.status !== 'infra_bound';
}

// ---------------------------------------------------------------------------
// Ordering
// ---------------------------------------------------------------------------

export const entrantOrder = (e: Entrant | undefined) => e?.display?.order ?? 1e9;

/**
 * Lane order within one comparability group.
 *
 * Ranked arms first, ordered by the PRIMARY metric in the direction that metric
 * declares. Everything else follows in descriptor order — `display.order`, then
 * the order the variants are declared in the descriptor.
 *
 * The fallback matters more than the ranking does. When nothing is
 * headline-eligible there is no measurement to order by, and the page says so
 * rather than presenting a descriptor's declaration order as if it were a
 * result.
 */
export function laneRank(
  rows: Row[],
  byId: Map<string, Entrant>,
): {order: Row[]; ranked: Map<string, number>} {
  const variantIndex = (r: Row) => {
    const vs = byId.get(r.entrant)?.variants ?? [];
    const i = vs.findIndex((v) => v.id === r.variant_id);
    return i < 0 ? vs.length : i;
  };
  const descriptor = (a: Row, b: Row) =>
    entrantOrder(byId.get(a.entrant)) - entrantOrder(byId.get(b.entrant)) ||
    variantIndex(a) - variantIndex(b) ||
    a.variant_id.localeCompare(b.variant_id);

  const rankable = rows.filter((r) => isRanked(r) && r.metrics[PRIMARY]);
  const hib = rankable[0]?.metrics[PRIMARY]?.higher_is_better ?? true;
  const byPrimary = [...rankable].sort((a, b) => {
    const av = a.metrics[PRIMARY].value;
    const bv = b.metrics[PRIMARY].value;
    return (hib ? bv - av : av - bv) || descriptor(a, b);
  });

  const ranked = new Map<string, number>();
  byPrimary.forEach((r, i) => ranked.set(r.key, i + 1));

  const rest = rows.filter((r) => !ranked.has(r.key)).sort(descriptor);
  return {order: [...byPrimary, ...rest], ranked};
}

// ---------------------------------------------------------------------------
// Scale
// ---------------------------------------------------------------------------

export type Scale = {
  /** Always zero. A truncated axis is the oldest way to make a small gap look big. */
  min: 0;
  max: number;
  /** Fraction of the track, 0..1. */
  at: (v: number) => number;
  ticks: number[];
};


/**
 * The axis for one facet: one metric, within one comparability group.
 *
 * ANCHORED AT ZERO, ALWAYS. Every metric here is a ratio quantity with a real
 * zero, and starting an axis anywhere else turns a 5% difference into a picture
 * of a rout. On a page whose author has a stake in the outcome that is not a
 * defensible convenience.
 *
 * ABSOLUTE, NOT RELATIVE TO THE BEST ARM. The previous design drew each bar as
 * a percentage of the leading arm, which quietly makes the leader the unit of
 * measurement and renames every other system "some fraction of the winner".
 * Real tick values cost one line of chrome and remove that framing entirely.
 *
 * The domain covers every arm that is POSITIONED, including the ones barred from
 * ranking, because an axis that a drawn mark overflows is a broken axis. That is
 * a display range, not a normaliser: no arm is "the bar the others are drawn
 * against", so a non-headline arm cannot become the reference by being fastest.
 */
export function scaleFor(rows: Row[], metric: string): Scale | null {
  const hi = rows
    .filter((r) => isPlotted(r) && r.metrics[metric])
    .map((r) => r.metrics[metric].hi ?? r.metrics[metric].value);
  if (!hi.length) return null;
  const max = niceCeil(Math.max(...hi));
  return {
    min: 0,
    max,
    at: (v: number) => Math.min(1, Math.max(0, v / max)),
    ticks: [0, max / 2, max],
  };
}

/**
 * Arms whose measured interval overlaps the leader's, per metric.
 *
 * The single most useful thing this page can say about its own numbers. With
 * three repetitions and a rig whose own control moves an unchanged arm by
 * several percent, a 10% lead is not a lead — and a benchmark run by one of its entrants that lets a reader infer one
 * anyway has done the damage whether or not any sentence on the page was false.
 *
 * Two intervals that overlap are not separated BY THIS DATA. That is a weaker
 * claim than a significance test and it is the strongest one three repetitions
 * support; the page states it in exactly those words.
 */
export function tiedWithLeader(rows: Row[], metric: string): {leader: Row | null; tied: Set<string>} {
  const eligible = rows.filter((r) => isRanked(r) && isPlotted(r) && r.metrics[metric]);
  if (!eligible.length) return {leader: null, tied: new Set()};
  const hib = eligible[0].metrics[metric].higher_is_better;
  const leader = eligible.reduce((a, b) => {
    const av = a.metrics[metric].value;
    const bv = b.metrics[metric].value;
    return (hib ? bv > av : bv < av) ? b : a;
  });
  const lm = leader.metrics[metric];
  const tied = new Set<string>();
  for (const r of eligible) {
    if (r.key === leader.key) continue;
    const m = r.metrics[metric];
    // Closed-interval overlap of [lo, hi] against the leader's.
    if (m.lo <= lm.hi && lm.lo <= m.hi) tied.add(r.key);
  }
  return {leader, tied};
}

// ---------------------------------------------------------------------------
// Formatting
//
// How a number is PRINTED lives in `format.ts`. What follows is only the
// non-numeric formatting the contract needs.
// ---------------------------------------------------------------------------

export const iso = (ms: number) => new Date(ms).toISOString().slice(0, 10);

/** The descriptor's own label for an arm, falling back to its id. */
export function variantOf(e: Entrant | undefined, id: string): Variant | undefined {
  return (e?.variants ?? []).find((v) => v.id === id);
}

/**
 * An arm's label with its system's name taken off the front.
 *
 * Descriptors label variants for a flat list — "Spate · RowBinary",
 * "Flink 2.2.1 · RowBinary" — because until now the page had nowhere else to say
 * which system an arm belonged to. Under a system band it does, and repeating the
 * name on every lane makes five configurations of one system read as five
 * competitors all over again, which is the exact thing the grouping exists to
 * stop. Matched against the descriptor's own `name` and `display.short`, so this
 * is a string the entrant supplied about itself and not a name the site knows.
 */
export function armLabel(
  e: Entrant | undefined,
  v: Variant | undefined,
  fallback: string,
): string {
  const label = v?.label;
  if (!label) return fallback;
  for (const prefix of [e?.entrant.name, e?.display?.short].filter(Boolean) as string[]) {
    if (label.toLowerCase().startsWith(prefix.toLowerCase())) {
      const rest = label.slice(prefix.length).replace(/^\s*[·:—-]\s*/, '').trim();
      if (rest) return rest;
    }
  }
  return label;
}
