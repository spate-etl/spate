// The decisions the results explorer makes, with nothing else in them.
//
// This module imports nothing, deliberately. It holds the rules that both the
// prerender and the browser have to agree on — how an axis is rounded, what it
// becomes once some rows are hidden, and how two rows compare under a sort — and
// it is the only copy of any of them.
//
// It is separate from `data.ts` so that it can be tested directly under
// `node --test`, which cannot follow the extensionless imports the bundler
// resolves. That is a real constraint rather than a preference: a filter that
// wrongly hides a row leaves no trace on the page, so it is precisely the logic
// on this surface that a reader cannot check for themselves and a test has to.

/**
 * Rounds an axis maximum up to a value a reader can hold in their head.
 *
 * One copy, used by the build and by the browser. Two roundings would make the
 * axis jump the moment JavaScript loaded, which reads as the numbers changing.
 */
export function niceCeil(v: number): number {
  if (!(v > 0) || !Number.isFinite(v)) return 1;
  const e = 10 ** Math.floor(Math.log10(v));
  const f = v / e;
  const step =
    f <= 1 ? 1 : f <= 1.5 ? 1.5 : f <= 2 ? 2 : f <= 2.5 ? 2.5 : f <= 3 ? 3 : f <= 4 ? 4 : f <= 5 ? 5 : f <= 8 ? 8 : 10;
  return step * e;
}

/** One cell's contribution to its column's axis. */
export type Extent = {
  /** The largest repetition, which is what the capsule reaches. */
  hi: number;
  /** Whether this figure is allowed a position at all. */
  plotted: boolean;
  /** Whether it is currently on screen. */
  visible: boolean;
};

/**
 * The axis maximum for one column, over what is left on screen.
 *
 * Only a PLOTTED value may set it. An infra-bound figure is shown and given no
 * position, so letting it stretch the axis would let a number this project has
 * disowned decide what every other mark's position means.
 *
 * An empty selection returns `niceCeil(0)` — 1 — rather than 0, so the marks
 * divide by something and a filtered-to-nothing column collapses visibly instead
 * of producing `Infinity`.
 */
export function visibleMax(cells: Extent[]): number {
  let max = 0;
  for (const c of cells) {
    if (!c.visible || !c.plotted) continue;
    max = Math.max(max, c.hi);
  }
  return niceCeil(max);
}

/** One lane's value on the metric currently being sorted by. */
export type Sortable = {
  /** The median. `null` where the arm did not measure this metric. */
  value: number | null;
  /** Whether the arm is headline-eligible. */
  ranked: boolean;
  /** Position in the descriptor's own order, as the tie-break of last resort. */
  index: number;
};

/**
 * Order two lanes by the metric a reader chose.
 *
 * Three properties, in the order they apply.
 *
 * **Ranked arms come first, always.** Sorting is a reader's convenience; rule 3
 * is not. A `stripped` arm that happens to be fastest must not be able to reach
 * the top of a column by any control the page offers, so eligibility outranks
 * the value on every sort.
 *
 * **Direction comes from the metric**, never from the control. `higher_is_better`
 * travels on every record, so sorting CPU-per-row puts the cheapest arm first
 * without the site holding an opinion about which metrics are good.
 *
 * **An arm that did not measure the metric sorts last** rather than as zero,
 * which on a lower-is-better column would otherwise make "not measured" win.
 */
export function compareLanes(a: Sortable, b: Sortable, higherIsBetter: boolean): number {
  if (a.ranked !== b.ranked) return a.ranked ? -1 : 1;
  if (a.value == null || b.value == null) {
    if (a.value == null && b.value == null) return a.index - b.index;
    return a.value == null ? 1 : -1;
  }
  const by = higherIsBetter ? b.value - a.value : a.value - b.value;
  return by || a.index - b.index;
}
