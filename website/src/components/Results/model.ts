// Derived facts about a row, an arm or a group that are not measurements.
//
// Everything here is a pure function of what the build already loaded. Nothing
// computes, adjusts or re-derives a published figure — `data.ts` owns the
// contract predicates and `format.ts` owns how a number is printed, and this
// file only answers questions the markup needs: which show-class a row is in,
// which metrics exist, what a group's mode was, what to call an anchor.
//
// NOTHING HERE MAY BRANCH ON AN ENTRANT ID.

import type {Entrant, Row} from './data.ts';

/**
 * The class that decides whether a reader sees this arm: its APPROACH.
 *
 * Not `unrankedBecause`, and the difference is the whole of a defect this
 * closes. That function answers "why is this arm not ranked?", and it answers it
 * with `infra-bound` ahead of the approach because a disowned number is the
 * stronger statement about ranking. Feeding the same sentence to the Show
 * control made it the stronger statement about *visibility* too: rule 3 leaves
 * only `realistic` ticked, so a realistic arm whose newest sitting came back
 * `infra_bound` was hidden from every reader with scripting on. `spate:rowbinary`
 * was in the prerendered HTML, with its digits and its chip, and rendered
 * nowhere.
 *
 * The two questions are genuinely different, and `isPlotted` in `data.ts`
 * already argues why. A `tuned` or `stripped` arm is a configuration a reader
 * did not ask to see, and rule 3 says keep it off the chart until they do. An
 * `infra_bound` arm is the realistic configuration; what is wrong with it is the
 * number, not the configuration — and the legend promises it "keeps its digits
 * and its reason but not its position", which is a promise about ranking and
 * position, not about whether the row exists. `methodology/envelope.md` gives
 * the reason the status is recorded at all: so that "we ran it and it blew the
 * limit" stays distinguishable from "we never ran it". An arm hidden by default
 * is not distinguishable from one that was never run.
 *
 * So infra-boundness reaches the reader through the chip, the void lane and the
 * disclosure — never through whether the row is drawn.
 */
export function showClassOf(r: Row): string {
  return r.approach || 'undeclared';
}

/** Every metric id present across a set of rows. */
export function metricsPresent(rows: Row[]): string[] {
  const s = new Set<string>();
  for (const r of rows) for (const k of Object.keys(r.metrics)) s.add(k);
  return [...s];
}

/**
 * A stable index for an arm in its descriptor's own declaration order.
 *
 * `filter.ts` documents `Sortable.index` as "position in the descriptor's own
 * order, as the tie-break of last resort". That is only true if the browser is
 * handed the descriptor's order rather than the order the rows happen to be in
 * — otherwise ties resolve against whatever the previous sort produced, and the
 * tie-break stops being stable across control changes. The build writes this
 * number into `data-index` so the enhancer has the real thing to pass.
 */
export function descriptorIndex(
  entrants: Entrant[],
  entrantId: string,
  variantId: string,
): number {
  const ei = entrants.findIndex((e) => e.entrant.id === entrantId);
  const e = ei < 0 ? undefined : entrants[ei];
  const vs = e?.variants ?? [];
  const vi = vs.findIndex((v) => v.id === variantId);
  return (ei < 0 ? entrants.length : ei) * 1000 + (vi < 0 ? vs.length : vi);
}

/**
 * The mode a comparability group was measured in, read back off its key.
 *
 * Mode splits groups — `rows_per_s` means "how fast can this go" in drain and
 * "the rate we asked for" in sustained, so the two are never drawn on one axis —
 * but the plugin does not surface it as its own field. It is a component of the
 * group key, and `plugins/bench-data/index.test.js` pins it there as a component
 * rather than as a suffix, so reading it back is safe against the key growing.
 */
export function modeOf(groupKey: string): string | null {
  const part = groupKey.split('|').find((p) => p.startsWith('mode-'));
  const mode = part?.slice('mode-'.length);
  return !mode || mode === '?' ? null : mode;
}

/**
 * A short, stable anchor for one arm.
 *
 * The row key is a dedup key — group tuple, entrant, variant, version, every
 * recorded knob, invocation — so slugifying it directly produces a 300-character
 * fragment. That matters because the whole point of giving each arm a real URL
 * is that someone can paste it into an argument about a number.
 *
 * FNV-1a over the key: deterministic, computed at build time, and stable across
 * rebuilds because the key is. Collisions inside one page would have to be
 * engineered; the anchor is a convenience, and the row is on the page either way.
 */
export function armAnchor(key: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < key.length; i++) {
    h ^= key.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return `arm-${h.toString(36)}`;
}

/** The searchable text for an arm, for the name filter. */
export const searchTextOf = (systemName: string, armLabel: string, variantId: string) =>
  `${systemName} ${armLabel} ${variantId}`.toLowerCase();
