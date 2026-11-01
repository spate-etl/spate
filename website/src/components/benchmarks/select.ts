// Row selection for the benchmarks page, kept free of `@docusaurus/*` so `node --test` can load it.
import {type Entrant, type Row} from '../Results/data.ts';
import {headlineLanes, vendorLane} from '../home/benchmark.ts';

/** The comparability group with the most rows, the first one seen on a tie. `null` when there are no rows. */
export function richestGroupKey(rows: Row[]): string | null {
  const counts = new Map<string, number>();
  for (const r of rows) counts.set(r.group, (counts.get(r.group) ?? 0) + 1);
  let best: string | null = null;
  for (const [g, n] of counts) if (best === null || n > (counts.get(best) ?? 0)) best = g;
  return best;
}

/**
 * The vendor's best eligible row in the richest comparability group, by the
 * homepage's headline rule. `undefined` when that group holds none; rows in
 * other groups are never used.
 */
export function vendorBestRow(rows: Row[], entrants: Entrant[]): Row | undefined {
  const group = richestGroupKey(rows);
  return vendorLane(headlineLanes(rows.filter((r) => r.group === group), entrants), entrants);
}
