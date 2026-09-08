import {isRanked, PRIMARY, type Entrant, type Row} from '../Results/data.ts';

/** Each system's best eligible configuration within the supplied comparison group. */
export function headlineLanes(rows: Row[]): Row[] {
  const eligible = rows.filter((r) => isRanked(r) && r.metrics[PRIMARY]);
  const hib = eligible[0]?.metrics[PRIMARY].higher_is_better ?? true;
  const best = new Map<string, Row>();
  for (const r of eligible) {
    const held = best.get(r.entrant);
    const v = r.metrics[PRIMARY].value;
    const h = held?.metrics[PRIMARY].value;
    if (h === undefined || (hib ? v > h : v < h)) best.set(r.entrant, r);
  }
  return [...best.values()];
}

/** The vendor's eligible result from the displayed lanes, without a cross-group fallback. */
export function vendorLane(lanes: Row[], entrants: Entrant[]): Row | undefined {
  const vendorIds = new Set(entrants.filter((e) => e.entrant.vendor === 'self').map((e) => e.entrant.id));
  return lanes.find((r) => vendorIds.has(r.entrant) && isRanked(r) && r.metrics[PRIMARY]);
}
