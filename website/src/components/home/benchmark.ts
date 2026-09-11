import {isRanked, PRIMARY, type Entrant, type Row} from '../Results/data.ts';
import {REALISTIC} from '../Results/columns.ts';

/** Entrant ids the descriptors mark as this site's own vendor. */
const vendorIdsOf = (entrants: Entrant[]): Set<string> =>
  new Set(entrants.filter((e) => e.entrant.vendor === 'self').map((e) => e.entrant.id));

/**
 * Each system's best eligible configuration within the supplied comparison
 * group. The vendor's own lane is restricted to `realistic`, so a vendor whose
 * only eligible result in the group is `tuned` gets no lane.
 */
export function headlineLanes(rows: Row[], entrants: Entrant[] = []): Row[] {
  const ours = vendorIdsOf(entrants);
  const eligible = rows.filter(
    (r) =>
      isRanked(r) &&
      r.metrics[PRIMARY] &&
      (!ours.has(r.entrant) || r.approach === REALISTIC),
  );
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
  const vendorIds = vendorIdsOf(entrants);
  return lanes.find((r) => vendorIds.has(r.entrant) && isRanked(r) && r.metrics[PRIMARY]);
}
