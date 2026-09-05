import {usePluginData} from '@docusaurus/useGlobalData';

import {EMPTY, PRIMARY, isRanked, type Attempt, type Data, type Entrant, type Env, type Row} from '../Results/data';

export type VendorArm = {row: Row; entrant: Entrant; env: Env | undefined; basePath: string};

/**
 * The newest ranked arm run by the vendor of the benchmark, chosen by the
 * descriptor's `vendor` field and the contract's ranking rule. `null` when
 * no such arm is published, so a page renders nothing rather than a stale
 * literal.
 */
export function useVendorArm(): VendorArm | null {
  const data = (usePluginData('bench-data') as Data | undefined) ?? EMPTY;
  const entrant = (data.entrants as Entrant[]).find((e) => e.entrant.vendor === 'self');
  if (!entrant) return null;
  const rows = (data.rows as Row[])
    .filter((r) => r.entrant === entrant.entrant.id && isRanked(r) && r.metrics[PRIMARY])
    .sort((a, b) => b.ts_ms - a.ts_ms);
  const row = rows[0];
  if (!row) return null;
  const env = (data.environments as Env[]).find((e) => e.id === row.env_id);
  return {row, entrant, env, basePath: data.basePath};
}

/** The comparability group with the most arms, and its rows in the data's order. */
export function useRichestGroup(): {rows: Row[]; entrants: Entrant[]; basePath: string; groupKey: string | null; attempts: Attempt[]} {
  const data = (usePluginData('bench-data') as Data | undefined) ?? EMPTY;
  const rows = data.rows as Row[];
  const counts = new Map<string, number>();
  for (const r of rows) counts.set(r.group, (counts.get(r.group) ?? 0) + 1);
  let best: string | null = null;
  for (const [g, n] of counts) if (best === null || n > (counts.get(best) ?? 0)) best = g;
  return {
    rows: best === null ? [] : rows.filter((r) => r.group === best),
    entrants: data.entrants as Entrant[],
    basePath: data.basePath,
    groupKey: best,
    attempts: best === null ? [] : (data.attempts as Attempt[]).filter((a) => a.group === best),
  };
}

export const isoDate = (ms: number) => new Date(ms).toISOString().slice(0, 10);
