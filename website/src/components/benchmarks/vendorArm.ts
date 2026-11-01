import {usePluginData} from '@docusaurus/useGlobalData';

import {EMPTY, type Attempt, type Data, type Entrant, type Env, type Row} from '../Results/data';
import {richestGroupKey, vendorBestRow} from './select';

export type VendorArm = {row: Row; entrant: Entrant; env: Env | undefined; basePath: string};

/**
 * The best ranked realistic arm run by the vendor of the benchmark, chosen
 * within the richest comparability group by the homepage's headline rule.
 * The vendor is the descriptor's `vendor` field. `null` when that group holds
 * no such arm, so a page renders nothing rather than a stale literal.
 */
export function useVendorArm(): VendorArm | null {
  const data = (usePluginData('bench-data') as Data | undefined) ?? EMPTY;
  const entrants = data.entrants as Entrant[];
  const row = vendorBestRow(data.rows as Row[], entrants);
  const entrant = row && entrants.find((e) => e.entrant.id === row.entrant);
  if (!row || !entrant) return null;
  const env = (data.environments as Env[]).find((e) => e.id === row.env_id);
  return {row, entrant, env, basePath: data.basePath};
}

/** The comparability group with the most arms, and its rows in the data's order. */
export function useRichestGroup(): {rows: Row[]; entrants: Entrant[]; environments: Env[]; basePath: string; groupKey: string | null; attempts: Attempt[]} {
  const data = (usePluginData('bench-data') as Data | undefined) ?? EMPTY;
  const rows = data.rows as Row[];
  const best = richestGroupKey(rows);
  return {
    rows: best === null ? [] : rows.filter((r) => r.group === best),
    entrants: data.entrants as Entrant[],
    environments: data.environments as Env[],
    basePath: data.basePath,
    groupKey: best,
    attempts: best === null ? [] : (data.attempts as Attempt[]).filter((a) => a.group === best),
  };
}

export const isoDate = (ms: number) => new Date(ms).toISOString().slice(0, 10);
