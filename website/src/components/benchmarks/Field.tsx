import Link from '@docusaurus/Link';
import clsx from 'clsx';
import React, {useId} from 'react';

import {isPlotted, laneRank, niceCeil, unrankedBecause, type Entrant, type Row} from '../Results/data';
import {fmt, unitLabel} from '../Results/format';
import {specOf} from '../Results/columns';
import {isoDate} from './vendorArm';

type Props = {
  rows: Row[];
  entrants: Entrant[];
  /** Metric id; the primary column by default. */
  metric: string;
  basePath: string;
  /** Reduces the per-arm meta line to the version and any unranked reason. */
  compact?: boolean;
};

/**
 * One bar per arm for one metric, under the fairness contract's display rules:
 * the lane order is the table's, a ranked arm takes the accent, an unranked
 * arm is drawn in grey with no ordinal, an infra-bound arm keeps its number
 * and its reason but gets no position, the capsule spans the repetitions with
 * the median as its notch, and the axis starts at zero and prints its end.
 */
export default function Field({rows, entrants, metric, basePath, compact}: Props): React.JSX.Element {
  const labelId = useId();
  const byId = new Map(entrants.map((e) => [e.entrant.id, e]));
  const {order, ranked} = laneRank(rows, byId);
  const spec = specOf(metric);
  const plotted = order.filter((r) => isPlotted(r) && r.metrics[metric]);
  const proto = plotted[0]?.metrics[metric] ?? order.find((r) => r.metrics[metric])?.metrics[metric];
  const max = niceCeil(Math.max(0, ...plotted.map((r) => r.metrics[metric].hi)));
  const unit = proto?.unit ?? '';
  const hib = proto?.higher_is_better ?? true;
  const units = spec.unitLabel ?? unitLabel(unit);
  // Each mark the legend names only where a lane carries it.
  const notes = [
    'The capsule spans the smallest to the largest repetition; the notch is the median.',
    ...(order.some((r) => !ranked.has(r.key)) ? ['Gray is shown, not ranked.'] : []),
    ...(order.some((r) => !(isPlotted(r) && r.metrics[metric]))
      ? ['An empty lane is a number the contract disowns.']
      : []),
    ...(order.some((r) => byId.get(r.entrant)?.entrant.vendor === 'self')
      ? ['† marks a system run by the author of this benchmark.']
      : []),
    'No system has a color.',
  ];

  return (
    <div className={clsx('field', compact && 'field--compact')}>
      <div className="field__axis">
        <span className="field__axis-zero" aria-hidden="true">0</span>
        <span className="field__axis-label" id={labelId}>
          {spec.label}
          {units ? `, ${units}` : ''} · {hib ? 'higher is better' : 'lower is better'}<span aria-hidden="true">{hib ? ' →' : ' ←'}</span>
        </span>
        <span className="field__axis-end" aria-hidden="true">{fmt(max, unit)}</span>
      </div>
      <ol className="field__lanes" aria-labelledby={labelId}>
        {order.map((row) => {
          const e = byId.get(row.entrant);
          const m = row.metrics[metric];
          const rank = ranked.get(row.key);
          const why = unrankedBecause(row);
          const positioned = isPlotted(row) && m;
          const pct = (v: number) => `${Math.min(100, (v / max) * 100).toFixed(2)}%`;
          const ours = e?.entrant.vendor === 'self';
          const name = e?.display?.short ?? e?.entrant.name ?? row.entrant;
          const label = e?.variants?.find((v) => v.id === row.variant_id)?.label ?? row.variant_id;
          const terse = [row.version ?? row.commit, positioned ? why : ''].filter(Boolean).join(' · ');
          return (
            <li
              key={row.key}
              className={clsx('field__lane', rank ? 'field__lane--ranked' : 'field__lane--context', !positioned && 'field__lane--empty')}>
              <span className="field__rank">{rank ?? '—'}</span>
              <span className="field__who">
                <Link to={`${basePath}systems/${row.entrant}`} className="field__name">
                  {name}
                  {ours && (
                    <span className="field__vendor" title="Run by the vendor of this benchmark">
                      {' '}†
                    </span>
                  )}
                </Link>
                {!compact && (
                  <span className="field__meta">
                    {label} · {row.wire_format ?? 'format not declared'} · {row.version ?? row.commit ?? 'version unknown'} ·{' '}
                    {isoDate(row.ts_ms)}
                    {why && positioned ? ` · ${why}` : ''}
                  </span>
                )}
                {compact && terse && <span className="field__meta">{terse}</span>}
              </span>
              <span className="field__track" aria-hidden="true">
                {positioned && (
                  <>
                    <span className="field__bar" style={{width: pct(m.value)}} />
                    <span
                      className="field__capsule"
                      style={{left: pct(m.lo), width: `max(2px, calc(${pct(m.hi)} - ${pct(m.lo)}))`}}
                    />
                    <span className="field__notch" style={{left: pct(m.value)}} />
                  </>
                )}
                {!positioned && <span className="field__empty">{why || 'not measured'}</span>}
              </span>
              <span className="field__value">
                {m ? fmt(m.value, m.unit) : '—'}
                {m && m.spread !== null && m.n > 1 && (
                  <span className="field__spread"> ±{(m.spread * 50).toFixed(1)}%</span>
                )}
              </span>
            </li>
          );
        })}
      </ol>
      <p className="field__legend">{notes.join(' ')}</p>
    </div>
  );
}
