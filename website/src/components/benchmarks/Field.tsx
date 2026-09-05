import clsx from 'clsx';
import React from 'react';

import {isPlotted, laneRank, niceCeil, unrankedBecause, type Entrant, type Row} from '../Results/data';
import {fmt, unitLabel} from '../Results/format';
import {specOf} from '../Results/columns';
import {armAnchor} from '../Results/model';
import {isoDate} from './vendorArm';

type Props = {
  rows: Row[];
  entrants: Entrant[];
  /** Metric id; the primary column by default. */
  metric: string;
  basePath: string;
  /** Fewer labels and no per-arm meta line. */
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
  const byId = new Map(entrants.map((e) => [e.entrant.id, e]));
  const {order, ranked} = laneRank(rows, byId);
  const spec = specOf(metric);
  const plotted = order.filter((r) => isPlotted(r) && r.metrics[metric]);
  const proto = plotted[0]?.metrics[metric];
  const max = niceCeil(Math.max(0, ...plotted.map((r) => r.metrics[metric].hi)));
  const unit = proto?.unit ?? '';
  const hib = proto?.higher_is_better ?? true;

  return (
    <div className={clsx('field', compact && 'field--compact')}>
      <div className="field__axis" aria-hidden="true">
        <span className="field__axis-zero">0</span>
        <span className="field__axis-label">
          {spec.label}
          {unitLabel(unit) ? `, ${unitLabel(unit)}` : ''} · {hib ? 'higher is better →' : '← lower is better'}
        </span>
        <span className="field__axis-end">{fmt(max, unit)}</span>
      </div>
      <ol className="field__lanes">
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
          return (
            <li
              key={row.key}
              className={clsx('field__lane', rank ? 'field__lane--ranked' : 'field__lane--context', !positioned && 'field__lane--empty')}>
              <span className="field__rank">{rank ?? '—'}</span>
              <span className="field__who">
                {/* A plain anchor: the target is the arm's disclosure in the
                    results table, which the table renders rather than a heading
                    the build could check. */}
                <a href={`${basePath}#${armAnchor(row.key)}`} className="field__name">
                  {name}
                  {ours && (
                    <span className="field__vendor" title="Run by the vendor of this benchmark">
                      {' '}†
                    </span>
                  )}
                </a>
                {!compact && (
                  <span className="field__meta">
                    {label} · {row.wire_format ?? 'format not declared'} · {row.version ?? row.commit ?? 'version unknown'} ·{' '}
                    {isoDate(row.ts_ms)}
                    {why ? ` · ${why}` : ''}
                  </span>
                )}
                {compact && why && <span className="field__meta">{why}</span>}
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
      <p className="field__legend">
        The capsule spans the smallest to the largest repetition; the notch is the median. Gray is shown, not ranked.
        An empty lane is a number the contract disowns. † marks a system run by the author of this benchmark. No
        system has a color.
      </p>
    </div>
  );
}
