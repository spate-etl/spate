import Link from '@docusaurus/Link';
import React from 'react';

import Field from '../benchmarks/Field';
import {isoDate, useRichestGroup} from '../benchmarks/vendorArm';
import {PRIMARY, type Entrant, type Env, type Row} from '../Results/data';
import {specOf} from '../Results/columns';
import {fmt, unitLabel} from '../Results/format';
import {headlineLanes, vendorLane} from './benchmark';

type Props = {rows: Row[]; entrants: Entrant[]; environments: Env[]; basePath: string};

export function BenchmarkEvidence({rows, entrants, environments, basePath}: Props): React.JSX.Element | null {
  const lanes = headlineLanes(rows);
  if (!lanes.length) return (
    <figure className="home-hero__chart">
      <figcaption className="home-hero__chart-cap">
        No eligible benchmark results are available. <Link to={basePath}>All results and comparison rules →</Link>
      </figcaption>
    </figure>
  );
  const ours = vendorLane(lanes, entrants);
  const vendor = entrants.find((e) => e.entrant.vendor === 'self');
  const name = vendor?.display?.short ?? vendor?.entrant.name;
  const metric = ours?.metrics[PRIMARY];
  const spec = specOf(PRIMARY);
  const unit = metric ? spec.unitLabel ?? unitLabel(metric.unit) : '';
  const dates = lanes.map((r) => isoDate(r.ts_ms)).sort();
  const measured = dates[0] === dates.at(-1) ? dates[0] : `${dates[0]}–${dates.at(-1)}`;
  const env = environments.find((e) => e.id === lanes[0].env_id);
  const chart = <Field rows={lanes} entrants={entrants} metric={PRIMARY} basePath={basePath} compact />;
  const compare = lanes.length === 5 ? 'Compare all five systems' : `Compare all ${lanes.length} systems`;
  return (
    <figure className="home-hero__chart">
      <div className="home-panel home-hero__panel home-benchmark__desktop">{chart}</div>
      <div className="home-panel home-hero__panel home-benchmark__mobile">
        {metric && ours ? (
          <div className="home-benchmark__summary">
            <p className="home-benchmark__label">{name} · {spec.label.toLowerCase()}</p>
            <p className="home-benchmark__metric">
              <strong>{fmt(metric.value, metric.unit)}</strong> <span>{unit}</span>
            </p>
            <p className="home-benchmark__spread">
              Median of {metric.n} {metric.n === 1 ? 'repetition' : 'repetitions'}
              {metric.spread !== null && metric.n > 1 && ` · ±${(metric.spread * 50).toFixed(1)}%`}
              {metric.n > 1 && <>. Range {fmt(metric.lo, metric.unit)}–{fmt(metric.hi, metric.unit)} {unit}.</>}
              {' '}{metric.higher_is_better ? 'Higher' : 'Lower'} is better.
            </p>
            <p className="home-benchmark__spread">Measured {isoDate(ours.ts_ms)}.</p>
          </div>
        ) : (
          <p className="home-benchmark__label">No eligible {name ? `${name} ` : ''}result in this comparison.</p>
        )}
        <details className="home-benchmark__compare">
          <summary>{compare}</summary>
          {chart}
        </details>
      </div>
      <figcaption className="home-hero__chart-cap">
        <p>
          {lanes.length} systems, each using its best configuration eligible under the published comparison rules.{' '}
          <Link to={basePath}>All results and comparison rules →</Link>
        </p>
        <p>
          Workload: Avro transform → ClickHouse{lanes[0].mode ? ` · ${lanes[0].mode} mode` : ''}.{' '}
          <span title={`Dataset ${lanes[0].dataset_version}`}>Measured {measured}.</span>{' '}
          Environment: {env?.host?.cpu ?? env?.host?.description ?? lanes[0].env_id}
          {env?.host?.cores ? `, ${env.host.cores}-core host` : ''}.
          {name && <> Benchmark run by {name}’s authors.</>}
        </p>
      </figcaption>
    </figure>
  );
}

export default function HeroBenchmark(): React.JSX.Element | null {
  return <BenchmarkEvidence {...useRichestGroup()} />;
}
