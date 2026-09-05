import Link from '@docusaurus/Link';
import React, {useState} from 'react';

import Field from '../components/benchmarks/Field';
import {isoDate, useRichestGroup, useVendorArm} from '../components/benchmarks/vendorArm';
import Results from '../components/Results';
import {defaultColumnsFor, specOf} from '../components/Results/columns';
import {PRIMARY} from '../components/Results/data';
import {fmt} from '../components/Results/format';
import {metricsPresent} from '../components/Results/model';
import SiteLayout from '../components/SiteLayout';

const DESCRIPTION =
  'A reproducible comparison of streaming ETL systems on one fixed pipeline: Kafka to Avro to ClickHouse, every system on the same hardware under the same delivery guarantee.';

function Headline() {
  const arm = useVendorArm();
  if (!arm) return null;
  const {row, entrant, basePath} = arm;
  const primary = row.metrics[PRIMARY];
  const tiles = ['rows_per_s', 'cores_used', 'peak_anon_bytes', 'duplicate_rows'].filter((id) => row.metrics[id]);
  const label = entrant.variants?.find((v) => v.id === row.variant_id)?.label ?? row.variant_id;
  return (
    <section className="bench-hero" aria-labelledby="bench-hero-title">
      <div className="site-container">
        <span className="home-eyebrow">
          {specOf(PRIMARY).label} · {row.approach} arm · ranked
        </span>
        <h1 id="bench-hero-title" className="bench-hero__number">
          <span className="bench-hero__value">{fmt(primary.value, primary.unit)}</span>
          <span className="bench-hero__unit">rows/s per core</span>
        </h1>
        <p className="home-mono home-muted bench-hero__prov">
          {entrant.entrant.name} <span title="Run by the vendor of this benchmark">†</span> · {label} ·{' '}
          <span className="home-chip">{row.version ?? row.commit}</span> · {row.env_id} · harness v{row.harness_version} ·
          corpus {row.dataset_version} · {isoDate(row.ts_ms)} · {row.reps_counted} reps
          {primary.spread !== null && ` · range ${(primary.spread * 100).toFixed(1)}%`}
        </p>
        <p className="home-lead bench-hero__lead">{DESCRIPTION}</p>
        <div className="home-ctas">
          <Link className="home-btn home-btn--ghost" to={`${basePath}contract/rules`}>
            The fairness contract
          </Link>
          <Link className="home-btn home-btn--ghost" to={`${basePath}reproduce`}>
            Reproducing this
          </Link>
        </div>
        <div className="home-proof__tiles bench-hero__tiles">
          {tiles.map((id) => {
            const m = row.metrics[id];
            return (
              <div key={id} className="home-proof__tile">
                <span className="home-proof__value">{fmt(m.value, m.unit)}</span>
                <span className="home-proof__label">
                  {specOf(id).label}
                  {m.spread !== null && m.n > 1 ? ` · ±${(m.spread * 50).toFixed(1)}%` : ' · no spread'}
                </span>
              </div>
            );
          })}
        </div>
        <p className="bench-hero__coi">
          <span aria-hidden="true">†</span> Spate is run by the author of this benchmark. Every row of that system
          carries the dagger, and no published number is reported by the system that produced it: throughput is a
          count against the warehouse, CPU and memory are cgroup counters read by a sidecar.
        </p>
      </div>
    </section>
  );
}

function TheField() {
  const {rows, entrants, basePath} = useRichestGroup();
  const [metric, setMetric] = useState(PRIMARY);
  if (!rows.length) return null;
  const choices = defaultColumnsFor(metricsPresent(rows));
  return (
    <section className="bench-field" aria-labelledby="field-title">
      <div className="site-container">
        <div className="bench-field__head">
          <h2 id="field-title" className="home-h2 home-h2--small">
            The field
          </h2>
          <div role="radiogroup" aria-label="Metric" className="home-tabs">
            {choices.map((id) => (
              <button
                key={id}
                type="button"
                role="radio"
                aria-checked={metric === id}
                className={metric === id ? 'home-tab home-tab--on' : 'home-tab'}
                onClick={() => setMetric(id)}>
                {specOf(id).label}
              </button>
            ))}
          </div>
        </div>
        <div className="home-panel">
          <Field rows={rows} entrants={entrants} metric={metric} basePath={basePath} />
        </div>
      </div>
    </section>
  );
}

export default function BenchmarksPage(): React.JSX.Element {
  return (
    <SiteLayout title="Benchmarks" description={DESCRIPTION} className="benchmarks-page">
      <Headline />
      <TheField />
      <section className="bench-table" aria-label="Every column, every arm">
        <div className="site-container site-container--wide">
          <h2 className="home-h2 home-h2--small">Every column, every arm</h2>
          <Results />
        </div>
      </section>
    </SiteLayout>
  );
}
