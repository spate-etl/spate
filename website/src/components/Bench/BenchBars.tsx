import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import {
  useBenchData,
  recordsFor,
  pickMetric,
  resolveMetricKey,
  aggregateReps,
  variantIdentity,
  variantValue,
  provenanceOf,
  formatTip,
  formatExact,
  directionCaption,
  type BenchRecord,
  type AggregatedRecord,
  type Metric,
} from './data';
import styles from './styles.module.css';

type VarKey = string | string[];
type Scalar = string | number | boolean;

interface BenchBarsProps {
  /** Which rig produced the records (matches `bench` in the JSONL). */
  bench: string;
  /** Metric key, or an ordered list of candidate keys (first present wins). */
  metric: string | string[];
  /** Variant key(s) that name each bar (single mode) or each group (grouped). */
  category: VarKey;
  /** Optional variant key that splits each group into a 2-series comparison. */
  series?: string;
  /** Keep only records whose variant matches every key/value here. */
  where?: Record<string, Scalar>;
  /** Drop records whose variant matches any key/value here (e.g. a control arm). */
  exclude?: Record<string, Scalar | Scalar[]>;
  /** The value to render in the accent hue (a category value, or a series value). */
  emphasize?: Scalar;
  title: string;
  caption?: React.ReactNode;
  /** Explicit ordering of category/group labels; otherwise sorted. */
  order?: string[];
  /** Explicit ordering of series values (grouped mode). */
  seriesOrder?: string[];
  /** Pretty labels for raw variant values. */
  labels?: Record<string, string>;
}

// ── viewBox geometry (scales responsively; text stays legible to ~360px) ─────
const W = 560;
const PAD_L = 4;
const PAD_R = 4;
const GUTTER_R = 88; // room for the value label at the bar tip
const TRACK_X0 = PAD_L;
const TRACK_W = W - PAD_L - PAD_R - GUTTER_R;

const roundedRightRect = (x: number, y: number, w: number, h: number): string => {
  if (w <= 0.5) return `M${x},${y} h0.5 v${h} h-0.5 Z`;
  const r = Math.min(4, w, h / 2);
  return [
    `M${x},${y}`,
    `H${x + w - r}`,
    `Q${x + w},${y} ${x + w},${y + r}`,
    `V${y + h - r}`,
    `Q${x + w},${y + h} ${x + w - r},${y + h}`,
    `H${x}`,
    'Z',
  ].join(' ');
};

const num = (v: Scalar | undefined): string => (v === undefined ? '' : String(v));

const labelOf = (v: Scalar | undefined, labels?: Record<string, string>): string => {
  const s = num(v);
  return labels?.[s] ?? s;
};

const categoryLabel = (
  rec: BenchRecord,
  category: VarKey,
  labels?: Record<string, string>,
): string => {
  const keys = Array.isArray(category) ? category : [category];
  return keys
    .map((k) => labelOf(variantValue(rec, k) as Scalar, labels))
    .filter(Boolean)
    .join(' · ');
};

const matchesWhere = (rec: BenchRecord, where?: Record<string, Scalar>): boolean => {
  if (!where) return true;
  return Object.entries(where).every(([k, v]) => num(variantValue(rec, k) as Scalar) === num(v));
};

const isExcluded = (
  rec: BenchRecord,
  exclude?: Record<string, Scalar | Scalar[]>,
): boolean => {
  if (!exclude) return false;
  return Object.entries(exclude).some(([k, v]) => {
    const rv = num(variantValue(rec, k) as Scalar);
    return Array.isArray(v) ? v.map(num).includes(rv) : num(v) === rv;
  });
};

function Placeholder({title}: {title: string}): React.ReactElement {
  return (
    <div className={styles.viz}>
      <div className={styles.placeholder}>
        <strong>{title}</strong>
        <div>
          No recorded data yet. These charts render from the committed{' '}
          <code>benchmarks/results/*.jsonl</code> once a rig has been run — see{' '}
          the <Link to="/docs/benchmarks/methodology">methodology</Link> for how
          to reproduce.
        </div>
      </div>
    </div>
  );
}

interface Bar {
  key: string;
  label: string;
  value: number;
  metric: Metric;
  emphasized: boolean;
  /** Repetition count backing this bar (>=1); annotated when >=2. */
  reps: number;
}

export default function BenchBars(props: BenchBarsProps): React.ReactElement {
  const {
    bench,
    metric,
    category,
    series,
    where,
    exclude,
    emphasize,
    title,
    caption,
    order,
    seriesOrder,
    labels,
  } = props;

  const data = useBenchData();
  const titleId = React.useId();
  const descId = React.useId();

  const records = recordsFor(data, bench)
    .filter((r) => r.kind === 'measurement')
    .filter((r) => matchesWhere(r, where))
    .filter((r) => !isExcluded(r, exclude))
    .filter((r) => pickMetric(r, metric) !== undefined);

  if (records.length === 0) return <Placeholder title={title} />;

  // Collapse repetitions (rigs append one row per rep) into one record per
  // variant identity: median of the selected metric, newest provenance.
  const agg: AggregatedRecord[] = aggregateReps(records, metric);

  // A panel renders one axis, so it must resolve one metric key and one unit
  // across every record — the fallback list (`metric={['rows_per_s', …]}`) can
  // otherwise pick different keys per record and silently mix units/directions.
  // SSR renders at build time, so throwing here is a loud CI failure.
  const keys = new Set<string>();
  const units = new Set<string>();
  for (const r of agg) {
    const k = resolveMetricKey(r, metric);
    if (k === undefined) continue;
    keys.add(k);
    units.add(r.metrics[k].unit);
  }
  if (keys.size > 1 || units.size > 1) {
    throw new Error(
      `[BenchBars] bench "${bench}" resolves inconsistent metrics within one ` +
        `panel — keys {${[...keys].join(', ')}}, units {${[...units].join(
          ', ',
        )}}. A single panel must share one metric key and one unit; split it ` +
        `into separate panels or align the metric names.`,
    );
  }

  const dirBetter = pickMetric(agg[0], metric)!.higher_is_better;
  const unit = pickMetric(agg[0], metric)!.unit;
  const prov = provenanceOf(agg);

  const emphStr = emphasize === undefined ? undefined : num(emphasize);
  const isEmph = (rec: BenchRecord): boolean => {
    if (emphStr === undefined) return false;
    if (series) return num(variantValue(rec, series) as Scalar) === emphStr;
    const keys = Array.isArray(category) ? category : [category];
    return keys.some((k) => num(variantValue(rec, k) as Scalar) === emphStr);
  };

  // ── build the model ────────────────────────────────────────────────────────
  const grouped = Boolean(series);

  interface Group {
    label: string;
    bars: Bar[];
  }
  const groups: Group[] = [];

  // Explicitly-ordered labels come first in `order`'s order; anything not named
  // sorts AFTER them (a raw indexOf returns −1 and would hoist unknowns first).
  const rankOf = (label: string, ord: string[]): number => {
    const i = ord.indexOf(label);
    return i === -1 ? ord.length : i;
  };

  if (!grouped) {
    const bars: Bar[] = agg.map((r) => ({
      key: variantIdentity(r.variant),
      label: categoryLabel(r, category, labels),
      value: pickMetric(r, metric)!.value,
      metric: pickMetric(r, metric)!,
      emphasized: isEmph(r),
      reps: r.reps,
    }));
    if (order) {
      bars.sort((a, b) => rankOf(a.label, order) - rankOf(b.label, order));
    } else {
      bars.sort((a, b) => (dirBetter ? b.value - a.value : a.value - b.value));
    }
    groups.push({label: '', bars});
  } else {
    const byGroup = new Map<string, Bar[]>();
    for (const r of agg) {
      const gl = categoryLabel(r, category, labels);
      const bar: Bar = {
        key: variantIdentity(r.variant),
        label: labelOf(variantValue(r, series) as Scalar, labels),
        value: pickMetric(r, metric)!.value,
        metric: pickMetric(r, metric)!,
        emphasized: isEmph(r),
        reps: r.reps,
      };
      const arr = byGroup.get(gl) ?? [];
      arr.push(bar);
      byGroup.set(gl, arr);
    }
    const groupLabels = [...byGroup.keys()];
    groupLabels.sort((a, b) =>
      order ? rankOf(a, order) - rankOf(b, order) : a.localeCompare(b),
    );
    for (const gl of groupLabels) {
      const bars = byGroup.get(gl)!;
      if (seriesOrder) {
        bars.sort(
          (a, b) => seriesOrder.indexOf(a.label) - seriesOrder.indexOf(b.label),
        );
      } else {
        // accent series first within each group
        bars.sort((a, b) => Number(b.emphasized) - Number(a.emphasized));
      }
      groups.push({label: gl, bars});
    }
  }

  const maxValue = Math.max(
    ...groups.flatMap((g) => g.bars.map((b) => b.value)),
    0,
  );
  const scale = (v: number): number => (maxValue > 0 ? (v / maxValue) * TRACK_W : 0);

  // ── layout ──────────────────────────────────────────────────────────────────
  const TOP = 8;
  const BAR_H = grouped ? 15 : 16;
  const SERIES_GAP = 2; // surface gap between the two bars in a group
  const ROW_GAP = grouped ? 0 : 12;
  const LABEL_H = grouped ? 16 : 18;

  interface Placed extends Bar {
    y: number;
  }
  const placed: Placed[] = [];
  const groupHeaders: {label: string; y: number}[] = [];
  let y = TOP;
  for (const g of groups) {
    if (grouped && g.label) {
      groupHeaders.push({label: g.label, y: y + 12});
      y += LABEL_H;
    }
    for (const b of g.bars) {
      if (!grouped) {
        // category label sits above its bar
        groupHeaders.push({label: b.label, y: y + 13});
        y += LABEL_H;
      }
      placed.push({...b, y});
      y += BAR_H + (grouped ? SERIES_GAP : ROW_GAP);
    }
    if (grouped) y += 12; // gap between groups
  }
  const H = y + 6;

  // ── accessible description ───────────────────────────────────────────────────
  const repNote = (b: Bar): string => (b.reps >= 2 ? ` (n=${b.reps})` : '');
  const descText = placed
    .map((b) => `${b.label}: ${formatTip(b.value, unit)}${repNote(b)}`)
    .join('; ');

  // Distinct series for the grouped legend, each with its own emphasis state.
  const seriesLegend: {label: string; emphasized: boolean}[] = [];
  if (grouped) {
    for (const g of groups) {
      for (const b of g.bars) {
        if (!seriesLegend.some((s) => s.label === b.label)) {
          seriesLegend.push({label: b.label, emphasized: b.emphasized});
        }
      }
    }
  }

  const showLegend = grouped || emphStr !== undefined;

  return (
    <figure className={clsx(styles.viz, styles.figure)}>
      <div className={styles.head}>
        <figcaption className={styles.title}>{title}</figcaption>
        <span className={styles.direction}>{directionCaption(dirBetter)}</span>
      </div>
      {caption ? <div className={styles.caption}>{caption}</div> : null}

      {showLegend ? (
        <div className={styles.legend}>
          {grouped ? (
            seriesLegend.map((s) => (
              <span key={s.label} className={styles.legendItem}>
                <span
                  className={clsx(
                    styles.swatch,
                    s.emphasized ? styles.swatchAccent : styles.swatchMuted,
                  )}
                />
                {s.label}
              </span>
            ))
          ) : (
            <>
              <span className={styles.legendItem}>
                <span className={clsx(styles.swatch, styles.swatchAccent)} />
                {labelOf(emphasize, labels)} (highlighted)
              </span>
              <span className={styles.legendItem}>
                <span className={clsx(styles.swatch, styles.swatchMuted)} />
                other arms
              </span>
            </>
          )}
        </div>
      ) : null}

      <div className={styles.chartScroll}>
        <svg
          className={styles.chart}
          viewBox={`0 0 ${W} ${H}`}
          style={{maxWidth: W}}
          role="img"
          aria-labelledby={`${titleId} ${descId}`}
        >
          <title id={titleId}>{`${title} — ${directionCaption(dirBetter)}`}</title>
          <desc id={descId}>{descText}</desc>
          {groupHeaders.map((h, i) => (
            <text
              key={`h${i}`}
              x={PAD_L}
              y={h.y}
              className={grouped ? styles.groupLabel : styles.rowLabel}
            >
              {h.label}
            </text>
          ))}
          {placed.map((b) => {
            const w = scale(b.value);
            const tipX = TRACK_X0 + w + 6;
            return (
              <g key={b.key}>
                <title>{`${b.label}: ${formatTip(b.value, unit)}${repNote(b)}`}</title>
                <path
                  className={b.emphasized ? styles.barAccent : styles.barMuted}
                  d={roundedRightRect(TRACK_X0, b.y, w, BAR_H)}
                />
                <text
                  x={tipX}
                  y={b.y + BAR_H - 3}
                  className={styles.valueLabel}
                >
                  {formatTip(b.value, unit)}
                </text>
              </g>
            );
          })}
        </svg>
      </div>

      <details className={styles.details}>
        <summary>Data table</summary>
        <div className={styles.tableWrap}>
          <table>
            <thead>
              <tr>
                {grouped ? <th>Group</th> : null}
                <th>{grouped ? 'Series' : 'Variant'}</th>
                <th>Value</th>
                <th>95% CI</th>
                <th>n</th>
              </tr>
            </thead>
            <tbody>
              {groups.flatMap((g) =>
                g.bars.map((b) => (
                  <tr key={b.key}>
                    {grouped ? <td>{g.label}</td> : null}
                    <td>{b.label}</td>
                    <td>{formatExact(b.value, unit)}</td>
                    <td>
                      {b.metric.ci95
                        ? `${b.metric.ci95[0]} – ${b.metric.ci95[1]}`
                        : '—'}
                    </td>
                    <td>{b.reps >= 2 ? b.reps : b.metric.n ?? '—'}</td>
                  </tr>
                )),
              )}
            </tbody>
          </table>
        </div>
      </details>

      <div className={styles.provenance}>
        {prov.synthetic ? (
          <span className={styles.synthetic}>SYNTHETIC FIXTURE · </span>
        ) : null}
        {[prov.cpu, prov.commit ? `commit ${prov.commit}` : null, prov.date]
          .filter(Boolean)
          .join(' · ')}
      </div>
    </figure>
  );
}
