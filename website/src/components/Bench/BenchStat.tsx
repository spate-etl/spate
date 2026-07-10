import React from 'react';
import clsx from 'clsx';
import {
  useBenchData,
  recordsFor,
  pickMetric,
  aggregateReps,
  variantValue,
  type BenchRecord,
  type Metric,
} from './data';
import styles from './styles.module.css';

type Scalar = string | number | boolean;

const num = (v: Scalar | undefined): string => (v === undefined ? '' : String(v));

/** Grid wrapper for a row of stat tiles. */
export function StatRow({children}: {children: React.ReactNode}): React.ReactElement {
  return <div className={styles.statRow}>{children}</div>;
}

interface BenchStatProps {
  /** Sentence-case caption, no trailing colon. */
  label: string;
  /** Static headline value (e.g. "~9", "48.7–70.6"). Omit in data modes. */
  value?: React.ReactNode;
  /** Unit suffix rendered smaller after the value (e.g. "ns/record", "%"). */
  unit?: string;
  /** Supporting line under the value. */
  sub?: React.ReactNode;
  /** Render the value in the site accent. */
  emphasis?: boolean;

  // ── data-derived modes (all require `bench` + `metric`) ─────────────────────
  //
  // Three data modes read the same JSONL the charts do; each selects records by
  // a variant match. Provide exactly one selector pair/triple:
  //
  //   ratio   — `numer` / `denom`   → "N.N×"       (metric_numer / metric_denom)
  //   delta   — `from`  / `to`      → "+N.N" / "−N.N" with unit "%"  ((to−from)/from)
  //   value   — `where`             → the metric's own value + its unit
  //
  /** Bench name (matches `bench` in the JSONL). */
  bench?: string;
  /** Metric key, or an ordered fallback list (first present wins per record). */
  metric?: string | string[];
  /** Ratio mode: variant match selecting the numerator record. */
  numer?: Record<string, Scalar>;
  /** Ratio mode: variant match selecting the denominator record. */
  denom?: Record<string, Scalar>;
  /** Percent-delta mode: baseline record (the `a` in (b−a)/a). */
  from?: Record<string, Scalar>;
  /** Percent-delta mode: compared record (the `b` in (b−a)/a). */
  to?: Record<string, Scalar>;
  /** Value mode: the single record whose metric becomes the headline. */
  where?: Record<string, Scalar>;
}

function matches(rec: BenchRecord, where: Record<string, Scalar>): boolean {
  return Object.entries(where).every(
    ([k, v]) => num(variantValue(rec, k)) === num(v),
  );
}

/** A readable headline number: integers grouped, else up to two decimals. */
function formatStat(v: number): string {
  return Number.isInteger(v)
    ? v.toLocaleString('en-US')
    : v.toLocaleString('en-US', {maximumFractionDigits: 2});
}

/** Signed percent with a unicode minus, matching the docs' "−2.2" style. */
function formatSignedPct(pct: number): string {
  const sign = pct >= 0 ? '+' : '−';
  return `${sign}${Math.abs(pct).toFixed(1)}`;
}

export default function BenchStat(props: BenchStatProps): React.ReactElement {
  const {label, unit, sub, emphasis} = props;
  const data = useBenchData();

  let value = props.value;
  let derivedUnit = unit;
  let derivedSub = sub;

  if (props.bench && props.metric) {
    const recs = recordsFor(data, props.bench).filter((r) => r.kind === 'measurement');
    const read = (where: Record<string, Scalar>): Metric | undefined => {
      const matched = recs.filter((r) => matches(r, where));
      if (matched.length === 0) return undefined;
      // Collapse repetitions of the arm (median), exactly like BenchBars. A
      // `where` spanning several distinct arms is a page-authoring bug —
      // fail the build (SSR) rather than headline an arbitrary record.
      const groups = aggregateReps(matched, props.metric!);
      if (groups.length > 1) {
        throw new Error(
          `BenchStat: where ${JSON.stringify(where)} on bench "${props.bench}" ` +
            `matches ${groups.length} distinct arms`,
        );
      }
      return pickMetric(groups[0], props.metric!);
    };
    const noData = (): void => {
      value = '—';
      derivedUnit = undefined;
      derivedSub = sub ?? 'No recorded data yet';
    };

    if (props.numer && props.denom) {
      const nVal = read(props.numer)?.value;
      const dVal = read(props.denom)?.value;
      if (nVal !== undefined && dVal !== undefined && dVal !== 0) {
        value = (nVal / dVal).toFixed(1);
        derivedUnit = '×';
      } else {
        noData();
      }
    } else if (props.from && props.to) {
      const aVal = read(props.from)?.value;
      const bVal = read(props.to)?.value;
      if (aVal !== undefined && bVal !== undefined && aVal !== 0) {
        value = formatSignedPct(((bVal - aVal) / aVal) * 100);
        derivedUnit = '%';
      } else {
        noData();
      }
    } else if (props.where) {
      const m = read(props.where);
      if (m) {
        value = formatStat(m.value);
        derivedUnit = unit ?? m.unit;
      } else {
        noData();
      }
    }
  }

  return (
    <div className={clsx(styles.stat, emphasis && styles.statEmphasis)}>
      <div className={styles.statLabel}>{label}</div>
      <div className={clsx(styles.statValue, emphasis && styles.statValueEmphasis)}>
        {value}
        {derivedUnit ? <span className={styles.statUnit}>{derivedUnit}</span> : null}
      </div>
      {derivedSub ? <div className={styles.statSub}>{derivedSub}</div> : null}
    </div>
  );
}
