// Shared types and selectors for the benchmark charts.
//
// The shape mirrors `benchmarks/src/report.rs`: every record carries its own
// `unit` and `higher_is_better` per metric, so a chart reads the direction of
// goodness FROM THE DATA and never hardcodes it.

import {usePluginData} from '@docusaurus/useGlobalData';

export interface Metric {
  value: number;
  unit: string;
  higher_is_better: boolean;
  ci95?: [number, number];
  n?: number;
}

export interface RunMeta {
  ts_ms: number;
  commit?: string;
  host: string;
  cpu: string;
  cores: number;
  os: string;
  profile: string;
}

export interface BenchRecord {
  schema: number;
  bench: string;
  kind: 'measurement' | 'verdict';
  run: RunMeta;
  variant: Record<string, string | number | boolean>;
  metrics: Record<string, Metric>;
  note?: string;
}

export interface BenchData {
  sourceDir?: string;
  generatedAt?: string;
  byBench: Record<string, BenchRecord[]>;
  counts?: {
    files: number;
    lines: number;
    kept: number;
    skippedSchema: number;
    skippedParse: number;
  };
}

const EMPTY: BenchData = {byBench: {}};

/** Global data loaded by the `benchmark-results` plugin (available at SSR). */
export function useBenchData(): BenchData {
  const data = usePluginData('benchmark-results') as BenchData | undefined;
  return data ?? EMPTY;
}

export function recordsFor(data: BenchData, bench: string): BenchRecord[] {
  return data.byBench?.[bench] ?? [];
}

/** First metric key present on the record, from one name or an ordered list. */
export function resolveMetricKey(
  rec: BenchRecord,
  keys: string | string[],
): string | undefined {
  const list = Array.isArray(keys) ? keys : [keys];
  for (const k of list) {
    if (rec.metrics && rec.metrics[k]) return k;
  }
  return undefined;
}

/** First metric present on the record, from one name or an ordered list. */
export function pickMetric(
  rec: BenchRecord,
  keys: string | string[],
): Metric | undefined {
  const k = resolveMetricKey(rec, keys);
  return k === undefined ? undefined : rec.metrics[k];
}

/** Human label for a variant value (numbers stay numeric). */
export function variantValue(
  rec: BenchRecord,
  key: string,
): string | number | boolean | undefined {
  return rec.variant ? rec.variant[key] : undefined;
}

// ── repetition aggregation ───────────────────────────────────────────────────
//
// Rigs APPEND to `benchmarks/results/*.jsonl`, so one variant is written once
// per repetition. Without aggregation a 2-rep run renders two bars for the same
// arm and `.find()` picks whichever line was written first. The selection layer
// therefore collapses records that share a full variant identity into one.

// Variant keys that vary per repetition in some committed records (the
// 2026-07-10 e2e runs recorded the per-rep producer count in `variant`).
// They are load-side measurements, not arm identity, and must not fragment
// repetition groups. Current rig versions no longer emit them in `variant`.
const VOLATILE_VARIANT_KEYS = new Set(['records_produced']);

/** A stable identity string for a variant map (order-independent). */
export function variantIdentity(
  variant: Record<string, string | number | boolean> | undefined,
): string {
  if (!variant) return '';
  return Object.keys(variant)
    .filter((k) => !VOLATILE_VARIANT_KEYS.has(k))
    .sort()
    .map((k) => `${k}=${String(variant[k])}`)
    .join('');
}

function median(xs: number[]): number {
  const s = [...xs].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
}

/** A record with its repetitions collapsed. `reps` counts the aggregated rows. */
export interface AggregatedRecord extends BenchRecord {
  reps: number;
}

/**
 * Group records by full variant identity and collapse repetitions.
 *
 * Within a group the *selected* metric is aggregated as the median across reps;
 * every other field — provenance/run, ts, note — is taken from the NEWEST
 * record so a re-run's date/commit win. `reps` exposes the repetition count for
 * annotation. Records are expected to be pre-filtered (bench/where/exclude); the
 * selected metric is resolved per group from the newest record.
 */
export function aggregateReps(
  records: BenchRecord[],
  metricKeys: string | string[],
): AggregatedRecord[] {
  const groups = new Map<string, BenchRecord[]>();
  for (const r of records) {
    const id = variantIdentity(r.variant);
    const arr = groups.get(id);
    if (arr) arr.push(r);
    else groups.set(id, [r]);
  }

  const out: AggregatedRecord[] = [];
  for (const reps of groups.values()) {
    const newest = reps.reduce((a, b) =>
      (b.run?.ts_ms ?? 0) > (a.run?.ts_ms ?? 0) ? b : a,
    );
    const key = resolveMetricKey(newest, metricKeys);
    const base = key === undefined ? undefined : newest.metrics[key];
    const metrics = {...newest.metrics};
    if (key !== undefined && base) {
      const values = reps
        .map((r) => r.metrics?.[key]?.value)
        .filter((v): v is number => typeof v === 'number');
      if (values.length > 0) {
        metrics[key] = {...base, value: median(values)};
      }
    }
    out.push({...newest, metrics, reps: reps.length});
  }
  return out;
}

/** A short, distinctive commit for provenance (already short in the data). */
function shortCommit(commit?: string): string | undefined {
  if (!commit) return undefined;
  return commit.length > 10 ? commit.slice(0, 10) : commit;
}

export interface Provenance {
  cpu?: string;
  commit?: string;
  date?: string;
  synthetic: boolean;
}

/** Collapse the provenance of the records backing one chart into one line. */
export function provenanceOf(recs: BenchRecord[]): Provenance {
  if (recs.length === 0) return {synthetic: false};
  // Provenance follows the NEWEST run backing the chart, so a re-run's commit
  // and date win even though the records arrive in append order.
  const newest = recs.reduce((a, b) =>
    (b.run?.ts_ms ?? 0) > (a.run?.ts_ms ?? 0) ? b : a,
  );
  const run = newest.run;
  const date =
    run && Number.isFinite(run.ts_ms)
      ? new Date(run.ts_ms).toISOString().slice(0, 10)
      : undefined;
  const synthetic =
    (run?.cpu ?? '').toLowerCase().includes('fixture') ||
    recs.some((r) => (r.note ?? '').toUpperCase().includes('SYNTHETIC'));
  return {cpu: run?.cpu, commit: shortCommit(run?.commit), date, synthetic};
}

// ── value formatting ───────────────────────────────────────────────────────

const COMPACT = new Intl.NumberFormat('en-US', {
  notation: 'compact',
  maximumFractionDigits: 1,
});

/**
 * Byte-rate units, ascending. Decimal (1000-step), matching the `bytes` branch
 * below and how the rigs divide — see `benchmarks/src/bin/s3_backfill.rs`,
 * which emits MB/s as `bytes / 1e6`.
 */
const BYTE_RATE = ['B/s', 'KB/s', 'MB/s', 'GB/s', 'TB/s'];

/**
 * Rescale a byte rate onto the rung that keeps it readable. Normalising to
 * bytes/s first means a small value scales *down* correctly (0.004 MB/s →
 * "4.00 KB/s"), not just a large one up.
 */
function formatByteRate(value: number, rung: number): string {
  let v = value * 1000 ** rung;
  let i = 0;
  const digits = (x: number): number => (x >= 100 ? 0 : x >= 10 ? 1 : 2);
  // Promote on the *rounded* value, not the raw one: 999.9 rounds to zero
  // decimals as "1000", which would print "1000 MB/s" rather than "1.00 GB/s".
  while (i < BYTE_RATE.length - 1) {
    const a = Math.abs(v);
    if (a < 1000 && Number(a.toFixed(digits(a))) < 1000) break;
    v /= 1000;
    i += 1;
  }
  const a = Math.abs(v);
  return `${v.toFixed(digits(a))} ${BYTE_RATE[i]}`;
}

/** A compact, unit-aware label for a bar tip (e.g. "54 ns", "18.5M/s"). */
export function formatTip(value: number, unit: string): string {
  // Byte rates carry their own magnitude prefix, so the generic "/s" branch
  // below would compact the NUMBER and drop the prefix — 2924 MB/s rendered as
  // "2.9K/s", a figure with no unit at all. Rescale within the ladder instead.
  const rung = BYTE_RATE.indexOf(unit);
  if (rung !== -1) return formatByteRate(value, rung);
  if (unit.endsWith('/s')) {
    // A count rate (records/s, rows/s): the noun is carried by the axis title,
    // so only the magnitude matters here.
    return `${COMPACT.format(value)}/s`;
  }
  if (unit === 'bytes') {
    if (value >= 1e6) return `${(value / 1e6).toFixed(2)} MB`;
    if (value >= 1e3) return `${(value / 1e3).toFixed(1)} KB`;
    return `${value} B`;
  }
  if (unit === 'ns' || unit === 'ms') {
    const n = value >= 100 ? value.toFixed(0) : value.toFixed(1);
    return `${n} ${unit}`;
  }
  if (unit === 's') {
    // Sub-second latencies read better in ms; compact notation would collapse
    // them to "0 s".
    return value < 1 ? `${(value * 1000).toFixed(1)} ms` : `${value.toFixed(2)} s`;
  }
  return `${COMPACT.format(value)} ${unit}`;
}

/**
 * The exact value for the data table — genuinely lossless. `String(value)` is
 * the shortest decimal that round-trips to the same f64, so two distinct values
 * can never collapse into one cell (a fixed fraction-digit cap did: three Native
 * `sink_flush_p99` arms all rendered as "0.025 s"). The integer part gets digit
 * grouping for readability; the full fractional part is preserved verbatim.
 */
export function formatExact(value: number, unit: string): string {
  const raw = String(value);
  let n = raw;
  if (!raw.includes('e') && !raw.includes('E')) {
    const [intPart, fracPart] = raw.split('.');
    const sign = intPart.startsWith('-') ? '-' : '';
    const grouped = Math.abs(Number(intPart)).toLocaleString('en-US', {
      maximumFractionDigits: 0,
    });
    n = fracPart ? `${sign}${grouped}.${fracPart}` : `${sign}${grouped}`;
  }
  return `${n} ${unit}`;
}

/** Direction caption straight from the data. */
export function directionCaption(higherIsBetter: boolean): string {
  return higherIsBetter ? 'Higher is better' : 'Lower is better';
}
