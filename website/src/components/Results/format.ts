// Number formatting for a page that shows thirty-one metrics instead of four.
//
// WHY THIS EXISTS SEPARATELY FROM `Results/data.ts`
//
// A formatter written against four curated metrics is not a formatter for
// thirty-one. The assumptions stop holding — most sharply for `us`, which across the full metric set spans
// 0.677 microseconds to 117,936,638 microseconds. Six orders of magnitude
// through one `toFixed(3)` produces figures like:
//
//     gc_pause_p99_us      174078.000     should read  174 ms
//     gc_pause_total_us    4186788.000    should read  4.19 s
//     throttled_us         37661896.000   should read  37.7 s
//     ch_written_rows      150000000.00   should read  150M
//     duplicate_rows       0.00           should read  0
//
// A benchmark that publishes `4186788.000` has technically published the number
// and practically not: nobody reads that as four seconds, and a reader who
// cannot read a figure cannot check it.
//
// THE HEADLINE RANGES ARE PINNED
//
// The five default columns are the figures most readers will ever quote, so
// their rendering is fixed by test rather than left to whatever a later edit to
// the scaling ladder happens to produce. `format.test.ts` pins them.
//
// This module imports nothing, so `node --test` can load it directly.

/** Units whose magnitude suffix is printed in the cell rather than the header. */
const INLINE_UNITS = new Set(['us']);

/**
 * Format one measurement.
 *
 * Bytes, rates and core counts keep their plain forms. Times carry their own scaled
 * unit, because a column holding both 0.68 µs and 4.19 s cannot have one honest
 * header. Counts drop their decimals entirely: a count
 * of rows is an integer and `150000000.00` claims a precision that does not
 * exist.
 */
export function fmt(v: number, unit: string): string {
  if (!Number.isFinite(v)) return '—';

  if (unit === 'records/s') {
    if (v >= 1e6) return `${(v / 1e6).toFixed(2)}M`;
    if (v >= 1e3) return `${(v / 1e3).toFixed(0)}k`;
    return v.toFixed(0);
  }

  if (unit === 'bytes') {
    if (v >= 1e9) return `${(v / 1e9).toFixed(2)} GB`;
    if (v >= 1e6) return `${(v / 1e6).toFixed(0)} MB`;
    if (v >= 1e3) return `${(v / 1e3).toFixed(0)} kB`;
    return `${v.toFixed(0)} B`;
  }

  // Microseconds, scaled to something a person reads. Below a millisecond the
  // three decimals are kept, because that is the CPU-per-row figure and its
  // whole point is the third digit.
  if (unit === 'us') {
    const a = Math.abs(v);
    if (a < 1e3) return `${v.toFixed(3)} µs`;
    if (a < 1e6) return `${(v / 1e3).toFixed(a < 1e5 ? 1 : 0)} ms`;
    return `${(v / 1e6).toFixed(2)} s`;
  }

  if (unit === 'cores') return v.toFixed(2);

  // Counts. Integers, scaled once they stop being readable.
  if (unit === 'rows' || unit === 'count' || unit === 'messages') {
    const a = Math.abs(v);
    if (a >= 1e6) return `${(v / 1e6).toFixed(2)}M`;
    if (a >= 1e4) return `${(v / 1e3).toFixed(0)}k`;
    return v.toFixed(0);
  }

  return v.toFixed(2);
}

/**
 * The unit as it belongs in a column header.
 *
 * Empty for units the cell prints for itself. A header reading "µs" over a
 * column of `174 ms` would be worse than no header at all.
 */
export function unitLabel(unit: string): string {
  if (INLINE_UNITS.has(unit)) return '';
  if (unit === 'records/s') return 'rows/s';
  if (unit === 'bytes') return 'bytes';
  return unit;
}

/**
 * What the sub-line under a figure says about its repetitions.
 *
 * Reporting "range not defined at zero" whenever the median is zero is accurate
 * and, on the duplicate-rows column, actively misleading: zero
 * duplicates in every repetition is the CORRECT result, and a reader meeting
 * that phrase three times reads it as a measurement that failed. The distinction
 * that matters is whether the repetitions agreed, so that is what it says.
 */
export function fmtReps(m: {
  n: number;
  lo: number;
  hi: number;
  value: number;
  unit: string;
  spread: number | null;
}): string {
  if (m.n < 2) return 'single repetition';
  if (m.lo === m.hi) return `no spread (${m.n} reps)`;
  if (m.spread == null) {
    return `${fmt(m.lo, m.unit)}–${fmt(m.hi, m.unit)}`;
  }
  return `range ${(m.spread * 100).toFixed(1)}%`;
}

/**
 * An arm's label with the facts the row states elsewhere taken out of it.
 *
 * A descriptor may repeat the released version in an arm's label —
 * "Flink 2.2.1 · RowBinary" — because a descriptor cannot know where its arms
 * will be drawn, and on a row whose meta line already carries the measured
 * version that part of the label is the same fact spelled a second time.
 * `armLabel` already makes this argument for the system name; this is the rest
 * of it.
 *
 * The removal is matched against what the ROW actually carries rather than
 * against a pattern, so a label cannot be mangled by coincidence: the version
 * is dropped only when it is byte-identical to the one read out of the image
 * that produced the number — and it is that measured one the meta line prints,
 * not the descriptor's claim about itself.
 *
 * Falls back to the untouched label if stripping would leave nothing.
 */
export function displayLabel(
  label: string,
  row: {version: string | null},
): string {
  let out = label;
  if (row.version) {
    const v = row.version.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    // Only as a whole token, so "2.2.1" never eats part of a longer number.
    out = out.replace(new RegExp(`(^|[\\s·:—-])${v}(?=$|[\\s·:—-])`, 'g'), '$1');
  }
  out = out.replace(/^[\s·:—-]+|[\s·:—-]+$/g, '').replace(/[\s·]*·[\s·]*/g, ' · ').trim();
  return out || label;
}

/**
 * The footer's count of arms drawn without a rank ordinal.
 *
 * Shared between the build and the enhancer rather than written twice, because
 * the enhancer has to rewrite it: the server counts every unranked arm in the
 * group, and the reader then hides some of them with the Show control. A count
 * the filter has invalidated is the same defect as a stale axis tick — a printed
 * number that describes a page the reader is no longer looking at — and that one
 * is already rewritten unconditionally for exactly this reason.
 *
 * Returns the empty string at zero, so the caller renders nothing rather than
 * "0 arms are shown without one".
 */
export function unrankedNote(n: number): string {
  if (n <= 0) return '';
  return `${n} arm${n === 1 ? ' is' : 's are'} shown without one. `;
}
