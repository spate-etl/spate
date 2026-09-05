// Formatting tests.
//
// The first block is the important one: it pins the exact rendering of the
// default columns and of CPU per row, which are the figures a reader is most
// likely to quote. They are written down as literals rather than derived, so a
// later change to the scaling ladder has to state its intent by editing a test
// rather than silently moving a published number.

import assert from 'node:assert/strict';
import {test} from 'node:test';

import {displayLabel, fmtReps, fmt, unitLabel, unrankedNote} from './format.ts';

test('the headline columns render exactly as published', () => {
  // Throughput per core, the lead column — real values from the published
  // archive: spate:native at 0.1.0, and the same arm at 0.2.0.
  assert.equal(fmt(618227, 'records/s'), '618k');
  assert.equal(fmt(2101775, 'records/s'), '2.10M');

  // Throughput — real values from the published archive.
  assert.equal(fmt(5764000, 'records/s'), '5.76M');
  assert.equal(fmt(2500000, 'records/s'), '2.50M');
  assert.equal(fmt(689000, 'records/s'), '689k');
  assert.equal(fmt(950, 'records/s'), '950');

  // CPU per row — a switchable column rather than a default one since the
  // per-core figure took the lead, and still quoted. The third digit is the
  // entire point of it.
  assert.equal(fmt(0.677, 'us'), '0.677 µs');
  assert.equal(fmt(1.452, 'us'), '1.452 µs');

  // Cores, and memory.
  assert.equal(fmt(3.9, 'cores'), '3.90');
  assert.equal(fmt(853000000, 'bytes'), '853 MB');
  assert.equal(fmt(16950000000, 'bytes'), '16.95 GB');
  assert.equal(fmt(25000000, 'bytes'), '25 MB');
  assert.equal(fmt(512, 'bytes'), '512 B');
});

test('microseconds scale to something a person reads', () => {
  // Below a millisecond the three decimals are kept.
  assert.equal(fmt(0.188, 'us'), '0.188 µs');
  assert.equal(fmt(999, 'us'), '999.000 µs');
  // These are the figures that made this module necessary.
  assert.equal(fmt(174078, 'us'), '174 ms', 'was 174078.000');
  assert.equal(fmt(21574, 'us'), '21.6 ms', 'was 21574.000');
  assert.equal(fmt(4186788, 'us'), '4.19 s', 'was 4186788.000');
  assert.equal(fmt(37661896, 'us'), '37.66 s', 'was 37661896.000');
  assert.equal(fmt(117936638, 'us'), '117.94 s', 'was 117936638.000');
});

test('counts are integers, not two decimal places', () => {
  assert.equal(fmt(0, 'rows'), '0', 'a duplicate count of zero is 0, not 0.00');
  assert.equal(fmt(150000000, 'rows'), '150.00M', 'was 150000000.00');
  assert.equal(fmt(262445.03, 'rows'), '262k', 'was 262445.03');
  assert.equal(fmt(4831, 'rows'), '4831');
});

test('a unit the site has never seen still prints something sane', () => {
  assert.equal(fmt(1.5, 'furlongs'), '1.50');
  assert.equal(fmt(Number.NaN, 'records/s'), '—');
  assert.equal(fmt(Number.POSITIVE_INFINITY, 'bytes'), '—');
});

test('a header never carries a unit the cell already prints', () => {
  assert.equal(unitLabel('us'), '', 'a µs header over a column of "174 ms" is a lie');
  assert.equal(unitLabel('records/s'), 'rows/s');
  // `rows_per_s_per_core` is emitted as `records/s` too, so the header over the
  // lead column comes from the catalogue's `unitLabel` override rather than
  // from here. See `columns.ts`.
  assert.equal(unitLabel('bytes'), 'bytes');
  assert.equal(unitLabel('cores'), 'cores');
});

test('repetitions that agreed say so, rather than reporting a range at zero', () => {
  // The duplicate-rows column: zero in every repetition is the CORRECT result,
  // and "range not defined at zero" reads as a measurement that failed.
  assert.equal(
    fmtReps({n: 3, lo: 0, hi: 0, value: 0, unit: 'rows', spread: null}),
    'no spread (3 reps)',
  );
  assert.equal(
    fmtReps({n: 3, lo: 5.5, hi: 5.9, value: 5.7, unit: 'records/s', spread: 0.0702}),
    'range 7.0%',
  );
  assert.equal(
    fmtReps({n: 1, lo: 1, hi: 1, value: 1, unit: 'cores', spread: null}),
    'single repetition',
  );
  // Spread undefined but the repetitions genuinely differed: show the interval
  // rather than a percentage of zero.
  assert.equal(
    fmtReps({n: 3, lo: -2, hi: 2, value: 0, unit: 'cores', spread: null}),
    '-2.00–2.00',
  );
});

// ---------------------------------------------------------------------------
// Label de-duplication
// ---------------------------------------------------------------------------

test('a label drops only the facts its own row states elsewhere', () => {
  // The version, when the descriptor repeated into the label what the meta line
  // prints from the image that actually produced the number.
  assert.equal(displayLabel('2.2.1 · RowBinary', {version: '2.2.1'}), 'RowBinary');
  assert.equal(displayLabel('Native 0.1.0', {version: '0.1.0'}), 'Native');
});

test('label de-duplication never fires on a coincidence', () => {
  // Not the version this row measured.
  assert.equal(
    displayLabel('2.2.1 · RowBinary', {version: '2.3.0'}),
    '2.2.1 · RowBinary',
  );
  // A label with no version at all is left alone.
  assert.equal(displayLabel('Native', {version: null}), 'Native');
  // A longer number that merely begins with the version.
  assert.equal(
    displayLabel('2.2.15 · RowBinary', {version: '2.2.1'}),
    '2.2.15 · RowBinary',
  );
  // Stripping everything falls back to the label rather than rendering blank.
  assert.equal(displayLabel('2.2.1', {version: '2.2.1'}), '2.2.1');
});

test('the unranked footnote agrees on number, and says nothing at zero', () => {
  assert.equal(unrankedNote(1), '1 arm is shown without one. ');
  assert.equal(unrankedNote(3), '3 arms are shown without one. ');
  // Nothing rather than "0 arms are shown without one", which is what a reader
  // sees the moment the Show control hides every unranked arm in a group.
  assert.equal(unrankedNote(0), '');
  assert.equal(unrankedNote(-1), '');
});
