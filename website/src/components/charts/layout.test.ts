import assert from 'node:assert/strict';
import {test} from 'node:test';

import {S1} from '../diagrams/scale.ts';
import {columns, labelBox, landscape, niceScale, rows, type ChartData, type Label, type Layout} from './layout.ts';

const DATA: ChartData = {
  groups: ['4 instances', '8 instances'],
  series: ['static split', 'NATS coordination', 'SQS queue', 'Kafka queue'],
  values: [
    [45.32, 41.98, 46.15, 46.12],
    [90.64, 59.81, 92.23, 92.24],
  ],
  decimals: 2,
};

type Box = {x0: number; y0: number; x1: number; y1: number};
const overlaps = (a: Box, b: Box) => a.x0 < b.x1 && b.x0 < a.x1 && a.y0 < b.y1 && b.y0 < a.y1;
const rectBox = ({x, y, w, h}: {x: number; y: number; w: number; h: number}): Box => ({x0: x, y0: y, x1: x + w, y1: y + h});

/** Every label the layout sets. */
function labels(l: Layout): Label[] {
  return [l.axisLabel, ...l.groupLabels, ...l.ticks.map((t) => t.label), ...l.bars.map((b) => b.value)];
}

/** Faults a reader would see: a label outside the viewBox, two labels touching, a label on a bar. */
function faults(l: Layout): string[] {
  const found: string[] = [];
  const all = labels(l);
  all.forEach((a, i) => {
    const box = labelBox(a);
    if (box.x0 < 0 || box.y0 < 0 || box.x1 > l.width || box.y1 > l.height) found.push(`${a.text} leaves the viewBox`);
    all.slice(i + 1).forEach((b) => {
      if (overlaps(box, labelBox(b))) found.push(`${a.text} overlaps ${b.text}`);
    });
    l.bars.forEach((bar) => {
      if (overlaps(box, rectBox(bar.rect))) found.push(`${a.text} overlaps the bar of ${bar.series} in ${bar.group}`);
    });
  });
  return found;
}

test('the scale starts at zero and ends on the first tick at or above the largest value', () => {
  assert.deepEqual(niceScale(92.24), {top: 100, step: 20, ticks: [0, 20, 40, 60, 80, 100]});
  assert.deepEqual(niceScale(46.15), {top: 50, step: 10, ticks: [0, 10, 20, 30, 40, 50]});
  assert.deepEqual(niceScale(100), {top: 100, step: 20, ticks: [0, 20, 40, 60, 80, 100]});
  assert.deepEqual(niceScale(1.2), {top: 1.25, step: 0.25, ticks: [0, 0.25, 0.5, 0.75, 1, 1.25]});
  assert.deepEqual(niceScale(7), {top: 8, step: 2, ticks: [0, 2, 4, 6, 8]});
});

test('a scale over nothing positive is refused', () => {
  assert.throws(() => niceScale(0), /positive/);
  assert.throws(() => niceScale(Number.NaN), /positive/);
});

for (const [name, layout, width] of [
  ['columns', columns, 560],
  ['rows', rows, 360],
  ['rows', rows, 300],
] as const) {
  test(`${name} at ${width}: every label inside the viewBox, clear of every other label and every bar`, () => {
    const l = layout(DATA, width, 'decoded, GB/s');
    assert.equal(l.width, width);
    assert.deepEqual(faults(l), []);
  });

  test(`${name} at ${width}: one bar per value, its length proportional from zero`, () => {
    const l = layout(DATA, width, 'decoded, GB/s');
    const {top} = niceScale(92.24);
    assert.equal(l.bars.length, 8);
    const vertical = name === 'columns';
    const full = vertical ? l.axis.y0 - l.axis.y1 : l.axis.x1 - l.axis.x0;
    for (const bar of l.bars) {
      const v = DATA.values[bar.group][bar.series];
      const length = vertical ? bar.rect.h : bar.rect.w;
      assert.ok(Math.abs(length - (v / top) * full) < 1e-9, `${v} drawn ${length} of ${full}`);
      // Every bar starts on the base line.
      if (vertical) assert.ok(Math.abs(bar.rect.y + bar.rect.h - l.base.y0) < 1e-9);
      else assert.ok(Math.abs(bar.rect.x - l.base.x0) < 1e-9);
      assert.equal(bar.value.text, v.toFixed(2));
    }
  });

  test(`${name} at ${width}: bars in a group keep series order and one pitch`, () => {
    const l = layout(DATA, width, 'decoded, GB/s');
    const along = (b: (typeof l.bars)[number]) => (name === 'columns' ? b.rect.x : b.rect.y);
    for (const g of [0, 1]) {
      const bars = l.bars.filter((b) => b.group === g);
      assert.deepEqual(bars.map((b) => b.series), [0, 1, 2, 3]);
      const pitches = bars.slice(1).map((b, i) => along(b) - along(bars[i]));
      assert.ok(pitches.every((p) => Math.abs(p - pitches[0]) < 1e-9 && p > 0), `pitches ${pitches}`);
    }
  });
}

test('columns: a value label sits S1 above its bar, centred on it', () => {
  const l = columns(DATA, 560, 'decoded, GB/s');
  for (const bar of l.bars) {
    assert.equal(bar.value.anchor, 'middle');
    assert.ok(Math.abs(bar.value.x - (bar.rect.x + bar.rect.w / 2)) < 1e-9);
    assert.ok(Math.abs(labelBox(bar.value).y1 - (bar.rect.y - S1)) < 1e-9);
  }
});

test('rows: a value label sits S1 after its bar, on its centre line', () => {
  const l = rows(DATA, 360, 'decoded, GB/s');
  for (const bar of l.bars) {
    assert.equal(bar.value.anchor, 'start');
    assert.ok(Math.abs(bar.value.x - (bar.rect.x + bar.rect.w + S1)) < 1e-9);
    const box = labelBox(bar.value);
    assert.ok(Math.abs((box.y0 + box.y1) / 2 - (bar.rect.y + bar.rect.h / 2)) < 1e-9);
  }
});

test('columns refuse a width whose bars are narrower than their value labels', () => {
  assert.throws(() => columns(DATA, 300, 'decoded, GB/s'), /too narrow/);
});

test('a ragged value table is refused', () => {
  assert.throws(() => columns({...DATA, values: [[1, 2, 3, 4], [1, 2, 3]]}, 560, 'x'), /one value per series/);
});

test('a negative or non-finite value is refused by both layouts', () => {
  for (const [layout, width] of [[columns, 560], [rows, 360]] as const) {
    for (const bad of [-5, Number.NaN, Number.POSITIVE_INFINITY]) {
      const values = [[45.32, 41.98, 46.15, bad], DATA.values[1]];
      assert.throws(() => layout({...DATA, values}, width, 'decoded, GB/s'), /finite value of at least zero/);
    }
  }
});

test('rows refuse a group label that would end past the viewBox, naming it', () => {
  const groups = ['4 instances on a c8g.16xlarge fleet, one region', '8 instances'];
  assert.throws(
    () => rows({...DATA, groups}, 360, 'decoded throughput, GB/s · higher is better'),
    /too narrow for the group 4 instances on a c8g\.16xlarge fleet, one region/,
  );
});

test('columns and rows refuse an axis label that would end past the viewBox, naming it', () => {
  const axis = 'decoded throughput across the whole fleet of instances in one region, GB/s · higher is better';
  for (const [layout, w] of [[columns, 560], [rows, 360]] as const) {
    assert.throws(() => layout(DATA, w, axis), /too narrow for the axis label decoded throughput across the whole fleet/);
  }
});

test('columns: bars in a group stand S1 apart', () => {
  const l = columns(DATA, 560, 'decoded, GB/s');
  for (const g of [0, 1]) {
    const bars = l.bars.filter((b) => b.group === g);
    bars.slice(1).forEach((b, i) => assert.ok(Math.abs(b.rect.x - (bars[i].rect.x + bars[i].rect.w) - S1) < 1e-9));
  }
});

test('a value table with a row per group missing or extra is refused', () => {
  for (const layout of [columns, rows]) {
    assert.throws(() => layout({...DATA, groups: ['4 instances']}, 560, 'x'), /one value row per group/);
    assert.throws(() => layout({...DATA, groups: ['a', 'b', 'c']}, 560, 'x'), /one value row per group/);
  }
});

test('rows refuse a width whose tick labels would touch', () => {
  assert.throws(() => rows(DATA, 150, 'decoded, GB/s'), /tick labels/);
});

test('the top tick is the max when the max sits on a tick, at any magnitude', () => {
  assert.equal(niceScale(0.00001).ticks.length, 6);
  assert.equal(niceScale(0.00001).top, 0.00001);
});

test('columns refuse a group label wider than its group', () => {
  assert.throws(() => columns({...DATA, groups: ['4 instances on a very long group label', '8']}, 560, 'x'), /too narrow for the group 4 instances on a very long group label/);
});

/** Four groups of three bars, each value label wider than a 560-unit column chart's bars. */
const SWEEP: ChartData = {
  groups: ['2 I/O workers', '4', '8', '16'],
  series: ['lz4', 'zstd:1', 'zstd:3'],
  values: [
    [31.78, 31.6, 32.09],
    [32.69, 32.47, 32.26],
    [32.51, 32.91, 32.65],
    [32.76, 32.98, 32.47],
  ],
  decimals: 2,
};

test('the landscape drawing stands bars over the axis when every value label fits over its bar', () => {
  const {layout, vertical} = landscape(DATA, 560, 'decoded, GB/s');
  assert.equal(vertical, true);
  assert.deepEqual(layout, columns(DATA, 560, 'decoded, GB/s'));
});

test('the landscape drawing lays bars along the axis when a value label is wider than its bar', () => {
  assert.throws(() => columns(SWEEP, 560, 'insert throughput, M rows/s'), /too narrow for the value/);
  const {layout, vertical} = landscape(SWEEP, 560, 'insert throughput, M rows/s');
  assert.equal(vertical, false);
  assert.deepEqual(layout, rows(SWEEP, 560, 'insert throughput, M rows/s'));
  assert.equal(layout.bars.length, 12);
  assert.deepEqual(faults(layout), []);
});

test('the landscape drawing refuses what neither drawing can set', () => {
  assert.throws(() => landscape({...SWEEP, groups: ['a group label long enough to run past the end of a 560-unit drawing in either layout', '4', '8', '16']}, 560, 'x'), /too narrow for the group/);
});

test('the landscape drawing lays bars along the axis when a group label is wider than its group', () => {
  const wideGroup: ChartData = {groups: ['2 I/O workers, default', '4', '8', '16'], series: ['a', 'b'], values: [[1, 2], [3, 4], [5, 6], [7, 8]], decimals: 0};
  assert.throws(() => columns(wideGroup, 560, 'x'), /too narrow for the group/);
  const {layout, vertical} = landscape(wideGroup, 560, 'x');
  assert.equal(vertical, false);
  assert.deepEqual(layout, rows(wideGroup, 560, 'x'));
});
