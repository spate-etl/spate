// What this file is for.
//
// The results explorer can hide rows, and a row that is wrongly hidden leaves no
// trace on the page — unlike a wrong number, which a reader can check against the
// archive. So the three rules that decide what stays visible, and what the axis
// then means, are the part of this surface a reader cannot audit for themselves.
//
// Two of them are also the rules the browser and the build have to agree on. If
// `niceCeil` here disagreed with the one the prerender used, every mark would
// shift the moment JavaScript loaded and a reader would see the numbers move.
//
// Run with `npm test`.

import assert from 'node:assert/strict';
import {test} from 'node:test';

// The `.ts` extension is required by `node --test`, which resolves this path
// itself rather than through the bundler. `allowImportingTsExtensions` in
// tsconfig.json exists for this import and no other.
import {compareLanes, niceCeil, visibleMax} from './filter.ts';

// Fictional ids, matching the plugin's own fixtures. `plugins/neutrality.test.js`
// forbids naming a real entrant anywhere under `src/`, and it is right to: a rule
// stated in terms of one system is a rule that has already stopped being general.
const filter = (over = {}) => ({
  systems: new Set(['alpha', 'beta']),
  showUnranked: true,
  ...over,
});

test('a disowned number never sets the axis', () => {
  // The infra-bound arm is the fastest figure in the column and is shown without
  // a position. Letting it stretch the axis would let a number this project has
  // disowned decide what every other mark's position means.
  const max = visibleMax([
    {hi: 4_000_000, plotted: true, visible: true},
    {hi: 9_000_000, plotted: false, visible: true},
  ]);
  assert.equal(max, niceCeil(4_000_000));
  assert.ok(max < 9_000_000);
});

test('hiding the fastest arm rescales the axis to what is left', () => {
  const cells = [
    {hi: 8_000_000, plotted: true, visible: true},
    {hi: 1_800_000, plotted: true, visible: true},
  ];
  const before = visibleMax(cells);
  const after = visibleMax([{...cells[0], visible: false}, cells[1]]);
  assert.equal(before, niceCeil(8_000_000));
  assert.equal(after, niceCeil(1_800_000));
  assert.ok(after < before, 'the point of rescaling is that the survivors grow');
});

test('a column filtered to nothing collapses rather than dividing by zero', () => {
  // Every mark divides by this. Returning 0 would put `Infinity` into a CSS
  // calc and blank the column with no indication of why.
  assert.equal(visibleMax([]), 1);
  assert.equal(visibleMax([{hi: 5, plotted: true, visible: false}]), 1);
});

test('the axis rounds up, never down, and never below the value it must contain', () => {
  // A maximum below the largest value would push a mark past the end of its
  // track, which is the one direction an axis may not err in.
  for (const v of [1, 1.2, 2.4, 3.1, 7.9, 40_562, 1_774_998, 27_553_967]) {
    assert.ok(niceCeil(v) >= v, `${v} -> ${niceCeil(v)}`);
  }
  assert.equal(niceCeil(0), 1, 'a zero maximum is still a usable divisor');
  assert.equal(niceCeil(-5), 1);
  assert.equal(niceCeil(Number.POSITIVE_INFINITY), 1);
});

const lane = (value: number | null, ranked = true, index = 0) => ({value, ranked, index});

test('sorting never lifts a non-headline arm above an eligible one', () => {
  // The `stripped` arm in the fixture archive is the fastest thing on its axis.
  // Rule 3 is not a preference a control may override, so no ordering this page
  // offers may put it first.
  const stripped = lane(9_000_000, false);
  const eligible = lane(4_000_000, true);
  assert.ok(compareLanes(eligible, stripped, true) < 0);
  assert.ok(compareLanes(stripped, eligible, true) > 0);
});

test('direction comes from the metric, not from the control', () => {
  const cheap = lane(0.68);
  const dear = lane(1.11);
  // Lower CPU per row is better, so the cheap arm leads.
  assert.ok(compareLanes(cheap, dear, false) < 0);
  // Higher throughput is better, so on that axis the larger figure leads.
  assert.ok(compareLanes(dear, cheap, true) < 0);
});

test('an arm that did not measure the metric sorts last, not first', () => {
  // Treating a missing value as zero would win every lower-is-better column,
  // which is the one direction this must not fail in.
  const measured = lane(1.11);
  const missing = lane(null);
  assert.ok(compareLanes(measured, missing, false) < 0);
  assert.ok(compareLanes(missing, measured, false) > 0);
  assert.ok(compareLanes(measured, missing, true) < 0);
});

test('equal values fall back to the descriptor order rather than to chance', () => {
  assert.ok(compareLanes(lane(5, true, 0), lane(5, true, 1), true) < 0);
  assert.equal(compareLanes(lane(null, true, 2), lane(null, true, 2), true), 0);
});
