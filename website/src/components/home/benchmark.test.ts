import assert from 'node:assert/strict';
import {test} from 'node:test';
import {PRIMARY, type Entrant, type Row} from '../Results/data.ts';
import {headlineLanes, vendorLane} from './benchmark.ts';

const entrant = (id: string, vendor = 'other'): Entrant => ({
  entrant: {id, vendor, name: id, status: 'active', runtime: 'native', licence: 'Apache-2.0'},
});
const row = (id: string, value: number, overrides: Partial<Row> = {}): Row => ({
  key: `${id}-${value}`, entrant: id, group: 'displayed', sitting: 'run', variant_id: 'default',
  version: '1.0', commit: null, env_id: 'host', harness_version: 2, dataset_version: 'corpus',
  ts_ms: 1000, status: 'ok', approach: 'realistic', wire_format: 'avro', reps_counted: 3, flags: [],
  metrics: {[PRIMARY]: {value, unit: 'records/s', higher_is_better: true, n: 3,
    lo: value * 0.9, hi: value * 1.1, values: [value * 0.9, value, value * 1.1], spread: 0.2}},
  ...overrides,
});

test('summary uses the same best eligible configuration as the chart', () => {
  const best = row('renamed-vendor', 200);
  const rows = [row('renamed-vendor', 100), best, row('competitor', 150),
    row('renamed-vendor', 900, {status: 'infra_bound'}),
    row('renamed-vendor', 800, {approach: 'tuned'}),
    row('renamed-vendor', 700, {approach: 'stripped'}),
    row('renamed-vendor', 600, {metrics: {}})];
  const original = structuredClone(rows);
  const lanes = headlineLanes(rows);
  assert.deepEqual(lanes.map((r) => r.metrics[PRIMARY].value), [200, 150]);
  assert.equal(vendorLane(lanes, [entrant('renamed-vendor', 'self'), entrant('competitor')]), best);
  assert.deepEqual(rows, original);
});

test('vendor metadata determines the summary independently of names and entrant ids', () => {
  const rows = headlineLanes([row('former-vendor', 300), row('another-id', 100)]);
  assert.equal(vendorLane(rows, [entrant('former-vendor'), entrant('another-id', 'self')]), rows[1]);
  assert.equal(vendorLane(rows, [entrant('former-vendor'), entrant('another-id')]), undefined);
});

test('no eligible vendor in the displayed group leaves comparison lanes intact', () => {
  const competitor = row('competitor', 150);
  const rows = [competitor, row('vendor', 900, {status: 'infra_bound'}),
    row('vendor', 800, {group: 'another-group'})];
  const lanes = headlineLanes(rows.filter((r) => r.group === 'displayed'));
  assert.deepEqual(lanes, [competitor]);
  assert.equal(vendorLane(lanes, [entrant('vendor', 'self'), entrant('competitor')]), undefined);
  assert.equal(vendorLane([], [entrant('vendor', 'self')]), undefined);
  assert.deepEqual(headlineLanes([]), []);
});

test('selection respects metric direction and retains the first equal result', () => {
  const rows = [row('vendor', 200), row('vendor', 100), row('vendor', 100)];
  for (const r of rows) r.metrics[PRIMARY].higher_is_better = false;
  assert.equal(headlineLanes(rows)[0], rows[1]);
});
