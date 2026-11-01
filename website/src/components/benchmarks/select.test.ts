import assert from 'node:assert/strict';
import {test} from 'node:test';
import {PRIMARY, type Entrant, type Row} from '../Results/data.ts';
import {vendorBestRow} from './select.ts';

const entrant = (id: string, vendor = 'other'): Entrant => ({
  entrant: {id, vendor, name: id, status: 'active', runtime: 'native', licence: 'Apache-2.0'},
});
const row = (id: string, value: number, overrides: Partial<Row> = {}): Row => ({
  key: `${id}-${value}`, entrant: id, group: 'wide', sitting: 'run', variant_id: 'default',
  version: '1.0', commit: null, env_id: 'host', harness_version: 2, dataset_version: 'corpus',
  ts_ms: 1000, status: 'ok', approach: 'realistic', wire_format: 'avro', reps_counted: 3, flags: [],
  metrics: {[PRIMARY]: {value, unit: 'records/s', higher_is_better: true, n: 3,
    lo: value * 0.9, hi: value * 1.1, values: [value * 0.9, value, value * 1.1], spread: 0.2}},
  ...overrides,
});
const entrants = [entrant('vendor', 'self'), entrant('competitor')];

test('the vendor arm is its fastest variant when a slower one ran later', () => {
  const fast = row('vendor', 1606, {variant_id: 'fast', ts_ms: 1000});
  const rows = [fast, row('vendor', 1495, {variant_id: 'slow', ts_ms: 2000}), row('competitor', 900)];
  assert.equal(vendorBestRow(rows, entrants), fast);
});

test('a newer vendor row in a smaller group does not displace the richest group', () => {
  const displayed = row('vendor', 100);
  const rows = [displayed, row('competitor', 150), row('competitor', 120, {variant_id: 'other'}),
    row('vendor', 900, {group: 'narrow', ts_ms: 9000})];
  assert.equal(vendorBestRow(rows, entrants), displayed);
});
