// Tests for the column catalogue and the facets.
//
// The entrant ids here are fictional — `alpha`, `beta`, `gamma` — because
// `plugins/neutrality.test.js` scans `.test.ts` files too, and a test naming a
// real system would fail the lint that makes this site's neutrality claim
// checkable. That is the right trade: these tests are about placement rules and
// derivation, and neither has anything to do with who is in the table.
//
// `./columns.ts` is imported with its extension because `node --test` strips
// types but does not resolve the bundler's extensionless imports.

import assert from 'node:assert/strict';
import {test} from 'node:test';

import {
  CATALOGUE,
  columnsFor,
  defaultColumnsFor,
  detailFor,
  facetValuesOf,
  facetsOf,
  humaniseMetric,
  placementOf,
  showClassesFor,
  specOf,
  type FacetSource,
} from './columns.ts';
import {isPlotted, unrankedBecause, type Row} from './data.ts';
import {showClassOf} from './model.ts';

const ALL = CATALOGUE.map((m) => m.id);

/**
 * A row carrying only the two fields the contract predicates read.
 *
 * Cast rather than filled out: `Row` is thirteen fields of provenance, and
 * spelling all of them would say that these tests depend on them.
 */
const armWith = (status: string, approach: string) =>
  ({status, approach}) as unknown as Row;

test('a metric the catalogue does not know is detail, never a column', () => {
  assert.equal(placementOf('some_metric_the_harness_added_later_us'), 'detail');
  assert.deepEqual(
    columnsFor(['rows_per_s', 'some_metric_the_harness_added_later_us']),
    columnsFor(['rows_per_s']),
    'an unknown metric must not become a switchable column on its own',
  );
  const detail = detailFor(['some_metric_the_harness_added_later_us']);
  assert.equal(detail.length, 1, 'but it must still reach the reader somewhere');
  assert.equal(detail[0].id, 'some_metric_the_harness_added_later_us');
});

test('an unknown metric gets a tidied label and no invented gloss', () => {
  const spec = specOf('gc_pause_p42_us');
  assert.equal(spec.label, 'Gc pause p42');
  assert.equal(spec.gloss, '', 'the site must not assert meaning it does not have');
  assert.equal(humaniseMetric('peak_charged_bytes'), 'Peak charged');
});

test('columns are restricted to metrics that were actually measured', () => {
  const cols = columnsFor(['rows_per_s', 'cores_used']);
  assert.deepEqual(
    cols.map((c) => c.id),
    ['rows_per_s', 'cores_used'],
    'a column no arm carries would be a column of "not measured"',
  );
  assert.deepEqual(columnsFor([]), []);
});

test('columns keep catalogue order regardless of the order metrics arrive in', () => {
  const forward = columnsFor(['rows_per_s', 'cpu_us_per_row', 'peak_anon_bytes']);
  const reverse = columnsFor(['peak_anon_bytes', 'cpu_us_per_row', 'rows_per_s']);
  assert.deepEqual(
    forward.map((c) => c.id),
    reverse.map((c) => c.id),
  );
});

test('the default columns are on, and are a subset of the switchable ones', () => {
  const defaults = defaultColumnsFor(ALL);
  const switchable = new Set(columnsFor(ALL).map((c) => c.id));
  assert.ok(defaults.length > 0);
  for (const id of defaults) assert.ok(switchable.has(id), `${id} must be a column`);
  assert.ok(
    defaults.length < switchable.size,
    'if every column were on by default the picker would be pointless',
  );
});

test('throughput per core leads the columns', () => {
  assert.equal(columnsFor(ALL)[0].id, 'rows_per_s_per_core');
});

// `rows_per_s_per_core` is `rows_per_s / cores_used`, which is `1e6 * rows /
// cpu_us` once the sampler's window cancels — exactly `1e6 / cpu_us_per_row`.
// The identity holds by construction in the harness and is invisible here, so
// nothing in the catalogue stops the two being turned on together. On by
// default they would put the lead figure in the table twice, side by side,
// where a reader has every reason to read the pair as corroboration.
test('the lead figure and its reciprocal are never both on by default', () => {
  const on = new Set(defaultColumnsFor(ALL));
  assert.ok(
    !(on.has('rows_per_s_per_core') && on.has('cpu_us_per_row')),
    'cpu_us_per_row is 1e6 / rows_per_s_per_core, so showing both by default ' +
      'is showing one measurement twice — put whichever does not lead in `available`',
  );
});

test('detail metrics never overlap the switchable columns', () => {
  const cols = new Set(columnsFor(ALL).map((c) => c.id));
  for (const d of detailFor(ALL)) {
    assert.ok(!cols.has(d.id), `${d.id} cannot be both a column and detail-only`);
  }
});

test('unknown detail metrics sort after the declared ones, stably', () => {
  const a = detailFor(['zzz_unknown', 'ch_cpu_us', 'aaa_unknown']).map((d) => d.id);
  const b = detailFor(['aaa_unknown', 'ch_cpu_us', 'zzz_unknown']).map((d) => d.id);
  assert.deepEqual(a, b, 'a new counter needs a stable place, not the JSON order');
  assert.equal(a[0], 'ch_cpu_us');
  assert.deepEqual(a.slice(1), ['aaa_unknown', 'zzz_unknown']);
});

// ---------------------------------------------------------------------------
// Facets
// ---------------------------------------------------------------------------

const entrant = (
  id: string,
  runtime: string,
  licence: string,
  delivery?: string,
): FacetSource => ({
  entrant: {id, runtime, licence, kind: 'stream-processor'},
  guarantees: delivery ? {delivery} : undefined,
});

test('a facet whose values are all the same is not offered', () => {
  const facets = facetsOf([
    entrant('alpha', 'native', 'Apache-2.0'),
    entrant('beta', 'native', 'Apache-2.0'),
  ]);
  assert.deepEqual(facets, [], 'a control that cannot change the page is furniture');
});

test('a facet appears as soon as its field genuinely varies', () => {
  const facets = facetsOf([
    entrant('alpha', 'native', 'Apache-2.0'),
    entrant('beta', 'jvm', 'Apache-2.0'),
  ]);
  assert.deepEqual(
    facets.map((f) => f.id),
    ['runtime'],
    'licence and kind are still uniform, so only runtime is worth offering',
  );
  assert.deepEqual(
    facets[0].options.map((o) => o.value).sort(),
    ['jvm', 'native'],
  );
});

test('facet options are counted, and ordered by how many carry them', () => {
  const facets = facetsOf([
    entrant('alpha', 'native', 'Apache-2.0'),
    entrant('beta', 'jvm', 'Apache-2.0'),
    entrant('gamma', 'jvm', 'MPL-2.0'),
  ]);
  const runtime = facets.find((f) => f.id === 'runtime');
  assert.ok(runtime);
  assert.deepEqual(runtime.options, [
    {value: 'jvm', count: 2},
    {value: 'native', count: 1},
  ]);
});

test('a facet built from a missing field simply does not appear', () => {
  const facets = facetsOf([
    entrant('alpha', 'native', 'Apache-2.0'),
    entrant('beta', 'jvm', 'Apache-2.0'),
  ]);
  assert.ok(
    !facets.some((f) => f.id === 'delivery'),
    'no entrant declared a guarantee, so there is nothing to filter by',
  );
});

test('the facet values written into the markup match the facets offered', () => {
  const e = entrant('alpha', 'native', 'Apache-2.0', 'at-least-once');
  assert.deepEqual(facetValuesOf(e), {
    runtime: 'native',
    kind: 'stream-processor',
    licence: 'Apache-2.0',
    delivery: 'at-least-once',
  });
});

// ---------------------------------------------------------------------------
// Show classes — rule 3
// ---------------------------------------------------------------------------

test('rule 3: realistic is the only class on by default', () => {
  const classes = showClassesFor(['realistic', 'stripped', 'tuned']);
  assert.deepEqual(
    classes.filter((c) => c.on).map((c) => c.id),
    ['realistic'],
    'the contract says the site defaults every chart to realistic',
  );
});

test('show classes keep their documented order and only offer what is present', () => {
  const classes = showClassesFor(['tuned', 'realistic', 'stripped']);
  assert.deepEqual(
    classes.map((c) => c.id),
    ['realistic', 'tuned', 'stripped'],
  );
  assert.deepEqual(
    showClassesFor(['realistic']).map((c) => c.id),
    ['realistic'],
    'a class nothing in the data carries is not worth a checkbox',
  );
});

/**
 * The defect: rule 3 hid a realistic arm.
 *
 * `showClassOf` used to answer with `unrankedBecause`, which puts `infra-bound`
 * ahead of the approach — right for ranking, and catastrophic for visibility,
 * because rule 3 leaves every class but `realistic` unticked. `spate:rowbinary`
 * was rendered into the HTML with its digits, its chip and its void lanes, and
 * then hidden from every reader with scripting on.
 *
 * Rule 3 is about which CONFIGURATIONS a reader asked to see. Whether a number
 * was disowned is a different question, and the legend already answers it: an
 * infra-bound arm "keeps its digits and its reason but not its position".
 */
test('an infra-bound arm is filtered by its approach, so rule 3 cannot hide it', () => {
  const infraBound = armWith('infra_bound', 'realistic');
  assert.equal(showClassOf(infraBound), 'realistic');

  const classes = showClassesFor([showClassOf(infraBound)]);
  assert.deepEqual(
    classes.filter((c) => c.on).map((c) => c.id),
    ['realistic'],
    'the row passes the default Show state like any other realistic arm',
  );

  // It is still unranked and still unplotted — what it loses is its position.
  assert.equal(unrankedBecause(infraBound), 'infra-bound');
  assert.equal(isPlotted(infraBound), false);

  // And an arm that is BOTH stripped and infra-bound is filtered as stripped:
  // that one really is a configuration a reader did not ask for.
  assert.equal(showClassOf(armWith('infra_bound', 'stripped')), 'stripped');
});

test('a class the contract gains later still reaches the control', () => {
  const classes = showClassesFor(['realistic', 'undeclared', 'something-new']);
  const ids = classes.map((c) => c.id);
  assert.deepEqual(ids, ['realistic', 'something-new', 'undeclared']);
  assert.ok(
    classes.every((c) => c.gloss.length > 0),
    'every class must describe itself, including ones this site has never seen',
  );
  assert.deepEqual(
    classes.filter((c) => c.on).map((c) => c.id),
    ['realistic'],
    'and it must not be shown by default merely for being unrecognised',
  );
});
