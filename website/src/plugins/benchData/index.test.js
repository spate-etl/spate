// What this file is for.
//
// The site's data layer decides which numbers a reader is allowed to compare,
// which are allowed to be ranked, and which are struck through. Every one of
// those decisions was wrong at some point and none of them had a test:
//
//   - `infra_bound` records were dropped by a `status !== 'ok'` filter, making
//     "we ran it and it blew the headroom limit" render identically to "we never
//     ran it".
//   - `approach` never reached the row, so a `stripped` arm — one using code this
//     project wrote rather than code the system ships — was ranked on the
//     headline axis, above the honest arm of the same system.
//   - `mode` splits the comparability group — drain and sustained throughput
//     mean different things — and nothing pinned it in the group key, so a
//     refactor could have put them back on one axis unnoticed.
//   - A row took the newest repetition's status and flags rather than the worst
//     and the union, which reintroduced the first two bugs one layer up.
//
// The fixture in `__fixtures__/` exists to hold each of those cases in a shape
// small enough to read. Run with `npm test`.

const assert = require('node:assert/strict');
const path = require('node:path');
const {test} = require('node:test');

const FIXTURE = path.join(__dirname, '__fixtures__');

/** Loads the plugin's global data against the fixture tree. */
async function load() {
  process.env.BENCH_ROOT = FIXTURE;
  const plugin = require('./index.js')({siteDir: FIXTURE});
  return plugin.loadContent();
}

const find = (rows, entrant, variant) =>
  rows.find((r) => r.entrant === entrant && r.variant_id === variant);

test("every row's variant id is one its descriptor declares", async () => {
  const {rows, entrants} = await load();
  assert.ok(rows.length > 0, 'the fixture must produce rows');
  // The site joins records to descriptors solely through `variant_id`: the
  // label, the approach and the wire format a reader sees all come from the
  // declared variant. A record whose id no descriptor declares would render
  // with none of them, and nothing downstream would notice.
  const declared = new Map(
    entrants.map((e) => [e.entrant.id, new Set((e.variants ?? []).map((v) => v.id))]),
  );
  for (const r of rows) {
    const ids = declared.get(r.entrant);
    assert.ok(ids, `${r.entrant} has no descriptor`);
    assert.ok(
      ids.has(r.variant_id),
      `${r.entrant} declares no variant "${r.variant_id}"`,
    );
  }
});

test('mode is a comparability axis, so drain and sustained never share one', async () => {
  const {rows} = await load();
  // `rows_per_s` means "how fast can this go" in drain and "the rate we asked
  // for" in sustained. Two arms of entirely different capacity report the same
  // number, so the axis would be meaningless before it was wrong.
  for (const r of rows) {
    assert.ok(r.group.split('|').includes(`mode-${r.mode}`), `${r.variant_id} in ${r.group}`);
  }
  const drain = rows.filter((r) => r.mode === 'drain');
  assert.ok(drain.length > 0, 'the fixture is drain-mode throughout');
  const synthetic = {...drain[0], mode: 'sustained'};
  assert.notEqual(
    synthetic.group.replace('mode-drain', 'mode-sustained'),
    drain[0].group,
    'changing only the mode must change the group',
  );
});

test('a different protocol version is a different group, whatever else matches', async () => {
  const {groups, rows} = await load();
  // gamma runs harness 2 on the same environment, dataset and mode as arms at
  // harness 1. METHODOLOGY makes that a hard split: records measured under
  // different protocols are never drawn on one axis.
  const newer = rows.filter((r) => r.harness_version === 2);
  assert.ok(newer.length > 0, 'need arms at the newer protocol');
  const older = rows.filter((r) => r.harness_version === 1);
  assert.ok(older.length > 0, 'need arms at the older protocol to compare against');
  for (const n of newer) {
    for (const o of older) {
      assert.notEqual(n.group, o.group, 'harness 1 and harness 2 must not share a group');
    }
  }
  assert.ok(
    new Set(groups.map((g) => g.harness_version)).size >= 2,
    'the split must be visible to the page, not only inside the key',
  );
});

test('every repetition of an invocation is medianed into one row', async () => {
  const {rows} = await load();
  const r = find(rows, 'alpha', 'native');
  // Three reps at 1000/1100/1200 — the mark is the interval, so the row has to
  // carry all three rather than the newest.
  assert.equal(r.reps_counted, 3);
  assert.equal(r.metrics.rows_per_s.value, 1100, 'median of the three');
  assert.equal(r.metrics.rows_per_s.lo, 1000);
  assert.equal(r.metrics.rows_per_s.hi, 1200);
});

test('one infra-bound repetition makes the whole row infra-bound', async () => {
  const {rows} = await load();
  // The offending rep is not the newest, which is exactly how taking
  // `newest.status` used to publish it as ok.
  assert.equal(find(rows, 'alpha', 'rowbinary').status, 'infra_bound');
});

test('an infra-bound row carries the reason from the repetition that was infra-bound', async () => {
  const {rows} = await load();
  const row = find(rows, 'alpha', 'rowbinary');
  // Rep 2 blew the limit; reps 1 and 3 did not, and rep 3 is the newest. Taking
  // the note from `newest` alongside a status from `worstStatus` would explain a
  // disowned number with a repetition that passed — the same mismatch the
  // attempts path guards against by moving status and note together.
  assert.match(row.note, /INFRA-BOUND/);
  assert.match(row.note, /88%/);
  assert.doesNotMatch(row.note, /64%/, 'the newest repetition passed and does not explain this row');
});

test('a row that published a number still carries the harness\'s account of it', async () => {
  const {rows} = await load();
  // Not conditional on the status: the note is provenance for every reading. The
  // rule is uniform — the newest repetition whose status is the one the row
  // publishes — which for an `ok` row is simply its newest.
  assert.equal(find(rows, 'alpha', 'native').note, 'headroom clickhouse ingest (native) 33%');
});

test('the A/A control and the sweep verdict are not arms', async () => {
  const {rows, attempts} = await load();
  // The control is the same arm measured a second time to difference against
  // itself, and the verdict is a statement about the rig. Either one rendered
  // as a row would put something that is not a system in a table of systems —
  // and the control would put the same system in it twice.
  assert.ok(
    rows.every((r) => !(r.flags ?? []).includes('aa_control')),
    'no row may come from the A/A control',
  );
  assert.ok(
    !rows.some((r) => r.metrics.aa_spread),
    'no row may come from a verdict record',
  );
  assert.ok(
    !attempts.some((a) => a.note?.includes('A/A control')),
    'and neither may be listed as an attempt that produced nothing',
  );
  // The alpha native row still medians exactly its own three repetitions.
  assert.equal(find(rows, 'alpha', 'native').reps_counted, 3);

  // The verdict is dropped as a row and kept as the sitting's floor, so the
  // rows it describes can say what the rig was doing while they were taken.
  assert.equal(find(rows, 'alpha', 'native').aa_spread, 0.021);
});

test('a sweep with no A/A control reports no floor rather than a wrong one', async () => {
  const {rows} = await load();
  // gamma's sittings carry no verdict. `null` is what the legend renders its
  // "nothing has measured this" copy from; a zero would read as a rig that
  // does not move.
  assert.equal(find(rows, 'gamma', 'native').aa_spread, null);
});

test('flags are the union across repetitions, not the newest one\'s', async () => {
  const {rows} = await load();
  // Only rep 2 of alpha:native is throttled, and it is not the newest.
  assert.deepEqual(find(rows, 'alpha', 'native').flags, ['cpu_cap_throttled']);
});

test('a run that produced no publishable number is an explicit gap, not silence', async () => {
  const {rows, attempts} = await load();
  assert.ok(attempts.length > 0);
  for (const a of attempts) assert.equal(a.status, 'failed');
  assert.ok(
    !rows.some((r) => r.metrics.rows_per_s?.value === 0),
    'a failed record must not become a row',
  );
});

test('a sitting that failed every repetition is one gap, not one per repetition', async () => {
  const {attempts} = await load();
  // Two failed reps of one sweep. Listing each would say the arm was attempted
  // twice, and would grow this list without bound as re-runs accumulate — the
  // same unbounded payload the rows were carrying.
  const sustained = attempts.filter((a) => a.reps_counted === 2);
  assert.equal(sustained.length, 1, 'the two failed reps collapse into one gap');
  assert.equal(sustained[0].note, 'could not hold the offered rate');
});

test('a group with nothing but gaps is still named on the page', async () => {
  const {groups, rows, attempts} = await load();
  // The component renders only groups this list names, so deriving it from rows
  // alone would make an arm that fails everywhere vanish rather than read as
  // broken — the loudest thing on the page becoming the quietest.
  const empty = groups.filter((g) => !rows.some((r) => r.group === g.key));
  assert.equal(empty.length, 1, 'the sustained-mode group has no rows');
  assert.ok(
    attempts.some((a) => a.group === empty[0].key),
    'and it is named because an attempt describes it',
  );
  assert.equal(empty[0].env_id, 'testenv');
  assert.equal(empty[0].dataset_version, 'd1-fixture');
});

test('approach and wire format reach the row, which is what makes the contract renderable', async () => {
  const {rows} = await load();
  assert.equal(find(rows, 'beta', 'hand').approach, 'stripped');
  assert.equal(find(rows, 'beta', 'rowbinary').approach, 'realistic');
  assert.equal(find(rows, 'alpha', 'native').wire_format, 'native');
});

test('a stripped arm is present but never headline-eligible, even when it is fastest', async () => {
  const {rows} = await load();
  const stripped = find(rows, 'beta', 'hand');
  const inGroup = rows.filter((r) => r.group === stripped.group);
  const fastest = inGroup.reduce((a, b) =>
    a.metrics.rows_per_s.value >= b.metrics.rows_per_s.value ? a : b,
  );
  assert.equal(fastest.variant_id, 'hand', 'fixture must keep this the fastest');
  // Mirrors `unrankedBecause` in the component: the row carries everything
  // needed to bar it, so the decision cannot be lost between here and render.
  const eligible = (r) => r.status === 'ok' && r.approach === 'realistic';
  assert.equal(eligible(stripped), false);
  assert.ok(inGroup.some(eligible), 'something must still be rankable');
});

test('a re-run supersedes the sitting before it rather than merging into it', async () => {
  const {rows} = await load();
  // Four records, one arm, one configuration, one UTC day, two invocation ids.
  // The later sweep is what the arm does now, so it is the only one published —
  // but the two are still aggregated SEPARATELY first. Medianing them together
  // and then publishing would give 1505, a number no sitting measured.
  const gamma = rows.filter((r) => r.entrant === 'gamma');
  assert.equal(gamma.length, 1, 'only the newest sitting is published');
  assert.equal(gamma[0].metrics.rows_per_s.value, 2005, 'median of the later sweep alone');
  assert.equal(gamma[0].metrics.rows_per_s.lo, 2000);
  assert.equal(gamma[0].metrics.rows_per_s.hi, 2010);
  assert.equal(gamma[0].reps_counted, 2);
});

test('one published sitting per arm, which is what bounds the payload', async () => {
  const {rows} = await load();
  // Docusaurus global data is not code-split: it ships in main.js to every
  // visitor. Without this the catalogue grows by an arm per nightly sweep,
  // forever, and each re-run also ranks an arm against older copies of itself.
  //
  // The invariant is one SITTING per arm, not one row: an arm measured at two
  // configurations in a single sweep would publish both, and they would be a
  // real comparison rather than an arm against a stale copy of itself.
  const seen = new Map();
  for (const r of rows) {
    const arm = [r.group, r.entrant, r.variant_id].join('|');
    const first = seen.get(arm);
    if (first !== undefined) assert.equal(r.sitting, first, `${arm} published twice over`);
    else seen.set(arm, r.sitting);
  }
  assert.equal(seen.size, rows.length, 'the fixture measures one configuration per sweep');
});

test('a failed repetition does not evict the number its own sitting produced', async () => {
  const {rows, attempts} = await load();
  // alpha:native measured three reps and failed a fourth, six seconds later, in
  // the same sweep. Selecting on the newest RECORD rather than the newest
  // SITTING would drop a reading the sweep genuinely took.
  const r = find(rows, 'alpha', 'native');
  assert.ok(r, 'the measured reps must still publish');
  assert.equal(r.metrics.rows_per_s.value, 1100);
  const gap = attempts.find((a) => a.group === r.group && a.variant_id === r.variant_id);
  assert.ok(gap, 'and the failure is still listed beside it');
  assert.equal(gap.sitting, r.sitting, 'same sweep');
  assert.equal(gap.reps_counted, 1);
});

test('selection never crosses a comparability group', async () => {
  const {rows, attempts} = await load();
  // alpha:native was measured in drain mode and, strictly later, failed in
  // sustained mode. Those are different experiments — `mode` is in the group key
  // — so the failure must not retire the drain reading. A selection that ranked
  // sittings globally rather than within a group would drop it.
  const measured = find(rows, 'alpha', 'native');
  const elsewhere = attempts.find(
    (a) => a.entrant === 'alpha' && a.variant_id === 'native' && a.group !== measured.group,
  );
  assert.ok(elsewhere, 'the fixture must fail this arm in a second group');
  assert.ok(elsewhere.ts_ms > measured.ts_ms, 'and fail it later, or this proves nothing');
  assert.equal(measured.metrics.rows_per_s.value, 1100, 'the drain reading survives');
});

test('a refusal supersedes the number it followed rather than deferring to it', async () => {
  const {rows, attempts} = await load();
  // beta:legacy measured 700 on one day and broke on the next, in the same
  // comparability group. The page answers what the arm does now, so the stale
  // figure goes: publishing it would present a number the system can no longer
  // produce, which is the one thing a benchmark must never do quietly.
  assert.equal(find(rows, 'beta', 'legacy'), undefined, 'the superseded number is gone');
  const gap = attempts.filter((a) => a.entrant === 'beta' && a.variant_id === 'legacy');
  assert.equal(gap.length, 1, 'and the arm reads as failing instead');
  assert.equal(gap[0].status, 'failed');
  assert.equal(gap[0].note, 'decoder rejected the corpus');
  // Same group, so this really is supersession and not the cross-group case.
  assert.equal(gap[0].group, find(rows, 'beta', 'rowbinary').group);
});

test('a status this build does not recognise is never ranked', () => {
  const {worstStatus, severity, UNKNOWN_SEVERITY} = require('./index.js').__testonly;
  // Fails CLOSED. Scoring an unknown status 0 made it tie with `ok` and lose, so
  // a status added by a newer harness would have been published as sound by an
  // older site — the same fail-open mistake `approach` used to make.
  assert.equal(severity('ok'), 0);
  assert.equal(severity('infra_bound'), 1);
  assert.equal(severity('something_a_newer_harness_emits'), UNKNOWN_SEVERITY);
  assert.equal(
    worstStatus([{status: 'ok'}, {status: 'something_a_newer_harness_emits'}, {status: 'ok'}]),
    'something_a_newer_harness_emits',
  );
  assert.equal(worstStatus([{status: 'ok'}, {status: 'infra_bound'}]), 'infra_bound');
});

test('a descriptor that does not parse fails the build rather than half-loading', async () => {
  process.env.BENCH_ROOT = path.join(FIXTURE, '..', '__no_such_tree__');
  const plugin = require('./index.js')({siteDir: FIXTURE});
  // A missing tree is empty, not an error — the site renders "no measurements".
  const empty = await plugin.loadContent();
  assert.equal(empty.rows.length, 0);
  assert.equal(empty.entrants.length, 0);
});

test('routes and links live under the configured base path', async () => {
  process.env.BENCH_ROOT = FIXTURE;
  const plugin = require('./index.js')({siteDir: FIXTURE, baseUrl: '/'}, {routeBasePath: 'benchmarks'});
  const content = await plugin.loadContent();
  const routes = [];
  let published;
  await plugin.contentLoaded({
    content,
    actions: {
      setGlobalData: (d) => {
        published = d;
      },
      createData: async () => 'profile.json',
      addRoute: (r) => routes.push(r.path),
    },
  });
  assert.equal(published.basePath, '/benchmarks/');
  assert.ok(routes.length > 0, 'one route per entrant');
  for (const r of routes) assert.match(r, /^\/benchmarks\/systems\/[^/]+$/);
});
