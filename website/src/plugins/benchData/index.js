// Reads the benchmark data from the `website/benchmark` submodule at build time
// and hands it to pages.
//
// Three sources, none of them importable by the bundler: the entrant descriptors
// (TOML), the environment profiles (TOML), and the results (JSONL). So they are
// read here and published through `usePluginData`.
//
// Options: `routeBasePath` (default `benchmarks`) is the site path the results
// pages and the per-system routes live under, and `repoUrl` is the benchmark
// repository every source link points at. Both reach the components as global
// data, so no component carries a path or a repository of its own.
//
// WHAT GOES INTO GLOBAL DATA, AND WHY IT IS BOUNDED
//
// Docusaurus global data is not code-split per route: whatever is put here ships
// in the main bundle to every visitor, including one who only opens the
// methodology page. The framework repository's equivalent plugin publishes every
// record, which at 706 records is ~975 KB of global data and ~227 KB gzipped
// inside main.js, growing linearly.
//
// So this publishes a CATALOGUE — sized by the number of entrants, environments
// and comparability groups, not by the number of records — plus a pre-aggregated
// summary row per arm. The full per-record archive is deliberately not here; when
// the archive justifies it, it becomes content-hashed shards under static/ that
// are fetched only when a reader expands history.
//
// Only the newest SITTING of each arm is published, so the summary is bounded by
// (groups x arms x configurations measured in one sitting) rather than growing
// with every sweep ever run: a nightly re-measurement of the same seven arms adds
// nothing to what every visitor downloads. A sweep measures an arm at one
// configuration, so that last factor is 1 in practice — but it is a property of
// how `bench run` is invoked, not one this file enforces, which is why the bound
// is stated with it rather than without.
//
// `results/` still keeps every record — the archive is append-only, and this is a
// selection made at build time, not a retention policy. See `latestSitting`.
//
// COMPARABILITY IS ENFORCED HERE, NOT IN THE COMPONENTS
//
// Records that differ in (harness_version, dataset_version, env_id, infra digest)
// describe different experiments and must never be averaged together. Grouping
// them at load time means a component cannot accidentally mix them, and the
// "these are not comparable" case becomes data the page can render rather than a
// mistake nobody notices.

const fs = require('node:fs');
const path = require('node:path');

const TOML = require('smol-toml');

const PLUGIN = 'bench-data';

/** The benchmark checkout: the submodule, unless a test points elsewhere. */
function repoRoot(siteDir) {
  return process.env.BENCH_ROOT || path.resolve(siteDir, 'benchmark');
}

/**
 * Reads one descriptor or environment profile.
 *
 * A real TOML parser, not the hand-rolled subset that used to live here. This is
 * the seam where the site has to agree with the harness about what a descriptor
 * says, and the two were parsing the same files with different implementations:
 * the Rust side uses the `toml` crate with `deny_unknown_fields`, while this side
 * mis-read an inline `key = "x" # note` as the value `x" # note`, reattached a
 * nested `[a.b]` under an open `[[array]]` to the root, and carried a
 * write-only `arrayMode` flag that betrayed the confusion. Anything mis-parsed
 * here is rendered beside a published number, and `entrants_are_valid` cannot
 * catch it because that test validates the other parser.
 *
 * `smol-toml` is dependency-free and build-time only, so the supply-chain
 * argument the old comment made against a real parser does not apply: it never
 * reaches a visitor's browser.
 *
 * A file that does not parse throws rather than yielding a partial object. A
 * silently half-read descriptor is how a system ends up on the page with its
 * guarantees missing.
 */
function readToml(file) {
  try {
    return TOML.parse(fs.readFileSync(file, 'utf8'));
  } catch (e) {
    throw new Error(`${file}: ${e.message}`);
  }
}

function readDirSafe(dir) {
  try {
    return fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return [];
  }
}

function loadEntrants(root) {
  const dir = path.join(root, 'entrants');
  return readDirSafe(dir)
    .filter((e) => e.isDirectory())
    .map((e) => path.join(dir, e.name, 'entrant.toml'))
    .filter((p) => fs.existsSync(p))
    .map((p) => {
      const spec = readToml(p);
      if (!spec.entrant || !spec.entrant.id) {
        throw new Error(`${p}: no [entrant].id — every system must say what it is`);
      }
      return spec;
    })
    .sort((a, b) => (a.display?.order ?? 0) - (b.display?.order ?? 0));
}

function loadEnvironments(root) {
  const dir = path.join(root, 'environments');
  return readDirSafe(dir)
    .filter((e) => e.isFile() && e.name.endsWith('.toml'))
    .map((e) => {
      const p = path.join(dir, e.name);
      const spec = readToml(p);
      if (!spec.id) throw new Error(`${p}: no id — an environment is the unit of comparability`);
      return spec;
    });
}

function walkJsonl(dir, out) {
  for (const e of readDirSafe(dir)) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) walkJsonl(p, out);
    else if (e.name.endsWith('.jsonl')) out.push(p);
  }
  return out;
}

function loadRecords(root) {
  const files = walkJsonl(path.join(root, 'results'), []).sort();
  const records = [];
  const counts = { files: files.length, lines: 0, kept: 0, skippedSchema: 0, skippedParse: 0 };
  for (const f of files) {
    for (const line of fs.readFileSync(f, 'utf8').split('\n')) {
      if (!line.trim()) continue;
      counts.lines += 1;
      let rec;
      try {
        rec = JSON.parse(line);
      } catch {
        counts.skippedParse += 1;
        continue;
      }
      // Schema 2 only. A v1 record has no system under test, no environment and
      // no comparability fields; rendering one would mean inventing all three.
      if (!rec || typeof rec !== 'object' || rec.schema !== 2) {
        counts.skippedSchema += 1;
        continue;
      }
      counts.kept += 1;
      records.push(rec);
    }
  }
  return { records, counts };
}

/**
 * The key that decides what may share an axis.
 *
 * The first four components are provenance: two records that differ in any of
 * them describe different experiments, and methodology/ makes three of them
 * hard splits.
 *
 * `mode` is here for a different reason and it is not optional. `rows_per_s`
 * means "how fast can this go" in drain and "the rate we asked for" in
 * sustained, so two arms of wildly different capacity report the same number;
 * the efficiency figures were taken with the broker serving writes and reads at
 * once and a generator competing for cores, which is the whole argument drain
 * exists for; and latency is single-mode by construction. A row is not an axis,
 * and it is the axis that misleads.
 */
function groupKey(rec) {
  return [
    rec.run?.env_id,
    rec.run?.harness_version,
    rec.run?.dataset_version,
    rec.run?.infra?.digest,
    `mode-${rec.variant?.mode ?? '?'}`,
  ].join('|');
}

/**
 * A stable fingerprint of an arm's configuration.
 *
 * Every knob the driver recorded, in sorted order. Two records that differ here
 * were not the same experiment and must not be medianed together, however close
 * in time they were: `--batches 150000` and `--batches 1500000` are a tenth of
 * the corpus apart and produce entirely different cache behaviour.
 */
function variantKey(rec) {
  const v = rec.variant ?? {};
  return JSON.stringify(v, Object.keys(v).sort());
}

/**
 * Which sitting a record belongs to.
 *
 * `invocation_id` is minted once per `bench run` from harness 2 on, so a sitting
 * is identified exactly. The UTC calendar day is the fallback for records written
 * before the field existed, and it is only an approximation: a sweep crossing
 * midnight splits in two, and two sweeps on one day merge into one.
 */
function sittingKey(rec) {
  return rec.run?.invocation_id || new Date(rec.run?.ts_ms ?? 0).toISOString().slice(0, 10);
}

/** The arm a row or attempt belongs to, within its comparability group. */
function armKey(x) {
  return [x.group, x.entrant, x.variant_id].join('|');
}

function median(xs) {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

/** Statuses that carry publishable numbers. Mirrors `Status::carries_metrics`. */
const CARRIES_METRICS = new Set(['ok', 'infra_bound']);

/**
 * Severity order for aggregating a repetition's status up to its row.
 *
 * A row takes the WORST status among the repetitions behind it, never the newest
 * one. An arm that crossed the 70% headroom limit on one repetition of three
 * demonstrably crossed it, and the aggregate of those three cannot be published
 * as a system comparison just because the last repetition happened to come in
 * under.
 */
const STATUS_SEVERITY = {ok: 0, infra_bound: 1};

/**
 * Severity of a status this build does not recognise.
 *
 * Above every known value, so an unknown status always wins and the row is never
 * ranked. Scoring it 0 — which this did — made it tie with `ok` and lose, so a
 * status added by a newer harness would have been silently published as sound by
 * an older site. That is the same fail-open mistake `approach` used to make, and
 * it fails in the same direction: towards publishing something we cannot vouch
 * for.
 */
const UNKNOWN_SEVERITY = Number.MAX_SAFE_INTEGER;

const severity = (status) => STATUS_SEVERITY[status] ?? UNKNOWN_SEVERITY;

function worstStatus(recs) {
  return recs.map((r) => r.status).reduce((a, b) => (severity(b) > severity(a) ? b : a), 'ok');
}

/**
 * One summary row per (group, entrant, variant, configuration, version, sitting),
 * of which [`latestSitting`] then publishes only the newest per arm.
 *
 * Repetitions within a single invocation are aggregated by median. Runs from
 * DIFFERENT sittings are not: they are aggregated separately and the older one is
 * dropped rather than merged. That is the correction to the framework site's
 * aggregator, which hashes only variant keys and so silently medians a re-run
 * months later into the original figure while captioning it with the newest date
 * — a mistake selection would otherwise reintroduce, since a superseded sitting
 * that had been medianed into its successor could never be dropped from it.
 *
 * Three properties this function is responsible for, each of which it previously
 * got wrong:
 *
 * - **`infra_bound` records are kept.** They were dropped here by a
 *   `status !== 'ok'` filter, which made "we ran it and it blew the headroom
 *   limit" render identically to "we never ran it" — the exact distinction
 *   `Status::InfraBound` exists to preserve. They are kept, carried through with
 *   their status, and the page refuses to rank them.
 * - **Configuration is part of the identity.** The key omitted `variant`
 *   entirely, so two sweeps of the same arm at different knob settings on the
 *   same day were medianed into one number captioned as run-to-run spread.
 *
 * Known remaining limitation, and it costs more now than it used to: a record
 * carrying no `invocation_id` falls back to the UTC calendar day (see
 * [`sittingKey`]), so a sweep straddling midnight reads as two sittings — and
 * selection then drops the earlier half rather than merely listing it separately.
 * Every record the harness writes carries the field, so this reaches only ones
 * written before it existed.
 */
function summarise(records) {
  const byKey = new Map();
  const byAttempt = new Map();
  /** Measured A/A spread, by the sitting whose control produced it. */
  const aaBySitting = new Map();
  for (const rec of records) {
    // Only measurements are arms. A `verdict` is a conclusion drawn across
    // arms — the sweep's A/A control is one — and rendering it as a row would
    // put a statement about the rig in a table of systems. Its number is kept
    // against the sitting it describes, so the rows from that sweep can say
    // what the rig was doing while they were taken.
    if ((rec.kind ?? 'measurement') !== 'measurement') {
      const floor = rec.metrics?.aa_spread?.value;
      if (typeof floor === 'number') aaBySitting.set(sittingKey(rec), floor);
      continue;
    }
    // The A/A control is the same arm measured a second time to difference
    // against itself. Its number is evidence about the rig and would be a
    // duplicate entrant in the comparison.
    if ((rec.flags ?? []).includes('aa_control')) continue;
    if (!CARRIES_METRICS.has(rec.status)) {
      // Attempted and produced no publishable number. Surfaced as an explicit
      // gap rather than an absence a reader would read as "not tried".
      //
      // Collapsed per sitting for the same reason rows are: a three-repetition
      // failure is one thing that went wrong, not three, and listing it once per
      // repetition would grow this list without bound as re-runs accumulate.
      const group = groupKey(rec);
      const sitting = sittingKey(rec);
      const ts = rec.run?.ts_ms ?? 0;
      const key = [group, rec.sut?.entrant, rec.sut?.variant_id, sitting].join('|');
      const prev = byAttempt.get(key);
      if (prev) {
        prev.reps_counted += 1;
        // Status and note come from the newest repetition TOGETHER. `worstStatus`
        // cannot order these: it knows `ok` and `infra_bound`, and every status
        // that reaches here scores `UNKNOWN_SEVERITY`, so it would have returned
        // the first repetition's status while the note came from the last — an
        // entry describing a failure that never happened in that pairing.
        if (ts >= prev.ts_ms) {
          prev.ts_ms = ts;
          prev.status = rec.status;
          prev.note = rec.note ?? null;
        }
        continue;
      }
      byAttempt.set(key, {
        group,
        entrant: rec.sut?.entrant,
        variant_id: rec.sut?.variant_id,
        sitting,
        status: rec.status,
        note: rec.note ?? null,
        ts_ms: ts,
        reps_counted: 1,
        // Carried so a group whose every arm refused can still be named on the
        // page: `groups` is derived from attempts as well as rows.
        env_id: rec.run?.env_id,
        harness_version: rec.run?.harness_version,
        dataset_version: rec.run?.dataset_version,
      });
      continue;
    }
    const key = [
      groupKey(rec),
      rec.sut?.entrant,
      rec.sut?.variant_id,
      rec.sut?.version ?? rec.sut?.commit ?? '?',
      variantKey(rec),
      // Distinct sittings stay distinct. Without this, a re-run silently joins
      // the original and its repetitions are medianed into one number captioned
      // as run-to-run spread.
      sittingKey(rec),
    ].join('|');
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push(rec);
  }

  const rows = [];
  for (const [key, reps] of byKey) {
    const newest = reps.reduce((a, b) => (a.run.ts_ms >= b.run.ts_ms ? a : b));

    const counted = reps;

    const metrics = {};
    const names = new Set(counted.flatMap((r) => Object.keys(r.metrics || {})));
    for (const name of names) {
      const vals = counted.map((r) => r.metrics?.[name]?.value).filter((v) => typeof v === 'number');
      if (!vals.length) continue;
      const proto = counted.find((r) => r.metrics?.[name])?.metrics[name];
      const sorted = [...vals].sort((a, b) => a - b);
      const mid = median(vals);
      metrics[name] = {
        value: mid,
        unit: proto.unit,
        higher_is_better: proto.higher_is_better,
        n: vals.length,
        // The measured extremes, not merely the distance between them.
        //
        // `spread` alone cannot place an interval on a chart. The median of
        // three repetitions is the middle MEASUREMENT, not the midpoint of the
        // range, so a site drawing `value ± spread / 2` would draw an interval
        // whose ends the harness never observed — and would do it worst exactly
        // where the repetitions are most skewed, which is where a reader most
        // needs the truth. A sweep's own A/A control measures the spread the
        // rig produces when nothing changes, and it is routinely wider than the
        // differences this page exists to show, so the interval is not
        // decoration.
        //
        // At three repetitions `lo`, `value` and `hi` ARE the three
        // measurements; `values` carries them explicitly so the site keeps
        // drawing every repetition if `reps` ever rises. Cost is bounded by
        // arms x metrics x reps — the same order as the metrics map it sits in.
        lo: sorted[0],
        hi: sorted[sorted.length - 1],
        values: sorted,
        // Relative range, or `null` when it has no meaning.
        //
        // This was `(max - min) / median` unguarded, and every all-zero metric
        // — `duplicate_rows` on a clean run, which is every published run —
        // computed 0/0 = NaN. NaN is not representable in JSON, so Docusaurus's
        // global data serialised it to `null`, and the component's
        // `(spread * 50).toFixed(1)` then rendered it as "±0.0%": a precision
        // claim about a quantity that was never measurable. Undefined is said
        // rather than implied.
        spread:
          vals.length < 2 ? 0 : mid === 0 ? null : (sorted[sorted.length - 1] - sorted[0]) / mid,
      };
    }
    // Hoisted: the note below is chosen against the status the row publishes,
    // and an object literal cannot read its own property.
    const status = worstStatus(counted);
    const reason = reps
      .filter((r) => r.status === status)
      .sort((a, b) => a.run.ts_ms - b.run.ts_ms)
      .pop();

    rows.push({
      key,
      sitting: sittingKey(newest),
      group: groupKey(newest),
      entrant: newest.sut.entrant,
      variant_id: newest.sut.variant_id,
      version: newest.sut.version ?? null,
      commit: newest.sut.commit ?? null,
      image_digest: newest.sut.image_digest,
      env_id: newest.run.env_id,
      harness_version: newest.run.harness_version,
      dataset_version: newest.run.dataset_version,
      ts_ms: newest.run.ts_ms,
      // Carried so the page can honour the contract without re-deriving any of
      // it: `approach` decides headline eligibility, `status` decides whether an
      // arm may be ranked at all, and `wire_format` is required beside every
      // number by rule 5.
      status,
      mode: newest.variant?.mode ?? null,
      // Fail closed. A record that does not say what it is cannot be
      // headline-eligible: defaulting to `realistic` meant a foreign or
      // hand-edited record was ranked by default, which is the wrong direction
      // for the valve rule 3 exists to be.
      approach: newest.variant?.approach ?? 'undeclared',
      wire_format: newest.variant?.wire_format ?? null,
      reps_counted: counted.length,
      // The union across repetitions, not the newest one's. A caveat that
      // applied to any repetition applies to the number they were medianed into
      // — a throttled rep does not stop being throttled because the next one
      // was not.
      flags: [...new Set(counted.flatMap((r) => r.flags || []))].sort(),
      // The harness's own account of this reading, so an arm can carry its
      // REASON and not only its verdict.
      //
      // Taken from the newest repetition whose status is the one the row
      // publishes, never simply from the newest. Those differ exactly when a
      // sitting is mixed, which is the case that matters: `spate:rowbinary`'s
      // published sitting is three repetitions at 70%, 65% and 79% of the
      // ClickHouse ingest ceiling, and `worstStatus` makes the row
      // `infra_bound` on the strength of the third. Pairing that verdict with
      // the newest note would explain a disowned number with a repetition that
      // passed. `byAttempt` above takes status and note together for the same
      // reason.
      //
      // Carried verbatim rather than parsed for the clause that explains the
      // status. The site does not re-derive a published figure, and a plugin
      // picking sentences out of a harness-authored string would be doing a
      // worse version of that.
      note: (reason ?? newest).note ?? null,
      // What the rig's own control measured during this sweep, when the sweep
      // ran one. The spread a reader is warned about is then this sweep's
      // measured floor rather than a figure quoted from somewhere else.
      aa_spread: aaBySitting.get(sittingKey(newest)) ?? null,
      metrics,
    });
  }
  const published = latestSitting(rows, [...byAttempt.values()]);
  published.rows.sort((a, b) => b.ts_ms - a.ts_ms);
  published.attempts.sort((a, b) => b.ts_ms - a.ts_ms);
  return published;
}

/**
 * Keeps each arm's newest sitting and drops the ones it superseded.
 *
 * The unit is the SITTING, not the record. An arm whose sweep measured three
 * repetitions and failed a fourth still publishes the number those three
 * produced: the failure is listed beside it as the gap it is, and it does not
 * evict a row taken on the same occasion. Only a strictly newer sitting
 * supersedes, and if that sitting produced no number the arm shows as failing
 * rather than as the stale figure it last managed: the page answers what these
 * systems do now, and an arm ranked on a reading its latest sweep could not
 * reproduce is the one way this page could mislead while every number on it is
 * individually true.
 *
 * Selection never crosses a comparability group: records differing in
 * (env_id, harness_version, dataset_version, infra digest, mode) describe
 * different experiments, so a fresh reading under a new protocol must not
 * silently retire the last reading taken under the old one.
 *
 * Ordering is by timestamp, then by sitting id, so an unchanged tree selects the
 * same records on every build and a diff in the output means the input moved.
 */
function latestSitting(rows, attempts) {
  const newest = new Map();
  for (const x of [...rows, ...attempts]) {
    const arm = armKey(x);
    const prev = newest.get(arm);
    if (!prev || x.ts_ms > prev.ts_ms || (x.ts_ms === prev.ts_ms && x.sitting > prev.sitting)) {
      newest.set(arm, { ts_ms: x.ts_ms, sitting: x.sitting });
    }
  }
  const kept = (x) => newest.get(armKey(x)).sitting === x.sitting;
  return { rows: rows.filter(kept), attempts: attempts.filter(kept) };
}

module.exports = function benchData(context, options = {}) {
  const root = repoRoot(context.siteDir);
  const routeBasePath = (options.routeBasePath ?? 'benchmarks').replace(/^\/+|\/+$/g, '');
  const basePath = `${context.baseUrl}${routeBasePath}/`;
  const repoUrl = options.repoUrl ?? 'https://github.com/spate-etl/benchmark';

  return {
    name: PLUGIN,

    getPathsToWatch() {
      return [
        path.join(root, 'entrants/**/entrant.toml'),
        path.join(root, 'environments/*.toml'),
        path.join(root, 'results/**/*.jsonl'),
      ];
    },

    async loadContent() {
      const entrants = loadEntrants(root);
      const environments = loadEnvironments(root);
      const { records, counts } = loadRecords(root);
      const { rows, attempts } = summarise(records);

      // Named from attempts as well as rows. A group whose every arm's newest
      // sitting refused has no rows at all, and deriving this from rows alone
      // would drop it from the page — hiding exactly the failure the page should
      // be loudest about, because the component renders only groups listed here.
      const described = [...rows, ...attempts];
      const groups = [...new Set(described.map((r) => r.group))]
        .map((g) => {
          const any = described.find((r) => r.group === g);
          return {
            key: g,
            env_id: any.env_id,
            harness_version: any.harness_version,
            dataset_version: any.dataset_version,
          };
        })
        // Ordered by key so an unchanged tree builds to an identical list — the
        // page re-sorts by richness before rendering, so this order only has to
        // be deterministic, not meaningful.
        .sort((a, b) => a.key.localeCompare(b.key));

      // Build determinism: derived from the newest record rather than the wall
      // clock, so rebuilding an unchanged tree produces an identical site and a
      // diff means something changed.
      const generatedAt = records.length
        ? new Date(Math.max(...records.map((r) => r.run?.ts_ms ?? 0))).toISOString()
        : null;

      return { entrants, environments, rows, attempts, groups, counts, generatedAt, root };
    },

    async contentLoaded({ content, actions }) {
      actions.setGlobalData({
        entrants: content.entrants,
        environments: content.environments,
        rows: content.rows,
        attempts: content.attempts,
        groups: content.groups,
        counts: content.counts,
        generatedAt: content.generatedAt,
        basePath,
        repoUrl,
      });

      // One profile page per declared system, generated from the descriptors.
      //
      // CONTRIBUTING.md promises that "adding entrant N+1 touches exactly one
      // new directory. There is no central registry to update." A hand-written
      // page per system would quietly break that: the twenty-first vendor would
      // owe the site a page as well as a descriptor, and pages written at
      // different times drift into flattering some systems more carefully than
      // others. So the route is derived, and every system gets the same shape.
      //
      // The module carries only the id. Everything else is already in global
      // data on every page, and shipping a second copy of a system's rows here
      // would put the same numbers in the bundle twice — which is exactly the
      // payload problem the header comment on `summarise` is about.
      //
      // Note what does NOT happen here: no entrant id appears as a literal.
      // They come from the descriptors that were just parsed, which is what
      // keeps `plugins/neutrality.test.js` green and the neutrality claim
      // checkable rather than asserted.
      for (const e of content.entrants) {
        const id = e.entrant.id;
        const profile = await actions.createData(
          `system-${id}.json`,
          JSON.stringify({ id }),
        );
        actions.addRoute({
          path: `${basePath}systems/${id}`,
          component: '@site/src/components/Results/system.tsx',
          modules: { profile },
          exact: true,
        });
      }
    },
  };
};

// Test-only. The plugin's contract is the Docusaurus hook above; these are the
// pure decisions inside it, exposed so `index.test.js` can pin behaviour that is
// not reachable through `loadContent` today — a status this build does not know
// is filtered out upstream by `CARRIES_METRICS`, so the fail-closed severity
// rule can only be exercised directly. Not part of the public surface.
module.exports.__testonly = {worstStatus, severity, groupKey, variantKey, UNKNOWN_SEVERITY};
