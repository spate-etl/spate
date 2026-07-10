// Docusaurus plugin: load committed benchmark results (JSONL) into global data.
//
// Webpack cannot import `.jsonl` (unknown module type) and the results live at
// `../benchmarks/results/`, outside the site root — so we read them here at
// build time and hand them to pages via `usePluginData('benchmark-results')`.
//
// Every line is one record of the versioned schema in `benchmarks/src/report.rs`.
// Only `schema === 1` records are kept; anything else (older ad-hoc shapes,
// blank lines, unparseable text) is counted and skipped. A missing directory or
// zero records is NOT an error: the plugin degrades to empty global data and the
// chart components render a "no recorded data yet" placeholder, so the site build
// stays green before any v1 data has been recorded.
//
// Develop against a fixture without touching benchmarks/results by pointing
// BENCH_RESULTS_DIR at plugins/benchmark-results/__fixtures__.

const fs = require('fs');
const path = require('path');

/** @param {number} schema */
const isV1 = (schema) => schema === 1;

/** Where the JSONL lives: BENCH_RESULTS_DIR (fixtures during dev) or the repo. */
function resolveDir(context) {
  return process.env.BENCH_RESULTS_DIR
    ? path.resolve(process.env.BENCH_RESULTS_DIR)
    : path.resolve(context.siteDir, '..', 'benchmarks', 'results');
}

module.exports = function benchmarkResultsPlugin(context) {
  return {
    name: 'benchmark-results',

    // Hot-reload the charts when a rig appends to (or a dev edits) the JSONL.
    getPathsToWatch() {
      return [path.join(resolveDir(context), '*.jsonl')];
    },

    async loadContent() {
      const dir = resolveDir(context);

      /** @type {Record<string, any[]>} */
      const byBench = {};
      const counts = {
        files: 0,
        lines: 0,
        kept: 0,
        skippedSchema: 0,
        skippedParse: 0,
      };
      // Build determinism: `generatedAt` follows the newest record's ts_ms, not
      // wall-clock `new Date()` (which changed every build and thrashed diffs).
      let newestTs = 0;

      let entries;
      try {
        entries = fs.readdirSync(dir).filter((f) => f.endsWith('.jsonl')).sort();
      } catch (err) {
        console.warn(
          `[benchmark-results] no results directory at ${dir} ` +
            `(${err.code || err.message}) — charts will render placeholders`,
        );
        return { sourceDir: dir, generatedAt: null, byBench, counts };
      }

      for (const file of entries) {
        counts.files += 1;
        let text;
        try {
          text = fs.readFileSync(path.join(dir, file), 'utf8');
        } catch {
          continue;
        }
        for (const raw of text.split('\n')) {
          const line = raw.trim();
          if (!line) continue;
          counts.lines += 1;
          let rec;
          try {
            rec = JSON.parse(line);
          } catch {
            counts.skippedParse += 1;
            continue;
          }
          // A record must be a JSON object. `null`, numbers, strings, booleans
          // and arrays all parse cleanly but are not records — and reading
          // `rec.schema` off `null` throws and would crash the whole build, so
          // treat any non-object as malformed (counted, skipped) like a parse
          // failure.
          if (rec === null || typeof rec !== 'object' || Array.isArray(rec)) {
            counts.skippedParse += 1;
            continue;
          }
          if (!isV1(rec.schema)) {
            counts.skippedSchema += 1;
            continue;
          }
          counts.kept += 1;
          if (rec.run && Number.isFinite(rec.run.ts_ms)) {
            newestTs = Math.max(newestTs, rec.run.ts_ms);
          }
          const bench = typeof rec.bench === 'string' ? rec.bench : 'unknown';
          (byBench[bench] ||= []).push(rec);
        }
      }

      console.log(
        `[benchmark-results] ${dir}: kept ${counts.kept} schema-1 record(s) ` +
          `across ${Object.keys(byBench).length} bench(es) from ${counts.files} file(s); ` +
          `skipped ${counts.skippedSchema} non-schema-1 and ${counts.skippedParse} unparseable line(s)`,
      );

      return {
        sourceDir: dir,
        generatedAt: newestTs > 0 ? new Date(newestTs).toISOString() : null,
        byBench,
        counts,
      };
    },

    async contentLoaded({ content, actions }) {
      actions.setGlobalData(content);
    },
  };
};
