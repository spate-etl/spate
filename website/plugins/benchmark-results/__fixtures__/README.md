# Synthetic fixtures — NOT measurements

These `*.jsonl` files exist only to develop and preview the benchmark charts
before real v1 data is recorded. Every value here is **invented** and every
record carries `run.cpu = "Synthetic Fixture CPU"` and a `SYNTHETIC FIXTURE`
note so it can never be mistaken for a published measurement.

Point the plugin at this directory to preview:

```sh
cd website
BENCH_RESULTS_DIR=plugins/benchmark-results/__fixtures__ npm start
```

Real published measurements live in `../../../../benchmarks/results/` and are
recorded by the benchmark rigs, never hand-written. Do not copy anything from
here into that directory.

## Deliberately malformed lines

The last lines of `fixture.jsonl` are **intentionally broken** and must stay
that way — they keep the plugin's degrade path exercised by the manual-preview
flow above:

- `{"schema":2,…}` — a future schema version (skipped, counted as non-schema-1).
- `{"bench":"legacy_no_schema",…}` — a pre-schema record with no `schema` key.
- a bare `null` line and a bare number line — JSON that parses to a non-object.
  A record must be an object; reading `.schema` off `null` would otherwise crash
  the whole docs build, so the plugin counts these as malformed and skips them.

Previewing the fixtures should log a non-zero skipped count and still render
every chart — no build error.
