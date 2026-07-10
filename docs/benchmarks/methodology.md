---
title: Methodology
sidebar_position: 7
description: How to reproduce the benchmarks, and the 2026-07 accounting correction.
---

# Methodology

Numbers are machine-specific; every chart renders its own provenance (CPU,
commit, date) from the data. The current results were recorded on an Apple M5 Max
(18-core), macos/aarch64, rustc 1.96.1, release profile, commit `63084c56a811`, on
a quiet machine — all unrelated Docker containers were paused for the runs.
Contended runs are called out where they matter.

## How the results are recorded

Every benchmark binary appends one JSON object per line to the file named by its
`RESULTS` environment variable, under `benchmarks/results/`. The record shape is
defined once, versioned, in `benchmarks/src/report.rs`:

- `schema` — the record schema version (currently `1`). The docs site ignores
  any line without `schema == 1`, so older ad-hoc records never render.
- `bench` — the rig that produced the record.
- `kind` — `measurement` or `verdict` (a go/no-go conclusion drawn across arms).
- `run` — provenance: timestamp, short commit, host, CPU brand, cores, OS,
  profile. This is what the "provenance" line under each chart reports.
- `variant` — the arm under test (an open map, e.g. `{"deser":"fast_borrowed",
  "format":"native"}`).
- `metrics` — each measured quantity with its `value`, `unit`,
  `higher_is_better`, and optional `ci95` / `n`. The direction of goodness
  travels **with** the number, so a chart can never silently draw a
  lower-is-better quantity as a taller bar.

## Reproducing

Reproduce per the usage headers in each binary under `benchmarks/src/bin/`. Each
rig appends to the result file named below; the exact env vars used for the
committed runs are:

```sh
# Framework ceiling → pipeline-synthetic-baseline.jsonl (no broker, no server)
# 30 s window, 256 B payloads, 2 shards, zero per-record work, threads 1/2/4/8.
THREADS_LIST=1,2,4,8 SHARDS=2 PAYLOAD=256 WORK_US=0 DURATION_S=30 \
  RESULTS=benchmarks/results/pipeline-synthetic-baseline.jsonl \
  cargo run -p benchmarks --release --bin pipeline_synthetic

# CPU-bound Avro A/B → avro-fast-pipeline.jsonl (no broker, no server; 50 events/datum)
# One invocation = one arm (chosen by STAGE/DESER/FORMAT env), a median over REPS.
# STAGE=decode isolates the decoder (3 DESER arms); STAGE=pipeline adds flat_map +
# filter + encode + shard handoff, sweeping DESER × FORMAT (6 arms). Nine records,
# n = 9 reps each.
for DESER in apache_owned fast_owned fast_borrowed; do
  STAGE=decode DESER=$DESER EVENTS=50 THREADS=1 REPS=9 \
    RESULTS=benchmarks/results/avro-fast-pipeline.jsonl \
    cargo run -p benchmarks --release --bin avro_pipeline
done
for DESER in apache_owned fast_owned fast_borrowed; do
  for FORMAT in rowbinary native; do
    STAGE=pipeline DESER=$DESER FORMAT=$FORMAT EVENTS=50 THREADS=1 REPS=9 \
      RESULTS=benchmarks/results/avro-fast-pipeline.jsonl \
      cargo run -p benchmarks --release --bin avro_pipeline
  done
done

# ClickHouse Native vs RowBinary → clickhouse-native-format.jsonl (Docker ClickHouse 25.6)
# 200k rows/insert, medians over 15 interleaved reps; SERVER=1 samples server CPU.
ROWS=200000 ITERS=41 REPS=15 SERVER=1 \
  RESULTS=benchmarks/results/clickhouse-native-format.jsonl \
  cargo run -p benchmarks --release --bin ch_native_format

# Kafka consumer topology → kafka-topology-confirm.jsonl (local single-broker Kafka)
# 16 partitions × 4 threads × 10 µs/record × 10M × 256 B. The binary takes a
# `produce|consume` subcommand (no-arg exits 2). Pre-fill the topic once — its name
# derives from PARTITIONS×PAYLOAD, so one fill serves both modes — then consume
# 2 reps per mode.
PARTITIONS=16 THREADS=4 WORK_US=10 MESSAGES=10000000 PAYLOAD=256 \
  cargo run -p benchmarks --release --bin kafka_topology produce
for MODE in perthread split; do for _ in 1 2; do \
  MODE=$MODE PARTITIONS=16 THREADS=4 WORK_US=10 MESSAGES=10000000 PAYLOAD=256 \
    RESULTS=benchmarks/results/kafka-topology-confirm.jsonl \
    cargo run -p benchmarks --release --bin kafka_topology consume; \
done; done

# End-to-end Kafka → ClickHouse → e2e-kafka-clickhouse.jsonl (local containers)
# RATE=0 saturates on purpose — the producer outruns the pipeline, so the measured
# rate is the ceiling. 30 s window, 20 event rows exploded per message, 4 partitions,
# 2 pipeline threads. One invocation = one arm (DESER/FORMAT); the committed matrix
# is the DESER=none rowbinary baseline plus four avro arms, three runs each; charts
# and quoted figures use per-arm medians.
for rep in 1 2 3; do
  for arm in "none rowbinary" "apache_owned native" "fast_owned native" \
             "fast_borrowed native" "fast_borrowed rowbinary"; do
    set -- $arm
    DESER=$1 FORMAT=$2 RATE=0 DURATION_S=30 EVENTS=20 PARTITIONS=4 THREADS=2 \
      RESULTS=benchmarks/results/e2e-kafka-clickhouse.jsonl \
      cargo run -p benchmarks --release --bin e2e_kafka_clickhouse
  done
done
```

Superseded reference point: an earlier rate-limited E2E smoke (100k rec/s target,
2026-07-05) recorded 89.4k rows/s against local containers. Its backing file
(`e2e-local-smoke.jsonl`) no longer exists — it was replaced by the saturated rig
above (`RATE=0`), which measures the pipeline ceiling instead of a throttled
target.

The micro-benchmarks are separate: `cargo bench -p etl-core` and
`cargo bench -p etl-avro --features fast` (criterion + divan; the fast-backend
decode variants are gated behind the `fast` feature). Divan's `AllocProfiler`
allocation assertions are hard failures in CI, and the counting-allocator
integration test (`crates/etl-core/tests/chain_alloc.rs`) hard-fails if
per-iteration allocations scale with record count.

A recurring harness lesson, now encoded in the binaries: metric handles created
**before** `metrics::install` bind to the no-op recorder and silently render
nothing. The harnesses install the recorder first; the examples should be
audited for the same pattern.

## Accounting note (2026-07 correction) {#accounting-note}

An earlier revision of the framework-ceiling table
([Framework overhead](./framework-overhead)) reported window-scoped `records`
against lifetime `sink_rows_total` (warmup + window + drain), which read as rows
exceeding records by ~10%. The harness now reports `produced_total` alongside
and asserts conservation; a quiet-machine re-run shows `sink_rows_total ==
produced_total` exactly (1,297,595,392 == 1,297,595,392 @ 1 thread) and healthy
1→2 thread scaling (39.4M → 74.6M records/s). The metric itself is proven exact
by `sink_records_metric_matches_rows_written_exactly` in `etl-core`.

## E2E rate accounting (2026-07 correction) {#e2e-rate-accounting}

An earlier revision of the end-to-end rig
([E2E rig](./avro-fast-pipeline.mdx#e2e-rig)) divided every row that landed —
window plus the grace and drain tail — by the 30 s window alone, which under
`RATE=0` (the producer deliberately outruns the pipeline) inflated the reported
`rows_per_s` by roughly 2×. It now divides by the elapsed consumption interval,
so the rate reflects the work actually done in that span; the committed records
were regenerated. Each arm's `events` (rows exploded per message) and `payload`
are now recorded in `variant`, so every row is self-describing.
