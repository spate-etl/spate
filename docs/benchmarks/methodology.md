---
title: Methodology
sidebar_position: 8
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
# The committed data was recorded on 25.6 — pin CLICKHOUSE_IMAGE explicitly, because
# the default image is now 26.3 (see the sink-saturation block below). The raw
# inserts also pin async_insert=0 and record SELECT version() into the note.
CLICKHOUSE_IMAGE=clickhouse/clickhouse-server:25.6 \
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
# and quoted figures use per-arm medians. Committed data is ClickHouse 25.6 — pin
# CLICKHOUSE_IMAGE (the default is now 26.3); the sink YAML pins async_insert=0 and
# the run records SELECT version() into its note.
export CLICKHOUSE_IMAGE=clickhouse/clickhouse-server:25.6
for rep in 1 2 3; do
  for arm in "none rowbinary" "apache_owned native" "fast_owned native" \
             "fast_borrowed native" "fast_borrowed rowbinary"; do
    set -- $arm
    DESER=$1 FORMAT=$2 RATE=0 DURATION_S=30 EVENTS=20 PARTITIONS=4 THREADS=2 \
      RESULTS=benchmarks/results/e2e-kafka-clickhouse.jsonl \
      cargo run -p benchmarks --release --bin e2e_kafka_clickhouse
  done
done

# ClickHouse sink saturation → ch-sink-saturation.jsonl (Docker ClickHouse 26.3)
# In-process generator (no broker) → real chain → sharded ClickHouse sink at full
# tilt. Throughput = etl_sink_records_total delta / window (works for ENGINE=Null,
# where SELECT count() is 0). The committed sweep is 33 arms × 3 reps = 99 records
# on ClickHouse 26.3.17.4 (the current default image — no CLICKHOUSE_IMAGE pin
# needed). Each record's `variant` is self-describing: it now carries the
# identity-defining budget/shape keys (max_inflight_mb, max_pending_batches,
# batch_max_mb, linger_ms, queue_cap, io_threads, shards, compression,
# clickhouse_cpus), and each record's `note` carries a per-record `limiter` verdict
# (sink / generator / indeterminate-checkpoint / budget) plus its evidence.
#
# Budget: MAX_INFLIGHT_MB is NOT pinned — the rig derives it per arm from the
# deployment sizing rule (2 × in-flight bytes / 0.5 low watermark), so it is always
# rule-compliant and never self-throttles the source below sink capacity. Override
# only to reproduce the invalidated first recording. QUEUE_CAP default is 256,
# MAX_PENDING_BATCHES default 8192.
export RESULTS=benchmarks/results/ch-sink-saturation.jsonl
export CLICKHOUSE_CPUS=8 SHARDS=4 IO_THREADS=4 DURATION_S=15 WARMUP_S=5 PAYLOAD=256
: > "$RESULTS"                                    # fresh; reuse the warm 26.3 server
for rep in 1 2 3; do                             # 3 reps/arm; charts median them
  # Null ceiling search (both formats, sync, threads 2/3/4/5/6/8): find the peak.
  # The binary self-matrixes THREADS_LIST into one arm per thread count.
  ENGINE=Null FORMAT=rowbinary ASYNC_INSERT=0 THREADS_LIST=2,3,4,5,6,8 \
    cargo run -p benchmarks --release --bin ch_sink_saturation
  ENGINE=Null FORMAT=native    ASYNC_INSERT=0 THREADS_LIST=2,3,4,5,6,8 \
    cargo run -p benchmarks --release --bin ch_sink_saturation
  # io-thread sensitivity pair at the peak: Null Native t4, IO_THREADS=8 vs the 4
  # already recorded above (Δ was +3.6%, within noise — not scaled across the matrix).
  RUN_ONE=1 THREADS=4 IO_THREADS=8 ENGINE=Null FORMAT=native ASYNC_INSERT=0 \
    cargo run -p benchmarks --release --bin ch_sink_saturation
  # One t12 contention datapoint (excluded from the ceiling claim).
  RUN_ONE=1 THREADS=12 ENGINE=Null FORMAT=native ASYNC_INSERT=0 \
    cargo run -p benchmarks --release --bin ch_sink_saturation
  # Real sink: MergeTree × {rowbinary, native} × {async 0, 1}, threads 2/4/8.
  for FMT in rowbinary native; do for A in 0 1; do
    ENGINE=MergeTree FORMAT=$FMT ASYNC_INSERT=$A THREADS_LIST=2,4,8 \
      cargo run -p benchmarks --release --bin ch_sink_saturation
  done; done
  # Part-size sweep (MergeTree Native sync t4): bigger batches → bigger parts.
  # The 4M-row arm carries a ~13 GB worst-case in-flight bound (printed at startup).
  for R in 262144 1048576 4194304; do
    RUN_ONE=1 THREADS=4 ENGINE=MergeTree FORMAT=native ASYNC_INSERT=0 \
      BATCH_MAX_ROWS=$R BATCH_MAX_MB=$((R / 2048 + 8)) \
      cargo run -p benchmarks --release --bin ch_sink_saturation
  done
  # Compression codec (MergeTree Native sync t4): zstd / off. The lz4 point IS
  # the MergeTree Native sync t4 matrix arm above (same variant identity), so it
  # is not re-run — the chart's lz4 bar reuses those reps.
  for C in zstd off; do
    RUN_ONE=1 THREADS=4 ENGINE=MergeTree FORMAT=native ASYNC_INSERT=0 COMPRESSION=$C \
      cargo run -p benchmarks --release --bin ch_sink_saturation
  done
  # Writer shards (MergeTree Native sync t4): 2 / 8. The 4-shard point is the
  # matrix arm above (SHARDS defaults to 4), so it too is reused, not re-run.
  for S in 2 8; do
    RUN_ONE=1 THREADS=4 SHARDS=$S ENGINE=MergeTree FORMAT=native ASYNC_INSERT=0 \
      cargo run -p benchmarks --release --bin ch_sink_saturation
  done
done                                              # → 33 arms × 3 reps = 99 records
```

For a **dedicated-server** ceiling (no client/server core contention), point the
rig at an external cluster instead of the local container:
`CLICKHOUSE_URL=http://host:8123 [CLICKHOUSE_USER=… CLICKHOUSE_PASSWORD=…]`.

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
