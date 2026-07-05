# Benchmarks

Methodology and recorded results. Numbers are machine-specific; every table
states its environment. Re-run instructions accompany each harness.

## Tiers

1. **Micro** — `cargo bench` (criterion + divan `AllocProfiler`) in
   `crates/etl-core/benches` and `crates/etl-avro/benches`.
2. **Framework overhead** — `benchmarks/pipeline_synthetic` (release binary,
   JSON report): in-memory source → chain → null sink; no broker in the loop.
3. **At scale** — `benchmarks/e2e_kafka_clickhouse` + `benchmarks/loadgen`
   against local containers or external clusters (env-configured endpoints).

Nightly CI compares criterion baselines with `critcmp`; divan allocation
assertions are hard failures.

## Recorded results

### Zero-copy seam (design validation, 2026-07)

Environment: Apple M-series (dev laptop), Rust 1.96.1, synthetic parse →
map → filter → flat_map → shard-encode chain, batch 512, ~5,340
records/drive. Source: seam prototype (branch
`worktree-agent-a162b0ef2e0a29f4e`).

| Variant | ns/record | Throughput | Allocations |
|---|---|---|---|
| Borrowed payloads, static chain | ~40 | 25.2M rec/s | **0/record** (1 Arc per batch, hard-asserted) |
| Borrowed, dyn boundary per batch | ~43 (+9%) | 23M rec/s | 0/record |
| Borrowed, dyn per stage | ~43 on 3 stages; scales with stages, blocks vectorization | | 0/record |
| **Owned payloads**, static chain | ~148 (**3.7× slower**) | 6.8M rec/s | 38,512/iteration |

Conclusions: zero-copy justifies the design; erase once per batch, never per
stage; the +9% dyn-per-batch delta amortizes to ~1–2% at realistic
per-record cost.

Production chain (post-merge, `crates/etl-core/benches/chain.rs`, batch 512
payloads → 1,536 records via flat_map, full metrics accumulators on):
borrowed **~9.2 ns/record, 109M records/s, 0 allocations/record** (5/iter
fixed); owned equivalent ~23.5 ns/record with one allocation per record —
2.5× slower on small payloads, consistent with the spike's 3.7× at larger
sizes. The counting-allocator integration test (`tests/chain_alloc.rs`)
hard-fails if per-iteration allocations scale with record count.

### Kafka consumer topology A/B (decision gate, 2026-07)

Environment: M5 Max 18-core / 128 GB, single local Kafka 4.1 broker
(Docker), rdkafka 0.39 (vendored librdkafka), 10M × 256 B messages
pre-produced, medians over 3–9 repetitions. Modes: **A** = one
`BaseConsumer` per thread; **B** = one consumer + `split_partition_queue`
fanned across threads. Harness: `benchmarks/src/bin/kafka_topology.rs`.

| Config | A: per-thread | B: split queues | B vs A |
|---|---|---|---|
| 16 partitions, 4 threads, no work | 7.74M rec/s | 2.90M rec/s | −62% |
| 16 partitions, 4 threads, 10 µs/record | 385.0k rec/s | 372.8k rec/s | **−3.2%** |
| 64 partitions, 4 threads, no work | 7.84M rec/s | 2.86M rec/s | −64% |
| Controls (16p, no work) | A 1 thread: 3.32M | B 1 thread: 3.69M; B 8 threads: 2.12M | |

Interpretation: the zero-work gap is a **single-librdkafka-instance fetch
ceiling** (~3.3–3.7M rec/s here regardless of polling arrangement; split
queues on one thread *beat* main-queue polling by 11%; extra threads on one
instance reduce throughput). Mode A's headline is 4 independent instances,
not a better polling model. With ≥ ~1.1 µs/record of real work at 4 threads,
per-record cost dominates and both modes run at 93–96% of theoretical.

**Decision: single consumer + split partition queues confirmed** for
etl-kafka. Caveats recorded: re-measure the per-instance ceiling against
multi-broker clusters (fetchers parallelize per broker); very-high-throughput
trivial pipelines with many threads remain the one shape favouring
per-thread consumers — it stays a documented escape hatch behind the Source
abstraction.

Semantics probes (same environment, `benchmarks/src/bin/kafka_semantics.rs`):
split-queue polls **do** reset `max.poll.interval.ms` (30 s of split-only
polling at a 10 s interval: zero evictions); messages fetched before the
first split land on the main queue (choreography: pause on assign → split →
distribute → resume); revoked partitions' queues go permanently silent and
queues must be re-split after every rebalance; `pause()` purges split-queue
prefetch and resume redelivers gap-free (1010/1010); rebalance/stats
callbacks fire only on the main-queue polling thread.

### ClickHouse insert transport (design validation, 2026-07)

Environment: dev laptop, ClickHouse 25.6 in Docker, 2M rows (~30 MB),
clickhouse crate 0.15.1. Source: spike branch
`worktree-agent-a01a4cdccd8d0ab10`
(`crates/etl-clickhouse/examples/rowbinary_spike.rs`).

| Path | Time |
|---|---|
| Typed `Insert` (validation on) | 116 ms |
| Typed `Insert` (validation off) | 95–102 ms |
| Pre-encoded RowBinary via `InsertFormatted` | encode 8.8 ms + send 103–105 ms |

Transport parity (~20M rows/s locally); typed-path validation costs
~15–20% and a `DESCRIBE TABLE` roundtrip. Dedup verified: same
`insert_deduplication_token` + identical batch → deduplicated; requires
`insert_deduplicate=1`, `wait_end_of_query=1`, and on plain `MergeTree` a
non-zero `non_replicated_deduplication_window` (server default 0 silently
disables dedup; replicated tables default to a window of 100).

### Framework baselines (permanent harness, 2026-07)

Environment: M5 Max 18-core / 128 GB (dev laptop), quiet machine, Rust
1.96.1, release build. Harness: `benchmarks/` binaries; raw JSON in
`benchmarks/results/`. Reproduce per the usage headers in each binary.

**Framework ceiling** (`pipeline_synthetic`: generator source → real chain
(filter, zero-work) → real sink pool → null writer; 256 B payloads,
2 shards, 2 I/O threads, 64 KiB chunks, 65,536-row batches, 5 ms linger):

| Pipeline threads | Records/s | Per thread |
|---|---|---|
| 1 | 37.0M | 37.0M |
| 2 | 58.9M | 29.5M |
| 4 | 48.4M | 12.1M |

Interpretation: ~27 ns/record full-framework cost at one thread (lane
poll, ack issue/resolve, chain, encode, chunk, queue handoff, batching,
checkpoint commits — the pure chain alone is ~9 ns). Zero-work records
saturate the fixed egress side (2 shard workers) near 59M records/s, so
4 threads regresses — this is a ceiling harness, not a scaling proof;
with real per-record work the pipeline threads dominate and the sink side
stops being the limiter.

⚠ Finding for tuning documentation: with the *default* sink batch config
(500k rows) and an unthrottled source, the 256 MiB in-flight budget fills
in tens of milliseconds while a batch needs hundreds of thousands of rows
to seal — per-thread pause controllers then sit in 500 ms `min_pause`
cycles and throughput collapses ~24× (measured 2.5M rec/s in that state).
Real sources rarely outrun the budget this way, but the interaction
(batch-seal thresholds vs budget watermarks vs `min_pause`) deserves a
documented sizing rule; recorded for the hardening pass.

**Consumer topology confirmation** (`kafka_topology`, 16 partitions ×
4 threads × 10 µs/record × 10M × 256 B, single local broker, 2 reps/mode):

| Mode | Records/s (reps) | vs per-thread |
|---|---|---|
| Per-thread consumers | 384.5k / 385.1k | — |
| Single + split queues | 376.2k / 376.8k | **−2.2%** |

Reproduces the spike's −3.2% at realistic work on the permanent harness.
(A contaminated earlier run under concurrent compile load showed −24% —
single-instance fetch is more sensitive to CPU starvation; benchmark on a
quiet machine.)

**Full pipeline smoke** (`e2e_kafka_clickhouse`, local containers, 60 s @
100k records/s target, 4 partitions, 2 threads, 64 B rows):

| Metric | Value |
|---|---|
| Produced (60 s window) | 6.02M |
| Rows in ClickHouse (window + 30 s grace) | 5.37M (89.4k rows/s) |
| Sink flush p99 | 24.7 ms |
| Backpressure pauses | 0 |

The un-drained tail replays on the next run — at-least-once, as designed.
Known gap: `etl_e2e_latency_seconds` renders no samples — the instrument
exists but observation is not yet wired into the sink ack path (recorded
framework TODO). Related harness lesson now encoded in the binaries:
metric handles created before `metrics::install` bind to the noop
recorder and silently render nothing — the harnesses install the recorder
first and the examples should be audited for the same pattern.
