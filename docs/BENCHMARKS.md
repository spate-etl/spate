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
