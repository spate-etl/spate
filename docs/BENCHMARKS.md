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

### ClickHouse Native vs RowBinary format (go/no-go, 2026-07)

Environment: M5 Max / 128 GB (dev laptop), Rust 1.96.1 release, ClickHouse
25.6 in Docker, single-threaded server (`max_threads=1`,
`max_insert_threads=1`), 200k rows/insert, medians over 15 interleaved reps.
Harness: `benchmarks/src/bin/ch_native_format.rs`
(`ROWS=200000 ITERS=41 REPS=15 SERVER=1`); raw JSON in
`benchmarks/results/`. Four schemas: `events` (mixed realistic),
`metrics` (fixed-width; regression guard), `dims` (LowCardinality-heavy),
and `dims_hc` (a client-only stress variant — `dims` with one LowCardinality
column at ~50k distinct values — guarding dictionary hash-collision
pathology).

Methodology note: the rig is now **symmetric** — both formats are timed
through their real `RowEncoder` impls (per-row `buffered_bytes` +
`finish_chunk`), reported as median-of-total-ns → f64 ns/row, with a bare
free-function RowBinary line kept as a wrapper-overhead reference. An earlier
revision of this table timed RowBinary at that bare function while Native
paid the full `RowEncoder` pipeline shape, and truncated ns/row to integers,
so the old and new numbers are **not** directly comparable.

**Client encode (ns/row, lower is better; symmetric rig — both formats via
their `RowEncoder` impls, bare-fn RowBinary shown as a wrapper reference):**

| Schema | RowBinary | RowBinary bare-fn | Native | Native vs RB |
|---|---|---|---|---|
| events | 32.9 (30.4M/s) | 32.1 | 64.3 (15.6M/s) | ~1.95× slower |
| metrics (fixed-width) | 10.4 (95.8M/s) | 9.9 | 32.7 (30.6M/s) | ~3.1× slower |
| dims (LC-heavy) | 26.3 (38.0M/s) | 25.8 | 43.7 (22.9M/s) | ~1.66× slower |
| dims_hc (LC stress) | 26.0 (38.4M/s) | 25.8 | 50.6 (19.8M/s) | ~1.95× slower |

**Compressed wire size (events, 200k rows):**

| Codec | RowBinary | Native | Native smaller |
|---|---|---|---|
| lz4 | 9.59 MB | 4.92 MB | **48.7%** |
| zstd:3 | 4.47 MB | 1.32 MB | **70.6%** |

(dims: lz4 75.3%, zstd 56.9% smaller; dims_hc: lz4 58.1%, zstd 46.2%;
metrics: lz4 35.1%, zstd 62.0% smaller. Wire encoding is byte-identical to
the previous pass — raw events 126→108 B/row.)

**Server CPU (events, `OSCPUVirtualTimeMicroseconds`, median over 15 reps):**

| Engine | RowBinary | Native | Native lower |
|---|---|---|---|
| `Null` (parse + block-form only) | 92.5 ms | 7.8 ms | **91.6%** |
| `MergeTree` (end-to-end) | 122.3 ms | 36.5 ms | **70.2%** |
| `MergeTree − Null` (format-independent) | 29.7 ms | 28.7 ms | ~equal (validates isolation) |

(Server CPU varies ±~10% run to run; the parse-isolated Native win is
consistently ~90%.)

Interpretation: Native moves the row→column pivot off the server onto the
client. It costs **~1.7–3.1× more client encode CPU** (schema-dependent) but
cuts **server parse CPU ~92%** (and ~70% end-to-end on MergeTree) and
**compressed wire ~50–75%**. The relative gap is largest on the fixed-width
`metrics` schema (~3.1×), where RowBinary is essentially a memcpy; in absolute
ns/row the Native cost is highest on `events` and the `LowCardinality`-heavy
`dims`/`dims_hc` schemas (dictionary + columnar building the server would
otherwise do row-by-row). The `MergeTree − Null` delta is format-independent
(29.7 ms ≈ 28.7 ms), confirming the parse-isolation method.

Encoder efficiency pass (measured, not guessed): making the per-row
`buffered_bytes` seal-check O(1) (a cached size refreshed every 16 rows) and
`#[inline]`-ing the column dispatch were kept. A hand-rolled FxHash for the
LowCardinality dictionary was **tried and reverted** — it collided badly on
high-cardinality keys (`city`, 5k distinct) and ran ~3× slower than the
default SipHash. The dictionary hasher was then switched to **foldhash**
(SMHasher-clean avalanche, per-instance seeding): the wire bytes stay
identical — dictionary order is first-seen, not hash-order — and dims Native
encode improved ~45%. (`dims_hc`, one LowCardinality column at ~50k distinct
values, was added to guard the new hasher against collision pathology.) The
first-record field-name check was also moved **off** the per-row path
(probe-based, run once), fixing a ~1.6–1.8 ns/field/row regression, alongside
micro-optimizations: a single up-front reserve in finalize-block, `Array`/`Map`
offsets stored as LE bytes at push time, and the `LowCardinality` key-width
match hoisted out of the write loop. The residual — Native ≈ **1.7–3.1×
RowBinary encode** depending on schema — is the irreducible serde
value-at-a-time dispatch + columnar transpose cost (scattered per-column
buffers + a finalize concatenation copy), deliberately traded for the
server/wire wins.

**Verdict: NO-GO on the strict gate** (which required *no* client-encode
regression — the fixed-width `metrics` schema still regresses well past 5%),
so **RowBinary stays the default and Native ships opt-in** (`format: native`).
Native is the better choice when the ClickHouse cluster is CPU-bound or
egress/wire is the constraint — the offload is exactly the point. RowBinary is
better when client CPU is the constraint. Not yet run: the sustained
end-to-end pass (Rig D) — point `e2e_kafka_clickhouse` at a `format: native`
sink and sample `system.metric_log`/`part_log`.

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

Accounting note (2026-07 correction): an earlier revision of this table
reported window-scoped `records` against lifetime `sink_rows_total`
(warmup + window + drain), which read as rows exceeding records by ~10%.
The harness now reports `produced_total` alongside and asserts
conservation; a quiet-machine re-run shows `sink_rows_total ==
produced_total` exactly (e.g. 475,555,328 == 475,555,328 @ 1 thread) and
healthy 1→2 thread scaling (36.6M → 54.4M records/s). The metric itself is
proven exact by `sink_records_metric_matches_rows_written_exactly` in
`etl-core`.

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
