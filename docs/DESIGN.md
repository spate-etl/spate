# Spate Design

This document records the architecture of Spate and the reasoning behind
its decisions. It is the source of truth when code and intuition disagree.

## Goals

- At-least-once delivery, end to end, including across consumer rebalances,
  crashes, and graceful shutdown.
- Extract maximum single-node performance: borrowed data and zero per-record
  allocations on the hot path, CPU work pinned to pipeline threads, I/O
  concurrency at the edges.
- Small, stable abstractions so technology-specific connectors (Kafka,
  ClickHouse, ...) are thin and third parties can implement their own.
- A chaining pipeline API defined in Rust code, where each operator is a
  stateful closure and a chain compiles to one loop; YAML configures
  connectors and tuning, never the operator graph.
- Kubernetes-native operations: one pipeline per process, scrape-based
  metrics, health probes, drain-on-SIGTERM.

## Non-goals (v1)

- Exactly-once / transactional sinks. Replays happen; sinks must tolerate
  duplicates (see *Delivery semantics*).
- General DAG topologies, stateful operators, windowing, watermark-based
  event-time processing. Pipelines are linear with multi-output routing at
  the sink end.
- Dead-letter queues. Record-level error policies are **Skip** (count and
  continue) or **Fail** (stop the pipeline) — always surfaced via metrics.
- Config hot-reload. Reconfigure via rolling restart (checksum-annotation
  pattern).

## Invariants

The properties the engine is arranged around. This list is canonical: the
statements below are what `AGENTS.md`, `CONTRIBUTING.md` and the pull request
template each restate for their own audience, and they cite these numbers so a
disagreement between them is a mismatched identifier rather than a difference in
phrasing somebody has to notice.

`scripts/check-invariants.sh` compares the *set of numbers* across those files.
It cannot compare their wording, so a restatement that drifts in substance while
citing the right number passes — the statements here are what the others must
mean, and any exception belongs in this list rather than only in the restatement.

Most changes touch none of these. A change that touches one is not thereby
wrong; it needs to say how the property still holds, and that is the review.

Numbering is append-only. A property that is ever retired keeps its number and
is marked retired rather than freeing it for reuse, so a pull request from a
year ago still means what it said.

- **INV-1 — delivery is at-least-once.** A source watermark is never committed
  past unacknowledged data, including across rebalances and shutdown. Everything
  below exists to make this one affordable; see *Delivery semantics*.
- **INV-2 — source threads never block on a channel send.** Backpressure is
  `try_send` plus `Source::pause` plus continuing to poll. A blocked poll loop
  gets the consumer evicted from its group, which is a worse failure than the
  one it was avoiding. See *Backpressure*.
- **INV-3 — the checkpoint tracker stays synchronous and free of async
  runtimes.** It is loom-tested, and loom can only model what stays this shape.
  See *Records and checkpointing*.
- **INV-4 — acks never block behind data.** The ack path is unbounded and
  atomic. An ack queued behind the data it acknowledges is a deadlock waiting
  for backpressure to arrive.
- **INV-5 — the sink worker's intake path never awaits outside its `select!`.**
  Anything it blocks on sits in a branch alongside the drain-deadline branch, or
  the deadline is not polled while it waits and shutdown deadlocks. See *Sink*.
- **INV-6 — no connector types in `spate-core`'s public API**, and no 0.x
  dependency types in any public trait bound. Those cannot enter our semver
  surface. The `metrics` facade is the one sanctioned exception, because the
  instrumentation API *is* that facade. See *Dependency policy*.
- **INV-7 — record error policies are Skip or Fail only**, and both are surfaced
  through metrics rather than only logged. There is deliberately no third policy
  that drops a record without counting it — see *Non-goals*.
- **INV-8 — metrics handles are pre-registered at build time.** A metric name or
  label resolved on the per-record path is a per-record allocation and a lookup
  in the one place neither is affordable. See *Metrics*.
- **INV-9 — every metric lives under the `spate_` umbrella.** The framework owns
  the reserved stage roots; connector and user families register through a
  `Meter`, which prefixes them and rejects a namespace shadowing a reserved
  root. The one sanctioned exception is a metric registered on the raw `metrics`
  facade, which is the deliberate opt-out for a name that must sit outside
  `spate_` — an exporter's own series, or one a downstream contract fixes.
- **INV-10 — a gauge series has exactly one live owner per process.** A
  duplicate claim on the same key is refused rather than shared. Assembly makes
  it fatal (`BuildError`/`StartError`); direct construction cannot fail a build,
  so it logs and the loser becomes a *shadow* — it still counts, since counters
  sum, but it publishes no gauge. Two live owners would be two writers racing to
  describe one piece of state, and the exposition cannot show that happened.

One documentation page is normative rather than descriptive:
[`user-guide/02-concepts/08-work-assignment.mdx`](user-guide/02-concepts/08-work-assignment.mdx).
Its own numbered invariants name the property tests that enforce them, and are
separate from these — changing the balancer means changing that page in the same
commit.

## Process anatomy

One process runs one pipeline.

```
                    ┌────────────────────────────────────────────────┐
 pinned std thread  │ lane.poll → deserialize (borrowed) → operator  │──try_send──▶ per-shard
 (× N, cgroup-aware)│ chain (map/filter/flat_map, monomorphized)     │              bounded queues
                    └────────────────────────────────────────────────┘                   │
                         ▲            full? pause lanes, keep polling                     ▼
                         │                                              ┌──────────────────────────────┐
                    ┌──────────┐   acks (unbounded, never block)        │ tokio runtime (small):       │
                    │ source   │◀───────────────────────────────────────│ shard workers: batch, seal,  │
                    │ control  │   watermarks → store/commit            │ rotate replicas, retry;      │
                    │ plane    │                                        │ checkpointer; admin server   │
                    └──────────┘                                        └──────────────────────────────┘
```

- **Pipeline threads** (plain `std::thread`, optionally core-pinned,
  count derived from `available_parallelism` minus an I/O reserve, always
  overridable in YAML): each owns a set of source *lanes* and runs
  poll → deserialize → operator chain → route-to-shard, single-threaded.
  Deserialization stays here — payload borrows from the source's buffers
  cannot cross threads, and this keeps all CPU work on pinned cores.
- **I/O runtime** (shared multi-thread tokio, default 2 workers): sink shard
  workers, flush timers, the checkpointer, the admin HTTP server, and any
  async edge work (e.g. schema-registry fetches).
- **Control plane**: a controller services source events (rebalances,
  statistics), owns shutdown, and runs commit ticks.

**Concurrency is not parallelism.** `io_threads` sizes the tokio worker pool
that drives *async* edge work — a small M driving N≫M concurrent tasks, since
every task yields at its `await`. It is deliberately *not* one thread per
in-flight operation, so it should not be scaled to the number of source lanes or
sink in-flight inserts. CPU-bound decode/encode runs on the **pipeline threads**
instead. A source's lane count is read *concurrency*: effective decode
parallelism is `min(lanes, pipeline.threads)`, so lanes beyond the driver-thread
count add read-ahead memory and open connections without more throughput —
size lane counts (Kafka partitions per member, a coordinator's
`max_in_flight`) near `pipeline.threads`. The S3 backfill source
makes this concrete: each lane holds at most one `prefetch_bytes` read-ahead
window and, because an object read is a sequence of bounded ranged GETs each
drained into memory before hand-off, never an idle open connection while
back-pressured.

## Source abstraction

Split into a control plane and a data plane:

- `Source` (control plane): lifecycle, lane assignment/revocation events,
  watermark commits, pause/resume. Poll-based — no `futures::Stream`. For
  Kafka, librdkafka already owns network I/O on its own threads; a Stream
  wrapper only adds waker overhead and `'static` bounds that break borrowing.
- `SourceLane` (data plane): a pollable unit pinned to one pipeline thread,
  yielding borrowed payloads. For Kafka a lane is a partition queue; for the
  in-memory test source, a single lane.

**Framing.** Sources emit `RawPayload` bytes; a deserializer decodes one
payload. Streaming sources must first cut a byte stream into records — that
step is *framing*. `spate_core::framing` owns only the **seam** — the
`RecordFramer` trait, the `FramerWriter` decompressor shim, and the
`FramingContract` handshake — because how a byte stream splits into records is
a property of the format, not the transport. The concrete framers are owned by
the **format** crates: `spate-json` provides the newline-delimited `NdjsonFramer`,
a future `spate-avro` an object-container block framer, and so on. A source stays
format-agnostic and receives its framer at pipeline-assembly time
(`S3Source::with_framer`), so one framer serves every streaming source (S3
today, HTTP/WebSocket tomorrow) and no source hard-codes a format. A framer
must be a pure function of the byte stream so resume-by-record-index is
deterministic. `Source::framing_contract` declares whether the source emits one
record per payload (`PerRecord`, what a framed source reports) or whole payloads
the deserializer frames (`WholePayload`, the default); the framework threads it
through `ChainCtx` so a deserializer builder derives its granularity and rejects
double-framing. Columnar, footer-indexed formats (Parquet/ORC/Arrow) have no
per-record byte boundary and do not fit this byte-payload model — they would
arrive through a separate record-batch reader source, not a framer.

**Bounded sources.** A source whose input can be permanently exhausted (an
object-storage backfill) reports `SourceEvent::Drained` from `poll_events`
once every lane has yielded its final batch (a lane's exhaustion may only be
decided by a poll that returned nothing *after* its final batch was pushed —
the poll→push→poll sequencing on the owning thread makes this race-free).
The controller then exits its loop into the ordinary drain choreography and
the pipeline finishes with `ExitState::Completed` — no external shutdown
trigger. `Completed` after a drained exit is a strong claim: the backstop
after the final commit converts the run to `Failed` if any batch is still
unacknowledged (a wedged sink) **or** the final watermark commit did not
persist — a bounded job never reports itself done with data or offsets left
behind. Unbounded sources never emit `Drained`.

The Kafka source uses a **single consumer per process** with
`split_partition_queue`: one consumer-group member per pod (small groups,
fast rebalances), partitions mapped m:n onto pipeline threads under local
control, borrows kept thread-local. Consumption parallelism is still bounded
by partition count — identical to per-thread consumers — the win is group
scale and unified drain choreography. Validated empirically (see
`docs/benchmarks/kafka-topology.mdx`): split-queue polls reset
`max.poll.interval.ms`, so
group liveness never depends on the controller; at realistic per-record work
the throughput delta vs per-thread consumers is ~3%; a per-thread-consumer
fallback remains a documented escape hatch behind the same traits.

Proven choreography spate-kafka must implement (spike-verified):

- **Assign**: `post_rebalance(Assign)` runs on the controller's main-queue
  poll and only gets `&BaseConsumer` — `pause()` all assigned partitions
  there (stops fetch before it starts, preventing pre-split messages from
  landing on the main queue), then after the callback returns the controller
  (holding the consumer `Arc`) splits every partition queue, distributes
  them to pipeline threads, and `resume()`s. Any message that still appears
  on the main queue is routed defensively, never dropped.
- **Revoke**: `pre_rebalance(Revoke)` → trip the drain barrier for owning
  threads → flush and ack → synchronous store+commit → drop the revoked
  `PartitionQueue`s → return from the callback. Dropping a `PartitionQueue`
  restores forwarding to the main queue, so queues are dropped only once
  revocation is complete. Queues are re-split after **every** rebalance;
  retained-partition queues happen to keep working across eager rebalances
  but this is not relied upon.
- The consumer lives in an `Arc` from the start (`split_partition_queue`
  requires it; each `PartitionQueue` keeps a clone). Rebalance and
  statistics callbacks fire only on the main-queue polling thread — stats
  parsing stays off pipeline threads. Idle lanes must block briefly on
  their queue (zero-timeout polling busy-spins); `set_nonempty_callback` is
  the planned refinement. `enable.partition.eof=false` is forced.

## Frozen v1 contracts

Validated by the zero-copy seam spike (borrowed static chain: ~40 ns/record,
25M records/s, **0 allocations/record** hard-asserted by a counting
allocator; owned records measured 3.7× slower) and the ClickHouse RowBinary
spike. The load-bearing shapes:

```rust
/// Data plane: a pollable unit pinned to one pipeline thread.
pub trait SourceLane: Send {
    type Batch<'a>: PayloadBatch<'a> where Self: 'a;
    fn poll(&mut self, max_records: usize) -> Option<Self::Batch<'_>>;
}

/// One poll's payloads, streamed out, all borrowing the batch lifetime.
pub trait PayloadBatch<'buf> {
    fn next_payload(&mut self) -> Option<RawPayload<'buf>>;
    fn ack(&self) -> &AckRef;              // one AckRef per poll batch
}

/// Lifetime→type family: lets a lifetime-parameterized record type cross
/// generic and dyn boundaries. `Owned<T>` is provided for non-borrowing
/// records.
pub trait RecFamily: 'static { type Rec<'buf>: Send; }

/// One payload in, 0..N records out, push-style. Dyn-compatible.
pub trait Deserializer<F: RecFamily>: Send {
    fn deserialize<'buf>(&mut self, raw: &RawPayload<'buf>, ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, F::Rec<'buf>>) -> Result<(), DeserError>;
}

/// Statically composed push stage (map/filter/flat_map monomorphize into
/// one loop). `CollectorFor<F>` is the family-erased, dyn-compatible form
/// used by flat_map's stack-borrowed `Emitter`.
pub trait Collector<T> { fn push(&mut self, rec: Record<T>) -> Flow; }

/// THE SEAM — the one erasure boundary, one virtual call per batch.
/// Records are born (deserialized) and die (serialized into shard frames)
/// inside a single call; no lifetime-parameterized type is ever stored
/// across it, so `Box<dyn RunnableChain>` is legal.
pub trait RunnableChain: Send {
    fn push_batch<'buf>(&mut self, batch: &mut dyn PayloadBatch<'buf>) -> PushOutcome;
    // + flush/drain lifecycle hooks
}

pub struct Record<T> { pub payload: T, meta: RecordMeta /* Copy */, ack: AckRef }

/// Sink I/O half: framework owns batching/routing/retry/rotation; a
/// connector writes one sealed batch to one replica endpoint.
pub trait ShardWriter: Send + Sync + 'static {
    type Endpoint: Send + Sync + 'static;
    fn write_batch(&self, ep: &Self::Endpoint, batch: &SealedBatch)
        -> impl Future<Output = Result<(), SinkError>> + Send;
}

/// Encoded on pipeline threads by the sink's pipeline-thread half
/// (a `RowEncoder`), shipped to workers as sealed byte frames.
pub struct SealedBatch {
    frames: Vec<Bytes>,          // pre-encoded wire frames (~1 MiB each)
    rows: u64, bytes: u64,
    dedup_token: String,         // deterministic per batch
    acks: Vec<AckRef>,
}
```

Deltas applied over the spike code at freeze time: `PushOutcome::Blocked`
carries a `BlockReason` — `Capacity` (sink pressure: engages the
backpressure controller) vs `NotReady` (an upstream dependency such as a
schema fetch: retried without pausing the source, counted on
`spate_deser_not_ready_total`); `push_batch` returns a
`PushOutcome` carrying a resume cursor (partial-batch backpressure: never
re-run operators for already-pushed records) instead of the spike's
`Result<usize, _>`; the boundary trait gains flush/drain hooks;
`RawPayload` carries the message key (for shard routing hashes); mapping to
a *different* borrowed family uses the explicit `map_rec::<Family, _>()` /
`flat_map::<Family, _>()` builder methods with `fn`-item arguments (closure
inference limitation, measured acceptable — see the E0582 note on the
chain-builder module).

## Records and checkpointing

`Record<T>` carries the payload, a `Copy` metadata struct (partition, offset,
event time, key hash), and an `AckRef` — a clone of an `Arc<AckState>` created
**per source poll batch** (Vector-finalizer style). Dropping a record resolves
its share: filter drops count as success; `flat_map` children clone the same
Arc; multi-sink routing clones it per output with worst-status merge. When the
last clone drops, `(partition, seq, status)` flows over an unbounded channel
to the checkpointer.

The checkpointer is a synchronous, tokio-free module (loom-tested). Per
partition (and per *epoch* — bumped on every rebalance so stale acks from
revoked assignments are discarded) it keeps a ring of outstanding batch seqs,
pops the contiguous acknowledged prefix, and advances the committable
watermark. Watermarks are stored on advance and committed on an interval
(default 5s); revocation and shutdown drain then commit synchronously.
The commit tick also broadcasts a chain flush to every driver thread,
sealing below-target chunk buffers: a low-volume split branch's partial
chunk would otherwise hold its records' acknowledgements — and every
watermark behind them — for as long as the branch takes to fill 64 KiB,
which under sustained load is unbounded (the driver's idle flush needs an
empty poll it never gets). Worst-case watermark staleness is therefore
~two commit intervals plus the sink linger and write, degenerating to the
idle-flush path when the pipeline goes quiet.

At-least-once invariant: **never commit past unacknowledged data.** A failed
batch stalls its partition's watermark (alert on
`spate_checkpoint_watermark_age_seconds`), it never silently advances.

Two contracts guard the drop-resolves-Delivered convention:

- **Collections fail by default.** Individual records keep drop=Delivered,
  but every *collection* of acknowledgements on the sink path (encoded
  chunks, worker ledgers, batch accumulators) is an `AckSet`, which fails
  its handles on drop and delivers only explicitly after a durable write.
  Teardown anywhere — a dropped queue, an aborted worker task, a
  runtime shutdown without drain — therefore stalls watermarks instead of
  committing unwritten data.

- **Teardown fails unsent output.** Any component holding acknowledgement
  handles for data not yet durably written must `fail()` them in its `Drop`
  (the chain's terminal stage does this for parked chunks and partial
  buffers; sink workers fail abandoned batches). Over-failing is always
  safe — it costs replay, never loss.
- **Eager assignment, additive gains.** `Checkpointer::begin_epoch`
  replaces all trackers, so `LanesAssigned` must describe the full
  assignment following a full revocation; the controller defensively
  drains and commits live lanes if a source violates this. Incremental
  gains use `SourceEvent::LanesAdded` / `Checkpointer::extend_epoch`
  instead: fresh partitions join the *current* epoch without disturbing
  in-flight acks — the coordinated-source gain path.

### Considered alternative: fail-by-default record acknowledgements

Inverting the record-level default (drop = Failed, explicit `deliver()`)
was evaluated as structural hardening: forgetting to deliver would cost
replay, while today forgetting to *fail* at a teardown seam costs silent
loss. Verdict: **keep drop=Delivered for records, fail-by-default for
collections** (the `AckSet` contract above). Rationale: every legitimate
record drop — `filter`, Skip policies, post-write release — is
framework-mediated and zero-ceremony today, and matches the
Vector-finalizer semantics the design borrows; inverting would turn each
of those sites into explicit bookkeeping whose failure mode (forgotten
`deliver`) is a watermark stall that halts pipelines on correct data.
Teardown loss, by contrast, only ever materializes where acknowledgements
travel in bulk — precisely the seams `AckSet` now guards, with regression
tests on each (handoff drop, queue drop, worker/runtime teardown).

## Operator chain

Push/collector model (Neumann-style): each operator implements a push
interface and calls its downstream inline; `flat_map` emits through a
stack-borrowed emitter, so fan-out allocates nothing. Within a chain,
composition is fully static and monomorphized — the whole chain compiles to
one loop. Type erasure happens exactly once, at the chain boundary, with one
virtual call **per batch** — per-record dyn dispatch would defeat
cross-operator inlining and vectorization.

Partial-push handling: if the terminal router's `try_send` fails mid-batch,
the boundary records the exact resume index; already-pushed records are never
re-processed (sealed-but-unsendable chunks park inside the terminal stage and
drain first on resume).

Builder ergonomics note: bare closures infer everywhere for owned-record
chains; for **borrowing** families, `map`/`try_map` closures hit a language
limit (E0582 — higher-ranked lifetimes must appear in an associated-type
projection), so those two combinators offer a `map_rec`/`try_map_rec` tier
that takes plain `fn` items (naturally higher-ranked). `filter`, `inspect`,
and `flat_map` are unaffected.

## Backpressure

Two layers, one invariant.

**Sizing rule** (motivated by a ~24× throughput collapse observed under an
unthrottled source with default settings — hand-recorded during the tuning
investigation, no committed record; see
`docs/benchmarks/framework-overhead.mdx`): the
in-flight budget must comfortably hold everything the sink legitimately
keeps in flight, or steady-state operation lives above the high watermark
and the pause controller duty-cycles at `min_pause`:

```text
max_inflight_bytes × low_ratio  ≥  headroom (≈2×) × (
    shards × inflight.max_per_shard × batch.max_bytes   // pending writes
  + shards × queue_capacity × chunk.target_bytes )      // queued chunks
```

Worked example: the defaults (2 shards × 2 in-flight × 128 MiB batches ≈
512 MiB pending alone) exceed the default 256 MiB budget — fine for
throttled sources that never fill batches, wrong for saturated ones. For a
saturated pipeline either raise `backpressure.max_inflight_bytes` to ≥ 2 GiB
or cap `batch.max_bytes` near `budget × low_ratio / (2 × shards ×
max_inflight)` (16 MiB with the defaults). The flagship example's YAML
carries this rule next to the knobs.

1. **Passive**: bounded per-shard queues plus a global in-flight byte budget.
2. **Active**: when a queue rejects or the budget crosses the high watermark,
   the source loop pauses the offending lanes and *keeps polling* (serving
   rebalance callbacks and liveness); it resumes under hysteresis (low
   watermark + minimum pause).

**Invariant: a source thread never blocks on a channel send.** A poll loop
parked in `send()` stops calling `poll()`, trips `max.poll.interval.ms`, gets
evicted from the consumer group, and turns sink slowness into a rebalance
storm. Acks are likewise never allowed to block: the ack path is
unbounded/atomic by construction.

A connector's own client-side buffering is outside the in-flight budget and is
left at the client library's defaults rather than pinned by the framework: the
budget governs what the pipeline holds, not what a driver prefetches. The Kafka
source therefore sets neither `queued.min.messages` nor
`queued.max.messages.kbytes`, and a deployment that needs that memory bounded
caps it through the source's `rdkafka` passthrough. (This crate previously
forced `queued.min.messages` to 1000 as a memory backstop and measured 11x
slower end-to-end on a backlogged consumer — 193k against 2.11M msg/s; sizing
guidance now lives with the connector.)

## Sink

The framework owns everything generic; a connector implements only a small
`ShardWriter` ("write this sealed batch to this replica endpoint").

**Routing.** The terminal stage routes every record to a shard strictly
before encoding. Two tiers share one seam: meta-only `ShardRouter` (default
`KeyHashRouter` — key hash, falling back to a stable partition hash) and
record-aware `RecordRouter<F>` (full `Record` access, mirroring
`RowEncoder<F>`'s family-generic shape) for payload-derived shard affinity —
the shape a ClickHouse cluster-parity router needs, and the only way to
route `flat_map` children independently, since children inherit their
parent's `RecordMeta`. A blanket bridge lifts every `ShardRouter` into
`RecordRouter`, so the builder takes one bound, existing routers work
unchanged, and a type implements exactly one of the two (the bridge makes
both a coherence error). Routers are pure and cheap: per record, on pipeline
threads, no I/O, no per-call allocation.

Per **shard**: one worker task accumulates rows to `max_rows` / `max_bytes` /
`linger`, seals the batch, then dispatches up to `max_inflight` (default 2)
concurrent flushes rotating across healthy replicas (round-robin skipping
open circuit breakers). Failures retry the **same sealed batch** on the next
healthy replica with capped exponential backoff. Per-shard-worker (rather
than per-replica) keeps batches full-sized — ClickHouse wants few big inserts
— while `max_inflight` still provides replica parallelism. When no replica of
a shard is circuit-closed, new write attempts park until the earliest of three
things: the next half-open probe window, an in-flight probe reporting, and a
re-check heartbeat clamped to `[100ms, 30s]`. The middle term matters because a
shard whose every replica is already probing has no window to wait for; the
heartbeat is a backstop against a wake that never arrives, not the recovery
path. Parked attempts hold their in-flight permits, so intake stalls and the
shard back-pressures the source (surfaced as `spate_sink_shard_healthy == 0`;
with a bounded `retry.max_attempts` a batch instead fails and replays
after restart — the watermark stalls either way). Records are never rerouted
to another shard, which would break placement parity and the dedup tokens.

ClickHouse specifics: direct-to-shard writes against local tables
(`internal_replication=true`) rather than Distributed-table inserts (bigger
blocks, less merge pressure, and a synchronous server ack that checkpointing
requires); one `INSERT` per sealed batch carrying a deterministic
`insert_deduplication_token` so in-session retries are idempotent. Writing to
one healthy replica and letting `ReplicatedMergeTree` replicate is the
durability model, and its explicit boundary: replication is asynchronous
(`insert_quorum=0`), so a batch acked by a replica destroyed before the part
propagates is lost — the edge of the sink's at-least-once guarantee, closable
per-sink via an operator-supplied `insert_quorum` at the cost of shard write
availability (the connector guide documents the verified quorum-plus-dedup
retry semantics). Rows are
encoded **on the pipeline threads** by spate-clickhouse's own serializer (the
crate's is private — a semver win, verified round-tripping against a live
server) and shipped as pre-formatted frames through
`Client::insert_formatted_with` + `InsertFormatted::send(Bytes)` — the same
transport the crate's typed path uses internally. The default format is
row-wise **RowBinary**; an opt-in columnar **Native** encoder
(`format: native`) transposes each chunk into one self-describing block —
proven to cut server parse CPU ~90% and wire ~35–75% (schema- and
codec-dependent) at ~2–3× the client
encode cost (see `docs/benchmarks/clickhouse-format.mdx`), so it ships opt-in
with RowBinary the
default. Both use the same frame transport: a Native insert body is a stream
of concatenated blocks, so batching/acks are format-agnostic. The terminal
stage's `RowEncoder` gained defaulted `buffered_bytes()`/`finish_chunk()`
hooks so a columnar encoder can buffer a block and flush it at each chunk
seal; the stage clones one encoder per shard so shards never share block
state. Per-insert settings via
`with_setting(..)` (`with_option` is deprecated), always including
`insert_deduplicate=1` and `wait_end_of_query=1`; success of `end()` is the
durable-ack point.

⚠ Dedup sharp edge (verified live): on plain `MergeTree`, deduplication
**silently no-ops** unless the table sets
`non_replicated_deduplication_window > 0` (server default 0).
`Replicated*MergeTree` defaults to a window of 100. spate-clickhouse
documentation must state this prominently.

Kafka specifics (the second consumer of the seam): the produce unit is
(key, headers, payload), not a row frame, so spate-kafka's encoder writes a
connector-internal length-delimited message framing into the opaque chunk
frames and its writer parses them back — the frozen contracts carry it
unchanged. One `ThreadedProducer` per sink instance (librdkafka owns broker
routing/batching, and the absolute-mapped statistics counters are only
sound with a single client); framework shards are worker parallelism over
clones of it, one replica each — rotation degenerates to a no-op while the
breaker still provides quarantine and the `spate_sink_shard_healthy`
backpressure signal. `write_batch` produces every message of the sealed
batch and awaits a per-batch delivery-report countdown (an `Arc` carried as
each message's librdkafka opaque; the callback never blocks) before
returning — returning `Ok` is the durable-ack point, so `acks=all` and
`enable.idempotence` are forced and their properties denied in the
passthrough. Producer queue-full is absorbed with bounded async backoff
inside the write. Report errors map: authorization/unknown-topic/fenced
idempotent-producer states are fatal (fail fast, not a retry spin);
everything else retryable — a whole-batch retry re-produces any delivered
prefix, which is the documented duplicate window alongside crash replay
(`dedup_token` has no Kafka equivalent and is unused). Oversized records
are caught at encode time (`max_message_bytes`, record-level → Skip/Fail
policy); the same limit is applied client-side as `message.max.bytes`, so
a writer-side size rejection means misaligned broker limits → fatal.

## Delivery semantics — honest version

At-least-once means duplicates happen. Dedup tokens make **same-boundary
retries** idempotent (a batch retried on another replica after a timeout).
They do **not** cover crash replay: after restart, data re-batches with
different boundaries and different tokens, and those rows will land twice.
Design target tables accordingly — `ReplacingMergeTree` with a version
column is the sanctioned pattern. Documentation must never imply
exactly-once.

## Errors, panics, shutdown, health

- **Error taxonomy**: retryable (transient I/O — retried by the sink layer),
  record-level (bad payload, failed map — subject to per-stage `Skip`/`Fail`
  policy; deser defaults to Skip, operators to Fail), fatal (invariant
  violations — pipeline fails).
- **Panic policy**: user-code panics are caught per batch, the batch resolves
  as Failed (watermark stalls), the pipeline transitions to Failed and the
  process exits non-zero. Kubernetes restarts it. No thread resurrection.
- **Stalled watermarks**: a fatal sink write abandons its batch (acks fail,
  the partition watermark stalls permanently); if any partition stays stalled
  behind a failed batch longer than `checkpoint.stalled_fail_after`
  (default 120s), the controller fails the pipeline so it restarts and
  replays instead of running on committing nothing for that partition.
- **Shutdown**: SIGTERM trips the same drain barrier as a full revocation:
  stop lanes → flush chain → sinks force-seal and drain in-flight under
  `drain_timeout` (default 25s, must be < `terminationGracePeriodSeconds`) →
  final synchronous commit → join. If a sink is down at the deadline,
  unflushed batches are abandoned loudly (metric + log) and replay on
  restart — at-least-once holds either way. The sink deadline is bounded
  twice: cooperatively (workers watch it and abandon under their own power)
  and then by a hard backstop 1s later that force-aborts a worker which has
  not returned, so a stall costs a lost drain report
  (`spate_sink_drain_overrun_total`) rather than a hang. The cooperative layer
  is only sound if nothing on the worker's intake path can block outside its
  deadline-armed `select!` — which is why `dispatch` parks batches for a
  permit instead of awaiting one. A bounded source's
  `SourceEvent::Drained` enters this same choreography, with one addition:
  abandoned batches or an unpersisted final commit downgrade the exit from
  `Completed` to `Failed` (see the bounded-sources note under Source
  abstraction).
- **Probes**: `/readyz` = assignment received and sinks connected;
  `/healthz` = poll-loop heartbeat fresh AND no watermark stuck while data
  flows. Both on the admin server next to `/metrics`.

## Configuration

YAML, loaded once at startup: `${VAR:-default}` interpolation (Kubernetes
Secrets/ConfigMaps as env or mounted files), a typed top-level section
(threads, checkpointing, backpressure, metrics — `deny_unknown_fields`,
humantime durations, byte sizes), and **opaque per-component sections**
passed through to connector factories, which deserialize their own config
(`serde_path_to_error` wrapping for actionable messages). The Kafka section
additionally passes a raw librdkafka property map, with a validation denylist
for properties that would break the framework's guarantees
(e.g. `enable.auto.offset.store`).

## Metrics

See `docs/METRICS.md` for the full taxonomy. **Assembly order matters**:
`metrics::install` must run before any metric handle is constructed —
handles bind to the recorder present at construction, and one built
earlier records into the void. `install` is idempotent (a second call
returns the first exporter; `exporter: none` never claims the process
slot), so assemblies call
`metrics::install(&pipeline::metrics_settings(&config))` right after
loading config and the runtime's own install reuses it. Implementation rules: the
`metrics` facade is the registry abstraction (implementors use it directly
for custom metrics); the Prometheus exporter mounts on the admin server; all
framework handles are pre-registered at build time; hot-path counting happens
at batch boundaries.

## Assembly: the builder and its desugaring

`pipeline::Pipeline` is the primary assembly path; the manual primitives
stay public, semver-committed, and documented (the hyper/axum layering
contract: the convenience layer is a thin composition of public
lower-level APIs, and its rustdoc shows the exact desugaring). Nothing in
the builder touches the data path — it assembles the cold path and hands
the user's chain factory through unchanged, so the operator chain stays
fully monomorphized behind the same one-dyn-call-per-batch boundary.

Design points, each earned by a footgun in the manual assembly:

- **Constructor owns init.** `Pipeline::from_config` installs telemetry
  and the metrics exporter and builds the I/O runtime before returning —
  you cannot hold a builder without a live recorder, so the
  silent-dead-handles ordering bug (§ Metrics) is unconstructible, and
  connectors get `io_handle()`/`block_on()` for pre-`run` edge work
  (schema fetchers, `validate_schema`). Coarse typestate — one session-like
  state object rather than state-parameter generics, so the builder stays
  nameable and storable. It refuses to be constructed
  inside an async runtime (it owns a blocking one).
- **One I/O runtime.** The builder's runtime moves into
  `PipelineRuntime` (`with_io_runtime`) instead of `run()` building a
  second one — assemblies no longer double `io_threads` behind the
  thread-reservation math.
- **Connector-agnostic sink slot.** `sink::SinkBundle` (`into_parts` →
  `SinkParts { writer, shard_endpoints, pool, component_type,
  replica_labels, probe }`, `#[non_exhaustive]`) is the seam between
  connector factories and the builder; its only bound is `ShardWriter`,
  preserving the dependency policy below. `SinkParts` implements the
  trait itself — hand-rolled sinks need no impl. Connector-typed flows
  that the framework cannot name (ClickHouse `validate_schema` →
  `RowSchema` → encoder) stay concrete pre-steps outside the trait.
- **Drop ordering is structural.** The sink drains only when every
  `ShardQueues` clone is gone; the builder lends queues per
  chain-factory call via `ChainCtx` (by value, not `Clone`) and never
  exposes them otherwise. Defense in depth: since the leaked-sender fix,
  a clone that outlives the drivers degrades shutdown to a
  deadline-bounded loud abandon instead of an unbounded hang.
- **Source stays a terminal generic.** `PipelineRuntime<S: Source>` is
  minted at `into_runtime(source)`/`run(source)`; connector construction
  remains one explicit line — no config-tag→constructor registry
  (topology is code-defined; a registry would reintroduce it as data).
- **Multiple sinks via `add_sink`.** A pipeline installs one or more named
  sinks (`add_sink(name, bundle)`; `sink(bundle)` is sugar for the reserved
  name `"default"`). The chain terminates either in a single `.sink(...)` or
  in a **split** — `split(cfg, unmatched).add::<F>(...)…​.route(closure)` —
  that routes each record to exactly one typed branch, each branch a full sink
  (own family/encoder/router/queues) resolved by name via `ctx.sink(name)`. The
  ack layer needs nothing new: each branch clones the poll batch's `AckRef`, so
  a source watermark holds until every destination it touched has durably
  written, with worst-status merge. Unmatched records follow a configurable
  `ErrorPolicy` (`Fail` default; `Skip` drops + counts `reason="unrouted"`).
  Branches store their encoder/router type-erased so the branch type keys on
  the destination family alone, letting a `Copy` typed handle recover it with
  one downcast. Per record that costs the downcast plus a virtual route and
  encode call (the single-sink terminal uses concrete types and pays neither);
  the user's routing closure stays monomorphic.

## Dependency policy

No rdkafka / clickhouse / apache-avro types appear in public trait bounds or
public structs of `spate-core` — all are 0.x crates whose breaking releases
must not become our breaking releases. Connector crates may re-export their
underlying crate for advanced use, clearly documented as exempt from our
stability promises.

The **one deliberate exception is the `metrics` facade** (also 0.x): its
`Counter` / `Gauge` / `Histogram` / `SharedString` handle types are part of
`spate-core`'s public surface, alongside the `Meter` scope and `ComponentLabels`
that mint them. This is intentional — the framework's instrumentation API *is*
the `metrics` facade, so a connector or pipeline author registering its own
metric family speaks in those types. The mitigation is that `spate-core`
re-exports the handle types (`spate_core::metrics::{Counter, ...}`); connectors
never take a direct `metrics` dependency, so exactly one facade version lives
in the tree and a breaking `metrics` release is upgraded in a single edit.

## Crate map

| Crate | Role |
|---|---|
| `spate` | Facade; feature-forwards connectors (`kafka`, `clickhouse`, `avro`, `json`, `s3`, `full`, plus opt-in backend/TLS/type features — see its `Cargo.toml`). The only crate applications depend on. |
| `spate-core` | Engine: record/ack types, source/sink traits, operator chain, checkpointer, backpressure, pipeline runtime, config, metrics, admin server, telemetry. |
| `spate-kafka` | Kafka source (single consumer + partition queues) and producer sink. |
| `spate-clickhouse` | ClickHouse `ShardWriter`. |
| `spate-json` | JSON deserializer: single/ndjson/array framings; opt-in `simd` backend. |
| `spate-s3` | Coordinated S3/object-storage backfill source: leader-planned splits (listing-order packing, digest-identified descriptors carrying member keys+ETags), one lane per leased split with composite split-relative offsets, per-split fenced progress as the only checkpoint, object-level poison → quarantine, self-terminating via `AllComplete` → `SourceEvent::Drained`. No coordination backend appears in its public API or its cargo features — the coordinator is injected at assembly (`with_coordinator`); solo runs fall back to `spate-coordination`'s in-process `MemoryStore`, which is linked unconditionally (ephemeral progress). |
| `spate-avro` | Avro deserializer: `apache-avro` backend, Confluent wire format, registry client + per-thread schema cache. |
| `spate-coordination` | Multi-instance coordination backend for broker-less sources: leader-elected planning and work assignment, TTL'd split leases with heartbeats, cooperative revocation, epoch-fenced resumable commits — over a public six-primitive store trait with an in-memory store (tests, embedding) and a NATS JetStream KV store (server ≥ 2.11, default `nats` feature). The seam it implements (`SplitPlanner`/`SplitCoordinator`) and the reusable source-side `CoordinationDriver` live in `spate-core::coordination` — synchronous and tokio-free (no async runtime in spate-core). |
| `spate-test` | Public in-memory source/sink mocks with scripting handles, plus a scripted `SplitCoordinator` for coordinated-source tests. |
| `benchmarks` | Unpublished: topology A/B, synthetic framework-overhead, e2e harness, loadgen. |

## Decision log

| Decision | Choice | Why (short) |
|---|---|---|
| Delivery | at-least-once | Kafka→ClickHouse standard; exactly-once machinery not worth v1 cost |
| Source API | poll-based, control/data split | librdkafka owns I/O; Streams add overhead and `'static` bounds |
| Kafka topology | single consumer + split partition queues | group scale, m:n thread mapping, one drain choreography (validated by benchmark) |
| Deser placement | pipeline thread | borrow lifetimes force it; keeps CPU pinned |
| Operator dispatch | static in-chain, dyn per batch at boundary | preserves inlining; ~amortized-zero dispatch cost |
| Ack design | refcounted per-batch Arc + contiguity ring | supports filter/flat_map/multi-sink for free; ~2 atomics/record |
| Multi-sink | additive `add_sink` + a typed split terminal | per-type tables move fan-out off the DB onto the scalable ETL tier; +56%–212% throughput, 4–10× part size, 16–31% lower server CPU/row vs `Null`+MV (benchmark-verified) |
| Sink workers | per shard, replica rotation, max_inflight | full-size batches (ClickHouse merge pressure) + parallelism |
| ClickHouse insert | pre-encoded RowBinary frames via `InsertFormatted` + dedup token | encode on pipeline threads; deterministic batch boundaries for acks and idempotent retries (spike-verified) |
| Zero-copy seam | untyped payload batches cross the dyn boundary; `RecFamily` lifetime→type family | records live and die inside one `push_batch`; 40 ns/rec, 0 allocs/rec vs 3.7× slower owned (spike-measured) |
| Metrics | `metrics` facade + prometheus exporter | facade *is* the MeterRegistry pattern; backend-pluggable |
| Config | YAML (`yaml_serde`), opaque passthrough | serde_yaml archived; serde-yml has RUSTSEC advisory. Provenance verified: yaml_serde is the YAML org's successor (github.com/yaml/yaml-serde, published by ingydotnet, YAML's co-creator) |
| Error policy | Skip / Fail only, metrics-surfaced | no owned DLQ topic in target environments |
| Kafka teardown | rebalance completes inline once closing | consumer close triggers a final revoke inside `BaseConsumer::drop`'s close-poll; a deferred intent there deadlocks teardown forever |
| Kafka startup deadline | checked before transport errors in `poll_events` | unreachable brokers surface endless Retryable errors; checked last, the fatal fail-fast would never fire |
| Sink readiness | `SinkRuntime.probe` hook polled by the runtime | `/readyz` needs live sink connectivity; nothing else could ever set it |
| Assembly API | non-generic `Pipeline` builder, source at the terminal call | nameable/storable without typestate generics; cold-path only, zero hot-path cost |
| Assembly init | constructor owns telemetry/metrics/IO runtime | makes the exporter-before-handles ordering unconstructible instead of documented |
| IO runtime ownership | builder-owned, adopted by `PipelineRuntime::run` | one runtime as designed; `io_threads` no longer silently doubled |
| Sink integration | `SinkBundle` → `#[non_exhaustive]` `SinkParts` | connector-agnostic with only a `ShardWriter` bound; fields can grow additively |
| MSRV | 1.94 (rolling N-2), edition 2024 | library-consumer reach; absorbs dep MSRV ratchets |
| Kafka sink ack wiring | `ThreadedProducer` + per-batch delivery-report countdown awaited inside `write_batch` | acks resolve only from write outcomes (frozen contract); O(1) await state vs per-message futures at 500k msgs/batch; a custom context is needed for statistics anyway |
| Kafka sink topology | one producer per sink; shards = workers over clones, 1 replica each | librdkafka owns broker routing/batching; absolute stats counters need a single client; breaker/backpressure still function |
| Kafka sink durability | `acks=all` + `enable.idempotence` forced, denied in passthrough (incl. aliases) | a delivery report must mean a durable write before offsets commit; weaker settings silently become at-most-once |
| Kafka sink framing | connector-internal length-delimited (key, headers, payload, tombstone bit) inside opaque frames | absorbs the message-unit mismatch without touching frozen contracts; format free to evolve |
| Kafka sink oversized records | encode-time `max_message_bytes` guard (record-level), same value forced as `message.max.bytes` | the writer path has no per-record policy — a batch is all-or-nothing; catching size at encode keeps Skip/Fail semantics and makes writer-side size errors a misconfiguration (fatal) |
| Kafka sink retry-duplicates stance | report failures classify retryable unless provably permanent; `NotEnoughReplicasAfterAppend` retries knowingly duplicate | at-least-once: replay over loss, idempotent-producer fatal states fail fast instead of spinning to `stalled_fail_after` |
| Kafka producer teardown | rely on rdkafka's `Drop` (purge queue+inflight, 500ms flush, poll thread joined) | bounded ≤ ~600ms; purged messages' reports reclaim the countdown opaques — no custom Drop to maintain |
| Kafka sink per-partition stats | deferred: aggregate produce-queue gauges only | `attach_metrics` has no `per_partition_detail` channel today (a `SourceCtx` concept); additive later |
| Source coordination model | **leader-computed sticky assignment**: the leader-elected worker runs the source's planner *and* publishes a desired assignment per instance (`assign.{instance}`); workers reconcile toward it — claim what is named, cooperatively drain what is not. Balance is on split *weight*, sticky, and converges by strictly-improving moves. Supersedes the dynamic work-stealing model this row used to describe | the reversal is recorded honestly, as the store reversal below was. Work-stealing distributed the *decision* along with the work, so each worker negotiated pairwise from a partial view — and every defect found in five days was the same shape: state visible to neither side of the negotiation (splits promised but not yet leased, grant acks lost on a CAS, annotations nothing retired). Mature implementations of this shape are unanimous the other way: planning is centralised in an elected leader. The designs that were genuinely peer-to-peer have since moved to one, having accumulated exactly our symptoms — a per-victim steal cap that could not usefully be raised, and a stable disbalance that nothing corrected. Centralising the decision costs nothing in safety because the assignment is not a fence (see below), and it moves the balance logic out of the async task and into a pure, property-tested function. Normative spec: `docs/user-guide/02-concepts/08-work-assignment.mdx` |
| Coordination store | **external low-latency KV (NATS JetStream) behind a public 6-primitive trait** — supersedes the earlier object-store CAS-lease design (PR #34, closed unmerged) | the reversal is recorded honestly: coordinating over the checkpoint store needed zero extra infrastructure, but made an object store the substrate for *every* coordinated source and its poll-based ~10–100 ms round-trips capped distribution speed. The requirement changed — from "coordinate one S3 backfill" to "a general work-distribution seam" — and with it the trade. Correctness still lives in our fencing protocol (the store supplies latency and watch, never safety); the trait (CAS + TTL keyspace + watch + list) keeps Redis/etcd backends one impl away |
| Embedded consensus (Raft) | still rejected — and centralising *assignment* does not reopen it | no worker framework embeds consensus for work distribution; a voter set fights autoscaling (stable membership, per-node state); the problem needs linearizability only at the per-split commit — exactly what one CAS provides. The leader that now computes assignments needs no consensus either, because an assignment carries no correctness: a stale, split-brained, or simply wrong leader can produce bad balance but never two owners, since the durable record CAS still decides ownership. Consensus would be buying a guarantee the fence already provides |
| Coordination fencing | the durable split record's CAS revision is the *only* correctness mechanism; epoch per ownership change; a fenced commit writes nothing and is followed by a `Lost` event; leaders are fenced the same way — a new leader's generation bump moves the plan record's revision, so a deposed leader's plan write loses its CAS | lease TTLs and watch events buy latency and may be late or lost; making them safety-relevant would put the store's clock in the correctness path (Kleppmann's fencing argument) |
| Split delivery attempts | increment only on non-graceful tenancy ends (expiry takeover, reclaim, explicit `fail`); quarantine at the cap; quarantined splits block `AllComplete` → the job ends `Stalled` | graceful releases and rebalance revocations are not poison evidence; completing a bounded job over unprocessed planned data would dress loss up as a green exit |
| Split revocation | cooperative first, forced second: the leader stops assigning a split, the owner stops intake at a safe boundary, chases its tail to a final fenced commit and releases (replaying nothing); a source that declines, or whose drain outruns `drain_deadline`, has the release forced and its uncommitted tail replays under the next owner; a revocation the leader takes back — it names the split for its current owner again while that owner still holds it — is cancelled instead, dropping the pending forced release, and the drain it leaves behind is bounded by silence (`drain_deadline` with no commit landing) rather than by the deadline | fencing a live owner mid-read costs up to a commit interval of duplicates per move, so consent is worth asking for. But a leader's revocation is a *decision*, not a proposal — under the previous peer-negotiated design an owner could simply ignore a request and the requester waited out a round budget, whereas here refusing must not be able to pin a rebalance open, hence the deadline. This deliberately reverses the "no driver-side deadline" decision taken with the negotiated handoff, which was correct while the request was advisory. The deadline serves the *rebalance*, so once the leader withdraws the move there is nothing left for it to protect and forcing would charge a replay for a handoff nobody is waiting on — hence the cancel, whose only effect is on a drain slower than the deadline, since a faster one was never going to be forced. It ends the revocation, not necessarily the drain: resuming intake a source has already stopped is a seam `SplitSource` deliberately lacks, so a drain in flight still hands the split back and the same worker re-claims it replay-free. What the cancel must NOT drop is the obligation to keep the split readable — a wedged drain with no deadline over it would leave the split owned, leased, and read by nobody forever, and a bounded job containing it could never finish — so the deadline stays over a cancelled drain in a weaker form, as a no-progress timeout: committed nothing for `drain_deadline`, release it and let the same worker re-claim with a fresh lane. A live drain commits as its tail acks, so only a wedged one trips it. Graceful-then-forced, with a bounded wait between the two, is the shape mature rebalancing protocols converge on |
| Rebalance delay | a departed instance's splits are withheld for `rebalance_delay` (default 20s), cancelled the moment it reappears; `0` takes a distinct code path meaning "immediately" | a rolling restart should not churn the fleet. The default is deliberately short, because a split starts by reading a descriptor and spawning a fetcher rather than by rebuilding a connector's clients — when starting is cheap, idle work costs more than movement. Zero is a distinct path because a delay knob whose zero flows through the general path as just another value is how "reassign at once" silently becomes "withhold indefinitely"; the code shape here is chosen so that bug is not expressible, and a regression test asserts it |
| Split ids | deterministic, planner-derived, `[A-Za-z0-9_-]{1,128}` | replans and leader failovers become create-if-absent no-ops; ids embed in store keys on any backend |
| Coordinated source DX | seam traits + a framework-owned `CoordinationDriver` in `spate-core` (sync, tokio-free); backends injected at assembly like the framer seam | the event↔assignment choreography (tenancy partitions, fenced quarantine, completion sweep) is source-generic and where the subtle bugs live — one audited implementation instead of one per connector |
| Coordinated gains & completion latency | additive gains: a gain mints a fresh, never-reused lane per tenancy (`LanesAdded`, `extend_epoch`) — no revoke-then-reassign cycle; eager terminal commit: end-of-input lanes surface as `CommitReady` (`SplitSource::take_finishing`) and the runtime chases their final acks within one commit interval; completed splits leave barrier-less via `LanesRetired` (terminal commit already proved nothing is in flight) | a split's completion must not cost a commit-tick quantum (~ interval / max_in_flight of wall time), and a routine gain must never drain flowing lanes — the drain-and-reassign cycle both crashed mid-flow commits and cost O(splits) re-reads of uncommitted tails |
| Split record layout | two durable records per split: an immutable spec record (descriptor, written once at planning) and a small progress record (owner, epoch, attempts, watermark) that is the CAS target for every claim, fence, and commit; `planned` is recounted from an authoritative listing at every publish | commit cost must not scale with descriptor size (a bin-packed object list can be hundreds of KiB, rewritten on every commit under a one-record layout); the recount makes a leader crash or failed publish between seeding and publishing self-heal instead of desynchronizing terminal detection forever |
| S3 scale-out | always-coordinated single path: solo = one instance over an internal in-process store (ephemeral progress, WARNed); the manifest checkpoint is deleted | two checkpoint mechanisms means two commit paths, two resume validators, and a lane/split impedance mismatch; at 0.1.x the dual path was pure liability — durable solo resume is delegated to a durable coordination store |
| S3 split packing | listing-order first-fit with a 10-bin lookback and a per-object open cost of `max(size, target/16)`; default target 64 MiB; an object ≥ target gets its own split | pure, streamable function of the listing (Iceberg/Trino practice; Spark's sort-then-pack costs O(files) driver memory and prefix locality); the cost floor structurally caps a split at ~16 members, bounding descriptors well under store value caps |
| S3 split identity | truncated SHA-256 over sorted member keys + ETags + a packing-version constant, base64url — public derivation (`split_id_for`) | replans are create-if-absent no-ops; an overwrite becomes new work instead of silently matching stale progress; a packing change is an explicit epoch; the digest is persisted identity where a collision silently drops work, so it must survive adversarial key names; out-of-process producers (event-driven planners, single-shot invocations) can mint identical ids |
| S3 poison policy | object-level failures (deleted after planning, `If-Match` drift, corrupt content, exhausted read budget) poison the split via `driver.fail`; credentials/config stay pipeline-fatal; `Stalled` stays fatal | one bad object must not kill a fleet-wide backfill, and a bounded job with quarantined splits must never end as a green `Completed`; the failure-class split follows scope (one object vs. every object on every instance) |
| Coordinator wiring | assembly-only: sources accept `Box<dyn SplitCoordinator>` (`with_coordinator`) and carry no backend config or features; the source YAML keeps only what shapes the work itself (planner knobs like `split_target_bytes`/`refresh_listing`) | per-connector backend config multiplies (connectors × backends) in features and config surface; the deployer's binary is the right owner of "which store" — new backends then need zero connector changes |
| Control-plane wait ownership | the **driver** parks, not the backend: `SplitCoordinator::poll` takes no timeout and must not block, `set_waker` (no default) hands it a single-slot `ControlWaker` that lanes clone too and signal on two edges — end-of-input and poison | a coordinated source waits on two signals arriving from different directions: backend events, and lanes reaching end-of-input *on pipeline threads*. Only the driver sees both, so a wait inside the backend can only ever be woken by half of them — a finishing lane was noticed between waits, and every completion sat out the remainder of one. Capping that wait bounded the damage at the cost of polling the control plane 200×/s; moving it removes both. A defaulted `set_waker` would be worse than none: the backend parks internally *and* the driver parks on top |
