# etl-rs Design

This document records the architecture of `etl-rs` and the reasoning behind
its decisions. It is the source of truth when code and intuition disagree.

## Goals

- At-least-once delivery, end to end, including across consumer rebalances,
  crashes, and graceful shutdown.
- Extract maximum single-node performance: borrowed data and zero per-record
  allocations on the hot path, CPU work pinned to pipeline threads, I/O
  concurrency at the edges.
- Small, stable abstractions so technology-specific connectors (Kafka,
  ClickHouse, ...) are thin and third parties can implement their own.
- A chaining pipeline API in the spirit of Flink / Java Streams, defined in
  Rust code; YAML configures connectors and tuning, never the operator graph.
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
step is *framing*. `etl_core::framing` owns only the **seam** — the
`RecordFramer` trait, the `FramerWriter` decompressor shim, and the
`FramingContract` handshake — because how a byte stream splits into records is
a property of the format, not the transport. The concrete framers are owned by
the **format** crates: `etl-json` provides the newline-delimited `NdjsonFramer`,
a future `etl-avro` an object-container block framer, and so on. A source stays
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

Proven choreography etl-kafka must implement (spike-verified):

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
`etl_deser_not_ready_total`); `push_batch` returns a
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

At-least-once invariant: **never commit past unacknowledged data.** A failed
batch stalls its partition's watermark (alert on
`etl_checkpoint_watermark_age_seconds`), it never silently advances.

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
- **Eager assignment.** `Checkpointer::begin_epoch` replaces all trackers,
  so `LanesAssigned` must describe the full assignment following a full
  revocation. Incremental/cooperative assignment requires an additive
  checkpointer extension (future work); the controller defensively drains
  and commits live lanes if a source violates this.

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
virtual call **per batch** (the Arroyo/DataFusion pattern) — per-record dyn
dispatch would defeat cross-operator inlining and vectorization.

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

librdkafka's own prefetch caps (`queued.min.messages`,
`queued.max.messages.kbytes`) are set as a hard memory backstop independent
of pipeline state.

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
a shard is circuit-closed, new write attempts park until the next half-open
probe window while their in-flight permits stay held, so intake stalls and
the shard back-pressures the source (surfaced as `etl_sink_shard_healthy ==
0`; with a bounded `retry.max_attempts` a batch instead fails and replays
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
encoded **on the pipeline threads** by etl-clickhouse's own serializer (the
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
`Replicated*MergeTree` defaults to a window of 100. etl-clickhouse
documentation must state this prominently.

Kafka specifics (the second consumer of the seam): the produce unit is
(key, headers, payload), not a row frame, so etl-kafka's encoder writes a
connector-internal length-delimited message framing into the opaque chunk
frames and its writer parses them back — the frozen contracts carry it
unchanged. One `ThreadedProducer` per sink instance (librdkafka owns broker
routing/batching, and the absolute-mapped statistics counters are only
sound with a single client); framework shards are worker parallelism over
clones of it, one replica each — rotation degenerates to a no-op while the
breaker still provides quarantine and the `etl_sink_shard_healthy`
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
  restart — at-least-once holds either way. A bounded source's
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
  (schema fetchers, `validate_schema`). Coarse typestate in the
  DataFusion `SessionState` style; no state-parameter generics, the
  builder stays nameable and storable. It refuses to be constructed
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
public structs of `etl-core` — all are 0.x crates whose breaking releases
must not become our breaking releases. Connector crates may re-export their
underlying crate for advanced use, clearly documented as exempt from our
stability promises.

The **one deliberate exception is the `metrics` facade** (also 0.x): its
`Counter` / `Gauge` / `Histogram` / `SharedString` handle types are part of
`etl-core`'s public surface, alongside the `Meter` scope and `ComponentLabels`
that mint them. This is intentional — the framework's instrumentation API *is*
the `metrics` facade, so a connector or pipeline author registering its own
metric family speaks in those types. The mitigation is that `etl-core`
re-exports the handle types (`etl_core::metrics::{Counter, ...}`); connectors
never take a direct `metrics` dependency, so exactly one facade version lives
in the tree and a breaking `metrics` release is upgraded in a single edit.

Avro's optional `fast` backend (`serde_avro_fast`) is
license-disclosed: crates.io metadata says LGPL-3.0-only while upstream is
MPL-2.0 (fix merged, unreleased), and its microdependency
`serde_serializer_quick_unsupported` is genuinely LGPL-3.0-only. It ships
behind the off-by-default `fast` feature — the default build contains no
trace of either crate, `deny.toml` carries scoped exceptions for exactly
the pinned versions, and enabling the feature is the user's own compliance
call. No `serde_avro_fast` types appear in the public API.

## Crate map

| Crate | Role |
|---|---|
| `etl` | Facade; feature-forwards connectors (`kafka`, `clickhouse`, `avro`, `json`, `s3`, `full`, plus opt-in backend/TLS/type features — see its `Cargo.toml`). The only crate applications depend on. |
| `etl-core` | Engine: record/ack types, source/sink traits, operator chain, checkpointer, backpressure, pipeline runtime, config, metrics, admin server, telemetry. |
| `etl-kafka` | Kafka source (single consumer + partition queues) and producer sink. |
| `etl-clickhouse` | ClickHouse `ShardWriter`. |
| `etl-json` | JSON deserializer: single/ndjson/array framings; opt-in `simd` backend. |
| `etl-s3` | Bounded S3/object-storage backfill source: sorted listing dealt into lanes, composite offsets, manifest checkpoints, drift-checked resume, self-terminating via `SourceEvent::Drained`. |
| `etl-avro` | Avro deserializer: `apache-avro` backend, Confluent wire format, registry client + per-thread schema cache; opt-in `fast` feature adds the `serde_avro_fast` backend (single-pass typed decode, borrowed/zero-copy records — see the dependency-policy note above). |
| `etl-test` | Public in-memory source/sink mocks with scripting handles. |
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
