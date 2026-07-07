# Delivery guarantees

etl-rs delivers **at-least-once**: a source offset is committed only after
every record derived from it has been durably written to the sink or
intentionally dropped. This page explains the machinery that enforces it,
what happens at the hard moments (rebalances, shutdown, crashes), and —
honestly — what at-least-once does *not* give you.

## The ack chain: from record to watermark

**One `AckRef` per source poll batch.** When a lane's poll yields a batch,
the framework creates a single refcounted acknowledgement state for it;
every record deserialized from that batch carries a clone. The refcount
follows the data wherever it goes:

- A record dropped by `filter` (or a Skip policy) resolves its share as
  success — an intentional drop must not stall commits.
- `flat_map` children clone the same ack, so fan-out is covered for free.
- Routing to multiple outputs clones it per output and merges the worst
  status.

When the last clone drops, `(partition, seq, status)` flows over an
**unbounded** channel to the checkpointer — the ack path can never block
behind data, by construction.

**The contiguity tracker.** The checkpointer (a synchronous, tokio-free,
loom-tested module — `etl::checkpoint`) keeps, per partition, a ring of
outstanding batch sequence numbers. It pops the contiguous *acknowledged
prefix* and advances that partition's committable watermark. Out-of-order
acks are fine; the watermark only ever covers a gap-free prefix of
acknowledged data.

**Commit cadence.** Watermarks are stored on advance and committed to the
source on an interval (`checkpoint.interval`, default 5s). The invariant:
**never commit past unacknowledged data.** A failed batch stalls its
partition's watermark rather than being skipped — watch
`etl_checkpoint_watermark_age_seconds`, the primary "stuck pipeline" alert
(see [Monitoring](../05-deployment/monitoring.md) and
[docs/METRICS.md](../../METRICS.md)).

**Durability is the sink's ack.** For ClickHouse, a batch counts as durable
when the insert completes with `wait_end_of_query=1` — a synchronous server
acknowledgement, which is exactly why the sink writes directly to shard-local
tables rather than through a Distributed table. See the
[ClickHouse connector](../04-connectors/clickhouse.md).

## Teardown can't lose data

Two structural contracts guard the seams where acknowledgements travel in
bulk (encoded chunks, worker ledgers, batch accumulators):

- **Collections fail by default.** Every collection of acks on the sink
  path delivers only after an explicit durable write; dropped on any other
  path (a dropped queue, an aborted worker, a runtime shutdown without
  drain), it *fails* its handles. Teardown therefore stalls watermarks
  instead of committing unwritten data.
- **Over-failing is always safe.** A false failure costs a replay; a false
  success costs data. The framework always chooses replay.

## Rebalances

When Kafka revokes partitions, the framework drains the owning threads,
flushes and acks in-flight data, and commits watermarks **synchronously
before the revoke completes** — so the next owner of the partition resumes
from a correct offset. Every rebalance also bumps a per-partition *epoch*
in the checkpointer, so stale acks from a revoked assignment can never
advance a watermark that no longer belongs to this process. Details of the
choreography are in the [Kafka connector](../04-connectors/kafka.md) and
[docs/DESIGN.md](../../DESIGN.md) § Source abstraction.

## Shutdown and crashes

**Graceful (SIGTERM):** the same drain barrier as a full revocation — lanes
stop, chains flush, sinks force-seal and drain in-flight batches under
`checkpoint.drain_timeout` (default 25s), then a final synchronous commit.
If a sink is down at the deadline, the remaining batches are abandoned
loudly (`etl_sink_abandoned_batches_total` plus a log line) and their
offsets are *not* committed — they replay on restart. See
[Graceful shutdown](../03-guides/graceful-shutdown.md).

**Crash (SIGKILL, OOM, panic-and-exit):** nothing was committed past the
last durable write, so on restart the source resumes from the committed
watermark and replays everything after it. At-least-once holds; duplicates
happen. Which brings us to:

## The honest part: duplicates and deduplication

At-least-once means duplicates *will* occur. The ClickHouse sink attaches a
deterministic `insert_deduplication_token` to every sealed batch, and it is
important to understand exactly what that covers:

- **Covered — same-batch retries.** A batch that times out on one replica
  and is retried (on the same or another replica) carries the same token;
  ClickHouse drops the duplicate insert. In-session retry is idempotent.
- **NOT covered — crash replay.** After a restart, replayed records are
  re-batched with different boundaries, producing different tokens. Those
  rows land twice, and no dedup window will catch them.

So design the target table for replay. The sanctioned pattern is
`ReplacingMergeTree` with a version column:

```sql
CREATE TABLE orders (
    id           UInt64,
    customer     String,
    amount_cents Int64,
    ts_ms        Int64
) ENGINE = ReplacingMergeTree(ts_ms) ORDER BY id;
```

Replayed rows collapse on merge (use `FINAL` or aggregate around it at
query time). If your data is append-only observations where rare duplicates
are tolerable, plain `MergeTree` may be acceptable — that's a product
decision, but make it consciously.

> [!WARNING]
> **The `non_replicated_deduplication_window` footgun.** On plain
> `MergeTree`, insert deduplication **silently no-ops** unless the table
> sets `non_replicated_deduplication_window` greater than 0 — the server
> default is 0, and ClickHouse reports success either way. Without it, even
> the covered case above (same-batch retries) writes duplicates.
> `Replicated*MergeTree` defaults to a window of 100. Verified against a
> live server; see [docs/DESIGN.md](../../DESIGN.md) § Sink.

> [!IMPORTANT]
> Nothing in etl-rs is exactly-once, and no configuration makes it so.
> If a downstream consumer cannot tolerate duplicates, deduplicate at the
> table engine or query layer.

## Further reading

- [docs/DESIGN.md](../../DESIGN.md) § Records and checkpointing, § Delivery
  semantics — the canonical treatment, including the evaluated alternative
  ack designs.
- [Error handling](04-error-handling.md) — what happens to the watermark
  when records fail.
- [Testing pipelines](../03-guides/testing-pipelines.md) — asserting on
  committed watermarks with the `etl-test` handles.
