# ADR-0007 — Pre-encoded RowBinary frames with a deterministic deduplication token

- **Status:** accepted
- **Date:** 2026-07-05 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The ClickHouse sink has to get rows from the pipeline into a table. The obvious
route is the client crate's typed insert path, which serializes a Rust value per
row. That puts encoding on whichever thread calls it, an async I/O thread, and
makes the batch boundary a property of the client's internal buffering rather
than something the framework controls.

Both are problems. Encoding is CPU work and belongs on the pinned pipeline
threads ([ADR-0003](0003-poll-based-source-api.md)). And a retry can only be
idempotent if the retried bytes are identical, which requires the framework to
own where a batch begins and ends.

## Considered options

- The client crate's typed insert path, one Rust value per row
- Insert through a Distributed table and let the server route rows to shards
- Encode RowBinary frames on the pipeline threads and ship them as pre-formatted
  bytes, writing direct to shard-local tables

## Decision outcome

Chosen option: "Encode RowBinary frames on the pipeline threads and write direct
to shard-local tables", because it is the only option where the framework owns
the batch boundary, which is what makes both acknowledgment accounting and
idempotent retry possible.

Rows are encoded by the connector's own serializer (the client crate's is
private, so writing our own is also a semver win) and shipped through
`Client::insert_formatted_with` and `InsertFormatted::send`, the same transport
the crate's typed path uses internally. Each sealed batch becomes one `INSERT`
carrying a deterministic `insert_deduplication_token`, so an in-session retry
after a timeout is idempotent.

Writing direct to shard-local tables rather than through a Distributed table
gives bigger blocks, less merge pressure, and, decisively, a **synchronous
server acknowledgment**, which checkpointing requires. A Distributed insert can
return before the data is durable anywhere the framework can observe.

The durability model is that one healthy replica accepts the write and
`ReplicatedMergeTree` replicates it. That has an explicit boundary: replication
is asynchronous, so a batch acknowledged by a replica destroyed before the part
propagates is lost. That is the edge of the sink's at-least-once guarantee, and
it is closable per sink with an operator-supplied `insert_quorum` at the cost of
shard write availability.

### Consequences

- Good, because encode CPU lands on pinned threads and the I/O runtime only
  moves bytes.
- Good, because a retry re-sends identical bytes with an identical token, so the
  server deduplicates it.
- Bad, because the connector carries its own serializer and has to track the
  wire format itself rather than inheriting the client crate's.
- Bad, because the durability boundary is real and asynchronous replication can
  lose an acknowledged batch. It is documented rather than closed, because
  closing it by default would trade availability every operator would notice for
  a failure most will never hit.

### Confirmation

Round-trip verified against a live server. The deduplication behavior has a
sharp edge that testing found rather than reasoning: on plain `MergeTree`,
deduplication **silently no-ops** unless the table sets
`non_replicated_deduplication_window` above zero, which the server defaults to
0. `Replicated*MergeTree` defaults to a window of 100. The connector
documentation states this prominently because nothing in the insert path reports
that the token was ignored.

## More information

- Landed in `c8973e6`; extended with the Native format and quorum notes later.
- [ADR-0006](0006-per-shard-sink-workers.md) — the worker model that produces
  the sealed batches this encodes.
- [ClickHouse sink](../user-guide/04-connectors/sinks/clickhouse/README.mdx) —
  configuration, the deduplication window requirement, and the quorum trade.
