---
description: "The DynamoDB store discovers changes by querying each watched prefix every poll interval, without Streams; lease-poll reads grow with the fleet squared."
---

# ADR-0052 — The DynamoDB store discovers changes by polling, without Streams

- **Status:** accepted
- **Date:** 2026-09-29
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Every worker needs every lease change and every change to the durable records
its watch covers. The DynamoDB store in `spate-coordination` has to back
`CoordinationStore::watch` with something DynamoDB offers.
[ADR-0050](0050-coordination-stores-may-declare-a-polled-watch.md) lets a store
declare a polled watch, and states what the coordinator then reads for itself.
DynamoDB Streams is the service's change feed. A stream's records sit in
shards that split and close as the table's partitions change, a reader must
follow each shard's lineage, and
[AWS's documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html)
states that more than two readers per shard can be throttled.

## Considered options

- Each worker reads the table's stream directly, following shard lineage with
  `DescribeStream`, `GetShardIterator` and `GetRecords`
- The DynamoDB Streams Kinesis adapter, a Java library that assigns each shard
  to one worker through a lease table of its own
- Each watch queries its prefix every `poll_interval` and reports the
  difference, and the store declares `WatchMode::Polled`

## Decision outcome

Chosen option: "Each watch queries its prefix every `poll_interval`", because
it needs nothing beyond the table and the read the store already makes for a
listing. Direct stream reads by every worker exceed two readers per shard at
three workers, and the store would carry shard discovery and lineage. The
Kinesis adapter delivers each record to one worker where every worker needs
every lease change, has no Rust implementation, and needs a second lease table.
Either stream option also needs the stream enabled on the table and four more
IAM actions.

A handle runs one poller per watched keyspace and prefix. The lease keyspace is
polled with a consistent query, and the narrow durable prefixes ADR-0050 names
with an eventually consistent one. The mechanism is ADR-0050's; this record
chooses it for DynamoDB.

### Consequences

- Good, because the store needs no stream, no extra IAM action and no
  process beyond the workers.
- Good, because a poll is a query the table already serves, so the store has
  one read path to get right.
- Bad, because discovery takes up to one `poll_interval`, against the
  milliseconds a pushed watch takes.
- Bad, because the lease poll reads every lease on every poll by every worker,
  so its read units grow with the square of the fleet. At the defaults it nears
  2,000 read units per second at about 100 workers, on one partition key.
- Bad, because every poll is billed, including a poll that finds no change.

### Confirmation

`DynamoDbStore::watch_mode` returns `WatchMode::Polled`, and ADR-0050's tests
pin what the coordinator does on such a store. In
`crates/spate-coordination/src/store/dynamodb/tests.rs`,
`one_poller_serves_every_watch_of_a_prefix_and_stops_with_the_last`,
`listings_and_polls_follow_every_page` and
`a_durable_ec_poll_emits_no_delete_on_absence_or_older_put` pin the poller, and
`polls_are_metered` pins `op="poll"` on
`spate_coordination_store_op_duration_seconds`. The multi-worker scenarios run
over the polled store in default CI and against DynamoDB Local in
`tests/dynamodb_integration.rs`.

## Evidence

The [cost model on #17](https://github.com/spate-etl/spate/issues/17#issuecomment-5879131523)
prices the lease poll at `W × ⌈(W × L × 184 B + W × 157 B + 164 B) / 4 KiB⌉ / P`
read units per second for `W` workers with `L` lanes polling every `P` seconds,
from serialized record sizes. At 10M rows per second with 64 MiB splits the
model gives 27 workers and 148.5 read units per second of
lease polling, out of 322 in all.

A real-table run in #819 checked the model: three 10-minute windows against an
on-demand table in `eu-west-2`, every worker in one process on a laptop with
its own store handle, 8 lanes, 6 s per split, a 2 s poll interval and a 30 s
lease, with consumed capacity read from CloudWatch.

| Workers | Completions/s | WCU/s measured | WCU/s model | RCU/s measured | RCU/s model |
|---|---|---|---|---|---|
| 3 | 2.87 | 21.2 | 24.8 | 13.0 | 18.0 |
| 10 | 9.03 | 65.7 | 82.6 | 59.9 | 75.2 |
| 20 | 17.45 | 126.1 | 165.2 | 151.9 | 195.6 |

Against the model evaluated at each run's measured completion rate, reads came
in 16–26% below and writes 11–13% below. The run
reads table totals, so it does not separate the lease poll from other reads,
and nothing has been run near 100 workers.

## More information

- Landed in #818, which modelled the store over an in-memory table on
  2026-09-29. #819 backed it with the AWS SDK and shipped it on 2026-09-30.
- [ADR-0050](0050-coordination-stores-may-declare-a-polled-watch.md) — the
  polled watch and what the coordinator reads on such a store.
- [ADR-0053](0053-one-dynamodb-table-holds-every-job.md) — why every lease of a
  job shares one partition key.
- [DynamoDB store](../user-guide/04-connectors/coordination/dynamodb/README.mdx#latency-and-cost)
  — the latency and cost terms as the store page states them.
