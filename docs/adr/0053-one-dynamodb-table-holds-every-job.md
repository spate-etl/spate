---
description: "One operator-provisioned DynamoDB table holds every job, keyed by job and keyspace, so IAM can scope a job; each keyspace of a job sits on one partition key."
---

# ADR-0053 — One operator-provisioned table holds every job, keyed by job and keyspace

- **Status:** accepted
- **Date:** 2026-09-29
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The DynamoDB store in `spate-coordination` has to place each coordinated job's
durable records, leases and settings in DynamoDB. A listing of one keyspace
prefix should be one `Query`, which reads a single partition key. Tables are
usually provisioned by an operator, with their own billing, backup and access
policy, and a worker's IAM role should be able to reach only its own job.
DynamoDB serves one physical partition at up to 3,000 read units and 1,000
write units per second, as its
[partition key guidance](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html)
states.

## Considered options

- A table per job, created by the store at startup
- A table per job, provisioned by the operator
- One operator-provisioned table for every job, with the job and keyspace as
  the partition key and the record key as the sort key
- The same table, with each keyspace of a job spread over several partition
  keys by a hash of the record key

## Decision outcome

Chosen option: "One operator-provisioned table for every job", because it
keeps a keyspace listing to one `Query`, lets one IAM condition confine a
worker to its job, and leaves table creation out of a production worker's
permissions.

Each job writes three partition keys: `{job}#d` for its durable records,
`{job}#e` for its leases and presence keys, and `{job}#m` for the settings it
started with. The record key is the sort key. The settings item records the
lease TTL and the layout version, and a worker whose values differ fails at
startup. `create_table` is off by default. When set, the store creates a
missing table with on-demand billing and enables time to live on it, which
suits DynamoDB Local and development accounts. At startup the store refuses a
table with another key schema, a local secondary index or replicas.

A table per job was rejected because every new job would need a table
provisioned, or `dynamodb:CreateTable` granted to the workers, and a fleet of
short jobs would leave a table behind for each. Sharding each keyspace was
rejected for now because every listing and poll would become several queries,
each billed at least one read unit, while no fleet has yet been run near one
partition key's limits. #848 tracks it.

### Consequences

- Good, because a `dynamodb:LeadingKeys` condition on `<job>#*` confines a
  worker to its own job's items, so jobs sharing a table cannot touch each
  other's records.
- Good, because a listing or a poll of one keyspace is one query on one
  partition key.
- Good, because the operator provisions and configures one table, and a worker
  needs no permission to create one.
- Bad, because all of a job's leases share one partition key, and so do all of
  its durable records. The lease poll's reads grow with the square of the
  fleet, and by the cost model on #17 they reach 3,000 read units per second at
  about 123 workers at the defaults. That bounds one job's fleet on this
  store, and #848 tracks raising it.
- Bad, because jobs on one table share its throughput and its table-level
  settings, including time to live and billing mode.
- Bad, because the store cannot use a table with a local secondary index,
  which caps a partition key's items at 10 GB, or a global table, whose
  conditional writes do not order across regions.

### Confirmation

`a_second_job_on_one_table_is_isolated` in
`crates/spate-coordination/tests/dynamodb_integration.rs` pins that two jobs on
one table see nothing of each other, against DynamoDB Local.
`startup_rejects_the_wrong_key_schema_and_an_lsi` and
`create_table_creates_and_adopts_the_table` pin the table checks and creation
there. In `crates/spate-coordination/src/store/dynamodb/tests.rs`,
`startup_rejects_the_wrong_schema_an_lsi_and_replicas`,
`a_missing_table_is_created_only_when_allowed` and `a_meta_mismatch_is_fatal`
pin the same checks over the in-memory table, and `a_write_that_landed_resolves_won`
reads the items back at `job#d` and `job#e`. Nothing pins the IAM condition.

## Evidence

No run has measured the partition-key ceiling. The
[cost model on #17](https://github.com/spate-etl/spate/issues/17#issuecomment-5879131523)
sizes each item from serialized records, and #848 derives from it about
0.2 × W² read units per second for the lease poll, with 8 lanes and a 2 s poll
interval. The real-table run in #819 measured table-wide capacity at 3, 10 and
20 workers, and nothing has been run near either partition limit.

## More information

- Landed in #818, which modelled the store over an in-memory table on
  2026-09-29. #819 backed it with the AWS SDK and shipped it on 2026-09-30.
- #848 — sharding a job's keyspaces over several partition keys.
- [ADR-0052](0052-dynamodb-store-polls-without-streams.md) — the poll whose
  reads concentrate on the lease partition key.
- [DynamoDB store](../user-guide/04-connectors/coordination/dynamodb/README.mdx#the-table)
  — the key schema, the IAM policy and the startup checks as the store page
  states them.
