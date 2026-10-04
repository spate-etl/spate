---
description: "A deleted DynamoDB lease or presence key leaves a revision floor that every create checks, so a re-created key lands above every revision it held."
---

# ADR-0057 — A deleted DynamoDB ephemeral key leaves a revision floor that every create checks

- **Status:** accepted
- **Date:** 2026-10-04
- **Supersedes:** [ADR-0051](0051-lease-expiry-judged-by-each-observer.md)
  (revision consequence), [ADR-0053](0053-one-dynamodb-table-holds-every-job.md)
  (partition keys)
- **Superseded by:** —

## Context and problem statement

The `CoordinationStore` contract in `store/mod.rs` says a key's revisions
strictly increase across its write history and are never reused. The DynamoDB
store in `spate-coordination` takes an ephemeral create's revision from the
creating handle's wall clock, kept above every revision that handle has seen
for the key, and a delete removes the item. A handle whose clock lags can then
re-create a lease, presence or leader key on a revision an earlier incarnation
held. A worker still holding that revision wins a CAS against the new lease
(#832), and a watch can report a delete above a create its own handle has won
(#831). [ADR-0051](0051-lease-expiry-judged-by-each-observer.md) accepted a
lower re-create as repaired on the watch. It did not cover the equal revision,
and a CAS never reads the watch.

## Considered options

- A revision floor per key in a partition of its own, raised by every delete
  and checked by every create in one transaction
- A tombstone left in place in the ephemeral partition, which a create
  replaces at a revision above it
- A read of the floor before an ordinary conditional create
- An incarnation id carried on every CAS, with a change to the contract
- A contract that allows a revision to repeat across incarnations under clock
  skew

## Decision outcome

Chosen option: "A revision floor per key in a partition of its own", because
it keeps the contract as stated while the table keeps the key's history, and
adds nothing to what the lease poll and the reconcile listing read.

Each job writes a fourth partition key, `{job}#f`, beside the three
[ADR-0053](0053-one-dynamodb-table-holds-every-job.md) lists. A floor item has
the record key as its sort key and holds `v`, the floor, and `x`, its
collection time. A delete of an ephemeral key at revision `R` first raises the
floor to at least `R + 1`, conditional on the floor being absent or lower, and
removes the key only once the raise has succeeded. An ephemeral create is one
`TransactWriteItems`: a `ConditionCheck` that the floor item is absent or below
the new revision, and the conditional put. A create that meets only a higher
floor retries once just above it, and a second floor answer is Retryable.

Floors are kept for a day: `x` is a day ahead of the delete on the deleting
handle's wall clock, and native TTL collects the item after that. The item
layout recorded in each job's settings moves to 2, so a job started at layout 1
is refused at startup until it finishes or its items are deleted. Workers need
`dynamodb:ConditionCheckItem`, which governs the `ConditionCheck` action
([IAM with transactions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis-iam.html)).

A tombstone in `{job}#e` was rejected because the lease poll and the reconcile
listing read that whole partition: at the 17.45 completed splits per second of
#819's 20-worker run, a day of tombstones is about 1.5 million items and
150 MB read on every poll. A read before an ordinary create was rejected
because a delete can land between the read and the write. A transactional
delete would cost 4 write units where the raise and the removal cost 2. The
incarnation id and the weaker contract were rejected by the maintainer on #832.

### Consequences

- Good, because a re-created key lands above every revision the key held while
  the table keeps its floor and its last item, so a stale CAS at an earlier
  incarnation's revision loses, and a create over a key a delete removed lands
  above the delete every watch reported for it. A takeover of a lease a watch
  reported expired can land on that delete's revision (#949).
- Good, because no poll or listing reads `{job}#f`, and the `LeadingKeys`
  condition on `<job>#*` covers it.
- Bad, because a create is a two-item transaction. DynamoDB charges two writes
  per item in a transaction, cancelled or not
  ([capacity for transactions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html#transaction-capacity-handling)),
  so a create costs 4 write units where it cost 1, and with the floor raise a
  completed split costs about 10 where it cost 6.
- Bad, because the SDK does not retry a cancelled transaction, so a create
  throttled inside it gets one attempt and returns Retryable.
- Bad, because while a create transaction is in flight on a key, a renewal, a
  floor raise or a removal of that key can fail with
  `TransactionConflictException`, which is Retryable and which the SDK does not
  retry, and two racing creates of one key can both get Retryable. Claims,
  presence and elections all create, and every caller treats Retryable as
  retry-later.
- Bad, because a delete's two to nine calls share the one `op_timeout` the
  coordinator gives the operation, so under slow calls the floor can be raised
  and the removal not run, and the key then lasts until it expires.
- Bad, because a job started before the floor is refused at startup, and the
  IAM policy gains an action.
- Neutral, because native TTL can collect a floor a day after the key's last
  delete and a live item a day after its last write, each by the clock of the
  handle that stamped its `x`, and a live item that TTL collects leaves no
  floor. Once either is gone, a re-create lands at or below the key's last
  revision only on a clock that trails the one that wrote that revision by at
  least a day, less however far the clock that stamped the collected item
  lagged. The watch keeps its repair for that case.

### Confirmation

In `crates/spate-coordination/src/store/dynamodb/tests.rs`,
`a_stale_cas_loses_to_a_key_recreated_on_a_lagging_clock`,
`an_own_create_sits_above_a_deleted_key_its_watch_delivered`,
`a_create_at_exactly_the_floor_lands_above_it`,
`a_recreate_on_a_lagging_clock_lands_above_the_watch_delete`,
`a_floor_answer_leaves_the_watch_delete_at_the_floor`,
`a_delete_raises_the_floor_before_removing_the_key`,
`a_failed_floor_raise_keeps_the_key` and
`a_job_started_at_layout_1_is_refused` pin the floor over the
in-memory table, and `a_recreated_key_between_two_polls_is_repaired_on_the_watch`
pins the repair once a floor is collected. In
`crates/spate-coordination/src/store/dynamodb/test_http.rs`,
`a_create_checks_the_floor_in_one_transaction`,
`a_cancelled_create_retries_above_the_floor_it_returned` and
`a_create_that_meets_a_floor_twice_is_retryable` pin the request and how its
cancellation reasons are read.
`a_create_lands_above_a_floor_on_dynamodb_local` and
`the_store_contract_holds_over_dynamodb_local` in
`crates/spate-coordination/tests/dynamodb_integration.rs` run the transaction
against DynamoDB Local.

## More information

- Landed in #953, which raises the floor on every delete, and in #956, which
  adds the create's check, on 2026-10-04.
- [ADR-0051](0051-lease-expiry-judged-by-each-observer.md) — the revision
  consequence this replaces; its expiry rule stands.
- [ADR-0053](0053-one-dynamodb-table-holds-every-job.md) — the table layout
  this adds a partition key to.
- [DynamoDB store](../user-guide/04-connectors/coordination/dynamodb/README.mdx#lease-expiry)
  — the floor and its retention as the store page states them.
