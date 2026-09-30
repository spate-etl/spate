---
description: "Each handle may judge lease expiry on its own clock, and the DynamoDB store does; a watching peer sees a dead lease expire within one lease plus two polls."
---

# ADR-0051 — Lease expiry may be judged by each observer, and the DynamoDB store does so

- **Status:** accepted
- **Date:** 2026-09-29
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

[ADR-0024](0024-coordination-store-external-kv.md) gave the store trait a TTL
keyspace: a lease key disappears one TTL after its last write, and watchers see
a delete. NATS JetStream expires a key on the server's clock, so one clock
decides for every worker. DynamoDB has no server-side expiry a lease can use.
Its time to live deletes an expired item "typically within a few days" of its
expiry time, by
[AWS's account of it](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/howitworks-ttl.html),
and a condition expression cannot compare an attribute with the server's time.
The DynamoDB store in `spate-coordination` has to decide when a lease has
lapsed, and the `CoordinationStore` contract in `store/mod.rs` has to say
whether a store may decide it that way.

## Considered options

- DynamoDB's time to live as the lease expiry
- A write time in each lease item, compared by the reader against its own wall
  clock
- Each handle judges expiry on its own monotonic clock: a lease is expired
  once a read that began one TTL after the handle first read the lease's
  current revision still returns that revision

## Decision outcome

Chosen option: "Each handle judges expiry on its own monotonic clock", because
it is the only option that decides within a lease without comparing clocks
across machines. The Kinesis Client Library applies the same rule to its own
DynamoDB lease table, which #17 cited as prior art.

The store trait allows it. `store/mod.rs` states that a store may judge expiry
on its own clock, or each handle may judge it from the first read that returned
the key's current revision, confirmed by a later read of that revision. The
DynamoDB store does the second. A read that fails judges nothing. Expiry reaches
the handle's watch as a delete at a revision above every revision the handle
has returned or written for the key. A takeover is a write conditional on the
revision the handle judged expired.

This qualifies ADR-0024's TTL keyspace: on such a store, "one TTL after its
last write" is measured by each reader from when it first saw that write. It
keeps [ADR-0026](0026-coordination-fencing.md): expiry makes a split claimable,
and ownership still moves only through the progress record's compare-and-swap.
Peers learn of a lease change at their next poll, as
[ADR-0050](0050-coordination-stores-may-declare-a-polled-watch.md) records for
a store with a polled watch.

### Consequences

- Good, because no decision compares clocks across machines, and clock skew
  between workers moves no expiry.
- Good, because a worker whose reads fail through an outage expires no lease in
  its view, so peers do not take over live work when the table returns.
- Bad, because a watching peer sees a dead worker's lease expire up to one
  lease plus two poll intervals after the last heartbeat, plus the time its
  polls spend querying. It reads the heartbeat up to one interval late, and the
  poll that confirms expiry starts up to one interval after the lease has run.
- Bad, because takeover then adds `rebalance_delay` and the assignment read:
  up to one poll interval for the leader's next assignment, one more for the
  new owner to read it, and one more for each of the new owner's eventually
  consistent polls that misses the assignment.
- Bad, because a worker that starts watching after the death waits one lease
  from its own first read of the lease, plus up to one poll interval. That is
  close to two leases after the death when it starts just before the lease
  would have run out.
- Bad, because the owner stops its splits at its first renewal tick or poll
  after one lease has run from its last renewal, while a peer measures from
  when its read returned. The two can overlap by one renewal's response time
  plus up to one poll interval and its query. ADR-0026's fence makes that
  overlap safe.
- Neutral, because a lease created by another worker takes its revision from
  that worker's wall clock, kept above every revision it has seen for the key.
  Skew can put a re-created lease below the revision a watcher holds. The
  watcher repairs that on its watch, with a delete above what it held and then
  the new lease.

### Confirmation

Unit tests over the in-memory table in
`crates/spate-coordination/src/store/dynamodb/tests.rs`:

- `expiry_needs_a_confirming_read` pins that failed polls judge nothing;
- `a_fresh_observer_expires_a_stale_lease_one_ttl_after_its_first_read` pins
  the late observer's window;
- `an_expiry_decision_older_than_an_own_renewal_is_dropped` pins that a stale
  judgment emits no delete over a renewal;
- `a_takeover_is_conditional_on_the_expired_version` pins that a renewal
  landing before the takeover wins;
- `a_recreated_key_between_two_polls_is_repaired_on_the_watch` and
  `a_recreated_key_on_one_handle_sits_above_its_watch_delete` pin the revision
  repair.

`store_contract::the_contract_holds_on_the_dynamodb_store` holds the trait
contract, expiry included, and the multi-worker scenarios run the store under
the coordinator. Both run in default CI, and `tests/dynamodb_integration.rs`
runs them against DynamoDB Local. Nothing pins
the latency bounds above; they follow from the poll cadence.

## More information

- Landed in #818, which modelled the store over an in-memory table on
  2026-09-29. #819 backed it with the AWS SDK and shipped it on 2026-09-30.
- [DynamoDB store](../user-guide/04-connectors/coordination/dynamodb/README.mdx#lease-expiry)
  — lease expiry and takeover latency as the store page states them.
- [Work assignment](../user-guide/02-concepts/08-work-assignment.mdx#discovery)
  — the normative statement of observer-judged expiry.
- [ADR-0052](0052-dynamodb-store-polls-without-streams.md) — why the store's
  watches poll.
