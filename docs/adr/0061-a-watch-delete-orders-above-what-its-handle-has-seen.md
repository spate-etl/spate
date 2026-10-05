---
description: "A watch delete orders above every revision of the key its own handle returned or delivered before the deletion, and may sit at or below another handle's."
---

# ADR-0061 — A watch delete orders above what its handle has seen of the key

- **Status:** accepted
- **Date:** 2026-10-05
- **Supersedes:** [ADR-0058](0058-a-dynamodb-ephemeral-write-leaves-its-predecessors-removal-revision.md)
  (the #959 consequence)
- **Superseded by:** —

## Context and problem statement

The `CoordinationStore` contract in `store/mod.rs` required a watch's delete
revision to be above every revision the key held before the deletion. The
DynamoDB store's polled watch makes up a vanish delete's revision from what its
handle has seen, so it cannot order the delete above a renewal another worker
writes and removes between two polls (#959).
[ADR-0058](0058-a-dynamodb-ephemeral-write-leaves-its-predecessors-removal-revision.md)
kept the contract as stated and listed this case as a remaining consequence.

## Considered options

- A delete orders above what the same handle returned or delivered, counting
  its clones, for every revision the key held before the deletion
- The DynamoDB watch reads the key's revision floor before it reports a vanish
  delete, and reports at least the floor
- A delete orders above every revision the same watch stream delivered

## Decision outcome

Chosen option: "A delete orders above what the same handle returned or
delivered", because it is the order the coordinator's comparisons use, and a
polled store can meet it without a read per delete.

The coordinator in `spate-coordination` compares a watch delete with revisions
from its own writes, its listings and earlier watch streams of the same
handle. No path compares one with a revision another worker produced.

The doc of `WatchEvent::Delete::revision` states the rule. It is above every
revision the key held before the deletion that this handle, or a clone of it,
returned from a create, update, read or listing, or that a watch of it
delivered. It can sit at or below a revision another handle wrote that this
handle never saw.

The stale-echo rule in the same doc stands. A revision the handle saw after
the deletion is outside the rule, and a consumer that rewrote the key ignores
a delete below its own write's revision.

The DynamoDB store records every revision a read of its handle returns, also
when an own write landed while the read ran. It keeps a key's record while a
watch of the handle holds a delivered revision of it, and while a poll read
that may still list the key is in flight. The memory and NATS stores draw
delete revisions from one sequence per keyspace and still meet the stronger
order.

The floor read costs one consistent read per vanished key per poller per handle.
The floor expires a day after the delete, so it leaves the same exception as the
chosen option. The stream-scoped rule leaves the coordinator's comparisons with
own writes, listings and earlier streams without a contract. A polled test
double that ordered deletes only above what it delivered lost deletes this way
(#805).

### Consequences

- Good, because every comparison the coordinator makes between a watch delete
  and a revision it holds rests on the contract.
- Good, because a polled store meets the contract from what its handle saw.
- Bad, because a consumer cannot order a watch delete against a revision
  another handle produced.
- Bad, because one store's watch narrows the trait for every store, the trade
  ADR-0058 declined.
- Neutral, because the memory and NATS stores still meet the stronger order,
  and a custom store that met it needs no change.
- Neutral, because once native TTL collects a key's floor or its last item, the
  exception in
  [ADR-0057](0057-a-deleted-dynamodb-ephemeral-key-leaves-a-revision-floor.md)
  still applies, and a DynamoDB watch can report a delete at or below a
  revision its handle returned before then.

### Confirmation

`delete_above_a_clones_write` in
`crates/spate-coordination/tests/support/contract.rs`, run on every store by
the conformance suite, and the three `a_polled_delete_*` tests in
`crates/spate-coordination/tests/store_contract.rs`. In
`crates/spate-coordination/src/store/dynamodb/tests.rs`,
`a_vanish_delete_sits_above_what_its_watch_delivered`,
`a_vanish_delete_sits_above_a_revision_its_handle_read`,
`a_vanish_delete_sits_above_a_revision_its_handle_listed`,
`a_vanish_delete_sits_above_an_own_write_its_watch_missed`,
`a_read_overtaken_by_an_own_write_still_orders_the_delete`,
`a_listing_overtaken_by_an_own_write_still_orders_the_delete`,
`a_snapshot_overtaken_by_an_own_write_still_orders_another_watchs_delete`,
`a_key_a_watch_holds_outlives_failed_polls_with_what_its_handle_saw`,
`a_poll_read_spanning_two_ttls_still_orders_the_delete`,
`a_gone_key_no_watch_holds_is_evicted`,
`a_failed_poll_read_stops_holding_back_eviction`,
`an_overtaken_get_during_a_spanning_poll_read_still_orders_the_delete`,
`a_key_a_second_watch_holds_outlives_the_first_dropping`,
`a_renewed_gone_key_is_evicted`,
`a_key_an_overtaken_snapshot_delivered_stays_held` and
`eviction_judges_against_the_oldest_read_in_flight` pin the DynamoDB store.

## More information

- Landed in the pull request closing #959.
- [ADR-0058](0058-a-dynamodb-ephemeral-write-leaves-its-predecessors-removal-revision.md)
  — the consequence this record resolves; its decision stands.
- [ADR-0051](0051-lease-expiry-judged-by-each-observer.md) — the handle rule
  for the DynamoDB store's expiry judgment.
- [ADR-0050](0050-coordination-stores-may-declare-a-polled-watch.md) — the
  polled watch.
- [DynamoDB store](../user-guide/04-connectors/coordination/dynamodb/README.mdx#lease-expiry)
  — the rule as the store page states it.
