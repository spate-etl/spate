---
description: "A DynamoDB lease renewal or takeover of R writes R + 2 or above, so a put lands above a reported delete until TTL collects. ADR-0061 supersedes one part."
---

# ADR-0058 — A DynamoDB ephemeral write leaves its predecessor's removal revision free

- **Status:** accepted
- **Date:** 2026-10-04
- **Supersedes:** [ADR-0057](0057-a-deleted-dynamodb-ephemeral-key-leaves-a-revision-floor.md)
  (watch-delete consequences)
- **Superseded by:** [ADR-0061](0061-a-watch-delete-orders-above-what-its-handle-has-seen.md)
  (the #959 consequence)

## Context and problem statement

The `CoordinationStore` contract in `store/mod.rs` orders a key's deletes and
puts only by revision. A watch of the DynamoDB store in `spate-coordination`
makes up the revision of an expiry or vanish delete, and other handles choose
their next revision without seeing it. A watch could then report a lease or
presence key deleted and then live again at the same revision in three ways
(#949): a renewal of `R`, written at `R + 1`; a takeover on a clock trailing by
about one lease, which also lands at `R + 1`; and two watches of one handle,
where the second reported the removal one above the first and a re-create
above the floor tied it.
[ADR-0057](0057-a-deleted-dynamodb-ephemeral-key-leaves-a-revision-floor.md)
records the takeover tie as a consequence.

## Considered options

- Ephemeral renewals and takeovers of `R` write `R + 2` or above, and a
  handle's watch deletes are kept apart from the revisions it read or wrote
- A contract that lets a put tie a delete, with consumers applying a put at a
  delete's revision
- A per-key memo in each handle of the delete revision its watches reported for
  one judged version

## Decision outcome

Chosen option: "Ephemeral renewals and takeovers of `R` write `R + 2` or
above", because it keeps the contract as stated and changes only the revision
values the store chooses.

An ephemeral `update` at `expected` writes `expected + 2`. A takeover in
`create_ephemeral` writes the highest of the handle's wall clock, one above
every revision the handle read, wrote or reported as a delete for the key, and
`old + 2`. Durable revisions come from the table and do not change.

Each handle's `Observed` keeps `hw`, the highest revision it read or wrote for
a key, apart from `emitted`, the highest delete revision any of its watches
reported. A watch reports a delete, on expiry or on removal, one above `hw` and
above what it delivered, and the delete does not raise `hw`. Every watch of one
handle therefore reports a removal at one revision, at most the floor the
removal left. A handle's own creates and takeovers start above both `hw` and
`emitted`.

The item attributes and the layout recorded in each job's settings do not
change. The store is unreleased, and a handle that still renews at `R + 1`
brings back the tie without losing a CAS, so the layout stays at 2.

The contract that lets a put tie a delete was rejected by the maintainer on
#832, and it would bind every store for one store's defect. The memo fixes the
two-watch case only. The renewal and the takeover need the gap in the writer
anyway, and with the gap a delete at `hw + 1` covers the two-watch case.

### Consequences

- Good, because while the table keeps the key's floor and its last item, a
  renewal, takeover or re-create lands above every delete a watch reported for
  the key, so a consumer that orders a key's events by revision applies the
  put.
- Good, because every watch of one handle reports a removal at the same
  revision.
- Bad, because during a rolling deploy a handle that still renews at `R + 1`
  can tie a delete a watch reported, and nothing refuses it.
- Neutral, because a key's revisions step by two on each renewal, and nothing
  reads the step.
- Neutral, because once native TTL collects a key's floor or its last item, the
  exception in
  [ADR-0057](0057-a-deleted-dynamodb-ephemeral-key-leaves-a-revision-floor.md)
  still applies, and a poller can list an item at exactly the revision of a
  delete it sent.
- Neutral, because a vanish delete can still sit at or below a revision the key
  held that the watch never read (#959).

### Confirmation

In `crates/spate-coordination/src/store/dynamodb/tests.rs`,
`a_renewal_after_an_expiry_delete_lands_above_it`,
`a_takeover_on_a_lagging_clock_lands_above_the_expiry_delete`,
`two_watches_of_one_handle_report_a_removal_below_a_lagging_recreate`,
`an_own_create_sits_above_a_vanish_delete_of_a_collected_key`,
`own_takeover_sits_above_its_expiry_delete_after_a_collected_key` and
`a_subscribing_read_lists_a_renewal_above_the_expiry_delete` pin the rule over
the in-memory table.

## More information

- Landed in the pull request closing #949.
- [ADR-0057](0057-a-deleted-dynamodb-ephemeral-key-leaves-a-revision-floor.md)
  — the watch-delete consequences this replaces; its floor stands.
- [ADR-0051](0051-lease-expiry-judged-by-each-observer.md) — the expiry
  judgment whose delete revisions this keeps apart from read and written ones.
- [DynamoDB store](../user-guide/04-connectors/coordination/dynamodb/README.mdx#lease-expiry)
  — the rule as the store page states it.
