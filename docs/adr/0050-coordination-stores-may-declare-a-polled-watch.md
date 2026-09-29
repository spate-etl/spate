---
description: "A coordination store may back its watch by polling and declare it; the coordinator then narrows its durable watch and reads what a poll can miss."
---

# ADR-0050 — A coordination store may declare a polled watch, and the coordinator reads what it misses

- **Status:** accepted
- **Date:** 2026-09-29
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

[ADR-0024](0024-coordination-store-external-kv.md) chose a store with a watch,
so that a worker learns of a change rather than discovering it on its next poll.
Some stores that suit coordination have no push channel a library can consume,
and can only back `watch` by listing its prefix at an interval. Such a watch
never reports a key written and removed between two listings, and it bills a
read for every key it covers on every poll. The coordinator in
`spate-coordination` assumed a push watch over both whole keyspaces, with a full
listing every `reconcile_interval` as the backstop.

## Considered options

- Require every store to push changes, and leave poll-only stores out
- Relax the watch contract so a watch may miss changes, with no change to the
  coordinator
- Let a store declare a polled watch; on such a store the coordinator watches
  only the durable keys every worker needs promptly, and reads the rest

## Decision outcome

Chosen option: "Let a store declare a polled watch", because it admits
poll-only stores without weakening what a push store's watch promises, and it
keeps the cost of a poll proportional to the keys a worker must see promptly
rather than to the job's history.

`CoordinationStore::watch_mode` returns `Push` unless the store says otherwise.
On a store that returns `Polled`, the durable watch covers the assignment
records, the plan record and a verdict marker. A worker reads the records of a
split it is assigned and has not seen. The leader re-reads each assigned
runnable split that shows no lease every poll interval, reads the spec of any
split it has seen without one, and is the only worker that reconciles, over the
split records only. A new leader lists every split and spec record before it plans or
publishes. A worker that reports the job terminal writes the marker, and a
worker that sees it judges from a listing.

The safety boundary of [ADR-0026](0026-coordination-fencing.md) is unchanged:
ownership moves only through the progress record's compare-and-swap, and these
reads only fill a view.

### Consequences

- Good, because a store with no push channel can host coordination, and a
  push store runs exactly as before.
- Good, because a follower's steady-state reads no longer grow with the job's
  split history, only with the fleet's leases and assignment records.
- Bad, because discovery on a polled store takes up to one poll interval, and a
  lease expiry reaches peers at their next poll.
- Bad, because the leader pays a point read per assigned split with no lease
  every interval, and every worker lists the split records once at the verdict.
- Bad, because the coordinator now has two discovery modes, and every change to
  it has to hold in both.

### Confirmation

`tests/polled_reads.rs` runs each mechanism over a polled test store and fails
with it removed. `polled_workers_list_only_the_records_no_watch_carries` pins
the narrowed watch and listings, and
`a_push_store_keeps_the_full_reconcile_and_no_marker` pins the push path. The
multi-worker scenarios run over the polled test store in default CI.

## More information

- Landed in #815.
- The user guide's [work assignment](../user-guide/02-concepts/08-work-assignment.mdx#discovery)
  page describes discovery on each kind of store.
- This record qualifies ADR-0024's consequence that "`watch` exists, so a worker
  learns about a change instead of discovering it on its next poll": on a store
  that declares a polled watch, that learning takes up to one poll interval.
