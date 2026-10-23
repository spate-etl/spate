---
description: "A feed trait with a spate-test double replaces spate-core's SplitPlanner and SplitCoordinator traits, their driver and scripted_coordinator."
---

# ADR-0066 — A feed trait replaces the split coordination traits in `spate-core`

- **Status:** accepted
- **Date:** 2026-10-12
- **Supersedes:** [ADR-0029](0029-framework-owned-coordination-driver.md),
  [ADR-0031](0031-coordinated-gains-and-completion-latency.md),
  [ADR-0037](0037-control-plane-wait-ownership.md),
  [ADR-0059](0059-final-coordinated-commit-runs-under-one-op-timeout-budget.md)
- **Superseded by:** —

## Context and problem statement

`SplitPlanner`, `SplitCoordinator` and `CoordinationDriver` shipped in 0.1.0
with `spate_test::scripted_coordinator` as their test double.
`Source::configure_coordination` and `commit_final` were added after it. Their
contract is a lease: a fenced durable commit, epochs, and a `Lost` outcome. The
work-feed record moves work distribution to a queue, which has a visibility
lease and no fence.

## Considered options

- Keep the traits, and have each feed backend implement `SplitCoordinator`
- Remove the traits, and add a feed trait with a `spate-test` double
- Keep the traits deprecated for one release, then replace them

## Decision outcome

Chosen option: "Remove the traits, and add a feed trait", because their
contract promises fencing and epochs that no queue backend can honor, and
keeping both shapes for a release doubles the code path a source is tested
against.

The feed trait, `spate_core::feed::Feed`, carries a unit from receipt to
acknowledgment under the ack rule its module docs state. It is synchronous and
dyn-compatible, and no method waits on I/O. A backend runs its receives, lease
renewals and publishes on a task or thread it owns. `receive` returns what
that task has buffered, and the other calls hand their request to it. A source
drives the feed from the controller thread and waits only by parking on the
`FeedPark` the backend signals, for at most its poll timeout, so INV-2 holds.

The trait also publishes a unit and reports through a `Confirmation` when the
feed has stored it, which the lister ([the lister-resume record](0074-a-lister-resumes-after-the-last-key-it-checkpointed-once-the-feed-confirmed-it.md)) and a worker
re-enqueueing a unit ([the notification-read record](0077-a-notification-units-read-classifies-a-deleted-or-overwritten-object.md)) use. Methods take `&self`,
so one `Arc<dyn Feed>` serves a reader and a publisher. Every operation carries
only `spate-core` and `std` types. No rdkafka delivery result, AWS SDK error or
`object_store` type appears in a public signature, so INV-6 holds.

A second delivery of a unit the worker holds arrives under a new receipt and is
read again from its start. INV-1 allows the duplicate, and no backend
deduplicates by unit id. `spate-core` gains an in-process feed for one
instance, and `spate-test` gains a scripted feed, so a source's behavior under
redelivery, duplicate delivery and dead-lettering is testable without a queue.
The coordination traits and `scripted_coordinator` leave in the v0.4.0 removal
that the work-feed record describes.

### Consequences

- Good, because the trait states the feed's guarantees in its own terms, with
  no lease vocabulary for a backend that has no fence.
- Good, because a source is tested under redelivery and duplicates with a
  first-party double.
- Bad, because a custom source that implemented the coordination traits has
  to be rewritten against the feed trait.
- Bad, because the measurement behind the work-feed record ran on the
  coordination traits, so no figure measures the trait this record ships.
- Bad, because the driver's settled behaviors go with it: additive gains
  (ADR-0031), the single wait point (ADR-0037) and the bounded final commit
  (ADR-0059). The graceful-stop record states the feed's stop.

### Confirmation

`crates/spate-test/src/feed.rs` provides the scripted feed, and
`crates/spate-s3/tests/feed_driver.rs` reads through it in
`a_redelivered_unit_is_read_again_from_its_start`,
`a_duplicate_delivery_of_a_held_unit_is_read_twice`,
`a_publish_is_confirmed_through_the_feed_trait` and
`a_unit_is_acknowledged_only_after_its_last_record_is_acknowledged`. The same
file reads the in-process feed in
`in_process_units_are_acknowledged_or_dead_lettered_at_the_cap`, and
`crates/spate-core/src/feed/memory.rs` pins its delivery cap in
`a_unit_at_the_cap_moves_to_the_in_memory_dead_letter_list`. INV-6 is held by
review; nothing checks the trait's signatures.

## More information

- Landed in #1124.
- The work-feed record — the work feed this trait serves, and the break that
  removes the coordination traits.
- [Work feed](../user-guide/02-concepts/09-work-feed.mdx) — units, the ack
  rule, redelivery and dead-lettering as a source sees them.
- [Feed-reading sources](../user-guide/06-extending/custom-source.mdx#feed-reading-sources)
  — implementing a source or a backend against the trait.
