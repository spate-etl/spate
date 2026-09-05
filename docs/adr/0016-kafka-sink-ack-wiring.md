---
description: "The Kafka sink resolves each batch's acknowledgment from a shared delivery-report countdown carried as the opaque pointer, avoiding a future per message."
---

# ADR-0016 — Kafka sink acknowledgments resolve from a per-batch delivery-report countdown

- **Status:** accepted
- **Date:** 2026-07-13 (recorded 2026-08-06 from the decision log)
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The frozen sink contract says a write returns when the batch is durably written,
and the framework resolves acknowledgments from that outcome. Kafka does not
work that way: a produce call enqueues a message locally and returns, and
durability is reported later, per message, through a delivery-report callback.

So the connector has to turn N per-message reports into one per-batch outcome,
without blocking the callback, which librdkafka invokes from its own poll thread,
and without allocating per message at batch sizes reaching hundreds of thousands.

## Considered options

- One future per message, awaited together
- A `ThreadedProducer` with a per-batch countdown carried as each message's
  opaque pointer, awaited once inside the write
- Fire and forget, resolving acknowledgments when produce returns

## Decision outcome

Chosen option: "A `ThreadedProducer` with a per-batch countdown", because it is
O(1) await state per batch rather than O(messages), and because a custom context
is needed for statistics anyway, so the callback machinery is not extra
apparatus.

Each message carries a shared countdown as its librdkafka opaque. The callback
decrements and never blocks. The write awaits the countdown once, and returning
`Ok` is the durable-acknowledgment point.

Fire-and-forget was rejected outright: it would resolve acknowledgments from a
local enqueue, so offsets would commit ahead of durability, which is
at-most-once wearing at-least-once's clothes. Per-message futures were rejected
on cost. At 500,000 messages in a batch, that is 500,000 allocations and wakers
for one outcome.

### Consequences

- Good, because acknowledgment state is a single counter per batch regardless
  of message count.
- Good, because the delivery-report callback stays non-blocking, so librdkafka's
  poll thread is never held up by our accounting.
- Bad, because the countdown is reached through a raw opaque pointer, so message
  lifetime and the countdown's lifetime are coupled by hand rather than by the
  type system.
- Bad, because a batch resolves only when its slowest message reports, so one
  straggler holds the whole batch's acknowledgment.

### Confirmation

Producer teardown is what closes the loop safely: rdkafka's `Drop` purges the
queue and in-flight messages, and the purged messages' reports reclaim the
countdown opaques, so no custom `Drop` is needed and none is maintained.

## Evidence

Teardown is bounded at roughly 600 ms — a purge, a 500 ms flush, and the poll
thread joined. Measured by a rig this repository no longer carries.

## More information

- Landed in `6af6861` (#24).
- [ADR-0018](0018-kafka-sink-forced-durability.md) — why returning `Ok` can be
  treated as durable at all.
- [Kafka sink](../user-guide/04-connectors/sinks/kafka/README.mdx).
