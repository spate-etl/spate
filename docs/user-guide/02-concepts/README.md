# Concepts

The mental model behind etl-rs. These four pages explain the contracts the
framework holds — and expects you to hold — with the reasoning behind each.
The canonical, exhaustive version lives in [docs/DESIGN.md](../../DESIGN.md);
these pages are the working subset a pipeline author needs.

## [1. Architecture](01-architecture.md)

The anatomy of a running pipeline process: pinned pipeline threads (poll →
deserialize → operator chain → route), the single shared tokio I/O runtime
(sink workers, admin server, async fetchers), and the control plane. Why one
process runs exactly one pipeline.

## [2. Delivery guarantees](02-delivery-guarantees.md)

How at-least-once actually works: refcounted acknowledgements per source
batch, the contiguity tracker, and watermarks that commit only after durable
sink acks — across rebalances and shutdown. Also the honest limits: what
ClickHouse dedup tokens do and do not cover, and how to design tables for
replay.

## [3. Backpressure](03-backpressure.md)

Flow control without ever blocking the source: `try_send`, pause, keep
polling. The in-flight byte budget, its watermarks, and the sizing rule that
keeps a saturated pipeline out of pause duty-cycling.

## [4. Error handling](04-error-handling.md)

The error taxonomy (retryable, record-level, fatal), the two record-level
policies (Skip and Fail — there is no dead-letter queue), and the metrics
that make every drop and every error visible.

## Related

- [Guides](../03-guides/README.md) — task-oriented how-tos built on these
  concepts.
- [Monitoring](../05-deployment/monitoring.md) and
  [docs/METRICS.md](../../METRICS.md) — observing the contracts at runtime.
- [Glossary](../07-reference/glossary.md) — the vocabulary (lane, shard,
  watermark, epoch, sealed batch, ...).
