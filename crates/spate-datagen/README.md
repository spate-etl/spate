# spate-datagen

The datagen source (`spate-datagen`, feature `datagen` on the `spate` facade)
generates a synthetic storefront event stream in process, so a pipeline runs
with no broker and no bucket behind it. It is a demo and test source, and it
keeps no durable progress.

## Enable

Turn on feature `datagen` on the [`spate`](https://crates.io/crates/spate)
facade, or `datagen-avro` for Avro payloads. The facade README has the Cargo
snippet.

## Configuration

The source reads the pipeline's `source: { datagen: ... }` section. Every key
has a default, so `datagen: {}` is a complete section. An unknown key is
rejected.

```yaml
source:
  datagen:
    dataset: storefront      # the built-in model, and the only one
    encoding: json           # json | avro (avro needs feature `datagen-avro`)
    partitions: 4            # lanes, and therefore framework partitions
    seed: 0                  # lane i derives its own stream from this
    tick_interval: 100ms     # per-lane release cadence; 0s = unthrottled
    events_per_tick: 10      # per lane per tick; ignored when unthrottled
    count: 10000             # total across all lanes; omit for unbounded
    clock: fixed             # fixed | wall
    epoch_ms: 1767225600000  # base for the fixed clock
```

Each value above is the default. `count` is unset by default.

`partitions × events_per_tick ÷ tick_interval` is the release ceiling, 400
events/s at the defaults. A lane polled a whole interval late re-anchors to the
present and skips the ticks it missed, so a pipeline that cannot absorb the
configured rate runs below it. There is no `rate:` key: a second spelling of
the same rate could come to disagree with the first.

With `count` set, the total splits across the lanes, no two of them more than
one event apart. The pipeline drains to a clean exit once every lane has
released its share. A `count` below `partitions` is rejected, because some lanes
would start with nothing to release.

The
[datagen source page](https://spate.kainth.dev/docs/user-guide/connectors/sources/datagen#configuration)
has the full key table and the validation rules.

## The dataset

One built-in, named dataset: a storefront. Orders are placed over a 32-entry
catalog in five regions for 1,024 customers. Each event is drawn from a fixed
mix of 60% order placements, 35% payment captures and 5% refunds.

The three event kinds under `encoding: json`, with illustrative values:

```json
{"type":"order_placed","order_id":12,"customer_id":7,"region":"eu-west","placed_at":1767225600005,"lines":[{"sku":"KBD-01","qty":2,"unit_cents":7900}]}
{"type":"payment_captured","order_id":12,"amount_cents":15800}
{"type":"refund_issued","order_id":12,"amount_cents":7900,"reason":"damaged"}
```

Under `encoding: avro`, each payload is a bare Avro datum against
`spate_datagen::EVENT_SCHEMA_JSON`, a union of the three records, and carries
no `type` field.

There is no `fields:` map, by design. A payment has to name an order that was
really placed, for an amount that matches its lines, on the same partition, at
a later offset. That is a property of the dataset as a whole, and no field-wise
schema can state it without growing into a small programming language. A named
dataset gets it for free.

## Referential integrity without coordination

Each lane owns a disjoint slice of the order-id space
(`order_id = n × partitions + lane_index`) and keeps two bounded rings of its
own, one of the orders it has placed and one of the orders it has captured. A
payment is drawn from the first and a refund from the second, so each references
an order the same lane minted, on the same partition, at a strictly greater
offset. A payment settles the order's line total exactly. A refund is that total
or a rounded-down half, third or quarter of it, never more. No lane reads
another's state, so nothing is shared on the record path. The payload key
carries the order id, so `KeyHashRouter` sends an order, its payment and its
refund to one shard.

The mix places faster than it captures, and an order evicted from a full ring
is never captured, so unpaid orders accumulate for as long as the pipeline
runs. A balance check downstream reconciles only the orders that were captured.

## Parallelism

`partitions` sets the lane count, from 1 to 1024, and each lane is one
framework partition. The framework spreads the lanes across pipeline threads,
and a lane is polled only by the thread that owns it, so decode parallelism is
`min(partitions, pipeline.threads)`. The
[architecture page](https://spate.kainth.dev/docs/user-guide/concepts/architecture#lane-count-is-read-concurrency-not-decode-parallelism)
explains why lanes beyond the thread count add no throughput. Changing
`partitions` reshuffles which lane mints which order.

Every process that opens the source runs all of its lanes, so instances do not
split one stream between them. Two instances with the same configuration and
`clock: fixed` generate the same events.

## Delivery

**This is a demo and test source. Do not build a production pipeline on it.**

- `commit()` stores watermarks in memory and nowhere else.
- The source claims no resumability. A restart or a crash begins every lane at
  offset 0, so the same configuration replays the whole stream from the
  beginning, with the same ids and offsets. Under `clock: wall`, only
  `placed_at` differs between runs. That is strictly more duplication than a
  real at-least-once source. Such a source replays only from its last committed
  position.
- A `resume_from:` file is deliberately declined. A demo source that appears to
  resume durably is one somebody builds on.
- A bounded run exits `Completed` only once every batch is acknowledged and the
  final watermark is committed, so a clean exit from a bounded run is an
  end-to-end delivery assertion.

Opening the source logs a `WARN` saying it keeps no durable progress. The
[delivery section of the datagen source page](https://spate.kainth.dev/docs/user-guide/connectors/sources/datagen#delivery-semantics)
and
[Delivery guarantees](https://spate.kainth.dev/docs/user-guide/concepts/delivery-guarantees)
cover the framework's side.

## Metrics

The source's own families, all prefixed `spate_datagen_source_`, are listed
with their labels in the
[Metrics section of the datagen source page](https://spate.kainth.dev/docs/user-guide/connectors/sources/datagen#metrics),
which also says that the source has no consumer-lag family, and what covers the
bounded case instead.

## Dependencies

Beside `spate-core`: `serde`, `serde_json`, `humantime-serde` and `tracing`,
with `apache-avro` behind the optional `avro` feature. The facade's
`datagen-avro` turns that feature on. The random generator is a hand-rolled
SplitMix64, so the crate takes no direct dependency on a random-number crate.

## Related

- [`spate`](https://crates.io/crates/spate) — the facade an application depends
  on, with the Cargo snippet.
- [Datagen source page](https://spate.kainth.dev/docs/user-guide/connectors/sources/datagen)
  — every key, the determinism rules and the metric families.
- [API documentation on docs.rs](https://docs.rs/spate-datagen) — the source,
  its configuration types and the storefront event types.
- [`storefront_pipeline.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/storefront_pipeline.rs)
  — a bounded stream through a whole pipeline with nothing installed
  (`cargo run -p spate --example storefront_pipeline`).
- [`clickhouse_load.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/clickhouse_load.rs)
  — generated Avro rows into a sharded ClickHouse sink, to measure how fast it
  takes them.
- [Examples index](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/README.md)
  — every example, with the features and infrastructure each needs.
- [`spate-test`](https://crates.io/crates/spate-test) — in-memory sources and
  sinks, for a test that drives a pipeline record by record.
- [ADR-0042](https://spate.kainth.dev/docs/adr/first-party-load-generator-source)
  — why the crate ships one named dataset, and the alternatives it turned down.
