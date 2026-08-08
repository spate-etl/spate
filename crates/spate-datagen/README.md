# spate-datagen

Synthetic commerce-event source for the
[Spate](https://github.com/spate-etl/spate) framework. It removes the
infrastructure prerequisite from a pipeline: no broker, no bucket, no
coordination store — just a stream of storefront events on as many partitions
as you ask for.

Applications should depend on the **`spate`** facade crate with the
**`datagen`** feature rather than on this crate directly.

## The dataset

One built-in, named dataset: a storefront. Orders are placed over a 32-entry
catalog in five regions for 1,024 customers; most of them are paid for, and a
few of those payments are refunded.

```json
{"type":"order_placed","order_id":12,"customer_id":7,"region":"eu-west","placed_at":1767225600000,"lines":[{"sku":"KBD-01","qty":2,"unit_cents":7900}]}
{"type":"payment_captured","order_id":12,"amount_cents":15800}
{"type":"refund_issued","order_id":12,"amount_cents":7900,"reason":"damaged"}
```

**There is no `fields:` map, and that is a decision rather than a gap.** A
payment has to name an order that was really placed, for an amount that matches
its lines, on the same partition, at a later offset — a property of the dataset
as a whole, which no field-wise schema can state without growing into a small
programming language. A named dataset gets it for free.

## Referential integrity without coordination

Each lane owns a disjoint slice of the order-id space
(`order_id = n × partitions + lane_index`) and keeps its own bounded ring of the
orders it has placed and captured. A payment or refund is drawn from that ring,
so it always references an order the same lane minted — same partition,
strictly greater offset, amount recomputed from the order's lines. No lane
reads another's state, so nothing is shared on the record path. The payload key
carries the order id, so `KeyHashRouter` colocates an order and its payment.

## Configuration

```yaml
source:
  datagen:
    dataset: storefront      # the built-in model
    encoding: json           # json | avro (avro needs the `avro` feature)
    partitions: 4            # lanes, and therefore framework partitions
    seed: 0                  # lane i derives its own stream from this
    tick_interval: 100ms     # per-lane release cadence; 0s = unthrottled
    events_per_tick: 10      # per lane per tick; ignored when unthrottled
    count: 10000             # total across all lanes; omit for unbounded
    clock: fixed             # fixed | wall
    epoch_ms: 1767225600000  # base for the fixed clock
```

The effective rate is `partitions × events_per_tick ÷ tick_interval` — 400
events/s at the defaults. There is deliberately no `rate:` key: expressing it
twice is how the two spellings come to disagree.

With `count` set, the source drains the pipeline to a clean exit once every
lane has released its share of the total.

## Delivery, stated plainly

**This is a demo and test source. Do not build a production pipeline on it.**

- `commit()` stores watermarks in memory and nowhere else.
- The source claims **no resumability**. A restart begins every lane at offset
  0, so with a fixed seed the entire stream replays from the beginning —
  strictly *more* duplication than a real at-least-once source, which would
  replay only from its last committed position.
- A `resume_from:` file is deliberately declined. A demo source that appears to
  resume durably is one somebody builds on.

Opening the source logs a `WARN` saying so.

## Metrics

Under `spate_datagen_source_*`. The lanes count
`events_generated_total{event}`, `ticks_total` and `tick_overrun_total`; the
control plane publishes `events_remaining`, `open_orders` and — only with
`metrics.per_partition_detail` — `committed_offset{partition}`.

There is no `spate_source_lag_records`: for an unbounded generator the lag is
infinite, so the series would appear and disappear with a configuration key.

## Dependencies

`serde`, `serde_json`, `humantime-serde`, `tracing`, and `apache-avro` behind
the optional `avro` feature. The PRNG is hand-rolled — forty lines of
SplitMix64 — so a crate whose job is removing prerequisites adds none of its
own.
