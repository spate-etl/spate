---
title: Introducing Spate
description: "Spate is an at-least-once streaming ETL framework for Rust. What it is, why I built it, why it is fast, and what it does not do."
authors: [marcus]
---

Spate is a framework for moving a stream into a warehouse. You write the
transform as ordinary Rust functions. The framework owns delivery: at-least-once
commits, backpressure, checkpointing, rebalancing and drain on shutdown. This
is the first post on this site, so it starts at the beginning.

<!-- truncate -->

## The choice I did not want to make

Every team that streams data into a warehouse meets the same choice.

The first option is a general-purpose stream processor. It gives you delivery
guarantees and years of operational history. It also decides the language your
transforms are written in, and the runtime is not yours to profile. When it is
slow, you tune knobs from the outside.

The second option is to write the consumer loop yourself. Now the language, the
allocator and the profile are yours. So is every guarantee, including the ones
you learn about in production: what happens on a rebalance, what happens when
the sink is slow, what happens when the pod gets a SIGTERM in the middle of a
batch.

I wanted a third option. The transform should be mine, in Rust, compiled into
the pipeline rather than interpreted by it. The guarantees should belong to the
framework, written down and tested before anyone runs it in production. That is
Spate. The first commit is from July 2026.

The name is an English word. A spate is a river in sudden flood.

## What Spate is

One process runs one pipeline, in four stages.

**Extract.** One consumer per process. Its partitions fan out across CPU-pinned
threads as lanes. A record is read from the source buffer and never copied on
the way in. A thread that cannot keep up pauses its lanes and keeps polling. It
never blocks on a channel send, because a blocked poll loop is how a consumer
gets evicted from its group.

**Transform.** Operators are stateful closures chained in Rust: map, filter,
flat_map, and a terminal sink stage. The chain compiles to one loop over
borrowed records, with no allocation per record. A record that fails is skipped
and counted, or it stops the pipeline. There is no third policy that drops a
record without counting it.

**Load.** Sinks are sharded and replicated, and they run on a shared I/O
runtime. The chain routes rows into bounded per-shard queues. Workers seal
batches, rotate across replicas and retry. The queue bound is the backpressure
signal, and it reaches all the way back to Extract.

**Observe.** A source offset commits only after every record derived from it
is durably written or intentionally dropped. That holds across rebalances,
shutdown and crashes. The watermark stalls rather than advancing past
unacknowledged data. Metrics use the `metrics` facade, so any recorder in that
ecosystem works, and a Prometheus endpoint and health probes ship on the admin
server.

The properties behind those four paragraphs are numbered in the
[invariants](/docs/INVARIANTS). A pull request that touches one has to say how
the property still holds. That rule is what makes me trust the framework, and I
wrote it.

Connectors ship as separate crates behind one feature each: Kafka in and out, a
ClickHouse sink, an S3 backfill source, Avro and JSON formats, a coordination
store for scaling out. Nothing is enabled by default. Writing your own source or
sink is a supported path, not a fork.

## Why it is fast

Speed in Spate comes from a set of decisions that each remove one cost from
the per-record path. Each has a decision record that says what it cost to make.

**The chain is monomorphized.** Each operator calls the next one inline. There
is exactly one dynamic call per batch, at the boundary where the chain is
stored, instead of one per record per operator. A chain of five operators over a
million records a second is five million dispatch decisions a second that never
happen ([ADR-0004](/docs/adr/static-operator-chain)).

**Records borrow the source's buffers.** A record is a view into the bytes the
source read, not a copy of them. Keeping that borrow alive across the erasure
boundary needed a specific design, and it is the reason the framework has no
per-record allocation on the hot path
([ADR-0013](/docs/adr/zero-copy-seam)).

**Nothing on the hot path looks anything up.** Metric handles are registered at
build time, so a counter increment is one write through a pointer. The chain shape
is fixed at build time too, which is why YAML configures the connectors and the
tuning but never the topology.

**The sink does the batching.** Rows are encoded on the pipeline threads into
the warehouse's wire format, merged into large batches, and written with a
deduplication token, so a retry is idempotent where the warehouse supports it.
Fewer, larger inserts is most of the difference between a fast sink and a slow
one.

**Backpressure pauses, it does not block.** A slow sink throttles the source by
pausing lanes. The poll loop keeps running, so the broker keeps seeing a live
consumer. The pipeline slows down and keeps its place in the group.

I measured the result on one fixed pipeline, Kafka to Avro to ClickHouse, with
every system under 32 cores and 96 GiB and the same at-least-once guarantee. On
2026-08-26, on a c8gd.metal-24xl, Spate 0.2.0 processed 1.59 million rows per
second per core, 20.37 million rows per second in total. Every number on the
[benchmark page](/benchmarks) carries its version, its machine, its harness
version, its date and its range across repetitions.

I run that benchmark, and Spate is mine. That is a conflict of interest, and the
only useful answer is to make it impossible to hide. Every Spate row carries a
dagger. No published number comes from the system that produced it: throughput
is a count against the warehouse, and CPU and memory are cgroup counters read
by a sidecar. Every competitor configuration is in the
[benchmark repository](https://github.com/spate-etl/benchmark), in full. If you
think an arm is configured badly, that is a bug, and I want the pull request.

## What it does not do

At-least-once means duplicates are possible. Retries within a session are
idempotent where the sink supports it, but a crash replay re-batches with new
boundaries and can land rows twice. Design your target tables to tolerate that.

There is no exactly-once sink, no general DAG, no windowing, no dead-letter
queue and no config hot reload in this version. Most of those were decided on
purpose and have a record that says why: the delivery guarantee in
[ADR-0002](/docs/adr/at-least-once-delivery), the single chain in
[ADR-0004](/docs/adr/static-operator-chain), the two error policies in
[ADR-0010](/docs/adr/skip-or-fail-record-error-policies). Some of them will
change in a later version.

## Open source

Spate is Apache-2.0, with no contributor agreement to sign. The crates are on
[crates.io](https://crates.io/crates/spate), the code is on
[GitHub](https://github.com/spate-etl/spate), and the API reference is on
[docs.rs](https://docs.rs/spate).

The most useful contribution is one that proves a delivery guarantee wrong.
Property tests cover the checkpoint tracker, the codecs and the assignment
protocol. loom models the tracker's concurrency directly. Kafka runs against a
mock cluster on every pull request, and real brokers, ClickHouse and object
stores run in containers whenever a change reaches them. If you can make a test
fail, I would rather know now.

## Try it

[Installation](/docs/user-guide/getting-started/installation) is one line in
`Cargo.toml`, with a feature for each connector you turn on. The [quickstart](/docs/user-guide/getting-started/quickstart) runs a whole
pipeline in memory in five minutes, with no broker and no warehouse. The
[user guide](/docs/user-guide/) covers the rest. If something is unclear or
wrong, [open an issue](https://github.com/spate-etl/spate/issues). I read all of
them.
