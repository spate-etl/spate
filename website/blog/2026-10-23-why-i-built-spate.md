---
title: Why I built Spate when Flink stalled on ClickHouse
description: "Flink 2.2.1 ran a Kafka to ClickHouse pipeline at 547k rows/s per core and Spate 0.3.0 at 1.61M. Why one Flink instance stopped scaling and what Spate changes."
slug: why-i-built-spate
authors: [marcus]
tags: [engineering]
---

Spate is a streaming ETL framework in Rust, for pipelines like Kafka to Avro to
ClickHouse. I built it because Flink stopped scaling on a pipeline of that
shape. One instance hit a ceiling long before the machine did, and garbage
collection meant a bigger instance would not help, so adding instances was the
only way forward.

On the [published benchmark](/benchmarks), the same pipeline runs at 547k rows
per second per core on Flink 2.2.1 and 1.61M on Spate 0.3.0, with each system
held to 32 cores on the same `c8gd.metal-24xl`.

<!-- truncate -->

## One Flink instance hit a ceiling

Before Spate, I ran a Flink pipeline that read from Kafka, decoded Avro, ran a
transform, and wrote to ClickHouse. Throughput stopped growing well before the
CPUs were busy. Giving the TaskManager more memory did not lift it, because the
heap it needed to hold large batches was the heap the garbage collector then had
to walk. This meant that the only way to get more rows per second was to add
instances, and every instance was one more JVM with a heap of its own for the
cluster to run.

The benchmark's Flink arm hits the same ceiling on the `c8gd.metal-24xl`. Its
[notes](https://github.com/spate-etl/benchmark/blob/70a6c506e3a9248618b1fdcdab509d68f690569d/entrants/flink/README.md)
record that "Flink reports every subtask 100% busy with `idle = 0` while the
cgroup uses 10.9 of its 32 cores". Doubling the rows per batch to 25,000 raised
its throughput by 12.9%, but cut rows per second per core by 5.9%, because the
arm spent more CPU than the bigger inserts saved. At 50,000 rows per batch the
arm "collapses into repeated Full GCs".

Spate has no garbage collector to tune around. A record is a view into the bytes
the source read, and the operator chain allocates nothing per record. Every byte
that has left the source and is not yet written to ClickHouse counts against one
in-flight budget. When the budget is spent or a shard's queue is full, the
source's partitions pause, so a slow ClickHouse slows the pipeline down and the
framework's memory stays inside the budget.

## More sink writes made more parts

The first thing I tried was more concurrent writes from Flink's ClickHouse sink.
That sent more inserts, but each one carried fewer rows, and ClickHouse creates
a part on disk for every insert. Its own guidance is to insert ["ideally between
10,000–100,000
rows"](https://clickhouse.com/docs/best-practices/selecting-an-insert-strategy)
at a time, and many small inserts end in "too many parts" errors. The merges
fell behind, and Flink still could not push rows through the sink fast enough.
The benchmark's Flink arm found the same: "Raising concurrency does not help: at
the same batch size, two requests in flight measured lower throughput and 26%
more CPU."

Spate keeps the batch size and the concurrency apart. Each ClickHouse shard has
one sink worker that fills a batch to its row, byte, or time limit, seals it,
and then sends up to `inflight.max_per_shard` sealed batches at once across
healthy replicas ([ADR-0006](/docs/adr/per-shard-sink-workers)). More
concurrency means more full-sized batches in flight, so the part count follows
the data volume and stays independent of how many requests are open.

## Flink chains operators only at matching parallelism

Flink runs two operators in one thread only when it can chain them, and chaining
needs matching parallelism between them. Where the parallelism differs, every
record leaves one task, is serialized, and is read by another. I tried
`rescale()`, which ["partitions elements, round-robin, to a subset of downstream
operations"](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/overview/),
and it helped a little because it keeps that exchange local, but every record
crossing it was still serialized.

Spate has no stages to match. Each pipeline thread owns a set of partitions and
runs the whole path itself: poll, decode, transform, route, and encode into the
sink's wire format. The operator chain compiles to one loop, so the operators
call each other directly, with no dispatch between them
([ADR-0004](/docs/adr/static-operator-chain)). The encoded rows then go into the
queue of the shard they belong to, so 32 pipeline threads feed one sink without
a record ever being serialized between operators. [The architecture
page](/docs/user-guide/concepts/architecture) has the thread model in full.

## Moving the column pivot off ClickHouse

ClickHouse stores columns, and a row-wise insert format leaves the server to
pivot each batch from rows into columns. The Flink ClickHouse connector's typed
mode sends `RowBinaryWithNamesAndTypes` only, so with Flink that pivot always
runs on the ClickHouse server.

Spate's ClickHouse sink can do that pivot itself. With `format: native` it
encodes columnar blocks on the pipeline threads, which scale with the client
machine, and ClickHouse receives them ready to write. On a development laptop
against an idle, single-threaded ClickHouse 25.6 server, that cut server CPU end
to end by 70%, at about twice the encode CPU per row on the client. The [Native
format page](/docs/user-guide/connectors/sinks/clickhouse/native-format) has the
measurement and when the trade pays.

## Compression on the pipeline threads

Flink's ClickHouse connector encodes each row in its sink writer and sends each
insert from there, so encoding and writing the request share the sink's task. In
the benchmark's Flink arm that request goes out uncompressed.

Spate splits that work. The pipeline thread that seals a chunk also compresses
it, and the I/O workers only send what is ready. In 0.3.0 the `clickhouse` crate
compressed each insert on the I/O workers, and with the default two of them the
sink inserted about 19.7M rows per second with LZ4 on the benchmark machine.
Moving the compression onto the pipeline threads lifts that to about 32M on the
same machine, with the same two I/O workers. Those were tuning runs, so the
benchmark page does not carry them, and the change will ship in 0.4.0. At that
rate Spate used about 21 of its 32 cores and ClickHouse about 15, so something
outside both caps the rate. That is
[benchmark#106](https://github.com/spate-etl/benchmark/issues/106). [Moving
insert compression onto the pipeline
threads](/blog/compression-on-the-pipeline-threads) has the runs.

## What the other tools do better

The [benchmark](/benchmarks) runs four systems besides Spate, and for each one
there is a case where you would choose it over Spate.

- **Flink** is built for stateful computation across a cluster, and Spate has no
  windowing, no stateful operators, and no general DAG.
- **The ClickHouse Kafka engine** is plain SQL inside ClickHouse, with nothing
  else to deploy and no Rust to write or maintain. At 1.12M rows per second per
  core, it came closer to Spate than any other arm.
- **Kafka Connect** comes with hundreds of connectors, and each one is set up
  through configuration. It has no step that turns one message into many rows,
  so the benchmark's Connect arm flattens each message into its rows in a
  ClickHouse materialized view.
- **Vector** collects and routes logs and metrics, as an agent on each host or
  as an aggregator, and transforms them in its own language. It is written in
  Rust and has no garbage collector either. Its ClickHouse sink ships
  `JSONEachRow` as its only generally available format. Vector ran at 83k rows
  per second per core.

If your transform is SQL, or your team does not want to own Rust code, the Kafka
engine is the simpler choice. Spate is for the case where the transform belongs
in compiled code and one machine has to carry the load.

## Who ran these numbers

I run the benchmark, and Spate is mine. Every Spate row on the [benchmark
page](/benchmarks) carries a dagger for that reason. Throughput is counted in
ClickHouse, and CPU and memory are read from cgroup counters by a sidecar, so no
number comes from the system being measured. Every arm's configuration is in the
[benchmark repository](https://github.com/spate-etl/benchmark), with the reasons
behind each setting. If an arm is configured badly, that is a bug, and I want
the pull request.

The [quickstart](/docs/user-guide/getting-started/quickstart) runs a whole
pipeline in memory in five minutes, with no broker and no warehouse.
