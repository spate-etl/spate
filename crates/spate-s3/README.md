# spate-s3

The S3 source (`spate-s3`, feature `s3` on the `spate` facade) reads the objects
and byte ranges that work units name, and the objects that storage notifications
announce, from a feed. It reads S3 and S3-compatible stores, and `file://` for
runs with no infrastructure. A pipeline with an S3 source and no `feed:` section
fails to start.

## Enable

Turn on feature `s3` on the [`spate`](https://crates.io/crates/spate) facade.
The facade README has the Cargo snippet.

The source also needs a record framer from a format crate, such as
`NdjsonFramer` behind the `json` feature. A fleet of workers shares a durable
feed, from the `sqs` or `kafka` feature. The in-process feed needs no feature.

## Configuration

The source reads the pipeline's `source: { s3: ... }` section. Only `url` is
required, and an unknown key is rejected. Its work arrives through the
pipeline's `feed:` section, beside it.

```yaml
source:
  s3:
    url: "s3://my-bucket/exports/2026-07/"  # required
    compression: auto          # auto | none | gzip | zstd
    split_target_bytes: 64MiB  # unit size target; at least 1MiB
    prefetch_bytes: 8MiB       # one ranged GET window; at least chunk_bytes
    chunk_bytes: 512KiB        # one buffered chunk; not zero
    store:                     # object_store options; empty by default
      region: eu-west-2

feed:
  memory: {}                   # the in-process feed: this run lists and reads
  units_held: 8                # units read at once; unset, 2 × pipeline.threads
```

`compression`, `split_target_bytes`, `prefetch_bytes` and `chunk_bytes` show
their defaults.

- `url` takes `s3://`, `s3a://`, `file://`, or `https://` on an S3 or R2
  endpoint host. Another S3-compatible store takes `s3://<bucket>/<prefix>` with
  its endpoint under `store`. A URL carrying userinfo or a query fails at load,
  and so do `memory://` and an S3 or R2 host in a form the client does not
  parse, such as `s3.amazonaws.com`. The `url` row of the page linked below
  lists the forms it parses.
- `compression: auto` decides per object by extension. `.gz` and `.gzip` are
  gzip, `.zst` and `.zstd` are zstd, and any other key is read uncompressed.
- `store` options apply on top of the AWS environment variables. A bucket key
  fails at load, because the bucket comes from `url`. Under an S3 URL, a key
  that is not an `object_store` S3 option fails at load too.
- The record format is not a key. `S3Source::with_framer` sets the framer in
  code, and a source built without one fails when it opens.

The
[Configuration section of the S3 source page](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#configuration)
has the full key table and the validation rules.
[Reading from a feed](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#reading-from-a-feed)
covers the `feed:` section, the lister run and storage notifications.

## Parallelism

Each part of a unit in progress is read by one lane, and each lane is one
framework partition. A unit names one whole object, one byte range of a large
object, or several objects that its lane reads in turn. The lister packs objects
read whole into units of at most 16 objects and `split_target_bytes` bytes. An
object read whole above the target is a unit of its own.

`units_held` in the `feed:` section bounds the units a worker reads at once, and
the lanes those units take. Unset, it is twice `pipeline.threads`, and 96 for a
source opened outside a pipeline. The source logs the value and where it came
from at `INFO` when it opens.

A lane is polled only by the pipeline thread that owns it, so decode parallelism
is `min(lanes, pipeline.threads)`. The
[architecture page](https://spate.kainth.dev/docs/user-guide/concepts/architecture#lane-count-is-read-concurrency-not-decode-parallelism)
explains why. The default of two lanes per thread gives each thread a second
lane to read while the first opens its next object.
[Sizing units against `pipeline.threads`](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#sizing-units-against-pipelinethreads)
covers how the unit count spreads the work across threads.

An object above `split_target_bytes` is cut into byte ranges, one unit each,
only when it is uncompressed, has an ETag, and the framer declares a resync
delimiter. The ranges of one object are read in parallel, so the object's
records arrive in no fixed order.
[Large objects and byte ranges](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#large-objects-and-byte-ranges)
lists every condition.

Every worker of a pipeline reads the same durable feed, and the feed hands each
unit to one worker at a time. Runs on the in-process feed share nothing, so each
one lists and reads the whole prefix. On a durable feed the source lists
nothing. A bounded job's units come from a separate lister run,
`S3Source::list_once`, once per job, and a continuous job's units come from
storage notifications or code.

On the ETag-pinned path, a lane holds at most three GET windows of about
`prefetch_bytes` each when every window spans at least four chunks, as it does
at the defaults. Framing and decoded records take memory on top.
[Read parallelism](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#read-parallelism)
has the sizing.

## Delivery

Delivery is at-least-once.

- The source acknowledges a unit to the feed once every part of it has been read
  and the sink has acknowledged every record in it.
- The source keeps no position inside a unit. A unit delivered again, after a
  crash or a failed worker, is read from its start, so records read before the
  failure reach the sink twice.
- On a graceful stop over a feed that hands units back, as the SQS and Kafka
  feeds do, the source receives no more units. It reads the units it holds to
  their end until 5 seconds before the drain deadline, then hands back the rest.
  Another worker reads each handed-back unit from its start.
- An object that is deleted, overwritten under its ETag pin, corrupt, or
  unreadable after retries fails its unit back to the feed. The feed delivers
  the unit again and dead-letters it at its cap. A `401` or `403` from the
  store, and a client configuration error, fail the pipeline. On AWS S3, a
  worker role without `s3:ListBucket` reads a deleted object as `403`, which
  fails the pipeline.
- Every read of an object that has an ETag is pinned to it with `If-Match`, so
  a rewrite during the read fails the unit and never splices two versions. A
  notification that names a version reads that version. One that names none
  re-enqueues its overwritten object at the new ETag.

A job on a durable feed is bounded when its `feed:` section names a `location`.
It ends once the lister run's end marker holds over a settling window, five
minutes by default, in which the feed reads empty. A worker ends the job only
while it holds no unit. When the feed's dead-letter destination then holds
units, the run fails with an error that includes `bounded feed job stalled:`.

A solo run on the in-process feed the runtime built ends once its listing is
read. If that feed holds dead letters at the end, the run fails with the
`bounded feed job stalled:` error. That feed keeps nothing across a restart, so
a restarted solo run lists and reads the whole prefix again, and a startup
`WARN` says so.

A run on a `MemoryFeed` passed with `Pipeline::seeded_feed` lists nothing and
ends once every unit is acknowledged or dead-lettered. It exits `Completed`
with dead letters, which the code that seeded the feed reads from
`MemoryFeed::dead_letters`. Any other job runs until it is shut down.

[How a bounded job ends](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#how-a-bounded-job-ends)
and
[Object errors fail the unit](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#object-errors-fail-the-unit)
on the S3 source page give the detail. The
[Work feed](https://spate.kainth.dev/docs/user-guide/concepts/work-feed) and
[Delivery guarantees](https://spate.kainth.dev/docs/user-guide/concepts/delivery-guarantees)
pages cover the framework's side.

## Sharp edges

- **A framer with no resync delimiter reads every object whole.** A format such
  as CSV declares none, so no object is cut into byte ranges, and the source
  logs that at `INFO` when it opens. See
  [Decoding records](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#decoding-records).
- **The job fingerprint holds `split_target_bytes` as a byte count.** The count
  enters it as a number of bytes, so a worker set to `64MB` is refused at
  startup against a lister on the `64MiB` default. See
  [Configuration](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#configuration).
- **Credentials come from environment variables.** For `s3://`, the client reads
  the standard AWS variables. They cover EKS Pod Identity, IRSA, ECS task roles
  and EC2 instance metadata. It reads no profile file and no SSO cache. The
  config's `Debug` output does not print `store` values. See
  [Security](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#security-credentials-and-region).
- **A listing and a notification for one object read it twice.** Their units
  carry different ids, and the source reads each unit in full. A bounded job's
  feed should carry no storage notifications, and nothing checks that. See
  [How a bounded job ends](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#how-a-bounded-job-ends).
- **An overwrite can be dropped at the re-enqueue cap.** A notification unit
  that has been re-enqueued as many times as the feed's `attempt_cap`, 10 when
  unset, is acknowledged on its next overwrite with no re-enqueue, and the
  source logs a `WARN`. See
  [Reading a notification's object](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#reading-a-notifications-object).
- **The lister lists the prefix once.** A key written after its listing is not
  part of the job. See
  [Unit identity and drift](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#unit-identity-and-drift).
- **An object with no ETag is read in one streamed GET.** The response stays
  open while the sink is slow, and a break mid-object fails the unit. Stores
  normally report an ETag. A notification record, a listing from an
  S3-compatible store, a HEAD response or an external descriptor can omit it.

## Metrics

The source's own families, all prefixed `spate_s3_source_`, are listed with
their labels in the
[Metrics section of the S3 source page](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3#metrics).

## Related

- [`spate`](https://crates.io/crates/spate) — the facade an application depends
  on, with the Cargo snippet.
- [S3 source page](https://spate.kainth.dev/docs/user-guide/connectors/sources/s3)
  — every key, the feed path, the stores CI runs against and the metric
  families.
- [API documentation on docs.rs](https://docs.rs/spate-s3) — the source, its
  configuration types and `S3Source::list_once` for the lister run.
- [`s3_backfill.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/s3_backfill.rs)
  — a bounded backfill on the in-process feed over a `file://` bucket, with
  nothing installed
  (`cargo run -p spate --features s3,json --example s3_backfill`).
- [`s3_sqs_backfill.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/s3_sqs_backfill.rs)
  — a bounded job whose work arrives through an SQS queue, with a job location.
- [`s3_kafka_backfill.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/s3_kafka_backfill.rs)
  — a backfill whose work arrives through Kafka topics.
- [`s3_sqs_notifications.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/s3_sqs_notifications.rs)
  — a continuous job on a bucket's storage notifications.
- [Examples index](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/README.md)
  — every example, with the features and infrastructure each needs.
- [`spate-json`](https://crates.io/crates/spate-json) — `NdjsonFramer`, the
  framer for NDJSON objects.
- [`spate-test`](https://crates.io/crates/spate-test) — in-memory sources and
  sinks, for a test with no infrastructure.
