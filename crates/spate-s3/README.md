# spate-s3

Coordinated object-storage backfill source for the
[Spate](https://github.com/spate-etl/spate) framework: read a prefix of an S3
(or S3-compatible) bucket as a pipeline source, across as many instances as
you care to run.

By default an elected leader plans the prefix into splits; workers lease them
with fenced per-split progress in a coordination store; resume is drift-checked
against ETag pins, so an object rewritten under a running job is detected
rather than silently half-read; and the job self-terminates when the plan
completes. This path is always coordinated — a single instance is just the
one-worker case.

With a pipeline `feed:` section the source reads work units from the feed,
each naming one object or one byte range of one object, and storage
notifications, one part per non-empty created object. It reads a large object in
byte ranges. It acknowledges each finished unit to the feed. A bounded job, one
whose section names a `location`, ends once the lister's end marker holds over a
settling window in which the feed reads empty, and ends with an error when the
feed's dead-letter destination then holds units. A solo run on the in-process
feed the runtime built ends once its listing is read, with no window; any other
job runs until shut down. On a durable feed the source lists nothing: a bounded
job's units come from a separate lister run, `S3Source::list_once`, once per
job, and a continuous job's from storage notifications or code. On the
in-process feed the run lists its own prefix.

Applications should depend on the [`spate`](https://crates.io/crates/spate)
facade with the `s3` feature.

## Sharp edges worth knowing

- **The framing is yours to choose and is not optional.** This crate is
  format-agnostic; it hands out byte ranges and something has to say where one
  record ends and the next begins. `with_framer` is required, and
  [`spate-json`](https://crates.io/crates/spate-json) supplies an
  NDJSON framer.
- **Backpressure runs through bounded ranged GETs.** For an object with an
  ETag, the source reads GETs sized by `prefetch_bytes` (8 MiB by default) and
  buffers each GET in full before handing its chunks on, so a slow sink stalls
  each read in progress with no request in flight. An object of at most
  `prefetch_bytes` arrives in one GET and is held in memory whole, unless
  `split_target_bytes` is set below its size and the object is cut into byte
  ranges. An object with no ETag is read in one streamed GET, and that
  response stays open while the sink is slow.
- Credentials come from the standard environment chain. Nothing is logged that
  could carry one — the config's `Debug` redacts secrets, and there is a test
  that fails if it stops doing so.
- Enumeration is a single listing pass today. Very large prefixes on plans
  without an inventory feed are the case to watch.
