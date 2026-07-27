# spate-s3

Coordinated object-storage backfill source for the
[Spate](https://github.com/spate-etl/spate) framework: read a prefix of an S3
(or S3-compatible) bucket as a pipeline source, across as many instances as
you care to run.

An elected leader plans the prefix into splits; workers lease them with fenced
per-split progress in a coordination store; resume is drift-checked against
ETag pins, so an object rewritten under a running job is detected rather than
silently half-read; and the job self-terminates when the plan completes. It is
always coordinated — a single instance is just the one-worker case, which
means there is no separate uncoordinated path to diverge.

Applications should depend on the [`spate`](https://crates.io/crates/spate)
facade with the `s3` feature.

## Sharp edges worth knowing

- **The framing is yours to choose and is not optional.** This crate is
  format-agnostic; it hands out byte ranges and something has to say where one
  record ends and the next begins. `with_framer` is required, and
  [`spate-json`](https://crates.io/crates/spate-json) supplies an
  NDJSON framer.
- **Backpressure runs through ranged GETs**, so a slow sink holds fewer bytes
  in memory rather than buffering an entire object. Objects are not read
  whole.
- Credentials come from the standard environment chain. Nothing is logged that
  could carry one — the config's `Debug` redacts secrets, and there is a test
  that fails if it stops doing so.
- Enumeration is a single listing pass today. Very large prefixes on plans
  without an inventory feed are the case to watch.
