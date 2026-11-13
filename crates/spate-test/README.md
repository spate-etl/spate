# spate-test

In-memory sources, sinks and feeds for testing pipelines built with
[Spate](https://spate.kainth.dev/). Each one comes with a handle that scripts
it and observes what the pipeline did to it, so a test drives a pipeline record
by record with no broker, bucket or database behind it. Add the crate as a
dev-dependency beside the [`spate`](https://crates.io/crates/spate) facade.

## Sources and sinks

- `memory_source` returns a `MemorySource` and its `SourceHandle`. The handle
  assigns and revokes lanes, pushes payloads, and records every commit and flush
  the source is asked for, along with the lanes paused now. `wait_committed`
  blocks until a partition's committed offset reaches a target, or a timeout
  passes. The source panics on misuse, such as pausing a lane that was never
  assigned.
- `capture_sink` returns a `CaptureSink` and its `SinkScript`. The sink is a
  whole sink bundle with a chosen number of shards and replicas, so it goes
  straight into a pipeline. Unscripted writes succeed.
- `capture_writer` returns a `CaptureWriter` and its own `SinkScript`, for a
  test that needs the shard writer alone. It shares nothing with a
  `capture_sink`.

`SinkScript` queues an outcome for the next write, to any replica or to one
shard and replica: success, a retryable failure or a fatal one. An outcome can
resolve after a delay on tokio time, or stay held until the test releases it.
The script also fails and heals endpoint probes, and records every write
attempt with its rows, bytes, dedup token, payload and result.

## Running a pipeline

- `passthrough_runtime` assembles the usual test pipeline: one chain that
  passes the source's payloads through an encoder to the sink. Signal handling
  is off, so the test stops the run through the shutdown handle.
- `PipelineRun` runs a pipeline on a thread of its own. The test waits for it
  to exit within a bound, or joins it for the exit report.
- `wait_until` polls a condition and panics naming it when the bound passes.
  `HANG` is the bound for a condition a working test always reaches.
- `wait_for_value` polls a read until a check holds for its value, and returns
  that value. On a timeout, its panic also prints the last value read.

## Feeds

`scripted_feed` returns a `ScriptedFeed` and its `FeedScript`, for testing a
source that reads a work feed. The script delivers units, redelivers and
duplicates them, dead-letters them, and sets publish outcomes and depth
readings. It can refuse the next `receive`, `renew`, `acknowledge` or `fail`.
It records every acknowledgement, failure, renewal, publish, depth read and stop
call. The feed has no queue and no clock behind it, and its backend key is
`scripted`.

## Stages in isolation

- `TestDeserializer` turns payloads into records. `passthrough` emits one record
  per payload, `split_on` one per delimited segment, and `fail_on_prefix` fails
  each payload that starts with a marker, to exercise the `Skip` and `Fail`
  error policies.
- `TestEncoder` writes each row with a length prefix, and `decode_rows` reads a
  captured frame back into rows.
- `EmitCollector` collects what a deserializer emits. `blocking_after` makes it
  report the output as blocked after a number of records, to test the blocked
  path.

## Building inputs

- `record` builds a record around a payload, and `raw_payload` builds a keyless
  raw payload. Both carry zeroed metadata.
- `test_ack` builds an acknowledgement handle whose resolution is discarded. A
  test that checks a resolution polls a batch against a real checkpointer, as
  the crate-level example on docs.rs does.
- `component_config` builds a component's config section from a type tag and
  a YAML body.
- `unique_name` adds a suffix no other call in the process returns. Gauge
  ownership is process-wide, so tests that share a binary name each pipeline
  and component with it.

## Observing metrics and logs

- `render_metrics` runs a closure under a recorder local to the calling thread
  and returns the Prometheus text exposition.
- `metric_series` reads every sample with a name and a set of labels out of
  that text. `metric_value` reads the one matching sample and panics if more
  than one match. `metric_sum` adds up the matches.
- `capture_logs` runs a closure under a `tracing` subscriber at a chosen level
  and returns the lines it logged. `show_logs` sends the lines only to the test
  harness. The harness prints them when the test fails. Both subscribers are
  scoped to the calling thread.
- `LogCapture` is a writer for a `tracing` subscriber the test installs itself.
  `LogCapture::wait_for` polls a check until it returns a value, and
  `LogCapture::wait_for_line` waits for a line containing a string. On a
  timeout, both panic with every line captured so far.
- `LogCapture::install_global` installs a subscriber at a chosen level as the
  process's global default and returns its capture, so lines from every
  thread reach it. A process has one global subscriber, so a test that calls
  it needs a binary of its own.

## Property tests

The `proptest` feature, off by default, adds the `strategies` module. It
generates payload sets, lane layouts, source scripts and sink write outcomes to
drive the mocks with.

## Documentation

- [Testing pipelines](https://spate.kainth.dev/docs/user-guide/guides/testing-pipelines)
  — the guide to testing a whole pipeline with these mocks.
- [`memory_pipeline.rs`](https://github.com/spate-etl/spate/blob/main/crates/spate/examples/memory_pipeline.rs)
  — a compiled example that builds a pipeline in memory, drives records through
  it and checks what the sink wrote
  (`cargo run -p spate --example memory_pipeline`).
- [API documentation on docs.rs](https://docs.rs/spate-test) — every helper
  with its signature, built with the `proptest` feature on.
