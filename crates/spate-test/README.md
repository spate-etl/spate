# spate-test

Testing utilities for [Spate](https://github.com/spate-etl/spate)
pipelines: every mock pairs with a scripting/observation handle (the
`tower-test` philosophy), so pipelines are tested deterministically with
zero infrastructure.

- `memory_source()` → `MemorySource` + `SourceHandle`: script lane
  assignment and revocation, push records, observe pause/resume and
  committed watermarks.
- `capture_writer()` → `CaptureWriter` + `SinkScript`: script per-replica
  write outcomes (success, retryable failure, fatal, delays on tokio test
  time) and inspect every captured batch down to decoded rows.
- `TestDeserializer` (passthrough / split / fail-on-prefix),
  `TestEncoder`/`decode_rows`, and `EmitCollector` for exercising stages
  in isolation.
- A `proptest` feature with strategies for payloads, lane layouts, and
  writer-outcome scripts.

See the crate docs for a walkthrough of a record's full life — push →
poll → deserialize → encode → acknowledge → commit — against a real
checkpointer.
