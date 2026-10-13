---
description: "A sink's FrameCodec codes each sealed chunk once on the pipeline thread; the budget and max_bytes keep counting the bytes before the codec."
---

# ADR-0067 — A sink codes each sealed chunk on the pipeline thread

- **Status:** accepted
- **Date:** 2026-10-13
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A sink that compresses its wire bytes has had to do it inside
`ShardWriter::write_batch`, on the shared I/O runtime that also runs every
shard worker and socket write ([ADR-0006](0006-per-shard-sink-workers.md)).
That work does not scale with `pipeline.threads`, and it repeats on every retry
of a batch. The terminal stage in `spate-core` (`ops/handoff.rs`) already seals
each chunk on the pipeline thread, after the connector's `RowEncoder` finishes
it ([ADR-0007](0007-clickhouse-insert-path.md)). The question is where a coding
step after encoding belongs, what it may assume, and what the in-flight budget
and `batch.max_bytes` count once frames are coded.

## Considered options

- Code in the connector's `RowEncoder::finish_chunk`, so the coded bytes are the
  frame
- Code inside `write_batch`, as before
- Code on the I/O runtime through `spawn_blocking`
- A framework step at seal: a `FrameCodec` the sink supplies, run by the terminal
  stage once per sealed chunk, carried to every chain in the `ShardQueues` it
  already receives

## Decision outcome

Chosen option: "a framework step at seal", because it moves the work onto the
threads that scale with `pipeline.threads`, codes each chunk once whatever
happens to its batch, and keeps every sizing rule independent of the codec.

The contract the terminal stage relies on, stated on the trait:

- The codec runs once per sealed chunk, on the pipeline thread that sealed it.
- Its output decodes on its own, and outputs concatenated in seal order form a
  legal body for the sink. Compressed database blocks and the members of a
  multi-frame zstd or gzip object body both fit.
- The output is a deterministic function of the input.
- The codec does no I/O.
- An instance may keep state. The framework makes one per sink per chain, which
  is one per pipeline thread.
- The trait is `Send + Sync`. The `Sync` bound keeps `SinkHandoff`, the
  terminal stage that holds the codec, `Sync`.
- The returned slice is copied before the next call.
- An `Err` stops the pipeline whatever its class. The chunk's rows and
  acknowledgments stay in the shard buffer, as on a failed `finish_chunk`.

A sealed frame never changes after seal. The worker merges frames into batches
and every write attempt sends the same allocations under the same token.

The in-flight budget and `batch.max_bytes` count the bytes before the codec.
`EncodedChunk::raw_len` carries that length from the seal to the worker, which
sums it into `SealedBatch::bytes`, the batch's budget release and the sink byte
metrics.

The codec rides in `ShardQueues` (`ShardQueues::with_frame_codec`), which every
chain and split branch already receives. `SinkParts::frame_codec` carries it out
of a bundle, and the builder attaches it before the queues are cloned into any
chain.

### Consequences

- Good, because compression scales with `pipeline.threads` and leaves the I/O
  runtime to sends.
- Good, because a retry resends stored bytes and does not code them again.
- Good, because the budget, `max_bytes` and their documented sizing rules mean
  the same thing under every codec.
- Good, because no chain signature changes: the codec arrives with the queues.
- Bad, because a host whose pipeline threads are already saturated loses rows
  per second when a codec moves work onto them.
- Bad, because the memory a chunk actually holds is the coded size, so the
  budget overstates queued memory under compression. Per-thread codec state
  sits outside the budget.
- Bad, because `EncodedChunk` gains a public field, which breaks code that
  builds one with a struct literal.
- Bad, because a high-ratio codec at a high level lengthens each seal, and a
  flush seals every shard of a chain in turn.
- Neutral, because `SealedBatch` carries no per-frame raw lengths. A sink that
  needs them, such as an object sink writing a seekable-zstd index, would
  revisit this.

### Confirmation

INV-2 holds structurally: the codec runs inside `seal_and_send`, whose only
queue interaction stays `try_send` plus park. The contract is pinned by tests:
`a_frame_codec_codes_each_chunk_once_on_the_pipeline_thread`,
`budget_counts_the_frame_before_the_codec`,
`parked_chunks_release_their_raw_bytes_on_drop`,
`a_codec_failure_is_fatal_and_keeps_the_rows`,
`an_oversized_row_does_not_pin_its_buffer` and
`split_branches_keep_their_queues_codec` in `crates/spate-core/src/ops/tests.rs`;
`each_sinks_frame_codec_reaches_every_chain` in
`crates/spate-core/src/pipeline/builder.rs`; `max_bytes_counts_raw_bytes`,
`settle_releases_what_the_handoff_budgeted` and
`retries_resend_the_sealed_frames_unchanged` in
`crates/spate-core/src/sink/pool_tests.rs`; and
`a_capture_sinks_codec_codes_every_captured_frame` in
`crates/spate-test/tests/bundle.rs`. The counted case
`chunk_sixteenth_batch_coded` in `crates/spate-core/benches/chain_gungraun.rs`
prices the step per seal against `chunk_sixteenth_batch`.

## Evidence

Spike-measured, hand-recorded; no committed rig. Wall-clock, loopback, the
`clickhouse` client with no TLS and no server: I/O-thread CPU per ~43 KB Native
frame, compressing inside the send against sending pre-compressed bytes.

- lz4: 48.60 → 5.51 µs (AWS c8g), 40.93 → 5.60 µs (c7i), 40.95 → 8.89 µs (c7a).
- zstd level 3: 88.26 → 4.92 µs (c8g), 80.15 → 5.90 µs (c7i), 84.04 → 7.81 µs
  (c7a).

The same spike measured what moves to the pipeline thread per frame: 38–45 µs
for lz4 and 82–85 µs for zstd level 3 on the same hosts. These figures motivate
moving the work. They do not measure end-to-end throughput, which depends on
whether a host's pipeline threads or its I/O runtime are the limit.

## More information

- Landed in #1137.
- [ADR-0006](0006-per-shard-sink-workers.md) — the shard workers that merge and
  write the coded frames.
- [ADR-0007](0007-clickhouse-insert-path.md) — frames encoded on pipeline
  threads, and identical bytes on retry.
- [Custom sinks](../user-guide/06-extending/custom-sink.mdx) — attaching a
  codec to a sink's parts.
