---
description: "The ClickHouse sink frames its own compressed blocks at seal and sends them with decompress=1; servers that cannot decode a block fail the sink."
---

# ADR-0068 — ClickHouse insert bodies are framed by spate and sent with `decompress=1`

- **Status:** accepted
- **Date:** 2026-10-13
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Under `compression: lz4` or `zstd`, the ClickHouse sink's `clickhouse` client
compressed every frame inside `InsertFormatted::send`, on the two-worker I/O
runtime that also runs every shard worker and socket write. At high byte rates
that work saturated the I/O runtime while the pipeline threads sat idle, and a
retry compressed each frame again. [ADR-0067](0067-a-sink-codes-each-sealed-chunk-on-the-pipeline-thread.md)
gives a sink a `FrameCodec` that runs once per sealed chunk on the pipeline
thread. The question is how the ClickHouse sink produces a compressed insert
body the server accepts from that hook, and what the writer does with frames
the hook did not make.

## Considered options

- Keep compressing inside the client's `send` on the I/O runtime
- Pre-compress with the client's own `CompressedData` and send it with
  `InsertFormatted::send_compressed`
- Frame each block in the sink: compress the chunk, write ClickHouse's
  checksum and block header, and send the blocks on a client with compression
  off, adding `decompress=1` to the insert as the compressing client does

## Decision outcome

Chosen option: "frame each block in the sink", because it is the only option
whose output a sealed chunk can hold as plain bytes, and it puts the
compression on the pipeline threads.

`CompressedData` holds only the client's codec output, has crate-private
fields, is not `Clone`, and is consumed by the send. A sealed chunk is a
`Bytes` that every retry resends, so it cannot carry one.

The sink's codec (`crates/spate-clickhouse/src/codec.rs`) writes each chunk as
one block: the CityHash128 v1.0.2 checksum of the rest of the block with its
halves swapped, the method byte (`0x82` lz4, `0x90` zstd), the compressed and
uncompressed sizes, then one lz4 block or one zstd frame. The bytes equal the
client's own for the same chunk. Each `ClickHouseEndpoint` keeps a second
client with compression off, sharing the first one's connection pool, for
inserts. The writer adds `decompress=1`. The request then carries the same
body, headers and query parameters as the compressing client sent for the raw
frames. Query clients keep the configured compression, so schema and probe
responses are decoded as before. `compression: off` attaches no codec and sends
exactly what it sent before.

The zstd method keeps one `CCtx` per codec instance, which is one per pipeline
thread per sink, and resets it per chunk, so no block depends on another.

lz4 is lz4_flex, declared with exactly the client's features
(`default-features = false, features = ["std"]`). Features unify into one
copy, and the default `safe-encode` costs 34–41% more instructions per block,
for the client's codec as well as the sink's. liblz4 was not taken: its
wall-clock gain over lz4_flex was not consistent across platforms, and it adds
a C dependency while the client still pulls in lz4_flex. zstd is declared directly
in the crate with `default-features = false`, because a member's flag is
ignored on a `workspace = true` entry that keeps the defaults, and the
workspace entry keeps them for the S3 source.

The writer checks every frame before it opens a request: at least the 25 bytes
of checksum and header, the configured method byte, and a recorded size that
spans the frame. A frame that fails fails the write `Fatal`, naming
`ShardQueues::with_frame_codec`, because the server would reject it and a
retry would send it again.

Server exception code 271 (`CANNOT_DECOMPRESS`) classifies `Fatal`. A
`decompress=1` body whose block fails its checksum or does not decode answers
271. Every compressed insert carries the sink's own blocks, and a retry resends
the same bytes, so a block the server cannot decode would fail on every attempt
under the default unbounded `retry.max_attempts`. This was the maintainer's
call, following the zstd-level check that rejects a deterministic server-side
failure at load. Code 40 (`CHECKSUM_DOESNT_MATCH`) stays `Retryable`. The server
answers it for data it reads itself, such as a materialized view reading a
corrupted part on one replica, and another replica can take the insert.

### Consequences

- Good, because compression runs on the pipeline threads and scales with
  `pipeline.threads`, leaving the I/O runtime to sends.
- Good, because a retry resends stored blocks and compresses nothing again.
- Good, because one zstd context per pipeline thread replaces a fresh context
  per frame.
- Good, because the request on the wire is unchanged, so no server setting or
  version changes what the sink can insert into.
- Bad, because the sink owns the block framing and its checksum. A change to
  ClickHouse's compressed-block format would need a change here, where the
  client crate would otherwise carry it.
- Bad, because a host whose pipeline threads are already the limit loses rows
  per second, since the compression they now run used to run elsewhere.
- Bad, because a manually assembled pipeline or a direct `write_batch` caller
  must attach or apply the codec, and fails `Fatal` when it does not.
- Bad, because a block the server cannot decode stops the sink, where it used
  to be retried until `retry.max_attempts`.

### Confirmation

`a_coded_insert_is_the_request_a_compressing_client_sends` in
`crates/spate-clickhouse/tests/coded_inserts.rs` pins the request against a
stock compressing client under lz4 and zstd at levels 1, 3 and 19, in both
formats. `into_parts_attaches_a_codec_only_when_compressing` and
`an_uncoded_frame_is_fatal_before_any_request` sit beside it.
`server_decode_failures_are_fatal` and
`a_server_side_checksum_failure_is_retryable` (`src/writer.rs`) and
`decompress_is_a_reserved_setting` (`src/config.rs`) pin the classification and
the reserved setting. The block codec's oracle, round-trip, context-reuse and
scratch tests are in `src/codec/tests.rs`. Against a real server on every
container lane, `tests/container/frame_codec.rs` inserts coded frames under
every compression, many blocks of very different sizes in one insert,
incompressible rows, and empty and one-row batches; checks that a corrupted
block fails `Fatal` with code 271; and checks that a retry after a lost
response resends identical bytes and inserts once.

## Evidence

Spike-measured, hand-recorded; no committed rig. Wall-clock, loopback, the
`clickhouse` client with no TLS and no server: I/O-thread CPU per ~43 KB Native
frame, compressing inside the send against sending pre-compressed bytes.

- lz4: 48.60 → 5.51 µs (AWS c8g), 40.93 → 5.60 µs (c7i), 40.95 → 8.89 µs (c7a).
- zstd level 3: 88.26 → 4.92 µs (c8g), 80.15 → 5.90 µs (c7i), 84.04 → 7.81 µs
  (c7a).
- lz4_flex `safe-encode` against the client's features: +34.4% (arm64) and
  +40.7% (x86_64) instructions per block, counted.
- liblz4 against lz4_flex, wall-clock: −9.8% (c8g), −3.1% (c7a), +2.4% (c7i),
  +1.6% (darwin).

Measured by `crates/spate-clickhouse/benches/frame_gungraun.rs` (CI x86_64,
counted, eight frames): the sink's codec against the client's,
lz4 5,092,996 → 4,850,387 instructions (−4.76%), zstd level 1
10,412,004 → 9,923,368 (−4.69%), zstd level 3 14,849,050 → 12,196,088
(−17.87%).

Measured by `crates/spate-clickhouse/benches/write_wall.rs`, wall-clock on
darwin (aarch64), a smoke run of 20 paired replicates against the parent of
the change. The process CPU to code and write one 1,600-frame Native batch,
including the in-process server's discard loop in both legs, rose 5.32% under
lz4 (median 74.67 → 79.04 ms, 90% interval +4.41% to +6.19%). Under zstd level
3 it moved −2.30%, inside the harness's 5% floor. Under `off` it did not
change. Writing pre-coded blocks alone, which is the work left on the I/O
runtime, took 22.80 ms under lz4 and 22.32 ms under zstd level 3 in a separate
run of the same tree. This does not measure end-to-end throughput.

PLACEHOLDER (AWS sitting, not yet run): the end-to-end figures on c8g and c7i
against a real server, rows per second and CPU per row over a fixed window,
are added here before this record lands.

## More information

- Landed in #1141 (the block codec) and in the pull request that closed #1133
  (the writer, the request and the decode codes).
- [ADR-0007](0007-clickhouse-insert-path.md) — pre-encoded frames on the
  pipeline threads and identical bytes on retry.
- [ADR-0067](0067-a-sink-codes-each-sealed-chunk-on-the-pipeline-thread.md) —
  the frame codec hook this sink uses.
- [ClickHouse performance tuning](../user-guide/04-connectors/sinks/clickhouse/performance-tuning.mdx#compression)
  — what compression costs and where.
