---
description: "The ClickHouse sink frames its own compressed blocks at seal and sends them with decompress=1; servers that cannot decode a block fail the sink."
---

# ADR-0068 — ClickHouse insert bodies are framed by spate and sent with `decompress=1`

- **Status:** accepted
- **Date:** 2026-10-14
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

Server exception code 271 (`CANNOT_DECOMPRESS`) classifies `Fatal` under
`compression: lz4` or `zstd`. A `decompress=1` body whose block fails its
checksum or does not decode answers 271. Every compressed insert carries the
sink's own blocks, and a retry resends the same bytes, so a block the server
cannot decode would fail on every attempt under the default unbounded
`retry.max_attempts`. This was the maintainer's call, following the zstd-level
check that rejects a deterministic server-side failure at load.

An insert also answers 271 when the server reads a stored block that passes
its checksum and fails to decode, such as a materialized view reading a
damaged part. Under `compression: off` no body carries a block, so 271 stays
`Retryable` there and another replica can take the insert. Under lz4 or zstd
the two causes share the code, so such a read stops the sink too. This is
rare, since the block must be damaged before its checksum is written, or read
with checksums off. Only the server's message text tells the causes apart, and
matching it would tie the class to the server's wording, so the maintainer
chose to gate on whether the sink codes its frames. Code 40
(`CHECKSUM_DOESNT_MATCH`) stays `Retryable` under every compression. The server
answers it when a block it reads itself fails its checksum, and another replica
can take the insert.

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
- Bad, because under lz4 or zstd a block the server cannot decode stops the
  sink, where it used to be retried until `retry.max_attempts`. That includes
  the rare stored block that passes its checksum and fails to decode when an
  insert's materialized view reads it.
- Bad, because a bit flip in transit over plain HTTP reaches the server as 271
  and stops the sink, where a resend would have cleared it. Under TLS the
  record check catches the flip first, and it arrives as a retryable transport
  error.
- Bad, because each pipeline thread keeps one zstd context per compressing
  sink, outside `max_inflight_bytes`. Its size grows with the level and the
  chunk target. Review-measured on darwin with `CCtx::sizeof`: 0.63 MiB at
  level 3 with 64 KiB chunks, 17.25 MiB at level 19 with 1 MiB chunks.

### Confirmation

`a_coded_insert_is_the_request_a_compressing_client_sends` in
`crates/spate-clickhouse/tests/coded_inserts.rs` pins the request against a
stock compressing client under lz4 and zstd at levels 1, 3 and 19, in both
formats. `into_parts_attaches_a_codec_only_when_compressing` and
`an_uncoded_frame_is_fatal_before_any_request` sit beside it.
`a_wrong_method_or_size_frame_is_fatal_before_any_request` pins the method and
size checks one at a time, and
`an_uncoded_frame_after_a_coded_one_is_fatal_before_any_request` pins that every
frame is checked. `server_decode_failures_are_fatal`,
`a_server_side_decode_failure_is_fatal_only_when_coded` and
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

Measured by `crates/spate-clickhouse/benches/frame_gungraun.rs`, counted,
eight frames: the sink's codec against the client's. On CI x86_64, lz4
5,092,996 → 4,850,387 instructions (−4.76%), zstd level 1
10,412,004 → 9,923,368 (−4.69%), zstd level 3 14,849,050 → 12,196,088
(−17.87%). On AWS c8g.8xlarge (Graviton4, valgrind 3.27.1), lz4
4,200,446 → 4,195,519 (−0.12%), zstd level 1 8,970,373 → 8,938,892 (−0.35%),
zstd level 3 10,084,541 → 9,917,559 (−1.66%).

Measured by `crates/spate-clickhouse/benches/frame_wall.rs`, wall-clock, 20
replicates on AWS c8g.8xlarge and c7i.8xlarge (Xeon Platinum 8488C). CPU per
eight frames, the sink's codec against the client's, medians: lz4 0.39 against
0.39 ms (c8g) and 0.33 against 0.34 ms (c7i); zstd level 1 0.76 against 0.77
and 0.64 against 0.67; zstd level 3 0.83 against 0.85 and 0.73 against 0.81.

Measured by `crates/spate-clickhouse/benches/write_wall.rs`, wall-clock, 20
paired replicates against the parent of the change on the same two hosts. The
process CPU to code and write one 1,600-frame Native batch, including the
in-process server's discard loop in both legs:

- lz4: 104.42 → 101.58 ms on c8g (−2.60%, 90% interval −4.51% to −0.71%) and
  94.95 → 93.24 ms on c7i (−1.68%, −3.72% to +0.58%), both inside the
  harness's 5% floor.
- zstd level 3: 193.99 → 187.56 ms on c8g (−3.30%, inside the floor) and
  187.84 → 164.29 ms on c7i (−12.53%).
- `off`: no change on either host.
- Writing pre-coded blocks alone, which is the work left on the I/O runtime,
  took 19.18 of 98.58 ms under lz4 and 13.67 of 187.85 ms under zstd level 3
  on c8g, and 21.05 of 92.12 and 13.19 of 164.14 ms on c7i, in a separate run
  of the same tree.
- A darwin (aarch64) smoke run of the same comparison showed lz4 +5.32%
  (74.67 → 79.04 ms, +4.41% to +6.19%), so the lz4 total depends on the
  platform.

Measured end to end on AWS eu-west-2 by a hand-run harness this repository
does not carry: the `clickhouse_load` example (datagen to 32 Native shards) on
a c8g.8xlarge and a c7i.8xlarge client, each against a ClickHouse 26.8 server
on an instance of the same type. Rows per second and per-thread CPU come from a
45-second window, five rounds per cell, against the parent of the change. In a
4-core CPU quota, where the one pipeline thread is the limit, with TLS off:

- Pipeline-thread CPU per row rose. On c8g, 1015.18 → 1047.77 ns (+32.60)
  under lz4 and 1015.11 → 1072.56 ns (+57.45) under zstd level 3, within 4 ns
  of the `spate-io` fall below. On c7i, 755.88 → 808.59 ns (+52.70) and
  755.78 → 826.24 ns (+70.46), 18.75 ns and 15.02 ns more than the
  `spate-io` fall.
- `spate-io` CPU per row fell. On c8g, 47.85 → 11.86 ns (lz4) and
  66.46 → 10.95 ns (zstd level 3). On c7i, 58.79 → 24.84 ns and
  79.31 → 23.87 ns.
- Process CPU per row moved −0.4% and +0.2% on c8g, +2.4% and +1.7% on c7i.
- Rows per second fell, medians [min–max]. On c8g, 0.98 [0.98–0.99] →
  0.95 [0.95–0.96] M under lz4 (−3.1%) and 0.98 [0.98–0.99] →
  0.93 [0.93–0.93] M under zstd level 3 (−5.4%). On c7i,
  1.32 [1.28–1.38] → 1.24 [1.23–1.28] M (−6.5%, within its 7.2% noise) and
  1.32 [1.29–1.37] → 1.21 [1.20–1.25] M (−8.5%). With TLS on the changes were
  −3.1%, −5.4%, −6.1% and −8.7%. Each change is taken from the unrounded
  medians.

The 32-core shapes do not measure the I/O-runtime limit this change removes.
There the client process used 28–31 of its 32 cores, and raising `io_threads`
from 2 to 8 at the parent moved rows per second −1.1% against 20.3% noise
(c8g, zstd level 3). The best median of this change at 32 cores was 18.63M
rows per second.

## More information

- Landed in #1141 (the block codec) and in the pull request that closed #1133
  (the writer, the request and the decode codes).
- [ADR-0007](0007-clickhouse-insert-path.md) — pre-encoded frames on the
  pipeline threads and identical bytes on retry.
- [ADR-0067](0067-a-sink-codes-each-sealed-chunk-on-the-pipeline-thread.md) —
  the frame codec hook this sink uses.
- [ClickHouse performance tuning](../user-guide/04-connectors/sinks/clickhouse/performance-tuning.mdx#compression)
  — what compression costs and where.
