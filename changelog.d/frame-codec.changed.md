**Breaking:** **A sink can code each sealed chunk on the pipeline thread** (`spate-core`, `spate-test`)

A sink can supply a `FrameCodec` through `SinkParts::with_frame_codec`. The pipeline thread runs the codec once on each chunk when the chunk seals. The shard worker sends the coded bytes unchanged, including on retries. The in-flight budget and `batch.max_bytes` still count the bytes before coding, so sizing rules do not change. `EncodedChunk` has a new public field, `raw_len`, the frame's length before coding. Code that builds an `EncodedChunk` with a struct literal must set it.

A manually assembled pipeline attaches a sink's codec with `ShardQueues::with_frame_codec(..)`, using `parts.frame_codec`, before it hands the queues to chains. `CaptureSink::with_frame_codec` lets tests attach a codec to the capturing sink.
