**Chunk buffer recovery after an encoder error** (`spate-core`)

When `RowEncoder::finish_chunk` returns an error, the pipeline now removes any
bytes that call appended to the chunk buffer. Previously, those bytes remained
in the buffer and could be sent before a complete block if another attempt
succeeded before shutdown. Removing them prevents a failed attempt's partial
output from corrupting a later frame.

Custom encoders must retain enough state to finalize the same chunk again,
including any rows added before the next attempt. The pipeline keeps the row
count but discards the failed call's output, so an encoder that moved rows into
the buffer must encode them again. The bundled encoders are unaffected: the
ClickHouse Native encoder rejects a failed block before writing bytes, and row
formats use the default `finish_chunk` implementation.
