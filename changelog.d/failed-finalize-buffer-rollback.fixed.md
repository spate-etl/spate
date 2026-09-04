**A failed finalize leaves only the encoded rows in the chunk buffer**
(`spate-core`) — a `RowEncoder::finish_chunk` that wrote into the chunk buffer
before returning an error left those bytes there, ahead of the next block, and
because the stop a finalize error triggers is asynchronous, a shard sealed again
before it landed shipped a frame carrying the failed attempt's bytes in front of
a complete one. The terminal stage rolls the buffer back to its length before
the call, matching what the record path already does on a failed `encode`, so a
frame that ships holds one complete block. The encoders in this repository are
unaffected: the ClickHouse Native encoder refuses a poisoned block before
writing anything, and row formats leave `finish_chunk` defaulted.
`RowEncoder::finish_chunk` states the obligation an implementer carries, that a
later seal finalizes the same chunk and may find rows in it the failed call
never saw. The stage discards what the failed call wrote and not the row count,
so an encoder that had already moved its rows into the buffer when it failed
re-encodes them rather than relying on their being kept.
