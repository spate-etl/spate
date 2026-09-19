**Consistent record framing after a decode error** (`spate-s3`, `spate-core`)

The object framer now preserves completed records when decoding a chunk or
validating the end of an object fails. Previously, an error could discard
records completed during that call, making the framer's output depend on how
the object was split into chunks. Preserving these records keeps record
indexes consistent when the same byte stream is read again. Source delivery
is unchanged: the source quarantines a failed object and discards its
undelivered records.

Custom `RecordFramer` implementations must check size limits against the
record accumulated so far and make completed records available before
returning an error. This keeps both the failure position and the completed
record sequence independent of chunk boundaries.
