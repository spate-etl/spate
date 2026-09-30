**The S3 source reads a split that covers a byte range of one object** (`spate-s3`, `spate-core`, `spate-json`)

A split descriptor can name a byte range of one object. Build it with
`SplitDescriptor::with_range` and a `SplitRange`, and mint its id with
`split_id_for_range`. The S3 source reads such a split and emits the records
that start inside the range. It skips the bytes before the first of those
records, and reads past the range's end to finish the last one. Ranges that
cover an object deliver each of its records once.

A ranged split needs an object with an ETag, and a framer that names the byte
a record can start after. `RecordFramer` has a new method, `resync_delimiter`,
which returns `None` unless a framer overrides it. `NdjsonFramer` returns
`\n`. A ranged split whose delimiter differs from the framer's stops the
pipeline. The planner does not produce ranged splits yet, so jobs still read
every object whole.
