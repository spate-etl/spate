**Breaking:** **The S3 source reads a split that covers a byte range of one object** (`spate-s3`, `spate-core`, `spate-json`)

A split descriptor can name a byte range of one object. Build it with
`SplitDescriptor::with_range` and a `SplitRange`, and mint its id with
`split_id_for_range`. The S3 source reads such a split and emits the records
that start inside the range. It skips the bytes before the first of those
records, and reads past the range's end to finish the last one. Ranges that
cover an object deliver each of its records once.

A ranged split needs an uncompressed object with an ETag, and a framer that
names the byte a record can start after. `RecordFramer` has a new method,
`resync_delimiter`, which returns `None` unless a framer overrides it.
`NdjsonFramer` returns `\n`. The pipeline stops on a ranged split whose
delimiter differs from the framer's, or whose object the source's
`compression` setting decodes.

The split descriptor format is now version 2, and the version is part of the
job fingerprint. A coordinated S3 job that started on a previous version
cannot resume on this one: the worker stops at startup with
`job fingerprint mismatch: this worker is configured as ... but the store
prefix belongs to ...`. Finish the job on the previous version, or give it a
new store prefix. For the NATS and DynamoDB stores that is a new `job` value,
in the `coordination:` section or on `NatsConfig` or `DynamoDbConfig` in code.
A custom store needs a new prefix of its own. A new prefix starts from
nothing and delivers every record again. A solo job, with neither a
`coordination:` section nor `with_coordinator`, keeps no progress across
restarts and is not affected.
