**Breaking:** **The S3 source reads a large object as several byte-range splits** (`spate-s3`)

The planner cuts an uncompressed object above `split_target_bytes` into byte
ranges of at most the target, one split each, so several workers read it at
once. This applies when the object has an ETag, is at most 50,000 GiB, and
the framer returns a delimiter from `RecordFramer::resync_delimiter`, as
`NdjsonFramer` does. Previously one lane read every such object from start to
end. Compressed objects, and objects under a framer that returns `None`, are
still read whole.

A backfill started on 0.2 cannot resume on this release. The split ids and the
job fingerprint change, so a worker fails at startup with
`job fingerprint mismatch: this worker is configured as ... but the store
prefix belongs to ...`. Finish the backfill on 0.2, or run it under a new job
name in the `coordination:` store, which delivers every record again.

The framer's delimiter is part of the job fingerprint, so every worker of a
job needs a framer that declares the same one. The records of one object no
longer arrive in object order, because its ranges are read in parallel.
`spate_s3_source_objects_completed_total` and
`spate_s3_source_objects_remaining` count each byte range as one object, so
completed objects can exceed `spate_s3_source_objects_listed_total`.
