**Breaking:** **The S3 source reads a large object as several byte-range splits** (`spate-s3`)

The planner cuts an uncompressed object above `split_target_bytes` into byte
ranges of at most the target, one split each, so several workers read it at
once. This applies when the object has an ETag, is at most 50,000 GiB, and
the framer returns a delimiter from `RecordFramer::resync_delimiter`, as
`NdjsonFramer` does. Previously one lane read every such object from start to
end. Compressed objects, and objects under a framer that returns `None`, are
still read whole.

The packing version changes, so every split id changes, and the framer's
delimiter joins the job fingerprint. Every worker of a job needs a framer that
declares the same delimiter. A coordinated job started on a previous version
cannot resume on this one. The entry "The S3 source reads a split that covers
a byte range of one object" gives the startup error and how to migrate.

The records of one object no longer arrive in object order, because its ranges
are read in parallel. `spate_s3_source_objects_completed_total` and
`spate_s3_source_objects_remaining` count each byte range as one object, so
completed objects can exceed `spate_s3_source_objects_listed_total`.
