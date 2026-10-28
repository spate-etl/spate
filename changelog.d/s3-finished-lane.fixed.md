**A pipeline thread no longer polls S3 lanes that have read their input to the end** (`spate-s3`)

A thread whose S3 lanes have all reached the end of their input waits on its control channel until they are retired. In previous versions it polled those lanes in a loop and ran a full core until the sink acknowledged their last records, which for small objects is up to one `batch.linger`.
