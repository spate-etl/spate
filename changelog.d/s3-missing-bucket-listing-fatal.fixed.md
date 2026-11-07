**A listing over a missing bucket is non-retryable** (`spate-s3`)

A listing the store answers `404`, as AWS S3, Garage, RustFS and Ceph RGW do for a bucket that does not exist, fails with a `Fatal` error, as a read of the same bucket does. In previous versions its class was `Retryable`, so a caller of `list_once` that retried on that class retried a listing that could not succeed. A lister run fails on the error either way.
