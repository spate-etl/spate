**An S3 lane takes any poll timeout** (`spate-s3`)

`S3Lane::poll` now waits at most a year when the timeout is past what the clock can represent, such as `Duration::MAX`. In previous versions such a timeout panicked the pipeline thread with an `Instant` overflow. The engine passes `RuntimeOptions::poll_timeout`, ten milliseconds by default, so this affected a runtime or a test that set it very large.
