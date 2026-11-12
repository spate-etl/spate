**An S3 lane takes any poll timeout** (`spate-s3`)

`S3Lane::poll` waits at most a year, so a timeout over a year ends at a year. In previous versions a timeout past what the clock can represent, such as `Duration::MAX`, panicked the pipeline thread with an `Instant` overflow. The engine passes `RuntimeOptions::poll_timeout`, ten milliseconds by default, so this affected a runtime or a test that set it over a year.
