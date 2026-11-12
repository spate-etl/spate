**An S3 lane takes any poll timeout** (`spate-s3`)

`S3Lane::poll` caps its deadline at a year, so a poll waiting for a chunk gives up at a year when its timeout is longer. In previous versions a timeout past what the clock can represent, such as `Duration::MAX`, panicked the pipeline thread with an `Instant` overflow. The engine passes `RuntimeOptions::poll_timeout`, ten milliseconds by default, so this affected a runtime or a test that set it over a year.
