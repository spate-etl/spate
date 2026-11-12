**An SQS feed's close takes any bound** (`spate-sqs`)

`SqsFeed::close` gives up at most a year out, so a bound over a year gives up at a year. In previous versions a bound past what the clock can represent, such as `Duration::MAX`, panicked the feed's close and settle tasks with an `Instant` overflow, and the close never confirmed. The runtime closes the feed with what is left of `checkpoint.drain_timeout`, at most a day, so this affected a caller that closed the feed directly with a bound over a year.
