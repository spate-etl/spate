**An SQS feed's close takes any bound** (`spate-sqs`)

`SqsFeed::close` now gives up at most a year out when its bound is past what the clock can represent, such as `Duration::MAX`. In previous versions such a bound panicked the feed's close and settle tasks with an `Instant` overflow, and the close never confirmed. The runtime closes the feed with what is left of the drain deadline, so this affected a caller that closed the feed directly with a very large bound.
