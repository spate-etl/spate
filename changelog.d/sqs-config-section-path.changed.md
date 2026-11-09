**SQS feed configuration errors start at the path of the key they refuse** (`spate-sqs`)

An error in the `sqs:` body of a `feed:` section starts with the path of the key it refuses, then a colon, such as `feed.sqs.visibility: must be whole seconds from 3s to 12h; got 2s` or `feed.sqs.queue_url: names a FIFO queue; the SQS feed reads standard queues only`. `SqsFeed::connect` reports a `queue_url` that is not a URL the same way. A refused `attempt_cap` or `settling_window`, which sit beside `sqs:`, still starts at `feed.sqs`, such as ``feed.sqs: `attempt_cap` does not apply to `sqs`: …``.

In previous versions every one of these errors started at `feed.sqs` and named the key in the message, such as ``feed.sqs: `visibility` must be whole seconds from 3s to 12h; got 2s``. Code that matches these errors by text needs updating. They are `ConfigError::Component` as before.
