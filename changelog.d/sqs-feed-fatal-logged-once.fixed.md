**The SQS feed logs its stop once** (`spate-sqs`)

A second Fatal error after the feed stopped logged `the SQS feed stopped` again, with its own error. The feed logs and rejects work with the first Fatal error, which is the one every later call returns.
