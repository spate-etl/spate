**The SQS feed logs its stop once** (`spate-sqs`)

The SQS feed logs `the SQS feed stopped` once, for the first error it cannot recover from, and every later call returns that error. In previous versions, a second such error from another of the feed's tasks logged the line again, with the second error's text.
