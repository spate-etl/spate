**`SqsFeed::connect` returns within 30 seconds on a current-thread runtime** (`spate-sqs`)

`SqsFeed::connect` ends with `SqsError::Unavailable` once 30 seconds pass, on any runtime its handle comes from. In previous versions, given the handle of a current-thread runtime with no other thread running `Runtime::block_on`, it never returned. On such a runtime the call still cannot reach SQS, so it ends at the bound: give `connect` a multi-thread runtime's handle. Its `# Panics` section also names a runtime built without its timer or its I/O driver.
