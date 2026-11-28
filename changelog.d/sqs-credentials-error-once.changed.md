**A failed AWS credentials load reads once in the SQS warning** (`spate-sqs`)

The warning logged when loading AWS credentials fails now prints the SDK's "an error occurred while loading credentials" once. In previous versions it appeared twice, because the SDK's error prints its source and also returns it.
