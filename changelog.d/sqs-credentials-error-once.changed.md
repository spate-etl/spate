**A failed AWS credentials load no longer repeats its text** (`spate-sqs`)

When loading AWS credentials fails, the error spate-sqs hands to the AWS SDK now prints "an error occurred while loading credentials" once in its source chain. In previous versions the phrase appeared twice, because spate-sqs wrapped the load's error in a type that printed the same text.
