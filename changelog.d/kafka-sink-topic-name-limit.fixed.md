**The Kafka sink refuses a topic name over 512 bytes at build** (`spate-kafka`)

A `sink.kafka` section whose `topic` is longer than 512 bytes fails pipeline load with an error that names `topic`. Previously the sink built, and its readiness probe panicked on every call and reported the failure as retryable. A name that long never worked, so no working pipeline is affected.
