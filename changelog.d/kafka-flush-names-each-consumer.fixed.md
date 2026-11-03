**A failed commit in `flush_commits` names its consumer** (`spate-kafka`)

The Kafka source logs each failed commit in `flush_commits` at `warn` with the consumer's index, and says that consumer's offsets replay. In previous versions only the pipeline's own line reported the failure, with no consumer named.
