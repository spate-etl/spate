**A Kafka batch that does not decompress stops the source or feed** (`spate-kafka`)

librdkafka reports such a batch on every fetch that reads it, so the error is fatal. In previous versions it was retried without end, and a bounded job on the feed over it never ended.
