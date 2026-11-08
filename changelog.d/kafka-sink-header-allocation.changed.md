**The Kafka sink no longer allocates a header list per message** (`spate-kafka`)

The Kafka sink now reads each message's headers directly from the batch it is writing. In previous versions it allocated a list of a message's headers for every message that had any, read the list once and dropped it. Messages without headers are unaffected. What the sink produces is unchanged, and librdkafka still allocates its own copy of each message's headers.
