**The Kafka feed logs a resend only for a write it queues** (`spate-kafka`)

An attempt, tombstone, retry or dead-letter write that fails transiently once the feed has stopped or failed logs no line, and its unit is delivered again. The feed previously logged that it would send the write again, and then gave it up.
