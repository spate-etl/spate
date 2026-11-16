**The Kafka feed's write resend backoff is jittered** (`spate-kafka`)

An attempt, tombstone, retry or dead-letter write that fails transiently now waits a random 80% to 100% of each step before it is sent again. The steps still start at 200 ms and double up to 5 s, and the warn line's `retry_in` field carries the wait drawn. In previous versions every write waited the exact step, so feeds whose writes failed together sent them again at the same instants.
