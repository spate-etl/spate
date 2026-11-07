**The Kafka feed logs `sending it again` only for a write it queues** (`spate-kafka`)

A write that fails transiently on a stopped or latched feed is dropped with no line. The feed previously logged that it would send the write again, and then dropped it.
