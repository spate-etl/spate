**A bounded job on the Kafka feed fails when a topic its depth reads need is deleted** (`spate-kafka`)

A bounded job whose dead-letter topic was deleted while it ran never ended: with its work done it read the missing topic again every 15 seconds. The feed's depth reads now fail the job on every error the crate's other consumer reads treat as permanent, which adds a topic the cluster no longer knows, a rejected credential, an invalid topic name and a protocol version the broker does not support. The failure names the topic.
