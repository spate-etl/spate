**Kafka feed depth thread panic** (`spate-kafka`)

A panic on the Kafka feed's depth thread now stops the feed with a fatal error, and a depth read on a feed stopped this way reports that error. In previous versions the thread ended without a trace in the feed's state, every later depth read was retried, and a bounded job that had finished its work never ended. A broker metadata reply naming a topic in bytes that are not UTF-8 causes such a panic. The feed's consumer, gate and follower threads already stopped the feed this way.
