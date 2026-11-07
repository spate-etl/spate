**A Kafka feed connect closes its startup check clients before it returns** (`spate-kafka`)

`KafkaFeed::connect` and `KafkaFeedPublisher::connect` close the two clients their startup checks use before they return. This holds when a rejected credential or TLS handshake refuses the connect, and when the checks run out of time. In previous versions the metadata client kept running for up to 30 seconds after such a refusal and kept reconnecting to the broker. A caller that retried a refused connect more often than that held several of these clients at once.
