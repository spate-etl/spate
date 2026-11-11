**Kafka feed: a drop with a revocation queued during its close** (`spate-kafka`)

Dropping the Kafka feed now returns when a revocation reaches a member that holds no partitions while the feed closes, and the group coordinator is then lost. The drop answers the revocation before the consumer closes, as the Kafka source's drop already does. In previous versions the consumer's close answered it after the group had terminated, and the drop never returned. The drop can now wait up to about 5 seconds for librdkafka to leave the group when a rejoin is in flight.
