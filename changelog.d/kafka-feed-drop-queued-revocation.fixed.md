**Kafka feed: a drop with a revocation queued during its close** (`spate-kafka`)

Dropping the Kafka feed could hang when a revocation reached a member that held no partitions while the feed closed, and the group coordinator was then lost. The drop now answers a revocation already queued before the consumer closes, as the Kafka source's drop does, and the member then leaves the group. In previous versions the consumer's close answered it after the group had terminated, and the drop did not return. A revocation that queues while the drop runs is still left to the close.
