**Dropping a Kafka feed waits for its depth thread** (`spate-kafka`)

Dropping a `KafkaFeed` now returns once its depth thread has ended and that thread's client has closed. In previous versions the drop left the thread running, so a process could exit beside a live client. A depth read in flight at the drop sends no further request once the feed has stopped, so the drop waits for the request in flight, 5 seconds at most, and then for that client's close.
