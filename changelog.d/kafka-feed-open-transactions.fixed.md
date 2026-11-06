**Breaking:** **A bounded job on the Kafka feed waits for another writer's open transaction, and ends after a transaction marker** (`spate-kafka`)

The job waits while another writer's open transaction holds a unit on its work or retry topic, and ends `Stalled` when a unit is dead-lettered behind one. In previous versions the feed's depth reads stopped at the last stable offset under librdkafka's default `read_committed`, so such a job ended `Completed` and exited 0 over unread units. The depth reader lists ends at the high watermark, at `read_uncommitted`, and the group's consumer stays at `read_committed`.

The group commits past transaction markers and aborted records, so a bounded job ends after a marker on its work or retry topic, where it used to wait with every unit read.

`isolation.level` under `feed.kafka.rdkafka` fails at load for workers and the lister, so remove it. `spate_feed_dead_letter_units` counts offsets, so another writer's transaction on the dead-letter topic shows on it and fails a bounded job as stalled.

The feed puts no bound on the wait. An operator ends a job held by a hung transaction with the broker's tools or a stop, and a later worker on the group reads the units.
