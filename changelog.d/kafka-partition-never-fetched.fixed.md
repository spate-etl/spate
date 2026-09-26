**A Kafka partition no longer stops fetching after an assignment** (`spate-kafka`)

The Kafka source reads the group's committed offsets before it accepts an
assignment, and starts every partition from a known offset. A partition with no
committed offset starts where `auto.offset.reset` says. In previous versions a
pause or resume that landed while librdkafka was starting a partition could
cancel the start. The partition then sent no records, with no error, until the
next rebalance. `spate_kafka_source_partition_not_fetching` read 1 for it
throughout.

`spate_source_lag_records` appears for a partition as soon as the group has
committed an offset for it, without waiting for this member's first commit.
Two cases still take the old path: a partition whose committed offset could
not be read, which the source logs as a warning, and a partition with no
committed offset under `auto.offset.reset: error`.
