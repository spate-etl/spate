**Two Kafka source refusals put the keys they name in backticks** (`spate-kafka`)

`consumers` above 1 with a static member id now reads ``source.kafka.consumers: above 1 cannot be combined with `rdkafka."group.instance.id"`: …; set `consumers` to 1 or remove the static member id``. An `rdkafka` that sets both `auto.offset.reset` and `topic.auto.offset.reset` reads ``source.kafka.rdkafka: sets both `auto.offset.reset` and `topic.auto.offset.reset`: …``, where it put the two keys in double quotes. The same values are refused as before.
