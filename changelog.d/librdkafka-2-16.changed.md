**Kafka source, sink and feed run on librdkafka 2.16.0** (`spate-kafka`)

The Kafka connectors now build on `rdkafka` 0.40.0, which bundles librdkafka 2.16.0. In previous versions they built on `rdkafka` 0.39.0 and librdkafka 2.12.1. Two changes in librdkafka are visible to a pipeline.

The Kafka source no longer commits offsets for an assignment that librdkafka has marked lost, as it does when the consumer's session times out on the client or `max.poll.interval.ms` is exceeded. In previous versions librdkafka sent that commit as it revoked the partitions. With `group.instance.id` set in the source's `rdkafka` map, the broker could answer it with `FENCED_INSTANCE_ID`, which librdkafka treats as fatal, and the run failed. The Kafka feed commits its offsets itself and is unaffected.

While no broker is reachable, each Kafka client reports `AllBrokersDown` once when the outage starts, then at most once per `reconnect.backoff.max.ms`, which is 10 seconds by default. Each report is a warning under the `librdkafka` target, such as `1/1 brokers are down`. In previous versions librdkafka could report it again each time it went back to the bootstrap servers during the outage.
