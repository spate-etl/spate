**Breaking:** **The Kafka sink's `batch`, `inflight`, `retry` and `breaker` blocks move beside `kafka:`** (`spate-kafka`)

These four blocks tune the framework's sink pool, so they now sit in the sink section beside `kafka:`, with the other framework sink keys. A block left inside `kafka:` fails at load as an unknown field. Move it up one level:

```yaml
# before
sink:
  kafka:
    brokers: localhost:9092
    topic: orders
    batch: { linger: 250ms }

# after
sink:
  batch: { linger: 250ms }
  kafka:
    brokers: localhost:9092
    topic: orders
```

`KafkaSinkConfig` no longer has the `batch`, `inflight`, `retry` and `breaker` fields, and `KafkaSink` no longer has `pool`. In code, set a block per sink with `SinkOptions::with_batch`, `with_inflight`, `with_retry` or `with_breaker`. The pool's bounds are checked when the pipeline installs the sink, and an error starts at `sink` or `sinks.<name>` in place of `sink.kafka` or `sinks.<name>.kafka`. Pool values are unchanged for a config that set none of the four blocks.
