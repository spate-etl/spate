**A Kafka source under `auto.offset.reset: error` fails on an out-of-range offset** (`spate-kafka`)

A partition whose fetch offset is out of range under `auto.offset.reset: error`, such as a committed offset that retention deleted below or one past the log end, fails the source with a fatal error that names the partition. In previous versions the error was retryable. librdkafka stops fetching the partition once it reports the reset, so the pipeline logged one warning and stayed healthy while the partition delivered nothing.
