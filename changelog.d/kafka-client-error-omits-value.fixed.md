**Kafka client creation errors no longer print rejected values** (`spate-kafka`)

A Kafka source or sink startup error names the `rdkafka:` property librdkafka
rejected and its result code, and does not include the value. In previous
versions a rejected value, including one interpolated from `${VAR}`, appeared in
the error twice: once from rdkafka and once quoted in librdkafka's reason.

librdkafka's reason text is no longer part of the message, so a startup failure
such as an `enable.idempotence` conflict with `max.in.flight` or `retries`
reports `Client creation error (librdkafka detail withheld)` and names no
property.
