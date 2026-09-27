**A rejected TLS handshake stops the Kafka source** (`spate-kafka`)

The Kafka source stops the pipeline when it cannot verify a broker's
certificate, or when a broker rejects the TLS handshake with an alert such as
`handshake failure`, `protocol version` or, under TLS 1.3,
`certificate required`. The exit report carries librdkafka's text, such as
`certificate verify failed` or `SSL alert number 40`. In previous versions
these failures were retryable. A source that could not connect failed only at
`startup_timeout` or `assignment_timeout`, with an error that did not name
TLS. Other TLS failures,
such as a connection reset during the handshake or an `internal error` alert,
stay retryable.
