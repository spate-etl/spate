**The Kafka connectors take a private CA in `tls.root_ca`** (`spate-kafka`)

The Kafka source, sink and feed sections accept core's `tls: { root_ca }` key, as the S3, SQS and ClickHouse connectors do. The file's certificates are trusted in addition to the system roots. The key needs the `kafka-tls` feature and `security.protocol` set to `ssl` or `sasl_ssl`, and while it is set the `rdkafka` passthrough refuses `ssl.ca.location`, `ssl.ca.pem`, `ssl.ca.certificate.stores` and `enable.ssl.certificate.verification`.

A `security.protocol` with leading whitespace is now read as librdkafka reads it, for the plaintext-credential warning and the `kafka-tls` check.
