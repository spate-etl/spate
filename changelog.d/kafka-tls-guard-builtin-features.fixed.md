**Kafka rejects OpenSSL-only `builtin.features` flags without TLS** (`spate-kafka`)

In a build without the `kafka-tls` feature, the Kafka source and sink reject a
`builtin.features` setting in their `rdkafka` settings that names `ssl`,
`sasl_scram` or `sasl_oauthbearer`. The check runs when the configuration is
loaded, and the error asks for a rebuild with `kafka-tls`. In previous versions
the configuration loaded, and the pipeline failed at startup with a librdkafka
error saying OpenSSL was not available. Builds with `kafka-tls` are unaffected.
