**Kafka rejects the unsecured OAUTHBEARER token handler without TLS** (`spate-kafka`)

In a build without the `kafka-tls` feature, the Kafka source and sink reject
`enable.sasl.oauthbearer.unsecure.jwt` in their `rdkafka` settings when the
configuration is loaded. The error asks for a rebuild with `kafka-tls`, as it
does for other TLS and SASL settings. In previous versions the configuration
loaded, and the pipeline failed at startup with a librdkafka error saying
OpenSSL was not available. Builds with `kafka-tls` are unaffected.
