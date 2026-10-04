**Kafka TLS trusts the system CA bundle with vendored OpenSSL** (`spate-kafka`)

With the `kafka-tls` feature and its default vendored OpenSSL, the Kafka source
and sink set `ssl.ca.location: probe` when their `rdkafka` settings name
neither `ssl.ca.location` nor `ssl.ca.pem` and neither `SSL_CERT_FILE` nor
`SSL_CERT_DIR` is set. librdkafka then loads the first standard CA bundle it
finds, such as `/etc/ssl/certs/ca-certificates.crt`, and falls back to
`/usr/local/ssl` when it finds none. In previous versions, on Linux and BSD,
these builds read only `/usr/local/ssl` and the two variables, so a broker
certificate signed by a system-trusted CA failed verification. If you placed a
CA under `/usr/local/ssl`, set `ssl.ca.location` to its path. Probing stops at
the first bundle, so set `ssl.ca.location` to a directory when that bundle
lacks your CA. A build linked against the system OpenSSL
(`OPENSSL_NO_VENDOR=1`) keeps librdkafka's native CA default, and macOS and
Windows behave as before.
