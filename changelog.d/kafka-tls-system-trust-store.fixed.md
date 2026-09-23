**Kafka TLS trusts the system CA store on Linux** (`spate-kafka`)

With the `kafka-tls` feature, the Kafka source and sink set
`ssl.ca.location: probe` when their `rdkafka` settings name neither
`ssl.ca.location` nor `ssl.ca.pem`. librdkafka then loads the system CA bundle,
such as `/etc/ssl/certs/ca-certificates.crt`. In previous versions, on Linux
and BSD, the client read only `/usr/local/ssl` and the `SSL_CERT_FILE` and
`SSL_CERT_DIR` variables, so a broker certificate signed by a system-trusted CA
failed verification. The connector still leaves the setting unset when either
variable is set. If you placed a CA under `/usr/local/ssl`, set
`ssl.ca.location` to its path. macOS behaves as before, and Windows still reads
the system certificate store.
