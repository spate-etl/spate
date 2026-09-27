**A TLS alert that rejects the handshake is fatal in the ClickHouse sink** (`spate-clickhouse`, `spate-core`, `spate-test`)

The ClickHouse sink fails a write as fatal when the server rejects the TLS
handshake with an alert such as `handshake_failure`, `protocol_version` or,
under TLS 1.3, `certificate_required`, and when client and server share no
protocol version, cipher suite or other handshake parameter. The error names
the alert or the mismatch. In previous
versions these were retryable, and the sink retried them without limit under
the default retry settings. Other alerts, such as `internal_error`, stay
retryable.

`spate::error::TLS_REJECTION_ALERTS` lists the alert numbers that count as a
rejection, for custom connectors to classify with. `spate_test::tls_alert_server`
starts a local server that answers every TLS client with one chosen alert.
