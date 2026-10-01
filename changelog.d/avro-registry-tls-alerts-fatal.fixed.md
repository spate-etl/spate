**A TLS alert that rejects the handshake stops the pipeline in the Avro deserializer** (`spate-avro`)

The Avro deserializer stops the pipeline when the schema registry rejects the
TLS handshake with an alert such as `handshake_failure` or `protocol_version`,
and when client and registry share no protocol version, cipher suite or other
handshake parameter. The error names the registry URL and the alert or the
mismatch. In previous versions these were retried without limit while the batch
was held. A registry that requires a client certificate stops the pipeline as
well, including a TLS 1.3 registry that refuses the client after the
handshake. Other alerts, such as `internal_error`, stay transient.
