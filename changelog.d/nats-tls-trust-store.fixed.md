**NATS `tls.root_ca` adds to the system trust store** (`spate-coordination`)

The NATS coordination store verifies servers against the system trust store,
or against the certificates that `SSL_CERT_FILE` or `SSL_CERT_DIR` name, plus
the PEM bundle in `tls.root_ca`. When the system store has no certificates, the
Mozilla root bundle takes its place and the store logs a warning. In previous
versions, setting `root_ca` replaced the system store, so a server signed by a
system-trusted CA failed verification. If you used `root_ca` to trust only your
private CA, the system roots are now trusted as well; set `SSL_CERT_FILE` to
restrict trust for the whole process.

A TLS connection no longer panics when the build also enables a connector that
brings rustls's `aws-lc-rs` provider, such as the `s3` or `clickhouse` feature.

A `root_ca`, `client_cert` or `client_key` file that cannot be read, a client
certificate and key that do not match, and, when TLS is required, an unreadable
system trust store now fail the first connection with a fatal error. In
previous versions an unusable file was retried until the startup budget ran
out.
