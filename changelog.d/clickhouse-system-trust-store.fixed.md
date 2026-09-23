**ClickHouse verifies `https://` replicas against the system trust store** (`spate-clickhouse`)

The ClickHouse sink verifies an `https://` replica against the system trust
store, or against the certificates that `SSL_CERT_FILE` or `SSL_CERT_DIR` name.
The new `tls.root_ca` setting adds a PEM bundle of private CAs to that store. In
previous versions the sink trusted only the Mozilla root bundle compiled into
the binary, so no configuration could reach a replica whose certificate a
private CA signed. The Mozilla bundle is still used when the system store has
no certificates. A host whose store has certificates, but not the CA of a
replica, fails the TLS handshake. Add that CA to the store or to `tls.root_ca`.
