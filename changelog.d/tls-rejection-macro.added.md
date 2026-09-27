**Classify a rustls rejection with one macro** (`spate-core`)

`spate::tls_rejection!(rustls, &err)` returns the TLS rejection in an error's
source chain, if there is one, as an `Option<&rustls::Error>`. A rejection is
a certificate the client failed to verify, an alert listed in
`spate::error::TLS_REJECTION_ALERTS`, or a peer that shares no protocol
version, cipher suite or other handshake parameter with the client. The first
argument is the path to your own `rustls` crate, such as `async_nats::rustls`,
so spate-core takes no rustls dependency. A custom connector can use it to
class a rejected handshake `Fatal`.
