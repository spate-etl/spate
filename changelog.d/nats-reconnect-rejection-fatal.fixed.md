**A credential or certificate rejected on a reconnect stops the pipeline in the NATS store** (`spate-coordination`)

The NATS coordination store fails with a fatal error, naming the rejection, once
every server has failed since the client last connected, each with a rejection
as its latest failure. That covers a
rejected credential, a certificate either side rejects, a TLS alert such as
`HandshakeFailure`, and no handshake parameter in common. In previous versions
the client retried these for as long as the process ran, and every store
operation failed retryably meanwhile. A server that refuses or times out the
connection keeps the store retrying.
