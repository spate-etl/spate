**A TLS alert that rejects the handshake stops the pipeline in the NATS store** (`spate-coordination`)

The NATS coordination store fails the first connection with a fatal error when
the server rejects the TLS handshake with an alert such as `HandshakeFailure` or
`ProtocolVersion`, and when client and server share no protocol version, cipher
suite or other handshake parameter. The message names the alert or the
mismatch. In previous versions each of these was retried until the
coordinator's startup budget ran out. Other alerts, such as `InternalError`,
stay retryable. A rejection met on a reconnect after startup is still retried.
