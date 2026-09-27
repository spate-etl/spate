**Breaking:** **NATS `ws://` servers with TLS configured** (`spate-coordination`)

`NatsStore::new` now returns a fatal error when `NatsConfig::tls` is set and a
server in `servers` uses `ws://`. In previous versions this configuration was
accepted, and the store connected to that server over a plain websocket, so
credentials and coordination records were sent unencrypted. Change the server
URL to `wss://`, or remove `tls` if the connection is not meant to use TLS.
