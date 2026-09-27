**A rejected TLS handshake fails the S3 source** (`spate-s3`)

The S3 source fails the pipeline when the store or the credential endpoint
rejects the TLS handshake. This covers a certificate that fails verification, an
alert such as `HandshakeFailure` or `CertificateRequired`, and a server that
shares no protocol version, cipher suite or other handshake parameter with the
client. The error names the alert, the mismatch or the verification failure. In
previous versions a listing was retried until the planner's budget of eight
attempts ran out, and a read was retried until its split was quarantined. Other
TLS failures, such as an `InternalError` alert, are still retried.
