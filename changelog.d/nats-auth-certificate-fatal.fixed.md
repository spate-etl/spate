**A rejected NATS credential or certificate stops the pipeline** (`spate-coordination`)

The NATS coordination store fails the first connection with a fatal error when
the server rejects the credentials, when the server certificate fails
verification, or when the server rejects the client certificate. The message
names the cause, such as `authorization violation` or `UnknownIssuer`. In
previous versions each of these was retried until the coordinator's startup
budget ran out, and the pipeline then stopped with the budget error. An
unreachable server is still retried within that budget.
