**A schema registry URL that is not `http://` or `https://` fails at load** (`spate-avro`)

The Avro deserializer fails at startup when `registry.url` does not parse as a
URL or its scheme is not `http` or `https`, such as `localhost:8081` without a
scheme. The error does not repeat the URL, which may carry credentials. In
previous versions the deserializer started, and every schema fetch failed and
was retried without limit while the batch was held.
