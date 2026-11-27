**Breaking:** **`ConnectError` moves to `spate_core::error` and is a struct** (`spate-core`, `spate-kafka`, `spate-sqs`)

`spate_core::feed::ConnectError` is now `spate_core::error::ConnectError`. It has private fields and the methods every role error has: `kind()`, `class()`, `reason()` and `with_source`. Match on `e.kind()` against the new `ConnectErrorKind`, which has the three former variant names: `Config`, `Refused` and `Unavailable`. `class()` gives `Retryable` for `Unavailable` and `Fatal` for the other two.

`refused`, `unavailable`, `from_class` and `From<ConfigError>` build one as before. The first argument names the component that could not start, such as `"Kafka feed"`, and `component()` returns it. A `Config` error has no component. It renders as its `ConfigError`, and `config_error()` returns that error.

An `Unavailable` error now reads "the Kafka feed is unavailable: …" where it read "Kafka is unavailable: …". The `Refused` text is unchanged.
