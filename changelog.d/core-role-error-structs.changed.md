**Breaking:** **`SourceError`, `SinkError`, `DeserError` and `FeedError` are structs with one set of methods** (`spate-core`)

Each of the four types has private fields and the same methods: `kind()`, `class()`, `reason()`, `new(kind, reason)` and `with_source`. `std::error::Error::source` returns the error given to `with_source`. Their text is unchanged.

`SourceError` and `SinkError` have no `Client` variant. Their kind is the new `SourceErrorKind` or `SinkErrorKind`, with `Retryable`, `RecordLevel` and `Fatal`. `fatal`, `retryable`, `record_level`, `with_class` and `map_reason` are unchanged. `new(class, reason)` is now `from_class(class, reason)`, and `new` takes the kind.

`DeserError` has no variants. Build one with `DeserError::malformed`, `schema_unavailable`, `not_ready` or `fatal`, and match on `e.kind()` against the new `DeserErrorKind`, which has the four former variant names. `class()` gives `RecordLevel` for `Malformed` and `SchemaUnavailable`, `Retryable` for `NotReady` and `Fatal` for `Fatal`. The chain acts on the kind as before.

`FeedError`'s `kind` and `reason` fields are private; read them with `kind()` and `reason()`. Two `FeedError`s are equal when their kind and reason match and they hold the same source: both none, or clones of one error.
