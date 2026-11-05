**`SourceError` and `SinkError` gain constructors and a `class()` accessor** (`spate-core`)

`new(class, reason)` builds an error of any class, and `fatal`, `retryable` and `record_level` build one of that class. `class()` returns the class of any variant, so a connector reads it without matching on the non-exhaustive enum, and `reason()` returns the cause. `with_class` and `map_reason` return the error with one field replaced. The methods are also available on `spate::error::SourceError` and `spate::error::SinkError`. Error messages are unchanged.
