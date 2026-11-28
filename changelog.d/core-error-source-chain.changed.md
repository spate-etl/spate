**Breaking:** **Logs and failure reasons carry an error's source, and `ReaderSignal` takes the error to report** (`spate-core`)

`ReaderSignal::latch` and `ReaderSignal::fail_soon` take a `FeedError`, where they took the reason as text. The signal keeps the error's reason and source and sets its kind to `Fatal` or `Retryable`. A feed backend that called `latch("…")` now passes `FeedError::new(FeedErrorKind::Fatal, "…")`, and can attach the client's error with `with_source`.

The pipeline's log lines and failure reasons now render a role error with `spate_core::error::chain`, so an error's source follows its reason. In previous versions they printed the kind and the reason only, and an attached source never reached a log. A fatal deserializer error's reason is its own reason followed by its source.

`chain` leaves out a link whose text the link before it already ends with. A client error that prints its source in its own text, as many do, now appears once.

`spate::tls_hint!` is new. It returns how to trust the issuer of a rejected certificate, for a connector that attaches the TLS error as the source and puts only the hint in its reason. `tls_reason!` is unchanged.
