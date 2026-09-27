**Find an error in a source chain** (`spate-core`)

`spate::error::find_source::<T>` returns the first `T` in an error's source
chain, the error itself included. At each `std::io::Error` it also follows the
error the `io::Error` wraps, which `io::Error::source` skips. A custom
connector can use it to find the TLS library's error behind its client's
wrappers and classify a rejected handshake as `Fatal`.
