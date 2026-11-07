**`spate_core::task::ThreadBound` bounds a future on a thread of its own** (`spate-core`)

`ThreadBound::new(name, limit)` holds a future to a limit that fires on any runtime, including one whose timer nothing drives. `with_clock` times it on an injected clock, and `with_rejection` adds a check that ends the work early. `run` returns the output, or a `BoundError` saying why it ended.
