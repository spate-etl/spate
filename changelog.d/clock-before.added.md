**`spate_core::clock::tokio::before` races a future against a deadline on an injected clock** (`spate-core`)

`before(clock, at, fut)` returns `Some` with the future's output, or `None` once `clock` reaches `at`. The future is polled first, so an output that is ready as the deadline passes is returned. The deadline still fires when the future spends tokio's cooperative scheduling budget on every poll. The sink pool's drain behaves as before.
