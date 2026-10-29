**A stopwatch on the tokio clock** (`spate-core`)

`spate_core::clock::tokio::Stopwatch` measures how far a tokio-timeline `Clock` moves between two reads, and exposes no `Instant`, so a measurement cannot feed a deadline. It mirrors `spate_core::clock::Stopwatch` for code on the tokio runtime.
