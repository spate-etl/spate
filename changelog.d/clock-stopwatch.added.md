**A stopwatch and a wall-clock reader in `spate_core::clock`** (`spate-core`)

`spate_core::clock::Stopwatch` measures how far a `Clock` moves between two reads, and exposes no `Instant`, so a measurement cannot feed a deadline. `spate_core::clock::wall_now` returns the system's wall-clock time, for timestamps and identifiers only. Code that reads time through an injected `Clock` uses them for durations and timestamps, so a test that moves its clock sees the same time.
