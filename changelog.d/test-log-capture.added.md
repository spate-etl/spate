**Log capture for tests** (`spate-test`)

`spate-test` now provides `capture_logs` and `show_logs` for asserting on what
a component logged, plus the `LogCapture` writer behind them for a test that
installs a subscriber of its own. `capture_logs` runs a closure under a
`tracing` subscriber and returns the lines it formatted. `show_logs` writes
those lines to the test harness, which prints them when the test fails. Both
take the maximum level as a parameter and install the subscriber on the calling
thread only, so tests sharing one process do not capture each other's output.
`spate-test` now carries `tracing` and `tracing-subscriber` as ordinary
dependencies, so both are built by anything that depends on it; previously they
were dev-dependencies.
