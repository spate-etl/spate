**Waiting on captured logs** (`spate-test`)

`LogCapture` now provides `wait_for` and `wait_for_line`. `wait_for` polls a
check until it returns a value, on the same cadence as `wait_until`.
`wait_for_line` returns the first captured line containing a given text. On
timeout, both panic with every line captured so far, so a failing test shows
what the component logged.
