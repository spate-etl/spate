**A thread-scoped log capture read during the call** (`spate-test`)

`LogCapture::during` runs a closure under the subscriber `capture_logs` uses, writing to a capture the test holds, and returns what the closure returns. The test can read that capture from another thread while the closure still runs, for example to wait for the line that names the admin server's address before it stops a pipeline. `capture_logs` returns its lines only once its closure ends. The subscriber is scoped to the calling thread, so the test needs no binary of its own.
