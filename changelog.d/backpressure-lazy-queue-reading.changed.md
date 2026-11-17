**Breaking:** **`WatermarkController::tick` takes the queue reading as a closure** (`spate-core`)

The second argument of `tick` is `impl FnOnce() -> bool`. The controller calls it only while paused, with no rejected send since the last tick and in-flight usage at or below the low watermark. In previous versions the argument was a `bool`, and a pipeline thread read every shard queue of its sinks on each poll to compute it, even while running unpaused. A caller that passed a `bool` now passes `|| value`. Pause and resume decisions are unchanged.
