**`DrainBarrier::wait` takes any timeout** (`spate-core`, `spate-test`)

`DrainBarrier::wait` and `spate_test::DrainBarrierProbe::wait` now wait with no deadline when the timeout is past what the clock can represent, such as `Duration::MAX`. In previous versions such a timeout panicked in the calling thread with an `Instant` overflow. The engine passes bounded timeouts, so this affected a custom `Source` or a test that called `wait` with a very large value.
