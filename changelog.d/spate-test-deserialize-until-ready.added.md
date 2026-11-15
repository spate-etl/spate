**A helper that replays a payload until the deserializer is ready** (`spate-test`)

`deserialize_until_ready` replays a payload through a deserializer until it answers something other than `NotReady`, as the pipeline does, and returns that answer. A closure runs before each replay, so a test can move a test clock or drive the runtime a fetch runs on. Its timeout only catches a hang: past it, the panic says the test hung and carries the last `NotReady` reason.
