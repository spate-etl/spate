**Held writes in the capture sink** (`spate-test`)

`WriteOutcome::held()` scripts a capture-sink write that stays open until the test calls `SinkScript::release_held()`. No amount of real or tokio time releases it, so a test can check what a pipeline does while a write is outstanding without timing the write. `SinkScript::held_writes()` reports how many writes are waiting. Release every held write before you shut the run down, because a shutdown waits for them.
