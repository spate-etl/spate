**Task and thread handles in `spate_core::task`** (`spate-core`)

`spate_core::task::spawn` and `spawn_on` start a tokio task and return a `TaskHandle`. Dropping a `TaskHandle` aborts its task, and awaiting it returns the task's output or a `JoinError`. `TaskHandle::detach` lets a task run on with no owner. `spate_core::task::spawn_thread` starts a named OS thread and returns a `ThreadHandle`, which joins the thread when dropped. Each spawn takes a name, and a panic in the task or thread is logged at `ERROR` with that name. `spate_core::task::panic_text` returns the text of a panic payload.
