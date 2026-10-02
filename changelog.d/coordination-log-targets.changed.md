**Coordination log targets** (`spate-coordination`)

Most log events from the coordination task carry a target below
`spate_coordination::task` that names their area. For example,
`assignment published` uses `spate_coordination::task::assignment`, and lease
and claim events use `spate_coordination::task::claim`. In previous versions
these events used the target `spate_coordination::task`. `RUST_LOG` directives
match targets by prefix, so `spate_coordination=debug` and
`spate_coordination::task=debug` still select every event. A log query or alert
that matches the exact target `spate_coordination::task` needs to match that
prefix instead.
