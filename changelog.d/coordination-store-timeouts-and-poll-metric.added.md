**Coordination stores can declare their timeout and record their own reads** (`spate-coordination`, `spate-core`)

`CoordinationStore` gains two provided methods. `op_timeout` returns the
per-operation deadline a store builds its own client timeouts from, and the
coordinator refuses a store whose value differs from `coordination.op_timeout`.
`attach_metrics` gives the store the coordinator's metrics when the coordinator
starts, so a store can record reads it runs outside the trait's operations. Both
do nothing by default, so existing stores behave as before.
`spate_coordination_store_op_duration_seconds` has a new `op="poll"` series
(`StoreOp::Poll`) for the listing a polled watch runs every interval.
