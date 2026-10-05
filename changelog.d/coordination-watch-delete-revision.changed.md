**A watch delete orders above what its store handle saw** (`spate-coordination`)

The revision of a `WatchEvent::Delete` that a store handle's watch reports
follows a narrower rule. It is above every revision the key held before the
deletion that this handle, or a clone of it, returned from a create, update,
read or listing, or that a watch of it delivered. It can sit at or below a
revision another handle wrote that this handle never saw. The DynamoDB
store's polled watch can report such a delete. In previous versions, the
`CoordinationStore` contract required it to be above every revision the key
held. The NATS and in-memory stores still meet that stronger rule, and a
custom store that met it needs no change.
