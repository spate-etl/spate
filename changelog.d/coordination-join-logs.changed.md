**A peer joining says so, rather than looking like a fault**
(`spate-coordination`) — an instance's startup probe writes and deletes a key
in the durable keyspace every peer is already watching, and each of those
deletes was reported as `durable record deleted externally`, the wording
reserved for a record vanishing from under the coordinator. The probe key is
recognized for what it is; a durable record deleted by something outside the
process still warns.

Fleet membership is on the log as well as in
`spate_coordination_live_workers`. Each worker reports peers arriving and
leaving at `INFO` with the resulting live count, a worker starting into a
running fleet names what it found, and the worker holding leadership logs one
line per rebalance carrying how many splits changed hands. Per-split detail
stays at `DEBUG`, so `RUST_LOG=info,spate_coordination=debug` is what turns
the narration on. `spate_core::telemetry` states the level split the rest of
the framework is written to.
