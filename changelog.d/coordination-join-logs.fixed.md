**A peer joining says so, rather than reading as a fault**
(`spate-coordination`) — an instance's startup probe writes and deletes a key
in the durable keyspace every peer is already watching, and each of those
deletes was reported as `durable record deleted externally`, the wording
reserved for a record vanishing from under the coordinator. The probe key is
recognized for what it is; a durable record deleted by something outside the
process still warns.

Fleet membership is on the log as well as in
`spate_coordination_live_workers`: `peer joined` and `peer left` on every
worker with the resulting live count, and `assignment published` on whichever
worker holds leadership, once per rebalance rather than once per completed
split. `split claimed`, `drain started` and `drain finished` sit a level below
at `RUST_LOG=info,spate_coordination=debug`, and `spate_core::telemetry`
states the level convention the rest of the framework is written to.
