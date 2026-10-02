**A leader keeps leading when a renewal applied but its reply was lost** (`spate-coordination`)

When a leader's renewal compare-and-swap loses, the leader reads the leader
key back. If the key still names this worker, the leader renews it again from
the revision it read, in the same heartbeat. This covers a renewal that reached
the store while its reply was lost, including a reply lost to `op_timeout`. In
previous versions the leader gave up leadership while the key still named it,
and the fleet had no leader until the key expired, up to one `lease_duration`.
The `leadership renewal fenced; demoting` warning appears only when the
read-back finds another worker's key or no key.
