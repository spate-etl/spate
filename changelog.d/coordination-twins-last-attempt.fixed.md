**A shared `instance_id` is fatal on a split's last attempt** (`spate-coordination`)

Two live workers that share an `instance_id` now stop with the
shared-instance-id fatal error when the split they meet on has one delivery
attempt left. In previous versions the second worker quarantined the split and
the first dropped it, and neither reported an error, so a bounded job could end
stalled on a healthy split. The split now stays runnable while the worker that
holds it is running. On NATS, a reconcile listing that omits the holder's lease
can still quarantine the split without an error. A worker restarted under its
predecessor's `instance_id` now quarantines a split on its last attempt only
after the predecessor's lease expires, as the store or the worker measures it.
In previous versions it quarantined the split at once. Splits with attempts
left are reclaimed at once, as before.
