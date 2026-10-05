**NATS listings read a prefix in one pass** (`spate-coordination`)

The NATS coordination store lists a prefix with one streamed read of the
latest message for each key under it. In previous versions, a listing read the
name of every key in the bucket and then fetched each matching key with its
own request, one after another. The reconcile pass lists the whole state
bucket, and the plan recount and the completion check list every split record,
so on a job with many splits each of them took seconds to minutes. A listing
still returns the same keys and revisions.
