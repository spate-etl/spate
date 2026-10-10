**NATS listings keep keys rewritten while they run** (`spate-coordination`)

The NATS coordination store returns every key that holds a value for the whole
of a listing, including keys rewritten while the listing runs. In previous
versions, a listing could end without a key whose value was replaced during
it, such as a lease renewed by its holder. A worker that read such a listing
treated the live lease as gone: it could drop a split it still held, or a peer
could quarantine the split at its last delivery attempt. Records were not
lost; the split's uncommitted records were delivered again and a delivery
attempt was spent.
