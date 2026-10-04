**A coordinator applies the watch events it has already received before it
acts on them** (`spate-coordination`)

The coordinator applies up to 257 watch events that are already waiting, then
runs one pass over the result. A leader writes each member's assignment at most
once for those events. In previous versions the coordinator ran a full pass
after each event in turn, so a leader could rewrite one member's assignment once
per event. On a store with a push watch, such as NATS, a busy fleet could leave
the leader's view behind the store, and a worker that finished a split waited
that much longer for its next one.
