**`NatsStore::watch` honours any key prefix** (`spate-coordination`)

A watch on the NATS store delivers every key that starts with its prefix,
including a prefix that ends partway through a dot-separated segment, such as
`plan`. It also completes its snapshot at once when no live key has that prefix.
In previous versions a prefix that did not end in `.` matched only the key equal
to it, and a watch on a prefix with no live keys failed with "watch snapshot
stalled" whenever the bucket held other keys. The coordinator watches every key,
so a pipeline was not affected; code that calls the store's `watch` directly
was.
