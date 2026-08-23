**Object framing stays chunking-independent when it fails** (`spate-s3`) — a
record the framer completed before a chunk failed to decode reaches the source
rather than being dropped with the error, so the records delivered before a
corrupt or over-cap object is quarantined no longer depend on where the fetcher
cut the object. The same drain runs when the end-of-object validation fails,
which is where a compressed codec hands over an object's last records.
At-least-once delivery was never affected, since the divergence added a
duplicate record rather than losing one.
