**Object framing stays chunking-independent when it fails** (`spate-s3`,
`spate-core`) — a record the framer completed before a chunk failed to decode
is queued for the source rather than dropped with the error, and the same drain
runs when the end-of-object validation fails, which is where a compressed codec
hands over an object's last records. What a lane delivers is unchanged, since a
failing object is quarantined and everything undelivered is discarded. This
restores the framer's own property, that the record sequence it emits is a
function of the object's byte stream, which is what a resume by record index
replays against. `RecordFramer` states the obligation an implementer carries
for that, bounding a record against the bytes accumulated so far so the framer
fails at the same position however the stream was split.
