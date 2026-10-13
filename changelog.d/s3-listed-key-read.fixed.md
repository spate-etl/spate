**Objects with reserved characters in the key are read** (`spate-s3`)

The S3 source reads every listed object under the key the listing returned. In previous versions, the source requested a different key for an object whose key contained any non-ASCII character or one of `%` `#` `[` `]` `{` `}` `<` `>` `^` `~` `|` `\` `` ` `` `"` `*` `?`. The store answered that request with "not found". The split was retried, then quarantined at its attempt cap, and a bounded job ended `Stalled` with that object unread. Keys made only of other printable ASCII characters were not affected.
