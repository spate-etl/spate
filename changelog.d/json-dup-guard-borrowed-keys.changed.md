**Object keys in the duplicate-key guard** (`spate-json`)

With `reject_duplicate_keys: true`, decoding now reads each object key straight
from the payload, and copies only a key written with an escape. Previously,
every key was copied out of the payload before the check saw it. A document
whose keys carry no escapes therefore runs the check with no per-key heap
allocation. Duplicate detection at every depth, the error message and the
`duplicate_key` metric label are unchanged.
