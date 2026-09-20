**Duplicate-key guard allocation** (`spate-json`)

With `reject_duplicate_keys: true`, decoding now moves each object key into the
set that checks for repeats. Previously, the set stored a copy and the original
was dropped, so every key cost one extra allocation. Documents with many object
keys therefore decode with fewer heap allocations. Duplicate detection at every
depth, the error message and the `duplicate_key` metric label are unchanged.
