**Rejecting duplicate JSON keys costs fewer instructions** (`spate-json`)

With `reject_duplicate_keys: true`, the guard that tracks each object's keys hashes them with a seeded fast hasher. In previous versions it used std's SipHash. On the decode benchmarks the guard's instruction count falls by 8–14% on clean objects and by about 36–40% when a duplicate is rejected at the end of a wide object. The error a duplicate raises is unchanged.
