**Rejecting duplicate JSON keys costs fewer instructions** (`spate-json`)

With `reject_duplicate_keys: true`, the guard that tracks each object's keys hashes them with a seeded quality hasher. In previous versions it used std's SipHash. On the decode benchmarks a clean document decoded with the guard on costs 8–14% fewer instructions, of which the guard's own share falls by about 19–40%. Rejecting a duplicate at the end of a wide object costs about 36–40% fewer. The error a duplicate raises is unchanged.
